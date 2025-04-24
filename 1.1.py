import psycopg2
import time
import re
from contextlib import contextmanager

# --- Конфигурация подключения к БД ---
DB_CONFIG = {
    "dbname": "real_estate_db",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5433"
}

# --- Запросы для тестирования ---
QUERIES = {
    "exact_match_postal": {
        "description": "Точный поиск по почтовому индексу",
        "query": "SELECT COUNT(*) FROM mutations_foncieres WHERE code_postal = '75015';",
        "index_type": "B-Tree",
        "index_column": "code_postal",
        "index_sql": "CREATE INDEX idx_btree_code_postal ON mutations_foncieres USING btree (code_postal);",
        "cleanup_sql": "DROP INDEX IF EXISTS idx_btree_code_postal;"
    },
    "range_scan_value": {
        "description": "Поиск по диапазону стоимости",
        "query": "SELECT COUNT(*) FROM mutations_foncieres WHERE valeur_fonciere BETWEEN 100000 AND 150000;",
        "index_type": "B-Tree",
        "index_column": "valeur_fonciere",
        "index_sql": "CREATE INDEX idx_btree_valeur_fonciere ON mutations_foncieres USING btree (valeur_fonciere);",
        "cleanup_sql": "DROP INDEX IF EXISTS idx_btree_valeur_fonciere;"
    },
    "range_scan_date": {
        "description": "Поиск по диапазону дат (для B-Tree)",
        "query": "SELECT COUNT(*) FROM mutations_foncieres WHERE date_mutation BETWEEN '2019-01-01' AND '2019-01-31';",
        "index_type": "B-Tree",
        "index_column": "date_mutation",
        "index_sql": "CREATE INDEX idx_btree_date_mutation ON mutations_foncieres USING btree (date_mutation);",
        "cleanup_sql": "DROP INDEX IF EXISTS idx_btree_date_mutation;"
    },
     "text_search_like": {
        "description": "Поиск по подстроке в названии улицы (LIKE '%...%')",
        "query": "SELECT COUNT(*) FROM mutations_foncieres WHERE adresse_nom_voie LIKE '%AVENUE%';",
        "index_type": "GIN (pg_trgm)",
        "index_column": "adresse_nom_voie",
        "extension_sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
        "index_sql": "CREATE INDEX idx_gin_trgm_adresse_nom_voie ON mutations_foncieres USING gin (adresse_nom_voie gin_trgm_ops);",
        "cleanup_sql": "DROP INDEX IF EXISTS idx_gin_trgm_adresse_nom_voie;"
    },
    "range_scan_date_brin": {
        "description": "Поиск по широкому диапазону дат (для BRIN)",
        "query": "SELECT COUNT(*) FROM mutations_foncieres WHERE date_mutation BETWEEN '2017-01-01' AND '2017-12-31';",
        "index_type": "BRIN",
        "index_column": "date_mutation",
        "index_sql": "CREATE INDEX idx_brin_date_mutation ON mutations_foncieres USING brin (date_mutation);",
        "cleanup_sql": "DROP INDEX IF EXISTS idx_brin_date_mutation;"
    }
}

@contextmanager
def db_connection():
    """Контекстный менеджер для управления соединением с БД."""
    conn = None
    try:
        print(f"Подключение к базе данных {DB_CONFIG['dbname']}...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Подключение успешно.")
        yield conn
        conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка подключения или выполнения операции: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Соединение закрыто.")

def execute_query(conn, sql, analyze=False):
    """Выполняет SQL-запрос и возвращает результат или время выполнения."""
    cur = conn.cursor()
    query_to_run = f"EXPLAIN ANALYZE {sql}" if analyze else sql
    start_time = time.time()
    try:
        cur.execute(query_to_run)
        result = cur.fetchall() if analyze else None
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000

        if analyze:
            explain_output = "\n".join([row[0] for row in result])
            match = re.search(r"Execution Time: ([\d\.]+) ms", explain_output)
            if match:
                return float(match.group(1)), explain_output
            else:
                print("Предупреждение: Не удалось извлечь 'Execution Time' из EXPLAIN ANALYZE. Используется общее время.")
                return execution_time_ms, explain_output
        else:
            return execution_time_ms, None

    except psycopg2.Error as e:
        print(f"Ошибка выполнения запроса:\n{sql}\nОшибка: {e}")
        conn.rollback()
        return None, None
    finally:
        cur.close()


def run_index_test(conn, test_name, config):
    """Запускает тест для конкретного запроса и индекса."""
    print(f"\n--- Тест: {test_name} ({config['description']}) ---")
    print(f"Целевая колонка: {config['index_column']}, Тип индекса: {config['index_type']}")

    query = config['query']
    results = {}

    # 0. Очистка перед тестом (на всякий случай)
    if "cleanup_sql" in config:
        print("Предварительная очистка индекса...")
        execute_query(conn, config['cleanup_sql'])
        conn.commit()

    # 1. Установка расширений (если необходимо)
    if "extension_sql" in config:
        print("Установка необходимого расширения...")
        _, _ = execute_query(conn, config['extension_sql'])
        conn.commit()

    # 2. Измерение БЕЗ индекса
    print("Измерение производительности БЕЗ индекса...")
    baseline_time, baseline_plan = execute_query(conn, query, analyze=True)
    if baseline_time is not None:
        print(f"Время БЕЗ индекса: {baseline_time:.2f} ms")
        results["baseline_ms"] = baseline_time
        results["baseline_plan"] = baseline_plan
    else:
        print("Не удалось измерить baseline.")
        return test_name, None

    # 3. Создание индекса
    if "index_sql" in config:
        print(f"Создание индекса ({config['index_type']} на {config['index_column']})...")
        index_creation_time, _ = execute_query(conn, config['index_sql'])
        conn.commit()
        if index_creation_time is not None:
            print(f"Индекс создан за {index_creation_time:.2f} ms")
            results["index_creation_ms"] = index_creation_time

            print("Выполнение ANALYZE для обновления статистики таблицы...")
            analyze_time, _ = execute_query(conn, f"ANALYZE mutations_foncieres;")
            conn.commit()
            print(f"ANALYZE выполнен за {analyze_time:.2f} ms")

        else:
            print("Ошибка создания индекса.")
            if "cleanup_sql" in config:
                execute_query(conn, config['cleanup_sql'])
                conn.commit()
            return test_name, results

    # 4. Измерение С индексом
    print("Измерение производительности С индексом...")
    indexed_time, indexed_plan = execute_query(conn, query, analyze=True)
    if indexed_time is not None:
        print(f"Время С индексом: {indexed_time:.2f} ms")
        results["indexed_ms"] = indexed_time
        results["indexed_plan"] = indexed_plan
    else:
        print("Не удалось измерить время с индексом.")

    # 5. Очистка индекса
    if "cleanup_sql" in config:
        print("Очистка индекса после теста...")
        cleanup_time, _ = execute_query(conn, config['cleanup_sql'])
        conn.commit()
        print(f"Индекс удален за {cleanup_time:.2f} ms")

    # Расчет ускорения
    if "baseline_ms" in results and "indexed_ms" in results:
         speedup = results['baseline_ms'] / results['indexed_ms']
         results["speedup"] = speedup
         print(f"Ускорение: {speedup:.2f}x")
    else:
        results["speedup"] = "N/A"

    return test_name, results

# --- Основной блок выполнения ---
if __name__ == "__main__":
    all_results = {}
    with db_connection() as connection:
        if connection:
            cur = connection.cursor()
            try:
                cur.execute("SELECT COUNT(*) FROM mutations_foncieres;")
                count = cur.fetchone()[0]
                print(f"Обнаружено строк в таблице mutations_foncieres: {count}")
                if count < 1000000:
                    print("Предупреждение: Количество строк кажется малым для демонстрации BRIN/GIN. Убедитесь, что данные загружены.")
            except psycopg2.Error as e:
                print(f"Ошибка проверки таблицы: {e}")
                exit()
            finally:
                cur.close()

            # Запуск тестов
            for name, test_config in QUERIES.items():
                test_name, result_data = run_index_test(connection, name, test_config)
                all_results[test_name] = result_data

    # --- Вывод итоговых результатов ---
    print("\n\n--- Итоговые результаты ---")
    print("-" * 80)
    print(f"{'Тест':<25} | {'Тип индекса':<15} | {'Колонка':<20} | {'Baseline (ms)':<15} | {'Indexed (ms)':<15} | {'Ускорение':<10}")
    print("-" * 80)

    for name, results in all_results.items():
        if results:
             print(f"{name:<25} | {QUERIES[name]['index_type']:<15} | {QUERIES[name]['index_column']:<20} | "
                   f"{results.get('baseline_ms', 'N/A'):<15.2f} | {results.get('indexed_ms', 'N/A'):<15.2f} | "
                   f"{results.get('speedup', 'N/A'):<10.2f}x")
        else:
            print(f"{name:<25} | {QUERIES[name]['index_type']:<15} | {QUERIES[name]['index_column']:<20} | {'FAIL':<15} | {'FAIL':<15} | {'N/A':<10}")

    print("-" * 80)

    # Вывод планов выполнения для детального анализа
    for name, results in all_results.items():
        if results:
            print(f"\n--- Планы выполнения для теста: {name} ---")
            print("--- Baseline Plan ---")
            print(results.get('baseline_plan', 'Нет данных'))
            print("\n--- Indexed Plan ---")
            print(results.get('indexed_plan', 'Нет данных'))
            print("-" * 50)