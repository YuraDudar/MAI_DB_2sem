import psycopg2
import time
import re
import secrets
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, STATUS_IN_TRANSACTION
from contextlib import contextmanager

# --- Конфигурация подключения к БД ---
DB_CONFIG = {
    "dbname": "real_estate_db",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5433"
}

# Глобальная переменная для отслеживания доступности pg_bigm
pg_bigm_available = False

# --- Вспомогательные функции ---
@contextmanager
def db_connection(autocommit=False, isolation_level=None):
    """Контекстный менеджер для управления соединением с БД."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        elif isolation_level:
            conn.set_session(isolation_level=isolation_level)
        yield conn
        if not autocommit and conn.status == STATUS_IN_TRANSACTION:
            conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка подключения или выполнения операции: {e}")
        if conn and not autocommit and conn.status == STATUS_IN_TRANSACTION:
            try:
                conn.rollback()
                print("Выполнен откат транзакции из-за ошибки.")
            except psycopg2.Error as rb_err:
                 print(f"Ошибка при попытке отката: {rb_err}")
        if "connection" not in str(e).lower():
            raise
    finally:
        if conn:
            conn.close()

def execute_query(conn, query, params=None, analyze=False, fetch_one=False, fetch_all=False):
    """Универсальная функция для выполнения SQL с улучшенной защитой в EXPLAIN ANALYZE."""
    cur = conn.cursor()
    base_sql_string = ""
    query_to_run = ""
    start_time = time.time()
    results = None
    plan = None
    exec_time_ms = None
    error_occurred = None

    try:
        if isinstance(query, sql.Composed):
            base_sql_string = query.as_string(conn)
        elif isinstance(query, str):
            base_sql_string = query
        else:
             raise ValueError(f"Неподдерживаемый тип запроса: {type(query)}")

        query_to_run = f"EXPLAIN ANALYZE {base_sql_string}" if analyze else base_sql_string

        cur.execute(query_to_run, params)

        if analyze:
            explain_output_rows = None
            try:
                explain_output_rows = cur.fetchall()
                plan_lines = []
                for i, row in enumerate(explain_output_rows):
                    if isinstance(row, tuple) and len(row) > 0 and row[0] is not None:
                        try:
                            plan_lines.append(str(row[0]))
                        except Exception as e_str:
                             print(f"Предупреждение: Ошибка приведения к строке элемента {i} плана: {row[0]} ({e_str})")
                             plan_lines.append(f"!!ERROR CONVERTING ROW {i}!!")
                    else:
                        print(f"Предупреждение: Пропущена неожиданная строка {i} в выводе EXPLAIN ANALYZE: {row}")

                explain_output = "\n".join(plan_lines)
                plan = explain_output
                match = re.search(r"Execution Time: ([\d\.]+) ms", explain_output)
                if match:
                    exec_time_ms = float(match.group(1))
                else:
                    exec_time_ms = (time.time() - start_time) * 1000
                    if plan and "ERROR" not in plan:
                         print("Предупреждение: Execution Time не найдено в плане, используется общее время.")
                results = None

            except psycopg2.Error as e_fetch_analyze:
                 print(f"Ошибка psycopg2 при fetchall для EXPLAIN ANALYZE: {e_fetch_analyze}")
                 plan = f"Ошибка получения плана: {e_fetch_analyze}"
                 exec_time_ms = (time.time() - start_time) * 1000
                 results = None
                 error_occurred = e_fetch_analyze
            except IndexError as e_index:
                 print(f"!!! Поймана IndexError при обработке плана EXPLAIN ANALYZE: {e_index}")
                 problem_row_info = f"Проблемная строка (возможно): {row}" if 'row' in locals() else "Строка неизвестна"
                 print(problem_row_info)
                 plan = f"Ошибка обработки плана: {e_index}\n{problem_row_info}"
                 exec_time_ms = (time.time() - start_time) * 1000
                 results = None
                 error_occurred = e_index


        elif fetch_one:
            try:
                results = cur.fetchone()
            except psycopg2.Error as e_fetch:
                 print(f"Ошибка psycopg2 при fetchone: {e_fetch}")
                 results = None
                 error_occurred = e_fetch
            exec_time_ms = (time.time() - start_time) * 1000
        elif fetch_all:
            try:
                results = cur.fetchall()
            except psycopg2.Error as e_fetch:
                 print(f"Ошибка psycopg2 при fetchall: {e_fetch}")
                 results = None
                 error_occurred = e_fetch
            exec_time_ms = (time.time() - start_time) * 1000
        else:
            exec_time_ms = (time.time() - start_time) * 1000

    except psycopg2.Error as e:
        print(f"Ошибка выполнения запроса:")
        print(f"SQL: {query_to_run[:500]}...")
        print(f"Параметры: {params}")
        print(f"Ошибка psycopg2: {e.pgcode} {e.pgerror}")
        error_occurred = e
    except ValueError as ve:
         print(f"Ошибка типа запроса: {ve}")
         error_occurred = ve
    except Exception as e_generic:
         print(f"Неожиданная ошибка в execute_query: {type(e_generic).__name__}: {e_generic}")
         error_occurred = e_generic
    finally:
        if cur:
            cur.close()

    # Формируем результат
    result_dict = {
        "time_ms": exec_time_ms,
        "results": results,
        "plan": plan,
        "error": error_occurred
    }
    if result_dict["time_ms"] is None:
        result_dict["time_ms"] = (time.time() - start_time) * 1000

    return result_dict

def get_index_size(conn, index_name):
    """Получает размер индекса."""
    try:
        query = "SELECT pg_size_pretty(pg_relation_size(oid)) FROM pg_class WHERE relname = %s;"
        result = execute_query(conn, query, params=(index_name,), fetch_one=True)
        if result and result.get('results') and result['results'][0] is not None:
             return result['results'][0]
        else:
             return "Не найден"
    except Exception as e:
        print(f"Ошибка получения размера индекса {index_name}: {e}")
        return "Ошибка"

def format_metric(value, precision=2):
    """Форматирует метрику для вывода, обрабатывая числа и 'N/A'."""
    if isinstance(value, (int, float)):
        return f"{value:.{precision}f}"
    else:
        return str(value)

# --- Основная логика ---
def setup_extensions(conn):
    """Устанавливает необходимые расширения и проверяет доступность pg_bigm."""
    global pg_bigm_available
    print("\n--- Установка расширений ---")
    extensions = {"pg_trgm": False, "pg_bigm": False, "pgcrypto": False}

    for ext in extensions:
        try:
            print(f"Попытка установки {ext}...")
            execute_query(conn, sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(sql.Identifier(ext)))
            check_sql = "SELECT 1 FROM pg_extension WHERE extname = %s;"
            result = execute_query(conn, check_sql, params=(ext,), fetch_one=True)
            if result and result.get('results'):
                 print(f"Расширение {ext} успешно установлено или уже существует.")
                 extensions[ext] = True
                 if ext == 'pg_bigm':
                     pg_bigm_available = True
            else:
                 print(f"Не удалось подтвердить установку расширения {ext}.")
                 if ext == 'pg_bigm':
                      pg_bigm_available = False

        except psycopg2.Error as e:
            if "is not available" in str(e.pgerror) or "does not exist" in str(e.pgerror):
                 print(f"Предупреждение: Расширение {ext} не доступно для установки в этой сборке PostgreSQL.")
                 print(f"  >> {e.pgerror}")
                 if ext == 'pg_bigm':
                      pg_bigm_available = False
            else:
                 print(f"Не удалось установить расширение {ext}: {e}")
        except Exception as e_other:
             print(f"Неожиданная ошибка при установке {ext}: {e_other}")
             if ext == 'pg_bigm':
                 pg_bigm_available = False

    if not pg_bigm_available:
        print("\n*** Расширение pg_bigm недоступно. Тесты для pg_bigm будут пропущены. ***\n")

def cleanup_test_objects(conn):
    """Удаляет тестовые индексы и колонки."""
    print("\n--- Очистка тестовых объектов ---")
    objects_to_drop = [
        ("INDEX", "idx_gin_trgm_adresse"),
        ("INDEX", "idx_gin_bigm_adresse"),
        ("COLUMN", "owner_secret_info")
    ]
    with conn.cursor() as cur:
        for obj_type, obj_name in objects_to_drop:
            try:
                if obj_type == "INDEX":
                    print(f"Попытка удаления индекса {obj_name}...")
                    cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(obj_name)))
                    print(f"Индекс {obj_name} удален или не существовал.")
                elif obj_type == "COLUMN":
                     print(f"Попытка удаления колонки {obj_name}...")
                     cur.execute("""
                         SELECT column_name
                         FROM information_schema.columns
                         WHERE table_schema = 'public' AND table_name = 'mutations_foncieres' AND column_name = %s;
                     """, (obj_name,))
                     if cur.fetchone():
                         cur.execute(sql.SQL("ALTER TABLE mutations_foncieres DROP COLUMN {}").format(sql.Identifier(obj_name)))
                         print(f"Колонка {obj_name} удалена.")
                     else:
                          print(f"Колонка {obj_name} не найдена, удаление не требуется.")

            except psycopg2.Error as e:
                 if "does not exist" not in str(e.pgerror):
                      print(f"Ошибка при удалении {obj_type} {obj_name}: {e}")
                 else:
                      print(f"{obj_type} {obj_name} уже не существует.")
            except Exception as e_other:
                 print(f"Неожиданная ошибка при удалении {obj_type} {obj_name}: {e_other}")
    print("Очистка завершена.")

def demo_trgm_bigm():
    """Демонстрация и сравнение pg_trgm и pg_bigm (если доступно)."""
    print("\n\n--- Демонстрация pg_trgm и pg_bigm ---")
    target_column = "adresse_nom_voie"
    results = {}

    with db_connection(autocommit=True) as conn:

        # --- pg_trgm ---
        print("\n--- Тестирование pg_trgm ---")
        index_name_trgm = "idx_gin_trgm_adresse"
        trgm_created_successfully = False
        try:
            print(f"Создание GIN индекса ({index_name_trgm}) на {target_column} с gin_trgm_ops...")
            timing_trgm_create = execute_query(conn,
                sql.SQL("CREATE INDEX {} ON mutations_foncieres USING gin ({} gin_trgm_ops);")
                    .format(sql.Identifier(index_name_trgm), sql.Identifier(target_column))
            )
            # Проверяем, что результат содержит время
            if timing_trgm_create and timing_trgm_create.get('time_ms') is not None:
                 results['trgm_create_ms'] = timing_trgm_create['time_ms']
                 print(f"Индекс {index_name_trgm} создан за {results['trgm_create_ms']:.2f} ms")
                 trgm_created_successfully = True # Флаг успешного создания
            else:
                 print(f"Не удалось создать индекс {index_name_trgm} или измерить время.")
                 results['trgm_create_ms'] = 'Error'
                 # Не устанавливаем флаг trgm_created_successfully

            # Продолжаем только если индекс создан
            if trgm_created_successfully:
                # Обновление статистики после создания индекса
                print("Выполнение ANALYZE...")
                execute_query(conn, "ANALYZE mutations_foncieres;")

                results['trgm_size'] = get_index_size(conn, index_name_trgm)
                print(f"Размер индекса {index_name_trgm}: {results['trgm_size']}")

                # Запрос LIKE
                print("Тест LIKE '%AVENUE%' с pg_trgm...")
                timing_trgm_like = execute_query(conn,
                     sql.SQL("SELECT COUNT(*) FROM mutations_foncieres WHERE {} LIKE %s")
                        .format(sql.Identifier(target_column)),
                     params=('%AVENUE%',), analyze=True
                )
                # Сохраняем время, если оно получено
                if timing_trgm_like and timing_trgm_like.get('time_ms') is not None:
                    results['trgm_like_ms'] = timing_trgm_like['time_ms']
                    print(f"Время выполнения LIKE с pg_trgm: {format_metric(results['trgm_like_ms'])} ms")
                else:
                    results['trgm_like_ms'] = 'Error'
                    print(f"Время выполнения LIKE с pg_trgm: Error")

                # Запрос на схожесть (%)
                search_term_similar = 'BOULVERD PEREIRE'
                print(f"Тест схожести (%) для '{search_term_similar}' с pg_trgm...")
                timing_trgm_similar = execute_query(conn,
                    sql.SQL("SELECT {}, similarity({}, %s) FROM mutations_foncieres WHERE {} % %s ORDER BY similarity({}, %s) DESC LIMIT 5")
                       .format(sql.Identifier(target_column),
                               sql.Identifier(target_column),
                               sql.Identifier(target_column),
                               sql.Identifier(target_column)),
                    params=(search_term_similar, search_term_similar, search_term_similar), analyze=True
                )
                if timing_trgm_similar and timing_trgm_similar.get('time_ms') is not None:
                    results['trgm_similar_ms'] = timing_trgm_similar['time_ms']
                    print(f"Время выполнения теста схожести: {format_metric(results['trgm_similar_ms'])} ms")
                else:
                     results['trgm_similar_ms'] = 'Error'
                     print(f"Время выполнения теста схожести: Error")

                print("Найденные похожие улицы:")
                similar_rows_data = None
                try: # Обернем получение данных в try/except
                    similar_rows_exec = execute_query(conn,
                        sql.SQL("SELECT {}, similarity({}, %s) FROM mutations_foncieres WHERE {} % %s ORDER BY similarity({}, %s) DESC LIMIT 5")
                           .format(sql.Identifier(target_column),
                                   sql.Identifier(target_column),
                                   sql.Identifier(target_column),
                                   sql.Identifier(target_column)),
                        params=(search_term_similar, search_term_similar, search_term_similar), fetch_all=True
                    )
                    # Более надежная проверка
                    if similar_rows_exec and isinstance(similar_rows_exec.get('results'), (list, tuple)):
                        similar_rows_data = similar_rows_exec['results']
                    elif similar_rows_exec is None:
                         print("Ошибка при выполнении запроса на схожесть.")
                    else:
                         print("Запрос на схожесть не вернул ожидаемых данных.")

                except Exception as e_fetch_sim:
                    print(f"Исключение при получении данных схожести: {e_fetch_sim}")
                    results['trgm_similar_ms'] = results.get('trgm_similar_ms', 'Error') # Обновляем, если не 'Error'


                if similar_rows_data: # Проверяем полученные данные
                    for row in similar_rows_data:
                        if isinstance(row, tuple) and len(row) >= 2:
                            try:
                                print(f"- {row[0]} (Схожесть: {row[1]:.4f})")
                            except IndexError:
                                 print(f"ERROR: IndexError в строке схожести: {row}")
                            except Exception as e_print:
                                 print(f"ERROR: Другая ошибка обработки строки схожести {row}: {e_print}")
                        else:
                            print(f"- Получена некорректная строка схожести: {row}")
                elif similar_rows_data is not None:
                    print("Похожих улиц не найдено.")
                # Если similar_rows_data is None, сообщение об ошибке уже было выведено выше.

                 # Запрос на дистанцию (<->)
                search_term_distance = 'RUE DE LA PAIX'
                print(f"Тест дистанции (<->) к '{search_term_distance}' с pg_trgm...")
                timing_trgm_distance = execute_query(conn,
                    sql.SQL("SELECT {}, {} <-> %s AS dist FROM mutations_foncieres ORDER BY {} <-> %s LIMIT 5")
                       .format(sql.Identifier(target_column),
                               sql.Identifier(target_column),
                               sql.Identifier(target_column)),
                    params=(search_term_distance, search_term_distance), analyze=True
                )
                if timing_trgm_distance and timing_trgm_distance.get('time_ms') is not None:
                    results['trgm_distance_ms'] = timing_trgm_distance['time_ms']
                    print(f"Время выполнения теста дистанции: {format_metric(results['trgm_distance_ms'])} ms")
                else:
                    results['trgm_distance_ms'] = 'Error'
                    print(f"Время выполнения теста дистанции: Error")

                print("Найденные ближайшие улицы:")
                distance_rows_data = None
                try: # Обернем получение данных в try/except
                    distance_rows_exec = execute_query(conn,
                         sql.SQL("SELECT {}, {} <-> %s AS dist FROM mutations_foncieres ORDER BY {} <-> %s LIMIT 5")
                           .format(sql.Identifier(target_column),
                                   sql.Identifier(target_column),
                                   sql.Identifier(target_column)),
                        params=(search_term_distance, search_term_distance), fetch_all=True
                    )
                    # Более надежная проверка
                    if distance_rows_exec and isinstance(distance_rows_exec.get('results'), (list, tuple)):
                        distance_rows_data = distance_rows_exec['results']
                    elif distance_rows_exec is None:
                         print("Ошибка при выполнении запроса на дистанцию.")
                    else:
                         print("Запрос на дистанцию не вернул ожидаемых данных.")

                except Exception as e_fetch_dist:
                    print(f"Исключение при получении данных дистанции: {e_fetch_dist}")
                    results['trgm_distance_ms'] = results.get('trgm_distance_ms', 'Error')


                if distance_rows_data: # Проверяем полученные данные
                    for row in distance_rows_data:
                        if isinstance(row, tuple) and len(row) >= 2:
                            try:
                                print(f"- {row[0]} (Дистанция: {row[1]:.4f})")
                            except IndexError:
                                 print(f"ERROR: IndexError в строке дистанции: {row}")
                            except Exception as e_print:
                                 print(f"ERROR: Другая ошибка обработки строки дистанции {row}: {e_print}")
                        else:
                            print(f"- Получена некорректная строка дистанции: {row}")
                elif distance_rows_data is not None:
                    print("Ближайших улиц не найдено.")
                # Если distance_rows_data is None, сообщение об ошибке уже было выведено выше.

        except Exception as e:
            print(f"Ошибка при тестировании pg_trgm: {e}")
            # Устанавливаем Error только для тех метрик, что могли не успеть посчитаться
            results['trgm_create_ms'] = results.get('trgm_create_ms', 'Error')
            results['trgm_size'] = results.get('trgm_size', 'Error')
            results['trgm_like_ms'] = results.get('trgm_like_ms', 'Error')
            results['trgm_similar_ms'] = results.get('trgm_similar_ms', 'Error')
            results['trgm_distance_ms'] = results.get('trgm_distance_ms', 'Error')
        finally:
            # Удаляем индекс только если он был успешно создан
            if trgm_created_successfully:
                try:
                    print(f"Удаление индекса {index_name_trgm}...")
                    execute_query(conn, sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(index_name_trgm)))
                except Exception as e_drop:
                    print(f"Не удалось удалить индекс {index_name_trgm}: {e_drop}")
            else:
                 print(f"Пропуск удаления индекса {index_name_trgm}, так как он не был успешно создан.")


        # --- pg_bigm ---
        if pg_bigm_available:
            print("\n--- Тестирование pg_bigm ---")
            index_name_bigm = "idx_gin_bigm_adresse"
            bigm_created_successfully = False
            try:
                print(f"Создание GIN индекса ({index_name_bigm}) на {target_column} с gin_bigm_ops...")
                timing_bigm_create = execute_query(conn,
                    sql.SQL("CREATE INDEX {} ON mutations_foncieres USING gin ({} gin_bigm_ops);")
                        .format(sql.Identifier(index_name_bigm), sql.Identifier(target_column))
                )
                if timing_bigm_create and timing_bigm_create.get('time_ms') is not None:
                    results['bigm_create_ms'] = timing_bigm_create['time_ms']
                    print(f"Индекс {index_name_bigm} создан за {results['bigm_create_ms']:.2f} ms")
                    bigm_created_successfully = True
                else:
                     print(f"Не удалось создать индекс {index_name_bigm} или измерить время.")
                     results['bigm_create_ms'] = 'Error'

                if bigm_created_successfully:
                    # Обновление статистики
                    print("Выполнение ANALYZE...")
                    execute_query(conn, "ANALYZE mutations_foncieres;")

                    results['bigm_size'] = get_index_size(conn, index_name_bigm)
                    print(f"Размер индекса {index_name_bigm}: {results['bigm_size']}")

                    # Запрос LIKE
                    print("Тест LIKE '%AVENUE%' с pg_bigm...")
                    timing_bigm_like = execute_query(conn,
                         sql.SQL("SELECT COUNT(*) FROM mutations_foncieres WHERE {} LIKE %s")
                            .format(sql.Identifier(target_column)),
                         params=('%AVENUE%',), analyze=True
                    )
                    if timing_bigm_like and timing_bigm_like.get('time_ms') is not None:
                        results['bigm_like_ms'] = timing_bigm_like['time_ms']
                        print(f"Время выполнения LIKE с pg_bigm: {format_metric(results['bigm_like_ms'])} ms")
                    else:
                        results['bigm_like_ms'] = 'Error'
                        print(f"Время выполнения LIKE с pg_bigm: Error")

                    print("Запросы на схожесть (%) и дистанцию (<->) не поддерживаются pg_bigm.")
                    results['bigm_similar_ms'] = 'N/A'
                    results['bigm_distance_ms'] = 'N/A'

            except Exception as e:
                print(f"Ошибка при тестировании pg_bigm: {e}")
                results['bigm_create_ms'] = results.get('bigm_create_ms', 'Error')
                results['bigm_size'] = results.get('bigm_size', 'Error')
                results['bigm_like_ms'] = results.get('bigm_like_ms', 'Error')
                results['bigm_similar_ms'] = 'N/A'
                results['bigm_distance_ms'] = 'N/A'
            finally:
                if bigm_created_successfully:
                    try:
                        print(f"Удаление индекса {index_name_bigm}...")
                        execute_query(conn, sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(index_name_bigm)))
                    except Exception as e_drop:
                        print(f"Не удалось удалить индекс {index_name_bigm}: {e_drop}")
                else:
                    print(f"Пропуск удаления индекса {index_name_bigm}, так как он не был успешно создан.")
        else:
            print("\n--- Тестирование pg_bigm пропущено (расширение недоступно) ---")
            results['bigm_create_ms'] = 'N/A'
            results['bigm_size'] = 'N/A'
            results['bigm_like_ms'] = 'N/A'
            results['bigm_similar_ms'] = 'N/A'
            results['bigm_distance_ms'] = 'N/A'

    # --- Сравнение и выводы ---
    print("\n--- Сравнение pg_trgm и pg_bigm ---")
    header = f"{'Метрика':<25} | {'pg_trgm':<20} | {'pg_bigm':<20}"
    print(header)
    print("-" * len(header))
    print(f"{'Время создания (ms)':<25} | {format_metric(results.get('trgm_create_ms', 'N/A')):<20} | {format_metric(results.get('bigm_create_ms', 'N/A')):<20}")
    print(f"{'Размер индекса':<25} | {str(results.get('trgm_size', 'N/A')):<20} | {str(results.get('bigm_size', 'N/A')):<20}")
    print(f"{'Время LIKE (ms)':<25} | {format_metric(results.get('trgm_like_ms', 'N/A')):<20} | {format_metric(results.get('bigm_like_ms', 'N/A')):<20}")
    print(f"{'Время схожести (ms)':<25} | {format_metric(results.get('trgm_similar_ms', 'N/A')):<20} | {format_metric(results.get('bigm_similar_ms', 'N/A')):<20}")
    print(f"{'Время дистанции (ms)':<25} | {format_metric(results.get('trgm_distance_ms', 'N/A')):<20} | {format_metric(results.get('bigm_distance_ms', 'N/A')):<20}")
    print("-" * len(header))

    # --- Плюсы и минусы ---
    print("\nПлюсы и минусы:")
    print("pg_trgm:")
    print("  [+] Богатый набор операторов для нечеткого поиска (схожесть %, дистанция <->).")
    print("  [+] Хорошая производительность для LIKE и операторов схожести (с GIN индексом).")
    print("  [-] Индекс может быть больше и медленнее обновляться, чем B-Tree.")
    print("  [-] Может быть менее эффективен для некоторых языков (например, CJK) по сравнению с pg_bigm.")
    print("pg_bigm:")
    print("  [+] Оптимизирован для поиска LIKE, особенно для языков CJK (китайский, японский, корейский), но работает и для других.")
    print("  [+] Индекс может быть компактнее и быстрее создаваться/обновляться, чем pg_trgm GIN (зависит от данных).")
    print("  [-] Поддерживает только LIKE, нет операторов схожести/дистанции.")
    print("  [-] Может быть менее эффективен для поиска по схожести на европейских языках по сравнению с pg_trgm.")
    print("  [-] Требует отдельной установки, не всегда доступен по умолчанию.")

def demo_pgcrypto():
    """Демонстрация pgcrypto."""
    print("\n\n--- Демонстрация pgcrypto ---")
    test_column_name = "owner_secret_info"
    encryption_key = "mySuperSecretKey123!@#"
    test_ids = [] # Будем хранить ID строк, которые мы изменили
    column_added = False # Флаг, что колонка была добавлена этим запуском

    with db_connection(autocommit=True) as conn_ddl: # Autocommit для ALTER TABLE
        # Добавляем колонку
        try:
            print(f"Проверка/Добавление тестовой колонки {test_column_name} BYTEA...")
            # Проверим, существует ли уже колонка
            with conn_ddl.cursor() as cur_check:
                 cur_check.execute("""
                     SELECT 1 FROM information_schema.columns
                     WHERE table_schema = 'public' AND table_name = 'mutations_foncieres' AND column_name = %s;
                 """, (test_column_name,))
                 if cur_check.fetchone():
                     print(f"Колонка {test_column_name} уже существует.")
                 else:
                      # Используем execute_query для выполнения ALTER TABLE
                      execute_query(conn_ddl, sql.SQL("ALTER TABLE mutations_foncieres ADD COLUMN {} BYTEA")
                                    .format(sql.Identifier(test_column_name)))
                      print(f"Колонка {test_column_name} добавлена.")
                      column_added = True # Устанавливаем флаг
        except Exception as e:
            print(f"Не удалось добавить колонку {test_column_name}: {e}")
            print("Демонстрация pgcrypto прервана из-за ошибки DDL.")
            return # Прерываем демонстрацию pgcrypto

    # Работаем в транзакции для шифрования/дешифрования
    try:
        with db_connection() as conn_tx:
            # Выбираем несколько ID для теста
            print("Выбор записей для шифрования...")
            selected_rows_result = execute_query(conn_tx,
                 "SELECT id, nom_commune FROM mutations_foncieres WHERE type_local = 'Maison' AND longitude IS NOT NULL LIMIT 3;", # Добавил условие, чтобы найти что-то
                 fetch_all=True
            )
            selected_rows = selected_rows_result.get('results')

            if not selected_rows:
                 print("Не найдены записи 'Maison' для теста. Пропуск шифрования.")
                 # Не выходим, чтобы оценка pgcrypto все равно вывелась
            else:
                test_ids = [row[0] for row in selected_rows]
                print(f"Будут зашифрованы данные для ID: {test_ids}")

                # Шифрование
                print("\nШифрование данных...")
                for row_id, commune_name in selected_rows:
                     secret_data = f"Секретный контакт для {commune_name}: {secrets.token_hex(8)}"
                     print(f"  Шифруем '{secret_data}' для ID {row_id}...")
                     # Используем pgp_sym_encrypt с паролем
                     encrypt_sql = sql.SQL("""
                         UPDATE mutations_foncieres
                         SET {} = pgp_sym_encrypt(%s, %s)
                         WHERE id = %s;
                     """).format(sql.Identifier(test_column_name))
                     execute_query(conn_tx, encrypt_sql, params=(secret_data, encryption_key, row_id))

                print("Шифрование завершено (в транзакции).")
                # Коммит произойдет при выходе из 'with'

    except Exception as e:
        print(f"Ошибка во время шифрования/подготовки: {e}")

    # Проверка и дешифрование (в новом соединении/транзакции)
    if test_ids: # Только если шифрование было запущено для каких-то ID
        try:
            with db_connection() as conn_verify:
                print("\nПроверка зашифрованных данных...")
                # 1. Попытка прочитать зашифрованные данные напрямую
                print("Чтение зашифрованной колонки напрямую:")
                select_raw_sql = sql.SQL("SELECT id, {} FROM mutations_foncieres WHERE id = ANY(%s);") \
                                     .format(sql.Identifier(test_column_name))
                raw_data_result = execute_query(conn_verify, select_raw_sql, params=(test_ids,), fetch_all=True)
                raw_data = raw_data_result.get('results')
                if raw_data:
                    for row_id, raw_bytes in raw_data:
                        # psycopg2 возвращает BYTEA как memoryview или bytes
                        display_bytes = bytes(raw_bytes)[:20] if raw_bytes else "NULL"
                        print(f"  ID {row_id}: {display_bytes}...")

                # 2. Дешифрование данных
                print("\nДешифрование данных с помощью ключа:")
                decrypt_sql = sql.SQL("""
                    SELECT id, pgp_sym_decrypt({}, %s) AS decrypted_info
                    FROM mutations_foncieres
                    WHERE id = ANY(%s);
                """).format(sql.Identifier(test_column_name))
                decrypted_data_result = execute_query(conn_verify, decrypt_sql, params=(encryption_key, test_ids), fetch_all=True)
                decrypted_data = decrypted_data_result.get('results')
                if decrypted_data:
                     for row_id, info in decrypted_data:
                          print(f"  ID {row_id}: {info}")

                # 3. Попытка дешифрования с неверным ключом
                print("\nПопытка дешифрования с неверным ключом:")
                wrong_key = "wrongKey"
                try:
                    # Важно: pgp_sym_decrypt вызывает ошибку при неверном ключе
                    execute_query(conn_verify, decrypt_sql, params=(wrong_key, test_ids), fetch_all=True)
                    print("!!! ОШИБКА: Дешифрование с неверным ключом не вызвало исключения !!!")
                except psycopg2.Error as e_decrypt:
                    # Ожидаемая ошибка: "Wrong key or corrupt data" (код SQLSTATE: 39000 или похожий в pgcrypto)
                    if "Wrong key" in str(e_decrypt.pgerror) or "corrupt data" in str(e_decrypt.pgerror):
                         print(f"  Успешно поймана ожидаемая ошибка дешифрования: {e_decrypt.pgcode} {e_decrypt.pgerror}")
                         # Откатываем транзакцию после ошибки, чтобы не оставлять ее висеть
                         conn_verify.rollback()
                    else:
                         print(f"!!! ПОЙМАНА НЕОЖИДАННАЯ ОШИБКА psycopg2 при дешифровании: {e_decrypt} !!!")
                         raise # Передаем неожиданную ошибку выше
                except Exception as e_other_decrypt:
                     print(f"!!! ПОЙМАНА НЕОЖИДАННАЯ ОШИБКА PYTHON при дешифровании: {e_other_decrypt} !!!")
                     raise

        except Exception as e:
            print(f"Ошибка во время проверки/дешифрования: {e}")

    # Обсуждение pgcrypto (выводится всегда)
    print("\n--- Оценка влияния pgcrypto на безопасность данных ---")
    # ... (остается без изменений) ...
    print("1. Конфиденциальность:")
    print("   - [+] Защищает данные от просмотра неавторизованными пользователями, имеющими доступ к БД (например, DBA, резервные копии), если ключ шифрования неизвестен.")
    print("   - [-] Безопасность полностью зависит от безопасности ключа шифрования. Хардкодинг ключа (как в демо) КРАЙНЕ небезопасен.")
    print("   - [-] Если приложение, выполняющее дешифрование, скомпрометировано, данные могут быть раскрыты.")
    print("2. Целостность:")
    print("   - Симметричное шифрование само по себе не гарантирует целостность на 100% (возможна атака 'бит-флиппинг' на некоторые режимы/алгоритмы).")
    print("   - [+] `pgcrypto` предлагает хеширование (`digest`, `hmac`) для проверки целостности. Хеш можно хранить рядом с зашифрованными данными.")
    print("   - [+] Функции PGP (`pgp_sym_encrypt`) включают встроенную проверку целостности (MDC - Modification Detection Code).")
    print("3. Доступность и Производительность:")
    print("   - [-] Шифрование/дешифрование требует процессорного времени, что замедляет операции INSERT/UPDATE/SELECT, работающие с зашифрованными полями.")
    print("   - [-] Прямой поиск по зашифрованным данным (например, `WHERE decrypted_field = 'value'`) очень неэффективен, так как требует дешифрования 'на лету' для каждой строки.")
    print("   - [-] Индексирование зашифрованных данных для быстрого поиска затруднено (требуются специальные подходы, например, детерминированное шифрование с ограниченной безопасностью или отдельные индексы на хешах).")
    print("4. Сложность:")
    print("   - [-] Требуется безопасное управление ключами шифрования (генерация, хранение, ротация, отзыв).")
    print("   - [-] Необходимо вносить изменения в логику приложения для выполнения шифрования/дешифрования.")

    # Финальная очистка - удаляем колонку в autocommit режиме
    print("\n--- Финальная очистка pgcrypto ---")
    try:
        with db_connection(autocommit=True) as conn_cleanup:
            cleanup_test_objects(conn_cleanup) # Попытаемся удалить все объекты в любом случае
    except Exception as e_final_clean:
        print(f"Ошибка во время финальной очистки pgcrypto: {e_final_clean}")


# --- Запуск демонстраций ---
if __name__ == "__main__":
    try:
        with db_connection(autocommit=True) as conn_init:
            setup_extensions(conn_init)
            cleanup_test_objects(conn_init)
    except Exception as e_init:
         print(f"Критическая ошибка при инициализации: {e_init}")
         print("Выполнение прервано.")
         exit(1)

    # Запуск демонстраций
    demo_trgm_bigm()
    demo_pgcrypto()

    print("\n\n--- Все демонстрации расширений завершены ---")