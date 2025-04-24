import psycopg2
import threading
import time
import random
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_SERIALIZABLE
from psycopg2 import errors as pg_errors

# --- Конфигурация подключения к БД ---
DB_CONFIG = {
    "dbname": "real_estate_db",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5433"
}

# --- Вспомогательные функции ---
def get_connection(autocommit=False, isolation_level=None):
    """Создает и возвращает новое соединение."""
    conn = psycopg2.connect(**DB_CONFIG)
    if isolation_level:
        conn.set_session(isolation_level=isolation_level)
    conn.autocommit = autocommit
    print(f"[{threading.current_thread().name}] Подключено к БД. Уровень изоляции: {conn.isolation_level}. Autocommit: {conn.autocommit}")
    return conn

def cleanup_indexes():
    """Удаляет индексы, созданные в задаче 1.1."""
    indexes_to_drop = [
        "idx_btree_code_postal",
        "idx_btree_valeur_fonciere",
        "idx_btree_date_mutation",
        "idx_gin_trgm_adresse_nom_voie",
        "idx_brin_date_mutation"
    ]
    print("\n--- Очистка индексов ---")
    conn = get_connection(autocommit=True)
    cur = conn.cursor()
    for index_name in indexes_to_drop:
        try:
            print(f"Удаление индекса {index_name}...")
            cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(index_name)))
            print(f"Индекс {index_name} удален или не существовал.")
        except psycopg2.Error as e:
            print(f"Ошибка при удалении индекса {index_name}: {e}")
    cur.close()
    conn.close()
    print("--- Очистка индексов завершена ---")

def get_random_mutation_id(conn):
    """Получает случайный ID из существующих записей для тестов."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM mutations_foncieres OFFSET random() * (SELECT COUNT(*) FROM mutations_foncieres) LIMIT 1;")
        result = cur.fetchone()
        return result[0] if result else None

def get_mutation_details(conn, mutation_id):
    """Получает детали мутации по ID."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, valeur_fonciere, nom_commune FROM mutations_foncieres WHERE id = %s;", (mutation_id,))
        return cur.fetchone()

# --- Определения Транзакций ---
def transaction_register_sale(conn, sale_details):
    """
    Транзакция 1: Регистрация новой продажи.
    Читает среднюю цену в коммуне, затем вставляет новую запись.
    """
    thread_name = threading.current_thread().name
    try:
        with conn:
            with conn.cursor() as cur:
                # Шаг 1: Прочитать среднюю цену (для примера)
                cur.execute("SELECT AVG(valeur_fonciere) FROM mutations_foncieres WHERE code_commune = %s;", (sale_details['code_commune'],))
                avg_price = cur.fetchone()[0]
                print(f"[{thread_name}] Средняя цена в {sale_details['code_commune']}: {avg_price:.2f}")

                # Имитация работы
                time.sleep(random.uniform(0.1, 0.3))

                # Шаг 2: Вставить новую запись
                insert_sql = """
                INSERT INTO mutations_foncieres (id_mutation, date_mutation, nature_mutation, valeur_fonciere, code_commune, nom_commune, type_local)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
                """
                cur.execute(insert_sql, (
                    f"SALE-{random.randint(10000, 99999)}",
                    sale_details['date_mutation'],
                    'Vente',
                    sale_details['valeur_fonciere'],
                    sale_details['code_commune'],
                    sale_details['nom_commune'],
                    sale_details['type_local']
                ))
                new_id = cur.fetchone()[0]
                print(f"[{thread_name}] Успешно вставлена запись с ID: {new_id}")
                return new_id
    except psycopg2.Error as e:
        print(f"[{thread_name}] Ошибка в транзакции регистрации продажи: {e}")
        return None

def transaction_update_commune_name(conn, code_commune, old_name, new_name):
    """
    Транзакция 2: Обновление названия коммуны.
    Читает текущее название и обновляет его.
    """
    thread_name = threading.current_thread().name
    try:
        with conn:
            with conn.cursor() as cur:
                # Шаг 1: Проверить текущее имя (для демонстрации)
                cur.execute("SELECT nom_commune FROM mutations_foncieres WHERE code_commune = %s LIMIT 1;", (code_commune,))
                current_name = cur.fetchone()
                if current_name and current_name[0] != old_name:
                    print(f"[{thread_name}] Предупреждение: Текущее имя '{current_name[0]}' не совпадает с ожидаемым '{old_name}' для {code_commune}.")

                print(f"[{thread_name}] Обновление коммуны {code_commune} с '{old_name}' на '{new_name}'...")
                # Имитация работы
                time.sleep(random.uniform(0.1, 0.5))

                # Шаг 2: Обновить записи
                cur.execute("UPDATE mutations_foncieres SET nom_commune = %s WHERE code_commune = %s AND nom_commune = %s;",
                            (new_name, code_commune, old_name))
                updated_count = cur.rowcount
                print(f"[{thread_name}] Обновлено {updated_count} записей для коммуны {code_commune}.")
                return True
    except psycopg2.Error as e:
        print(f"[{thread_name}] Ошибка в транзакции обновления коммуны: {e}")
        if isinstance(e, pg_errors.SerializationFailure):
            print(f"[{thread_name}] !!! Поймана ОШИБКА СЕРИАЛИЗАЦИИ !!!")
        return False

# --- Функции для демонстрации аномалий ---
def demo_non_repeatable_read():
    """Демонстрация Non-Repeatable Read на уровне READ COMMITTED."""
    print("\n\n--- Демонстрация Non-Repeatable Read (Уровень: READ COMMITTED) ---")
    # Выберем ID для теста
    conn_setup = get_connection(autocommit=True)
    mutation_id = get_random_mutation_id(conn_setup)
    if not mutation_id:
        print("Не удалось получить ID для теста.")
        conn_setup.close()
        return
    initial_details = get_mutation_details(conn_setup, mutation_id)
    conn_setup.close()

    if not initial_details:
         print(f"Не удалось получить детали для ID {mutation_id}.")
         return

    print(f"Тестируем на записи с ID={mutation_id}, начальное значение valeur_fonciere={initial_details[1]}")

    barrier = threading.Barrier(2)
    results = {}

    def tx1_reader():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
        thread_name = threading.current_thread().name
        try:
            with conn:
                with conn.cursor() as cur:
                    # Первое чтение
                    cur.execute("SELECT valeur_fonciere FROM mutations_foncieres WHERE id = %s;", (mutation_id,))
                    val1 = cur.fetchone()[0]
                    results['tx1_read1'] = val1
                    print(f"[{thread_name}] Первое чтение: valeur_fonciere = {val1}")

                    # Ждем, пока tx2 изменит данные
                    print(f"[{thread_name}] Ожидание на барьере...")
                    barrier.wait()
                    print(f"[{thread_name}] Прошли барьер, ожидание tx2...")
                    time.sleep(1) # Даем время tx2 закоммитить

                    # Второе чтение
                    cur.execute("SELECT valeur_fonciere FROM mutations_foncieres WHERE id = %s;", (mutation_id,))
                    val2 = cur.fetchone()[0]
                    results['tx1_read2'] = val2
                    print(f"[{thread_name}] Второе чтение: valeur_fonciere = {val2}")

                    if val1 != val2:
                        print(f"[{thread_name}] !!! АНОМАЛИЯ Non-Repeatable Read: {val1} != {val2} !!!")
                    else:
                        print(f"[{thread_name}] Аномалия не проявилась (возможно, tx2 не успел или откатился).")
        except Exception as e:
            print(f"[{thread_name}] Ошибка: {e}")
        finally:
            if conn: conn.close()

    def tx2_writer():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
        thread_name = threading.current_thread().name
        new_value = initial_details[1] + 10000
        try:
             # Ожидаем, пока tx1 сделает первое чтение
            print(f"[{thread_name}] Ожидание на барьере...")
            barrier.wait()
            print(f"[{thread_name}] Прошли барьер, обновление данных...")

            with conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE mutations_foncieres SET valeur_fonciere = %s WHERE id = %s;", (new_value, mutation_id))
                    print(f"[{thread_name}] Установлено valeur_fonciere = {new_value} для ID {mutation_id}. Коммит...")
            results['tx2_wrote'] = new_value
        except Exception as e:
            print(f"[{thread_name}] Ошибка: {e}")
            results['tx2_wrote'] = None
        finally:
            if conn: conn.close()

    t1 = threading.Thread(target=tx1_reader, name="TX1_Reader")
    t2 = threading.Thread(target=tx2_writer, name="TX2_Writer")

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Восстанавливаем исходное значение
    print("--- Восстановление исходного значения ---")
    conn_cleanup = get_connection(autocommit=True)
    try:
        with conn_cleanup.cursor() as cur:
             cur.execute("UPDATE mutations_foncieres SET valeur_fonciere = %s WHERE id = %s;", (initial_details[1], mutation_id))
             print(f"Восстановлено valeur_fonciere = {initial_details[1]} для ID {mutation_id}.")
    except Exception as e:
        print(f"Ошибка восстановления: {e}")
    finally:
        conn_cleanup.close()


def demo_phantom_read():
    """
    Демонстрация Phantom Read (проявление на READ COMMITTED, предотвращение на REPEATABLE READ).
    """
    print("\n\n--- Демонстрация Phantom Read ---")
    test_commune = '01053'
    test_value = 9999999

    # --- Тест 1: Уровень READ COMMITTED ---
    print("\n--- Уровень: READ COMMITTED (ожидаем фантомы) ---")
    barrier_rc = threading.Barrier(2)
    results_rc = {}

    def tx1_reader_rc():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
        thread_name = threading.current_thread().name
        try:
            with conn:
                with conn.cursor() as cur:
                    # Первое чтение (количество дорогих объектов)
                    cur.execute("SELECT COUNT(*) FROM mutations_foncieres WHERE code_commune = %s AND valeur_fonciere > %s;", (test_commune, test_value -1))
                    count1 = cur.fetchone()[0]
                    results_rc['tx1_read1'] = count1
                    print(f"[{thread_name}] Первое чтение: Count = {count1}")

                    print(f"[{thread_name}] Ожидание на барьере...")
                    barrier_rc.wait()
                    print(f"[{thread_name}] Прошли барьер, ожидание tx2...")
                    time.sleep(1) # Даем время tx2 вставить и закоммитить

                    # Второе чтение
                    cur.execute("SELECT COUNT(*) FROM mutations_foncieres WHERE code_commune = %s AND valeur_fonciere > %s;", (test_commune, test_value - 1))
                    count2 = cur.fetchone()[0]
                    results_rc['tx1_read2'] = count2
                    print(f"[{thread_name}] Второе чтение: Count = {count2}")

                    if count1 != count2:
                        print(f"[{thread_name}] !!! АНОМАЛИЯ Phantom Read: {count1} != {count2} !!!")
                    else:
                         print(f"[{thread_name}] Фантом не появился (возможно, tx2 не успел или откатился).")
        except Exception as e:
            print(f"[{thread_name}] Ошибка: {e}")
        finally:
            if conn: conn.close()

    def tx2_inserter_rc():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
        thread_name = threading.current_thread().name
        sale = {
            'code_commune': test_commune, 'date_mutation': '2024-01-01', 'valeur_fonciere': test_value,
            'nom_commune': 'Phantom Test', 'type_local': 'Maison'
        }
        new_id = None
        try:
            print(f"[{thread_name}] Ожидание на барьере...")
            barrier_rc.wait()
            print(f"[{thread_name}] Прошли барьер, вставка 'фантомной' записи...")
            new_id = transaction_register_sale(conn, sale) # Используем нашу транзакцию вставки
            results_rc['tx2_inserted'] = new_id is not None
        except Exception as e:
            print(f"[{thread_name}] Ошибка: {e}")
            results_rc['tx2_inserted'] = False
        finally:
            if conn: conn.close()
            # Очистка фантомной записи
            if new_id:
                conn_cleanup = get_connection(autocommit=True)
                try:
                    with conn_cleanup.cursor() as cur:
                        cur.execute("DELETE FROM mutations_foncieres WHERE id = %s;", (new_id,))
                        print(f"[Cleanup] Удалена фантомная запись ID: {new_id}")
                except Exception as e_clean:
                    print(f"[Cleanup] Ошибка удаления фантома {new_id}: {e_clean}")
                finally:
                    conn_cleanup.close()


    t1_rc = threading.Thread(target=tx1_reader_rc, name="TX1_RC_Reader")
    t2_rc = threading.Thread(target=tx2_inserter_rc, name="TX2_RC_Inserter")
    t1_rc.start(); t2_rc.start(); t1_rc.join(); t2_rc.join()


    # --- Тест 2: Уровень REPEATABLE READ ---
    print("\n--- Уровень: REPEATABLE READ (ожидаем предотвращение фантомов) ---")
    barrier_rr = threading.Barrier(2)
    results_rr = {}

    def tx1_reader_rr():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_REPEATABLE_READ)
        thread_name = threading.current_thread().name
        try:
            with conn:
                with conn.cursor() as cur:
                    # Первое чтение
                    cur.execute("SELECT COUNT(*) FROM mutations_foncieres WHERE code_commune = %s AND valeur_fonciere > %s;", (test_commune, test_value - 1))
                    count1 = cur.fetchone()[0]
                    results_rr['tx1_read1'] = count1
                    print(f"[{thread_name}] Первое чтение: Count = {count1}")

                    print(f"[{thread_name}] Ожидание на барьере...")
                    barrier_rr.wait()
                    print(f"[{thread_name}] Прошли барьер, ожидание tx2...")
                    time.sleep(1)

                    # Второе чтение
                    cur.execute("SELECT COUNT(*) FROM mutations_foncieres WHERE code_commune = %s AND valeur_fonciere > %s;", (test_commune, test_value - 1))
                    count2 = cur.fetchone()[0]
                    results_rr['tx1_read2'] = count2
                    print(f"[{thread_name}] Второе чтение: Count = {count2}")

                    if count1 == count2:
                        print(f"[{thread_name}] === Фантом не появился (как и ожидалось на REPEATABLE READ): {count1} == {count2} ===")
                    else:
                         print(f"[{thread_name}] !!! ОШИБКА: Фантом появился на REPEATABLE READ: {count1} != {count2} !!!")
        except Exception as e:
            print(f"[{thread_name}] Ошибка: {e}")
        finally:
            if conn: conn.close()

    def tx2_inserter_rr():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
        thread_name = threading.current_thread().name
        sale = {
            'code_commune': test_commune, 'date_mutation': '2024-01-01', 'valeur_fonciere': test_value,
            'nom_commune': 'Phantom Test RR', 'type_local': 'Maison'
        }
        new_id = None
        try:
            print(f"[{thread_name}] Ожидание на барьере...")
            barrier_rr.wait()
            print(f"[{thread_name}] Прошли барьер, вставка 'фантомной' записи...")
            new_id = transaction_register_sale(conn, sale)
            results_rr['tx2_inserted'] = new_id is not None
        except Exception as e:
            print(f"[{thread_name}] Ошибка: {e}")
            results_rr['tx2_inserted'] = False
        finally:
            if conn: conn.close()
            if new_id:
                conn_cleanup = get_connection(autocommit=True)
                try:
                    with conn_cleanup.cursor() as cur:
                        cur.execute("DELETE FROM mutations_foncieres WHERE id = %s;", (new_id,))
                        print(f"[Cleanup] Удалена фантомная запись ID: {new_id}")
                except Exception as e_clean:
                    print(f"[Cleanup] Ошибка удаления фантома {new_id}: {e_clean}")
                finally:
                    conn_cleanup.close()

    t1_rr = threading.Thread(target=tx1_reader_rr, name="TX1_RR_Reader")
    t2_rr = threading.Thread(target=tx2_inserter_rr, name="TX2_RR_Inserter")
    t1_rr.start(); t2_rr.start(); t1_rr.join(); t2_rr.join()


def demo_serialization_failure():
    """Демонстрация Serialization Failure на уровне REPEATABLE READ."""
    print("\n\n--- Демонстрация Serialization Failure (Уровень: REPEATABLE READ) ---")
    test_code_commune = '01344'
    conn_setup = get_connection(autocommit=True)
    try:
        with conn_setup.cursor() as cur:
            cur.execute("SELECT nom_commune FROM mutations_foncieres WHERE code_commune = %s LIMIT 1;", (test_code_commune,))
            original_name = cur.fetchone()[0]
            print(f"Тестируем на коммуне {test_code_commune}, исходное имя: '{original_name}'")
    except Exception as e:
        print(f"Не удалось получить имя для коммуны {test_code_commune}: {e}")
        conn_setup.close()
        return
    conn_setup.close()

    barrier = threading.Barrier(2)
    results = {'tx1_success': False, 'tx2_success': False}

    def tx1_updater():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_REPEATABLE_READ)
        thread_name = threading.current_thread().name
        try:
            conn.autocommit = False
            print(f"[{thread_name}] Начало транзакции.")
            # time.sleep(0.1)

            print(f"[{thread_name}] Ожидание на барьере...")
            barrier.wait() # Синхронизация перед обновлением
            print(f"[{thread_name}] Попытка обновления на 'Name TX1'...")
            success = transaction_update_commune_name(conn, test_code_commune, original_name, f"{original_name}_TX1_{random.randint(0,99)}")
            results['tx1_success'] = success
            if success:
                 print(f"[{thread_name}] Коммит...")
                 conn.commit()
            else:
                 print(f"[{thread_name}] Откат (автоматический или из-за ошибки)...")
                 conn.rollback()

        except pg_errors.SerializationFailure as e:
            print(f"[{thread_name}] !!! ПОЙМАНА ОЖИДАЕМАЯ ОШИБКА СЕРИАЛИЗАЦИИ !!!")
            results['tx1_success'] = False
            conn.rollback()
        except psycopg2.Error as e:
            print(f"[{thread_name}] Неожиданная ошибка psycopg2: {e}")
            conn.rollback()
        except Exception as e:
            print(f"[{thread_name}] Неожиданная ошибка Python: {e}")
            conn.rollback()
        finally:
            print(f"[{thread_name}] Завершение.")
            if conn: conn.close()

    def tx2_updater():
        conn = get_connection(isolation_level=ISOLATION_LEVEL_REPEATABLE_READ)
        thread_name = threading.current_thread().name
        try:
            conn.autocommit = False
            print(f"[{thread_name}] Начало транзакции.")
            # time.sleep(0.1)

            print(f"[{thread_name}] Ожидание на барьере...")
            barrier.wait()
            print(f"[{thread_name}] Попытка обновления на 'Name TX2'...")
            time.sleep(0.3)
            success = transaction_update_commune_name(conn, test_code_commune, original_name, f"{original_name}_TX2_{random.randint(0,99)}")
            results['tx2_success'] = success
            if success:
                 print(f"[{thread_name}] Коммит...")
                 conn.commit()
            else:
                 print(f"[{thread_name}] Откат...")
                 conn.rollback()

        except pg_errors.SerializationFailure as e:
            print(f"[{thread_name}] !!! ПОЙМАНА ОЖИДАЕМАЯ ОШИБКА СЕРИАЛИЗАЦИИ !!!")
            results['tx2_success'] = False
            conn.rollback()
        except psycopg2.Error as e:
            print(f"[{thread_name}] Неожиданная ошибка psycopg2: {e}")
            conn.rollback()
        except Exception as e:
            print(f"[{thread_name}] Неожиданная ошибка Python: {e}")
            conn.rollback()
        finally:
             print(f"[{thread_name}] Завершение.")
             if conn: conn.close()

    t1 = threading.Thread(target=tx1_updater, name="TX1_Updater")
    t2 = threading.Thread(target=tx2_updater, name="TX2_Updater")

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"\nРезультат Serialization Failure: TX1 успех={results['tx1_success']}, TX2 успех={results['tx2_success']}")
    if results['tx1_success'] != results['tx2_success']:
        print("=== Одна из транзакций успешно завершилась, другая откатилась из-за конфликта (ожидаемый результат) ===")
    else:
        print("--- Результат неопределен или обе транзакции завершились одинаково (возможно, конфликт не произошел как ожидалось) ---")

    # Восстановление исходного имени
    print("--- Восстановление исходного имени коммуны ---")
    conn_cleanup = get_connection(autocommit=True)
    try:
        with conn_cleanup.cursor() as cur:
             cur.execute("UPDATE mutations_foncieres SET nom_commune = %s WHERE code_commune = %s;", (original_name, test_code_commune))
             print(f"Восстановлено имя '{original_name}' для коммуны {test_code_commune}. Затронуто строк: {cur.rowcount}")
    except Exception as e:
        print(f"Ошибка восстановления имени коммуны: {e}")
    finally:
        conn_cleanup.close()

def transaction_bulk_price_adjustment(conn, code_postal, percentage_change):
    """
    Транзакция 3: Массовое обновление цен на недвижимость в районе (по почтовому индексу).
    Изменяет valeur_fonciere на percentage_change процентов.
    """
    thread_name = threading.current_thread().name
    # Рассчитываем множитель: +5% -> 1.05, -10% -> 0.90
    adjustment_factor = 1 + (percentage_change / 100.0)
    print(f"[{thread_name}] Запуск массового обновления цен для code_postal={code_postal}, множитель={adjustment_factor:.4f}")
    try:
        with conn: # Автоматический BEGIN/COMMIT/ROLLBACK
            with conn.cursor() as cur:
                sql_update = sql.SQL("""
                    UPDATE mutations_foncieres
                    SET valeur_fonciere = valeur_fonciere * %s
                    WHERE code_postal = %s AND valeur_fonciere IS NOT NULL AND valeur_fonciere > 0;
                """)
                cur.execute(sql_update, (adjustment_factor, code_postal))
                updated_count = cur.rowcount
                print(f"[{thread_name}] Обновлено {updated_count} записей для code_postal {code_postal}.")

                if updated_count > 0:
                     cur.execute("SELECT MIN(valeur_fonciere), MAX(valeur_fonciere) FROM mutations_foncieres WHERE code_postal = %s;", (code_postal,))
                     min_val, max_val = cur.fetchone()
                     print(f"[{thread_name}] Новые мин/макс цены в {code_postal}: {min_val:.2f} / {max_val:.2f}")
                     if min_val <= 0:
                          print(f"[{thread_name}] ПРЕДУПРЕЖДЕНИЕ: Минимальная цена стала <= 0! Возможно, стоит откатить.")

                return updated_count # Возвращаем кол-во обновленных строк
    except psycopg2.Error as e:
        print(f"[{thread_name}] Ошибка в транзакции массового обновления цен: {e}")
        return -1

def transaction_archive_old_mutations(conn, cutoff_date):
    """
    Транзакция 4: Архивация (удаление) старых записей о мутациях старше cutoff_date.
    ВНИМАНИЕ: Эта функция предназначена для вызова внутри обертки, которая сделает ROLLBACK.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Запуск архивации (удаления) записей старше {cutoff_date}")
    deleted_count = -1
    try:
        with conn.cursor() as cur:
            # Шаг 1 (для информации): Посчитать, сколько будет удалено
            sql_count = sql.SQL("SELECT COUNT(*) FROM mutations_foncieres WHERE date_mutation < %s;")
            cur.execute(sql_count, (cutoff_date,))
            count_to_delete = cur.fetchone()[0]
            print(f"[{thread_name}] Найдено {count_to_delete} записей для удаления (старше {cutoff_date}).")

            if count_to_delete == 0:
                 print(f"[{thread_name}] Нет записей для удаления.")
                 # Если нечего удалять, считаем операцию успешной (0 удалено)
                 # Нужно закоммитить пустую транзакцию или откатить - зависит от внешней логики
                 # Здесь просто вернем 0, чтобы внешний код решил.
                 return 0 # Важно вернуть не -1

            # Шаг 2: Удаление
            print(f"[{thread_name}] Выполнение DELETE...")
            sql_delete = sql.SQL("DELETE FROM mutations_foncieres WHERE date_mutation < %s;")
            cur.execute(sql_delete, (cutoff_date,))
            deleted_count = cur.rowcount
            print(f"[{thread_name}] Успешно удалено {deleted_count} записей (внутри транзакции).")
            if deleted_count != count_to_delete:
                 print(f"[{thread_name}] ПРЕДУПРЕЖДЕНИЕ: Количество удаленных ({deleted_count}) не совпадает с подсчитанным ({count_to_delete})!")
                 # Это может случиться, если кто-то удалил строки между COUNT и DELETE,
                 # даже на REPEATABLE READ, если другая транзакция закоммитила удаление.
                 # На SERIALIZABLE это вызвало бы ошибку сериализации.

            # Возвращаем количество для внешней логики (которая сделает ROLLBACK)
            return deleted_count
    except psycopg2.Error as e:
        print(f"[{thread_name}] Ошибка в транзакции архивации: {e}")
        # Откат будет выполнен снаружи
        return -1 # Явная индикация ошибки

# --- Функция для безопасного теста архивации с принудительным откатом ---
def run_archive_test_with_rollback(cutoff_date):
    """
    Запускает транзакцию архивации в отдельной сессии
    и гарантированно откатывает изменения после выполнения.
    """
    print(f"\n--- Тестовый запуск архивации (с гарантированным ROLLBACK) для даты < {cutoff_date} ---")
    conn = None
    initial_count = -1
    try:
        # Получаем соединение без автокоммита
        conn = get_connection(autocommit=False, isolation_level=ISOLATION_LEVEL_READ_COMMITTED)

        # 1. Проверяем исходное количество записей до начала
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mutations_foncieres WHERE date_mutation < %s;", (cutoff_date,))
            initial_count = cur.fetchone()[0]
            print(f"[TestRunner] Исходное количество записей старше {cutoff_date}: {initial_count}")

        if initial_count == 0:
            print("[TestRunner] Нет записей для тестового удаления. Пропуск демонстрации удаления.")
            return

        print("[TestRunner] Начало транзакции для теста архивации.")
        # 2. Выполняем функцию архивации (которая делает DELETE)
        deleted_count = transaction_archive_old_mutations(conn, cutoff_date)

        if deleted_count >= 0:
            print(f"[TestRunner] Транзакция архивации сообщила об удалении {deleted_count} записей.")
            # 3. Проверка ВНУТРИ транзакции (удаленные строки не должны быть видны)
            with conn.cursor() as cur:
                 cur.execute("SELECT COUNT(*) FROM mutations_foncieres WHERE date_mutation < %s;", (cutoff_date,))
                 count_after_delete = cur.fetchone()[0]
                 print(f"[TestRunner] Проверка ВНУТРИ транзакции: записей старше {cutoff_date} = {count_after_delete} (ожидается 0)")
                 if count_after_delete != 0:
                      print("[TestRunner] ПРЕДУПРЕЖДЕНИЕ: Проверка внутри транзакции показала не 0 записей после удаления!")
        else:
            print("[TestRunner] Транзакция архивации завершилась с ошибкой.")

    except Exception as e:
        print(f"[TestRunner] Ошибка во время теста архивации: {e}")
    finally:
        if conn and not conn.closed:
            # 4. Гарантированный ROLLBACK в любом случае
            print("[TestRunner] Выполнение ROLLBACK для отмены изменений теста архивации...")
            conn.rollback()
            print("[TestRunner] ROLLBACK выполнен.")
            # 5. Проверка ПОСЛЕ отката (данные должны вернуться)
            try:
                with conn.cursor() as cur:
                     cur.execute("SELECT COUNT(*) FROM mutations_foncieres WHERE date_mutation < %s;", (cutoff_date,))
                     count_after_rollback = cur.fetchone()[0]
                     print(f"[TestRunner] Проверка ПОСЛЕ ROLLBACK: записей старше {cutoff_date} = {count_after_rollback} (ожидается {initial_count})")
                     if count_after_rollback != initial_count:
                          print(f"[TestRunner] !!! ОШИБКА: Количество записей после отката ({count_after_rollback}) не совпадает с исходным ({initial_count}) !!!")

            except Exception as e_check:
                 print(f"[TestRunner] Ошибка проверки после отката: {e_check}")
            finally:
                 conn.close()
                 print("[TestRunner] Соединение для теста архивации закрыто.")
        elif conn and conn.closed:
             print("[TestRunner] Соединение уже было закрыто перед блоком finally (неожиданно).")
        else:
             print("[TestRunner] Соединение не было установлено.")


# --- Основной блок выполнения ---
if __name__ == "__main__":
    # 1. Очистка индексов от предыдущего задания
    cleanup_indexes()

    # 2. Демонстрация аномалий
    demo_non_repeatable_read()
    demo_phantom_read()
    demo_serialization_failure()


    print("\n\n--- Демонстрация дополнительных полезных транзакций ---")

    # 3.1 Демонстрация массового обновления цен
    print("\n--- Тест транзакции: Массовое обновление цен (+5% в 75015) ---")
    test_postal_code = '51200.0' # Пример: 15-й округ Парижа
    percentage = 5.0 # Увеличить на 5%
    initial_avg_price = 0
    conn_setup = get_connection(autocommit=True)
    try:
        with conn_setup.cursor() as cur:
            sql_avg = sql.SQL("SELECT COALESCE(AVG(valeur_fonciere), 0.0) FROM mutations_foncieres WHERE code_postal = %s;")
            cur.execute(sql_avg, (test_postal_code,))
            res = cur.fetchone()
            initial_avg_price = res[0]
            print(f"Начальная средняя цена для {test_postal_code}: {initial_avg_price:.2f}")
    except psycopg2.Error as e:
        print(f"Ошибка БД при получении начальной цены для {test_postal_code}: {e}")
        initial_avg_price = 0.0
    except Exception as e:
        print(f"Не удалось получить начальную цену для {test_postal_code}: {e}")
        initial_avg_price = 0.0
    finally:
        if conn_setup: conn_setup.close()

    # Выполняем транзакцию обновления
    conn_update = get_connection(autocommit=False, isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
    updated_count = -1
    if initial_avg_price > 0:
        try:
            updated_count = transaction_bulk_price_adjustment(conn_update, test_postal_code, percentage)
            if updated_count >= 0:
                print(f"Транзакция обновления цен для {test_postal_code} вернула {updated_count}. Коммит...")
                conn_update.commit()
                print("Коммит выполнен.")

                # Проверка после коммита
                conn_check = get_connection(autocommit=True)
                final_avg_price = 0.0
                try:
                    with conn_check.cursor() as cur:
                        sql_avg_check = sql.SQL(
                            "SELECT COALESCE(AVG(valeur_fonciere), 0.0) FROM mutations_foncieres WHERE code_postal = %s;")
                        cur.execute(sql_avg_check, (test_postal_code,))
                        final_avg_price = cur.fetchone()[0]
                        print(f"Финальная средняя цена для {test_postal_code}: {final_avg_price:.2f}")

                        # Сравнение
                        if initial_avg_price > 0:
                            expected_price = initial_avg_price * (1 + percentage / 100.0)
                            print(f"(Ожидаемая цена ~ {expected_price:.2f})")
                            if abs(final_avg_price - expected_price) / initial_avg_price < 0.01:
                                print("Изменение цены соответствует ожидаемому.")
                            else:
                                print("ПРЕДУПРЕЖДЕНИЕ: Изменение цены НЕ соответствует ожидаемому.")
                        else:
                            print("Не удалось сравнить с исходной ценой (она была 0 или не получена).")

                except psycopg2.Error as e:
                    print(f"Ошибка БД при проверке после коммита: {e}")
                except Exception as e:
                    print(f"Неожиданная ошибка проверки после коммита: {e}")
                finally:
                    if conn_check: conn_check.close()

            else:
                print(
                    f"Транзакция обновления цен для {test_postal_code} не удалась. Откат (автоматический через with)...")
                # conn_update.rollback() не нужен
        except Exception as e:
            print(f"Внешняя ошибка во время выполнения обновления цен: {e}")
            if conn_update and not conn_update.closed and conn_update.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                print("Выполнение отката из-за внешней ошибки...")
                conn_update.rollback()
        finally:
            if conn_update and not conn_update.closed: conn_update.close()

    else:
        print(f"Пропуск обновления цен для {test_postal_code}, так как исходная средняя цена была 0 (нет данных).")
        if conn_update and not conn_update.closed: conn_update.close()  # Закрываем неиспользованное соединение

        # Откатываем изменение, ЕСЛИ обновление было применено (updated_count > 0)
    if updated_count > 0:
        print(f"\n--- Откат массового обновления цен для восстановления данных ({test_postal_code}) ---")
        conn_rollback = get_connection(autocommit=False, isolation_level=ISOLATION_LEVEL_READ_COMMITTED)
        try:
            rollback_factor = 1.0 / (1 + percentage / 100.0)
            rollback_percentage = (rollback_factor - 1) * 100.0
            print(
                f"Выполнение обратного обновления с множителем: {rollback_factor:.6f} (процент ~{rollback_percentage:.4f}%)")
            rollback_count = transaction_bulk_price_adjustment(conn_rollback, test_postal_code, rollback_percentage)
            if rollback_count >= 0:
                print(f"Откат обновления цен выполнен. Затронуто {rollback_count} записей. Коммит отката...")
                conn_rollback.commit()
                # Финальная проверка
                conn_final_check = get_connection(autocommit=True)
                restored_avg_price = 0.0
                try:
                    with conn_final_check.cursor() as cur:
                        sql_avg_final = sql.SQL(
                            "SELECT COALESCE(AVG(valeur_fonciere), 0.0) FROM mutations_foncieres WHERE code_postal = %s;")
                        cur.execute(sql_avg_final, (test_postal_code,))
                        restored_avg_price = cur.fetchone()[0]
                        print(
                            f"Восстановленная средняя цена для {test_postal_code}: {restored_avg_price:.2f} (исходная была {initial_avg_price:.2f})")
                        if initial_avg_price > 0 and abs(
                                restored_avg_price - initial_avg_price) / initial_avg_price < 0.001:  # Допуск 0.1%
                            print("Цена успешно восстановлена.")
                        elif initial_avg_price == 0 and restored_avg_price == 0:
                            print("Цена осталась 0 (как и была).")
                        else:
                            print("ПРЕДУПРЕЖДЕНИЕ: Цена после отката отличается от исходной.")
                except psycopg2.Error as e:
                    print(f"Ошибка БД при финальной проверке отката: {e}")
                except Exception as e:
                    print(f"Неожиданная ошибка финальной проверке отката: {e}")
                finally:
                    if conn_final_check: conn_final_check.close()
            else:
                print("Транзакция отката обновления цен не удалась. Откат...")
                conn_rollback.rollback()

        except Exception as e_rb:
            print(f"Ошибка при откате обновления цен: {e_rb}")
            if conn_rollback and not conn_rollback.closed and conn_rollback.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                conn_rollback.rollback()
        finally:
            if conn_rollback and not conn_rollback.closed: conn_rollback.close()

        # 3.2 Демонстрация архивации с гарантированным откатом
    archive_date_test_1 = '2017-01-10'
    archive_date_test_2 = '2018-01-01'

    run_archive_test_with_rollback(archive_date_test_1)
    run_archive_test_with_rollback(archive_date_test_2)

    print("\n\n--- Все демонстрации завершены ---")