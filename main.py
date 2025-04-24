import zipfile
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional, Tuple
import io

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация ---
SOURCE_ZIP_DIR = Path("./data_zips")
# Директория для СОХРАНЕНИЯ итогового файла
OUTPUT_DIR = Path("./data_processed") # Изменено имя для ясности
ZIP_FILENAMES = ["full_2017.csv.zip", "full_2018.csv.zip", "full_2019.csv.zip"]
# Ожидаемые имена CSV файлов ВНУТРИ архивов
EXPECTED_CSV_NAMES = [name.replace('.zip', '') for name in ZIP_FILENAMES]
# Имя для итогового объединенного CSV файла
OUTPUT_CSV_FILENAME = "full_171819.csv"

def read_combine_from_zips(zip_files: List[str],
                           csv_names: List[str],
                           source_dir: Path) -> Optional[pd.DataFrame]:
    """
    Читает CSV файлы напрямую из zip-архивов и объединяет их в один DataFrame.

    Args:
        zip_files: Список имен zip-файлов для обработки.
        csv_names: Список ожидаемых имен CSV-файлов внутри соответствующих zip-архивов.
        source_dir: Путь к директории с zip-архивами.

    Returns:
        Объединенный pandas DataFrame или None в случае критической ошибки.
    """
    if len(zip_files) != len(csv_names):
        logging.error("Количество zip-файлов и ожидаемых имен CSV должно совпадать.")
        return None

    logging.info("Начало чтения и объединения CSV файлов из архивов...")
    dataframes: List[pd.DataFrame] = []
    first_file_columns = None

    for zip_filename, csv_filename in zip(zip_files, csv_names):
        zip_path = source_dir / zip_filename
        if not zip_path.is_file():
            logging.warning(f"Файл архива не найден: {zip_path}. Пропуск.")
            continue

        try:
            logging.info(f"Открытие архива: {zip_path} для чтения {csv_filename}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                if csv_filename not in zip_ref.namelist():
                    logging.warning(f"Файл {csv_filename} не найден внутри архива {zip_path}. Пропуск.")
                    continue

                with zip_ref.open(csv_filename, 'r') as csv_file:
                    df = pd.read_csv(csv_file, low_memory=False)
                    logging.info(f"Прочитан {csv_filename} из {zip_path}. Форма: {df.shape}")

                    if first_file_columns is None:
                        first_file_columns = set(df.columns)
                    elif set(df.columns) != first_file_columns:
                        logging.warning(f"Набор колонок в {csv_filename} отличается от первого файла!")

                    dataframes.append(df)

        except zipfile.BadZipFile:
            logging.error(f"Ошибка: Файл {zip_path} не является zip-архивом или поврежден.")
        except pd.errors.EmptyDataError:
            logging.warning(f"Файл {csv_filename} в архиве {zip_path} пуст. Пропуск.")
        except pd.errors.ParserError as e:
            logging.error(f"Ошибка парсинга файла {csv_filename} из {zip_path}: {e}.")
        except MemoryError:
             logging.error(f"Ошибка нехватки памяти при чтении {csv_filename} из {zip_path}.")
             logging.error("Попробуйте использовать чтение по частям (chunking) или увеличьте доступную память.")
             return None
        except Exception as e:
            logging.error(f"Неожиданная ошибка при обработке {zip_path}/{csv_filename}: {e}")

    if not dataframes:
        logging.error("Не удалось загрузить ни одного DataFrame. Объединение невозможно.")
        return None

    try:
        logging.info(f"Объединение {len(dataframes)} DataFrame'ов...")
        combined_df = pd.concat(dataframes, ignore_index=True)
        logging.info(f"DataFrame'ы успешно объединены. Итоговая форма: {combined_df.shape}")
        del dataframes
        import gc
        gc.collect()
        return combined_df
    except MemoryError:
        logging.error("Ошибка нехватки памяти при объединении DataFrame'ов.")
        return None
    except Exception as e:
        logging.error(f"Неожиданная ошибка при объединении DataFrame'ов: {e}")
        return None

def analyze_dataframe(df: pd.DataFrame, df_name: str = "Объединенный датасет") -> None:
    """
    Выполняет и выводит первичный анализ pandas DataFrame.
    (Остается без изменений от предыдущей версии)
    """
    if df is None or df.empty:
        logging.warning(f"DataFrame '{df_name}' пуст или отсутствует. Анализ невозможен.")
        return

    logging.info(f"--- Начало анализа датасета: {df_name} ---")

    # 1. Общая информация
    logging.info(f"Форма датасета (строки, колонки): {df.shape}")

    # 2. Список колонок
    logging.info("Список колонок:")
    if len(df.columns) > 50:
         logging.info(f"  Всего {len(df.columns)} колонок. Первые 50:")
         for i, col in enumerate(df.columns):
            if i < 50:
                logging.info(f"  - {col}")
            else:
                logging.info(f"  ... и еще {len(df.columns) - 50}")
                break
    else:
        for col in df.columns:
            logging.info(f"  - {col}")


    # 3. Информация о типах данных и памяти
    buffer = io.StringIO()
    df.info(verbose=True, memory_usage='deep', buf=buffer)
    info_output = buffer.getvalue()
    logging.info("Информация о типах данных и использовании памяти:\n" + info_output)

    # 4. Процент пропущенных значений
    logging.info("Процент пропущенных значений по колонкам (топ 20 с пропусками):")
    missing_percentage = (df.isnull().sum() * 100 / len(df)).sort_values(ascending=False)
    missing_info = missing_percentage[missing_percentage > 0]
    if not missing_info.empty:
        logging.info("\n" + missing_info.head(20).to_string()) # Показываем топ 20
        if len(missing_info) > 20:
            logging.info(f"... и еще {len(missing_info) - 20} колонок с пропусками.")
    else:
        logging.info("Пропущенные значения отсутствуют.")

    # 5. Описательные статистики для числовых колонок
    logging.info("Описательные статистики для числовых колонок:")
    try:
        numeric_desc = df.describe(include='number').round(2)
        logging.info("\n" + numeric_desc.to_string())
    except ValueError:
         logging.info("Числовые колонки не найдены.")


    # 6. Описательные статистики для нечисловых (object/category) колонок
    logging.info("Описательные статистики для нечисловых (object/categorical) колонок:")
    try:
        object_desc = df.describe(exclude='number')
        logging.info("\n" + object_desc.to_string())
    except ValueError:
        logging.info("Нечисловые колонки для описательной статистики не найдены.")
    except Exception as e:
         logging.error(f"Ошибка при расчете статистик для нечисловых колонок: {e}")


    # 7. Количество уникальных значений (топ 20 по убыванию уникальности)
    logging.info("Количество уникальных значений по колонкам (топ 20):")
    unique_counts = df.nunique().sort_values(ascending=False)
    logging.info("\n" + unique_counts.head(20).to_string())
    if len(unique_counts) > 20:
        logging.info(f"... и еще {len(unique_counts) - 20} колонок.")


    logging.info(f"--- Анализ датасета '{df_name}' завершен ---")

def save_dataframe_to_csv(df: pd.DataFrame, output_dir: Path, filename: str) -> bool:
    """
    Сохраняет DataFrame в CSV файл.

    Args:
        df: DataFrame для сохранения.
        output_dir: Директория для сохранения файла.
        filename: Имя выходного CSV файла.

    Returns:
        True в случае успеха, иначе False.
    """
    if df is None:
        logging.error("Нет данных для сохранения (DataFrame is None).")
        return False

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Директория {output_dir} готова для сохранения.")
    except OSError as e:
        logging.error(f"Не удалось создать директорию {output_dir}: {e}")
        return False

    output_path = output_dir / filename
    logging.info(f"Начало сохранения DataFrame в файл: {output_path}...")

    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        logging.info(f"DataFrame успешно сохранен в {output_path}")
        return True
    except MemoryError:
        logging.error(f"Ошибка нехватки памяти при сохранении DataFrame в {output_path}.")
        return False
    except OSError as e:
        logging.error(f"Ошибка записи файла {output_path}: {e}")
        return False
    except Exception as e:
        logging.error(f"Неожиданная ошибка при сохранении DataFrame: {e}")
        return False

# --- Основной блок выполнения ---
if __name__ == "__main__":
    logging.info("Запуск скрипта обработки данных...")

    if not SOURCE_ZIP_DIR.is_dir():
        logging.error(f"Директория с исходными zip-файлами не найдена: {SOURCE_ZIP_DIR}")
        exit(1)

    # Шаг 1: Чтение из архивов и объединение
    combined_dataframe = read_combine_from_zips(ZIP_FILENAMES, EXPECTED_CSV_NAMES, SOURCE_ZIP_DIR)

    # Шаг 2: Анализ объединенного DataFrame
    if combined_dataframe is not None:
        analyze_dataframe(combined_dataframe, df_name=OUTPUT_CSV_FILENAME)

        # Шаг 3: Сохранение результата в один CSV файл
        save_successful = save_dataframe_to_csv(combined_dataframe, OUTPUT_DIR, OUTPUT_CSV_FILENAME)
        if not save_successful:
            logging.error("Не удалось сохранить итоговый DataFrame.")
            exit(1)
    else:
        logging.error("Не удалось создать объединенный DataFrame. Анализ и сохранение не выполнены.")
        exit(1)

    logging.info("Скрипт успешно завершил работу.")