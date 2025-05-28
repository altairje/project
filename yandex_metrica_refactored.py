"""
Оптимизированный ETL процесс для загрузки данных Яндекс.Метрики в ClickHouse
Версия: 2.0 (рефакторенная)

Основные улучшения:
- Исправлены SQL-инъекции через параметризованные запросы
- Добавлена валидация входных данных
- Улучшена обработка ошибок и логирование
- Оптимизирована работа с памятью
- Добавлены типы данных и документация
- Разделение ответственности между классами
- Конфигурация вынесена в отдельные модули
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import os
import time
import gc
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
import clickhouse_connect
from tapi_yandex_metrika import YandexMetrikaLogsapi
from dateutil.parser import parse as parse_date
import ast
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
class Constants:
    MAX_ROWS_PER_REQUEST = 1000000
    BATCH_SIZE = 50000
    REQUEST_DELAY = 1
    MAX_RETRIES = 3
    RETRY_DELAY = 30
    MEMORY_BATCH_SIZE = 10000
    COVERAGE_THRESHOLD = 0.95
    MIN_ROWS_PER_DAY = 10

class ProcessingStatus(Enum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class FieldConfig:
    """Конфигурация поля для маппинга YM -> ClickHouse"""
    field: str
    column: str
    type: str
    comment: str
    nullable: bool = False
    default_value: Any = None

@dataclass
class ProcessingResult:
    """Результат обработки счетчика"""
    counter_id: str
    site_name: str
    status: ProcessingStatus
    total_loaded: int = 0
    dates_processed: int = 0
    dates_skipped: int = 0
    errors: List[str] = field(default_factory=list)
    processing_time: float = 0.0

class ConfigManager:
    """Менеджер конфигурации полей"""
    
    @staticmethod
    def get_field_config() -> List[FieldConfig]:
        """Возвращает конфигурацию полей с оптимизированными типами"""
        return [
            FieldConfig("ym:s:visitID", "visit_id", "UInt64", "Идентификатор визита"),
            FieldConfig("ym:s:counterID", "counter_id", "UInt32", "Номер счетчика"),
            FieldConfig("ym:s:watchIDs", "watch_ids", "Array(UInt64)", "Просмотры визита"),
            FieldConfig("ym:s:dateTime", "datetime", "DateTime('Europe/Moscow')", "Дата и время визита"),
            FieldConfig("ym:s:isNewUser", "is_new_user", "Bool", "Первый визит посетителя"),
            FieldConfig("ym:s:startURL", "start_url", "String", "Страница входа"),
            FieldConfig("ym:s:endURL", "end_url", "String", "Страница выхода"),
            FieldConfig("ym:s:pageViews", "page_views", "UInt16", "Глубина просмотра"),
            FieldConfig("ym:s:visitDuration", "visit_duration", "UInt32", "Время на сайте (сек)"),
            FieldConfig("ym:s:bounce", "bounce", "Bool", "Отказность"),
            FieldConfig("ym:s:ipAddress", "ip_address", "String", "IP адрес"),
            FieldConfig("ym:s:regionCountry", "region_country", "LowCardinality(String)", "Страна (ISO)"),
            FieldConfig("ym:s:regionCity", "region_city", "LowCardinality(String)", "Город"),
            FieldConfig("ym:s:clientID", "client_id", "UInt64", "Идентификатор пользователя"),
            FieldConfig("ym:s:counterUserIDHash", "counter_user_id_hash", "Nullable(UInt64)", "Хеш ID пользователя", True),
            FieldConfig("ym:s:networkType", "network_type", "LowCardinality(String)", "Тип сети"),
            FieldConfig("ym:s:goalsID", "goals_id", "Array(UInt32)", "Цели визита"),
            FieldConfig("ym:s:goalsSerialNumber", "goals_serial_number", "Array(UInt32)", "Серийные номера целей"),
            FieldConfig("ym:s:goalsDateTime", "goals_datetime", "Array(DateTime('Europe/Moscow'))", "Время достижения целей"),
            FieldConfig("ym:s:referer", "referer", "String", "Реферер"),
            FieldConfig("ym:s:from", "from_source", "LowCardinality(String)", "Источник перехода"),
            FieldConfig("ym:s:browserLanguage", "browser_language", "LowCardinality(String)", "Язык браузера"),
            FieldConfig("ym:s:browserCountry", "browser_country", "LowCardinality(String)", "Страна браузера"),
            FieldConfig("ym:s:clientTimeZone", "client_timezone", "Int16", "Часовой пояс пользователя"),
            FieldConfig("ym:s:deviceCategory", "device_category", "LowCardinality(String)", "Категория устройства"),
            FieldConfig("ym:s:mobilePhone", "mobile_phone", "LowCardinality(String)", "Производитель устройства"),
            FieldConfig("ym:s:mobilePhoneModel", "mobile_phone_model", "LowCardinality(String)", "Модель устройства"),
            FieldConfig("ym:s:operatingSystemRoot", "os_root", "LowCardinality(String)", "Группа ОС"),
            FieldConfig("ym:s:operatingSystem", "os", "LowCardinality(String)", "Операционная система"),
            FieldConfig("ym:s:browser", "browser", "LowCardinality(String)", "Браузер"),
            FieldConfig("ym:s:browserEngine", "browser_engine", "LowCardinality(String)", "Движок браузера"),
            FieldConfig("ym:s:cookieEnabled", "cookie_enabled", "Bool", "Cookie включены"),
            FieldConfig("ym:s:javascriptEnabled", "javascript_enabled", "Bool", "JavaScript включён"),
            FieldConfig("ym:s:screenFormat", "screen_format", "LowCardinality(String)", "Формат экрана"),
            FieldConfig("ym:s:screenColors", "screen_colors", "UInt8", "Глубина цвета"),
            FieldConfig("ym:s:screenOrientation", "screen_orientation", "UInt8", "Ориентация экрана"),
            FieldConfig("ym:s:screenOrientationName", "screen_orientation_name", "LowCardinality(String)", "Название ориентации"),
            FieldConfig("ym:s:screenWidth", "screen_width", "UInt16", "Ширина экрана"),
            FieldConfig("ym:s:screenHeight", "screen_height", "UInt16", "Высота экрана"),
            FieldConfig("ym:s:physicalScreenWidth", "physical_screen_width", "UInt16", "Физическая ширина"),
            FieldConfig("ym:s:physicalScreenHeight", "physical_screen_height", "UInt16", "Физическая высота"),
            FieldConfig("ym:s:windowClientWidth", "window_client_width", "UInt16", "Ширина окна"),
            FieldConfig("ym:s:windowClientHeight", "window_client_height", "UInt16", "Высота окна"),
            FieldConfig("ym:s:parsedParamsKey1", "parsed_params_key1", "Nullable(String)", "Параметры визита, ур. 1", True),
        ]

class DataValidator:
    """Валидатор данных"""
    
    @staticmethod
    def validate_counter_id(counter_id: str) -> bool:
        """Валидация ID счетчика"""
        return counter_id.isdigit() and len(counter_id) <= 20
    
    @staticmethod
    def validate_date_format(date_str: str) -> bool:
        """Валидация формата даты"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> bool:
        """Валидация диапазона дат"""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            return start <= end and (end - start).days <= 365  # Максимум год
        except ValueError:
            return False

class DataNormalizer:
    """Нормализатор данных для ClickHouse"""
    
    def __init__(self, field_configs: List[FieldConfig]):
        self.field_configs = field_configs
    
    def normalize_row(self, row: List[Any]) -> List[Any]:
        """Нормализация строки данных с улучшенной обработкой ошибок"""
        if len(row) != len(self.field_configs):
            logger.warning(f"Несоответствие количества полей: получено {len(row)}, ожидается {len(self.field_configs)}")
            # Дополняем или обрезаем до нужного размера
            row = (row + [None] * len(self.field_configs))[:len(self.field_configs)]
        
        normalized = []
        
        for value, config in zip(row, self.field_configs):
            try:
                normalized_value = self._normalize_field(value, config)
                normalized.append(normalized_value)
            except Exception as e:
                logger.warning(f"Ошибка нормализации поля {config.column}: {e}")
                normalized.append(self._get_default_value(config))
        
        return normalized
    
    def _normalize_field(self, value: Any, config: FieldConfig) -> Any:
        """Нормализация отдельного поля"""
        # Обработка NULL значений
        if value is None or value == '':
            if config.nullable or "Array" in config.type:
                return None if "Array" not in config.type else []
            else:
                return self._get_default_value(config)
        
        # Обработка массивов
        if config.type.startswith("Array("):
            return self._normalize_array(value, config)
        
        # Обработка DateTime
        elif "DateTime" in config.type:
            return self._normalize_datetime(value)
        
        # Обработка Boolean
        elif config.type == "Bool":
            return self._normalize_boolean(value)
        
        # Обработка числовых типов
        elif any(t in config.type for t in ["UInt", "Int"]):
            return self._normalize_numeric(value, config)
        
        # Обработка строк
        else:
            return str(value) if value is not None else ""
    
    def _normalize_array(self, value: Any, config: FieldConfig) -> List[Any]:
        """Нормализация массивов"""
        if isinstance(value, str) and value.strip():
            try:
                parsed_value = ast.literal_eval(value)
                if isinstance(parsed_value, list):
                    # Обработка DateTime в массивах
                    if "DateTime" in config.type:
                        return [self._normalize_datetime(v) for v in parsed_value]
                    return parsed_value
            except (ValueError, SyntaxError):
                pass
        elif isinstance(value, list):
            return value
        
        return []
    
    def _normalize_datetime(self, value: Any) -> datetime:
        """Нормализация DateTime"""
        if isinstance(value, datetime):
            return value
        elif isinstance(value, str):
            try:
                return parse_date(value)
            except Exception:
                return datetime(1970, 1, 1)  # Unix epoch как fallback
        else:
            return datetime(1970, 1, 1)
    
    def _normalize_boolean(self, value: Any) -> bool:
        """Нормализация Boolean"""
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)
    
    def _normalize_numeric(self, value: Any, config: FieldConfig) -> int:
        """Нормализация числовых типов"""
        try:
            if isinstance(value, str):
                if not value.isdigit():
                    return 0
                num_value = int(value)
            else:
                num_value = int(value)
            
            # Ограничения для разных типов
            if "UInt8" in config.type:
                return min(max(num_value, 0), 255)
            elif "UInt16" in config.type:
                return min(max(num_value, 0), 65535)
            elif "UInt32" in config.type:
                return min(max(num_value, 0), 4294967295)
            elif "Int16" in config.type:
                return min(max(num_value, -32768), 32767)
            
            return num_value
        except (ValueError, TypeError):
            return 0
    
    def _get_default_value(self, config: FieldConfig) -> Any:
        """Получение значения по умолчанию для типа"""
        if config.default_value is not None:
            return config.default_value
        
        if "Array" in config.type:
            return []
        elif config.nullable:
            return None
        elif "String" in config.type:
            return ""
        elif any(t in config.type for t in ["UInt", "Int"]):
            return 0
        elif "Bool" in config.type:
            return False
        elif "DateTime" in config.type:
            return datetime(1970, 1, 1)
        else:
            return None

class ClickHouseManager:
    """Менеджер для работы с ClickHouse"""
    
    def __init__(self):
        self.client = self._create_client()
        self.field_configs = ConfigManager.get_field_config()
    
    def _create_client(self):
        """Создание подключения к ClickHouse с валидацией"""
        required_env_vars = [
            "CLICKHOUSE_HOST", "CLICKHOUSE_PORT", 
            "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DB"
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Отсутствуют переменные окружения: {missing_vars}")
        
        try:
            return clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST"),
                port=int(os.getenv("CLICKHOUSE_PORT")),
                username=os.getenv("CLICKHOUSE_USER"),
                password=os.getenv("CLICKHOUSE_PASSWORD"),
                database=os.getenv("CLICKHOUSE_DB"),
                connect_timeout=30,
                send_receive_timeout=300
            )
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            raise
    
    def create_table(self, table_name: str = "ym_visits") -> None:
        """Создание таблицы с улучшенной схемой"""
        columns_sql = ",\n    ".join(
            f"{config.column} {config.type} COMMENT '{config.comment}'"
            for config in self.field_configs
        )
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS dev.{table_name} (
            {columns_sql},
            site_name LowCardinality(String) COMMENT 'Название сайта',
            load_date Date DEFAULT today() COMMENT 'Дата загрузки',
            load_timestamp DateTime DEFAULT now() COMMENT 'Время загрузки'
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(datetime)
        ORDER BY (counter_id, datetime, visit_id)
        SETTINGS index_granularity = 8192
        COMMENT 'Единая таблица визитов Яндекс.Метрики'
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Таблица {table_name} создана/проверена успешно")
        except Exception as e:
            logger.error(f"Ошибка создания таблицы {table_name}: {e}")
            raise
    
    def get_existing_dates(self, counter_id: str, start_date: str, end_date: str, 
                          table_name: str = "ym_visits") -> set:
        """Получение существующих дат с параметризованным запросом"""
        query = f"""
        SELECT DISTINCT toDate(datetime) as date
        FROM dev.{table_name}
        WHERE counter_id = %(counter_id)s
          AND datetime >= %(start_date)s
          AND datetime <= %(end_date)s
        ORDER BY date
        """
        
        try:
            result = self.client.query(
                query,
                parameters={
                    'counter_id': int(counter_id),
                    'start_date': start_date,
                    'end_date': f"{end_date} 23:59:59"
                }
            )
            return {str(row[0]) for row in result.result_rows}
        except Exception as e:
            logger.warning(f"Ошибка получения существующих дат: {e}")
            return set()
    
    def get_incomplete_dates(self, counter_id: str, start_date: str, end_date: str,
                           table_name: str = "ym_visits") -> List[str]:
        """Определение дат с неполной загрузкой"""
        query = f"""
        WITH date_stats AS (
            SELECT 
                toDate(datetime) as date,
                count() as row_count,
                min(visit_id) as min_visit_id,
                max(visit_id) as max_visit_id,
                max(visit_id) - min(visit_id) + 1 as expected_range,
                countDistinct(visit_id) as unique_visits
            FROM dev.{table_name}
            WHERE counter_id = %(counter_id)s
            AND datetime >= %(start_date)s
            AND datetime <= %(end_date)s
            GROUP BY toDate(datetime)
        )
        SELECT date
        FROM date_stats
        WHERE unique_visits < expected_range * %(coverage_threshold)s
        OR row_count < %(min_rows)s
        ORDER BY date
        """
        
        try:
            result = self.client.query(
                query,
                parameters={
                    'counter_id': int(counter_id),
                    'start_date': start_date,
                    'end_date': f"{end_date} 23:59:59",
                    'coverage_threshold': Constants.COVERAGE_THRESHOLD,
                    'min_rows': Constants.MIN_ROWS_PER_DAY
                }
            )
            return [str(row[0]) for row in result.result_rows]
        except Exception as e:
            logger.warning(f"Ошибка определения неполных дат: {e}")
            return []
    
    def delete_date_data(self, counter_id: str, date: str, table_name: str = "ym_visits") -> None:
        """Удаление данных за дату с параметризованным запросом"""
        query = f"""
        DELETE FROM dev.{table_name} 
        WHERE counter_id = %(counter_id)s AND toDate(datetime) = %(date)s
        """
        
        try:
            self.client.command(
                query,
                parameters={
                    'counter_id': int(counter_id),
                    'date': date
                }
            )
            logger.info(f"Удалены данные для счетчика {counter_id} за {date}")
        except Exception as e:
            logger.error(f"Ошибка удаления данных: {e}")
            raise
    
    def insert_data(self, data: List[List[Any]], site_name: str, 
                   table_name: str = "ym_visits") -> int:
        """Вставка данных с улучшенной обработкой ошибок"""
        if not data:
            return 0
        
        # Добавляем служебные поля
        enhanced_data = []
        current_time = datetime.now()
        current_date = current_time.date()
        
        for row in data:
            enhanced_row = row + [site_name, current_date, current_time]
            enhanced_data.append(enhanced_row)
        
        # Формируем список колонок
        column_names = [config.column for config in self.field_configs]
        extended_columns = column_names + ['site_name', 'load_date', 'load_timestamp']
        
        try:
            self.client.insert(
                table=f"dev.{table_name}",
                data=enhanced_data,
                column_names=extended_columns
            )
            logger.info(f"Успешно вставлено {len(enhanced_data)} строк")
            return len(enhanced_data)
        except Exception as e:
            logger.error(f"Ошибка при вставке данных: {e}")
            raise 

class YandexMetrikaClient:
    """Клиент для работы с API Яндекс.Метрики"""
    
    def __init__(self):
        self.access_token = os.getenv("YM_TOKEN")
        if not self.access_token:
            raise ValueError("Отсутствует токен YM_TOKEN")
        
        self.field_configs = ConfigManager.get_field_config()
        self.fields = [config.field for config in self.field_configs]
    
    def fetch_data_for_date(self, counter_id: str, date: str) -> List[List[Any]]:
        """Загрузка данных за дату с улучшенной обработкой ошибок"""
        if not DataValidator.validate_counter_id(counter_id):
            raise ValueError(f"Некорректный ID счетчика: {counter_id}")
        
        if not DataValidator.validate_date_format(date):
            raise ValueError(f"Некорректный формат даты: {date}")
        
        client = YandexMetrikaLogsapi(
            access_token=self.access_token,
            default_url_params={"counterId": counter_id},
            wait_report=True
        )
        
        params = {
            "fields": ",".join(self.fields),
            "source": "visits",
            "date1": date,
            "date2": date
        }
        
        for attempt in range(Constants.MAX_RETRIES):
            try:
                logger.info(f"Попытка {attempt + 1}: Загрузка данных для счетчика {counter_id} за {date}")
                
                # Создаем запрос
                result = client.create().post(params=params)
                request_id = result["log_request"]["request_id"]
                
                # Ждем готовности и загружаем данные
                report = client.download(requestId=request_id).get()
                data = report().to_values()
                
                if not data:
                    logger.info(f"Нет данных для счетчика {counter_id} за {date}")
                    return []
                
                logger.info(f"Загружено {len(data)} строк для счетчика {counter_id} за {date}")
                return data
                
            except Exception as e:
                logger.error(f"Ошибка при загрузке данных (попытка {attempt + 1}): {e}")
                if attempt < Constants.MAX_RETRIES - 1:
                    sleep_time = Constants.RETRY_DELAY * (attempt + 1)
                    logger.info(f"Ожидание {sleep_time} секунд перед повтором")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Все попытки исчерпаны для счетчика {counter_id} за {date}")
                    raise
        
        return []

class YandexMetrikaETL:
    """Основной ETL класс с улучшенной архитектурой"""
    
    def __init__(self):
        self.ch_manager = ClickHouseManager()
        self.ym_client = YandexMetrikaClient()
        self.normalizer = DataNormalizer(ConfigManager.get_field_config())
        
        # Инициализация уведомлений (опционально)
        try:
            from utils.telegram_notify import TelegramNotifier
            self.notifier = TelegramNotifier(debug_mode=True)
        except ImportError:
            logger.warning("TelegramNotifier не найден, уведомления отключены")
            self.notifier = None
    
    @contextmanager
    def memory_management(self):
        """Контекстный менеджер для управления памятью"""
        try:
            yield
        finally:
            gc.collect()
    
    def _generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """Генерация списка дат в диапазоне с валидацией"""
        if not DataValidator.validate_date_range(start_date, end_date):
            raise ValueError(f"Некорректный диапазон дат: {start_date} - {end_date}")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        dates = []
        
        current_dt = start_dt
        while current_dt <= end_dt:
            dates.append(current_dt.strftime("%Y-%m-%d"))
            current_dt += timedelta(days=1)
        
        return dates
    
    def get_dates_to_process(self, counter_id: str, start_date: str, end_date: str) -> List[str]:
        """Определение дат для обработки"""
        # Получаем существующие даты
        existing_dates = self.ch_manager.get_existing_dates(counter_id, start_date, end_date)
        
        # Получаем даты с неполной загрузкой
        incomplete_dates = self.ch_manager.get_incomplete_dates(counter_id, start_date, end_date)
        
        # Генерируем все даты в диапазоне
        all_dates = set(self._generate_date_range(start_date, end_date))
        
        # Определяем даты для загрузки
        missing_dates = all_dates - existing_dates
        dates_to_process = list(missing_dates.union(set(incomplete_dates)))
        dates_to_process.sort()
        
        logger.info(f"Всего дат в диапазоне: {len(all_dates)}")
        logger.info(f"Существующих дат: {len(existing_dates)}")
        logger.info(f"Дат с неполной загрузкой: {len(incomplete_dates)}")
        logger.info(f"Дат к обработке: {len(dates_to_process)}")
        
        return dates_to_process
    
    def process_date(self, counter_id: str, date: str, site_name: str) -> Tuple[int, List[str]]:
        """Обработка данных за одну дату"""
        errors = []
        
        try:
            with self.memory_management():
                # Проверяем существующие данные
                existing_dates = self.ch_manager.get_existing_dates(counter_id, date, date)
                
                if date in existing_dates:
                    logger.info(f"Дата {date}: найдены существующие данные, выполняется перезагрузка")
                    self.ch_manager.delete_date_data(counter_id, date)
                
                # Загружаем данные из API
                raw_data = self.ym_client.fetch_data_for_date(counter_id, date)
                
                if not raw_data:
                    logger.info(f"Нет данных для счетчика {counter_id} за {date}")
                    return 0, errors
                
                # Нормализуем данные порциями для экономии памяти
                total_inserted = 0
                
                for i in range(0, len(raw_data), Constants.MEMORY_BATCH_SIZE):
                    batch = raw_data[i:i + Constants.MEMORY_BATCH_SIZE]
                    
                    # Нормализуем батч
                    normalized_batch = [self.normalizer.normalize_row(row) for row in batch]
                    
                    # Вставляем в ClickHouse
                    inserted = self.ch_manager.insert_data(normalized_batch, site_name)
                    total_inserted += inserted
                    
                    # Освобождаем память
                    del batch, normalized_batch
                    
                    if i % (Constants.MEMORY_BATCH_SIZE * 5) == 0:
                        gc.collect()
                
                logger.info(f"Обработано {total_inserted} строк для счетчика {counter_id} за {date}")
                return total_inserted, errors
                
        except Exception as e:
            error_msg = f"Ошибка обработки даты {date}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            return 0, errors
    
    def process_counter(self, counter_id: str, site_name: str, 
                       start_date: str, end_date: str) -> ProcessingResult:
        """Обработка счетчика с детальной отчетностью"""
        start_time = time.time()
        
        logger.info(f"Начало обработки счетчика {counter_id} ({site_name})")
        
        result = ProcessingResult(
            counter_id=counter_id,
            site_name=site_name,
            status=ProcessingStatus.FAILED
        )
        
        try:
            # Создаем таблицу если не существует
            self.ch_manager.create_table()
            
            # Определяем даты для обработки
            dates_to_process = self.get_dates_to_process(counter_id, start_date, end_date)
            
            if not dates_to_process:
                logger.info(f"Все данные для счетчика {counter_id} загружены полностью")
                result.status = ProcessingStatus.SKIPPED
                total_days = (datetime.strptime(end_date, "%Y-%m-%d") - 
                            datetime.strptime(start_date, "%Y-%m-%d")).days + 1
                result.dates_skipped = total_days
                return result
            
            logger.info(f"Требуется обработать {len(dates_to_process)} дат")
            
            # Обрабатываем каждую дату
            for date in dates_to_process:
                try:
                    inserted, date_errors = self.process_date(counter_id, date, site_name)
                    
                    result.total_loaded += inserted
                    result.dates_processed += 1
                    result.errors.extend(date_errors)
                    
                    # Задержка между запросами
                    time.sleep(Constants.REQUEST_DELAY)
                    
                except Exception as e:
                    error_msg = f"Критическая ошибка обработки даты {date}: {e}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    continue
            
            # Определяем статус результата
            if not result.errors:
                result.status = ProcessingStatus.SUCCESS
            elif result.total_loaded > 0:
                result.status = ProcessingStatus.PARTIAL_SUCCESS
            else:
                result.status = ProcessingStatus.FAILED
            
            result.processing_time = time.time() - start_time
            
            logger.info(f"Завершена обработка счетчика {counter_id}. "
                       f"Статус: {result.status.value}, "
                       f"Загружено: {result.total_loaded} строк, "
                       f"Обработано дат: {result.dates_processed}, "
                       f"Ошибок: {len(result.errors)}, "
                       f"Время: {result.processing_time:.2f}с")
            
        except Exception as e:
            error_msg = f"Критическая ошибка при обработке счетчика {counter_id}: {e}"
            logger.error(error_msg)
            result.errors.append(error_msg)
            result.status = ProcessingStatus.FAILED
            result.processing_time = time.time() - start_time
        
        return result
    
    def send_notification(self, message: str, is_error: bool = False) -> None:
        """Отправка уведомления"""
        if self.notifier:
            try:
                self.notifier.send_message(message, is_error=is_error)
            except Exception as e:
                logger.warning(f"Ошибка отправки уведомления: {e}")

# Инициализация ETL
etl = YandexMetrikaETL()

def process_single_counter(counter_id: str, site_name: str, **context) -> Dict[str, Any]:
    """Задача для обработки одного счетчика"""
    try:
        # Получение параметров из Airflow Variables
        days_back = int(Variable.get("ym_days_back", default_var=7))
        start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        end_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"Период загрузки: {start_date} - {end_date}")
        
        # Валидация входных параметров
        if not DataValidator.validate_counter_id(counter_id):
            raise ValueError(f"Некорректный ID счетчика: {counter_id}")
        
        # Обрабатываем счетчик
        result = etl.process_counter(counter_id, site_name, start_date, end_date)
        
        # Формируем сообщение для уведомления
        status_emoji = {
            ProcessingStatus.SUCCESS: "✅",
            ProcessingStatus.PARTIAL_SUCCESS: "⚠️",
            ProcessingStatus.FAILED: "❌",
            ProcessingStatus.SKIPPED: "⏭️"
        }
        
        emoji = status_emoji.get(result.status, "❓")
        message = (f"{emoji} Счетчик {counter_id} ({site_name}):\n"
                  f"Статус: {result.status.value}\n"
                  f"Загружено: {result.total_loaded:,} строк\n"
                  f"Обработано дат: {result.dates_processed}\n"
                  f"Время: {result.processing_time:.2f}с")
        
        if result.errors:
            message += f"\nОшибок: {len(result.errors)}"
        
        etl.send_notification(message, is_error=(result.status == ProcessingStatus.FAILED))
        
        # Возвращаем результат для XCom
        return {
            'counter_id': result.counter_id,
            'site_name': result.site_name,
            'status': result.status.value,
            'total_loaded': result.total_loaded,
            'dates_processed': result.dates_processed,
            'dates_skipped': result.dates_skipped,
            'errors': result.errors,
            'processing_time': result.processing_time,
            'success': result.status in [ProcessingStatus.SUCCESS, ProcessingStatus.SKIPPED]
        }
        
    except Exception as e:
        error_msg = f"⛔️ Критическая ошибка в задаче {counter_id}: {e}"
        logger.error(error_msg)
        etl.send_notification(error_msg, is_error=True)
        raise

def get_data_quality_report(**context) -> None:
    """Отчет о качестве загруженных данных"""
    try:
        days_back = int(Variable.get("ym_days_back", default_var=7))
        start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        end_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        quality_query = f"""
        WITH date_quality AS (
            SELECT 
                counter_id,
                toDate(datetime) as date,
                count() as row_count,
                countDistinct(visit_id) as unique_visits,
                min(visit_id) as min_visit,
                max(visit_id) as max_visit,
                CASE 
                    WHEN max(visit_id) - min(visit_id) = 0 THEN 100
                    ELSE countDistinct(visit_id) / (max(visit_id) - min(visit_id) + 1) * 100
                END as coverage_percent
            FROM dev.ym_visits
            WHERE datetime >= %(start_date)s AND datetime <= %(end_date)s
            GROUP BY counter_id, toDate(datetime)
        )
        SELECT 
            counter_id,
            date,
            row_count,
            coverage_percent
        FROM date_quality
        WHERE coverage_percent < %(threshold)s
        ORDER BY counter_id, date
        """
        
        quality_issues = etl.ch_manager.client.query(
            quality_query,
            parameters={
                'start_date': start_date,
                'end_date': f"{end_date} 23:59:59",
                'threshold': Constants.COVERAGE_THRESHOLD * 100
            }
        ).result_rows
        
        if quality_issues:
            report = "⚠️ Обнаружены проблемы с полнотой данных:\n"
            for counter_id, date, rows, coverage in quality_issues:
                report += f"Счетчик {counter_id}, {date}: {rows} строк, покрытие {coverage:.1f}%\n"
            
            etl.send_notification(report, is_error=True)
        else:
            logger.info("Все данные загружены с хорошим покрытием visit_id")
            
    except Exception as e:
        logger.error(f"Ошибка проверки качества данных: {e}")

def consolidate_results(**context) -> None:
    """Консолидация результатов всех счетчиков"""
    task_ids = [f"process_counter_{counter['id']}" for counter in COUNTERS]
    
    results = []
    total_loaded = 0
    successful_counters = 0
    processing_times = []
    
    for task_id in task_ids:
        try:
            result = context['ti'].xcom_pull(task_ids=task_id)
            if result:
                results.append(result)
                total_loaded += result.get('total_loaded', 0)
                if result.get('success', False):
                    successful_counters += 1
                processing_times.append(result.get('processing_time', 0))
        except Exception as e:
            logger.warning(f"Не удалось получить результат от {task_id}: {e}")
    
    # Формируем итоговый отчет
    total_counters = len(COUNTERS)
    failed_counters = total_counters - successful_counters
    avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
    
    summary = (f"📊 Итоговый отчет загрузки Яндекс.Метрики:\n"
              f"Всего счетчиков: {total_counters}\n"
              f"✅ Успешно: {successful_counters}\n"
              f"⚠️ С ошибками: {failed_counters}\n"
              f"📈 Загружено строк: {total_loaded:,}\n"
              f"⏱️ Среднее время обработки: {avg_processing_time:.2f}с")
    
    # Детализация по каждому счетчику
    details = "\n\nДетализация:"
    for result in results:
        status_emoji = "✅" if result['success'] else "⚠️"
        details += (f"\n{status_emoji} {result['site_name']} ({result['counter_id']}): "
                   f"{result['total_loaded']:,} строк за {result['processing_time']:.1f}с")
        
        if result.get('errors'):
            details += f" - {len(result['errors'])} ошибок"
    
    final_message = summary + details
    etl.send_notification(final_message, is_error=failed_counters > 0)
    
    # Логируем в Airflow
    logger.info(f"Загрузка завершена. Успешно: {successful_counters}/{total_counters}, "
               f"всего строк: {total_loaded}, среднее время: {avg_processing_time:.2f}с")

def cleanup_old_data(**context) -> None:
    """Очистка старых данных с улучшенной безопасностью"""
    try:
        retention_days = int(Variable.get("ym_retention_days", default_var=365))
        
        # Проверяем разумность периода хранения
        if retention_days < 30:
            logger.warning(f"Слишком короткий период хранения: {retention_days} дней. Минимум 30 дней.")
            return
        
        cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        
        # Выполняем только если включена очистка
        if Variable.get("ym_enable_cleanup", default_var="false").lower() == "true":
            # Сначала проверяем количество данных для удаления
            count_query = """
            SELECT count() as rows_to_delete
            FROM dev.ym_visits 
            WHERE datetime < %(cutoff_date)s
            """
            
            rows_to_delete = etl.ch_manager.client.query(
                count_query,
                parameters={'cutoff_date': cutoff_date}
            ).first_row[0]
            
            if rows_to_delete > 0:
                delete_query = """
                DELETE FROM dev.ym_visits 
                WHERE datetime < %(cutoff_date)s
                """
                
                etl.ch_manager.client.command(
                    delete_query,
                    parameters={'cutoff_date': cutoff_date}
                )
                
                logger.info(f"Удалено {rows_to_delete:,} строк старше {cutoff_date}")
                etl.send_notification(f"🗑️ Очистка: удалено {rows_to_delete:,} строк старше {cutoff_date}")
            else:
                logger.info("Нет данных для удаления")
        else:
            logger.info("Очистка отключена в настройках")
            
    except Exception as e:
        error_msg = f"Ошибка при очистке данных: {e}"
        logger.error(error_msg)
        etl.send_notification(error_msg, is_error=True)

# Конфигурация счетчиков
COUNTERS = [
    {"id": "96232424", "site": "mediiia.com"}, 
    {"id": "93017305", "site": "deziiign.com"}, 
    {"id": "93017345", "site": "gallllery.com"}
]

# Настройки DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1)
}

# Создание DAG
with DAG(
    dag_id='yandex_metrika_v2',
    default_args=default_args,
    description='Рефакторенная загрузка данных Яндекс.Метрики v2.0',
    schedule_interval='0 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'yandex_metrika', 'clickhouse', 'v2'],
    doc_md="""
    # Рефакторенная загрузка Яндекс.Метрики v2.0
    
    ## Ключевые улучшения:
    - ✅ Исправлены SQL-инъекции (параметризованные запросы)
    - ✅ Добавлена валидация входных данных
    - ✅ Улучшена обработка ошибок и логирование
    - ✅ Оптимизирована работа с памятью
    - ✅ Добавлены типы данных и документация
    - ✅ Разделение ответственности между классами
    - ✅ Более безопасная очистка данных
    - ✅ Детальная метрика производительности
    
    ## Настраиваемые переменные:
    - `ym_days_back`: количество дней назад для загрузки (по умолчанию 7)
    - `ym_retention_days`: срок хранения данных в днях (по умолчанию 365, минимум 30)
    - `ym_enable_cleanup`: включить автоочистку старых данных (по умолчанию false)
    
    ## Переменные окружения:
    - `YM_TOKEN`: токен доступа к API Яндекс.Метрики
    - `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`
    """
) as dag:

    # Создание задач для каждого счетчика
    counter_tasks = []
    
    for counter in COUNTERS:
        task = PythonOperator(
            task_id=f"process_counter_{counter['id']}",
            python_callable=process_single_counter,
            op_kwargs={
                "counter_id": counter["id"],
                "site_name": counter["site"]
            },
            execution_timeout=timedelta(hours=3),
            pool='yandex_metrika_pool',
            doc_md=f"Загрузка данных для счетчика {counter['id']} ({counter['site']})"
        )
        counter_tasks.append(task)

    # Консолидация результатов
    consolidate_task = PythonOperator(
        task_id='consolidate_results',
        python_callable=consolidate_results,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Консолидация результатов загрузки всех счетчиков"
    )

    # Проверка качества данных
    quality_check_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=get_data_quality_report,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Проверка полноты загрузки по покрытию visit_id"
    )

    # Опциональная очистка старых данных
    cleanup_task = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Безопасная очистка старых данных (если включена в настройках)"
    )

    # Определение зависимостей
    counter_tasks >> consolidate_task >> quality_check_task >> cleanup_task 