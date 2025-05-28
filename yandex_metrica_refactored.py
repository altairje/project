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