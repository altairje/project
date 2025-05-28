"""
Конфигурационный файл для ETL процесса Яндекс.Метрики
"""

import os
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Конфигурация базы данных ClickHouse"""
    host: str
    port: int
    username: str
    password: str
    database: str
    table_name: str = "ym_visits"
    schema: str = "dev"
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Создание конфигурации из переменных окружения"""
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DB", "default"),
            table_name=os.getenv("CLICKHOUSE_TABLE", "ym_visits"),
            schema=os.getenv("CLICKHOUSE_SCHEMA", "dev")
        )

@dataclass
class YandexMetrikaConfig:
    """Конфигурация API Яндекс.Метрики"""
    access_token: str
    max_retries: int = 3
    retry_delay: int = 30
    request_delay: int = 1
    batch_size: int = 50000
    memory_batch_size: int = 10000
    
    @classmethod
    def from_env(cls) -> 'YandexMetrikaConfig':
        """Создание конфигурации из переменных окружения"""
        token = os.getenv("YM_TOKEN")
        if not token:
            raise ValueError("Отсутствует обязательная переменная YM_TOKEN")
        
        return cls(
            access_token=token,
            max_retries=int(os.getenv("YM_MAX_RETRIES", "3")),
            retry_delay=int(os.getenv("YM_RETRY_DELAY", "30")),
            request_delay=int(os.getenv("YM_REQUEST_DELAY", "1")),
            batch_size=int(os.getenv("YM_BATCH_SIZE", "50000")),
            memory_batch_size=int(os.getenv("YM_MEMORY_BATCH_SIZE", "10000"))
        )

@dataclass
class ProcessingConfig:
    """Конфигурация процесса обработки"""
    coverage_threshold: float = 0.95
    min_rows_per_day: int = 10
    max_date_range_days: int = 365
    min_retention_days: int = 30
    
    @classmethod
    def from_env(cls) -> 'ProcessingConfig':
        """Создание конфигурации из переменных окружения"""
        return cls(
            coverage_threshold=float(os.getenv("YM_COVERAGE_THRESHOLD", "0.95")),
            min_rows_per_day=int(os.getenv("YM_MIN_ROWS_PER_DAY", "10")),
            max_date_range_days=int(os.getenv("YM_MAX_DATE_RANGE_DAYS", "365")),
            min_retention_days=int(os.getenv("YM_MIN_RETENTION_DAYS", "30"))
        )

# Конфигурация счетчиков
COUNTERS_CONFIG = [
    {"id": "96232424", "site": "mediiia.com", "enabled": True}, 
    {"id": "93017305", "site": "deziiign.com", "enabled": True}, 
    {"id": "93017345", "site": "gallllery.com", "enabled": True}
]

def get_enabled_counters() -> List[Dict[str, str]]:
    """Получение списка активных счетчиков"""
    return [counter for counter in COUNTERS_CONFIG if counter.get("enabled", True)]

# Настройки логирования
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        },
        'simple': {
            'format': '%(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'yandex_metrika_etl.log',
            'mode': 'a'
        }
    },
    'loggers': {
        'yandex_metrika_etl': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
            'propagate': False
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    }
} 