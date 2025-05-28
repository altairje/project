"""
Тесты для ETL процесса Яндекс.Метрики
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к модулю
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from yandex_metrica_refactored import (
    DataValidator, DataNormalizer, ConfigManager, 
    FieldConfig, ProcessingStatus, ProcessingResult
)

class TestDataValidator(unittest.TestCase):
    """Тесты валидатора данных"""
    
    def test_validate_counter_id_valid(self):
        """Тест валидации корректного ID счетчика"""
        self.assertTrue(DataValidator.validate_counter_id("12345"))
        self.assertTrue(DataValidator.validate_counter_id("96232424"))
    
    def test_validate_counter_id_invalid(self):
        """Тест валидации некорректного ID счетчика"""
        self.assertFalse(DataValidator.validate_counter_id("abc123"))
        self.assertFalse(DataValidator.validate_counter_id(""))
        self.assertFalse(DataValidator.validate_counter_id("1" * 25))  # Слишком длинный
    
    def test_validate_date_format_valid(self):
        """Тест валидации корректного формата даты"""
        self.assertTrue(DataValidator.validate_date_format("2024-01-15"))
        self.assertTrue(DataValidator.validate_date_format("2023-12-31"))
    
    def test_validate_date_format_invalid(self):
        """Тест валидации некорректного формата даты"""
        self.assertFalse(DataValidator.validate_date_format("2024/01/15"))
        self.assertFalse(DataValidator.validate_date_format("15-01-2024"))
        self.assertFalse(DataValidator.validate_date_format("invalid"))
        self.assertFalse(DataValidator.validate_date_format(""))
    
    def test_validate_date_range_valid(self):
        """Тест валидации корректного диапазона дат"""
        self.assertTrue(DataValidator.validate_date_range("2024-01-01", "2024-01-31"))
        self.assertTrue(DataValidator.validate_date_range("2024-01-01", "2024-01-01"))
    
    def test_validate_date_range_invalid(self):
        """Тест валидации некорректного диапазона дат"""
        # Конечная дата раньше начальной
        self.assertFalse(DataValidator.validate_date_range("2024-01-31", "2024-01-01"))
        # Слишком большой диапазон
        self.assertFalse(DataValidator.validate_date_range("2023-01-01", "2024-12-31"))

class TestDataNormalizer(unittest.TestCase):
    """Тесты нормализатора данных"""
    
    def setUp(self):
        """Настройка тестов"""
        self.field_configs = [
            FieldConfig("ym:s:visitID", "visit_id", "UInt64", "ID визита"),
            FieldConfig("ym:s:isNewUser", "is_new_user", "Bool", "Новый пользователь"),
            FieldConfig("ym:s:dateTime", "datetime", "DateTime('Europe/Moscow')", "Дата и время"),
            FieldConfig("ym:s:watchIDs", "watch_ids", "Array(UInt64)", "Массив ID"),
            FieldConfig("ym:s:startURL", "start_url", "String", "URL"),
            FieldConfig("ym:s:counterUserIDHash", "user_hash", "Nullable(UInt64)", "Хеш пользователя", True)
        ]
        self.normalizer = DataNormalizer(self.field_configs)
    
    def test_normalize_row_valid_data(self):
        """Тест нормализации корректных данных"""
        row = [
            "12345",  # visit_id
            "true",   # is_new_user
            "2024-01-15 10:30:00",  # datetime
            "[1, 2, 3]",  # watch_ids
            "https://example.com",  # start_url
            "67890"   # user_hash
        ]
        
        normalized = self.normalizer.normalize_row(row)
        
        self.assertEqual(len(normalized), len(self.field_configs))
        self.assertEqual(normalized[0], 12345)  # UInt64
        self.assertTrue(normalized[1])  # Bool
        self.assertIsInstance(normalized[2], datetime)  # DateTime
        self.assertEqual(normalized[3], [1, 2, 3])  # Array
        self.assertEqual(normalized[4], "https://example.com")  # String
        self.assertEqual(normalized[5], 67890)  # Nullable UInt64
    
    def test_normalize_row_with_nulls(self):
        """Тест нормализации данных с NULL значениями"""
        row = [None, "", None, "", "", None]
        
        normalized = self.normalizer.normalize_row(row)
        
        self.assertEqual(normalized[0], 0)  # UInt64 default
        self.assertFalse(normalized[1])  # Bool default
        self.assertEqual(normalized[2], datetime(1970, 1, 1))  # DateTime default
        self.assertEqual(normalized[3], [])  # Array default
        self.assertEqual(normalized[4], "")  # String default
        self.assertIsNone(normalized[5])  # Nullable default
    
    def test_normalize_row_wrong_length(self):
        """Тест нормализации строки неправильной длины"""
        # Слишком короткая строка
        short_row = ["12345", "true"]
        normalized = self.normalizer.normalize_row(short_row)
        self.assertEqual(len(normalized), len(self.field_configs))
        
        # Слишком длинная строка
        long_row = ["12345", "true", "2024-01-15", "[1,2]", "url", "hash", "extra1", "extra2"]
        normalized = self.normalizer.normalize_row(long_row)
        self.assertEqual(len(normalized), len(self.field_configs))

class TestConfigManager(unittest.TestCase):
    """Тесты менеджера конфигурации"""
    
    def test_get_field_config(self):
        """Тест получения конфигурации полей"""
        config = ConfigManager.get_field_config()
        
        self.assertIsInstance(config, list)
        self.assertGreater(len(config), 0)
        
        # Проверяем первое поле
        first_field = config[0]
        self.assertIsInstance(first_field, FieldConfig)
        self.assertEqual(first_field.field, "ym:s:visitID")
        self.assertEqual(first_field.column, "visit_id")
        self.assertEqual(first_field.type, "UInt64")

class TestProcessingResult(unittest.TestCase):
    """Тесты структуры результата обработки"""
    
    def test_processing_result_creation(self):
        """Тест создания результата обработки"""
        result = ProcessingResult(
            counter_id="12345",
            site_name="test.com",
            status=ProcessingStatus.SUCCESS,
            total_loaded=1000,
            dates_processed=5
        )
        
        self.assertEqual(result.counter_id, "12345")
        self.assertEqual(result.site_name, "test.com")
        self.assertEqual(result.status, ProcessingStatus.SUCCESS)
        self.assertEqual(result.total_loaded, 1000)
        self.assertEqual(result.dates_processed, 5)
        self.assertEqual(result.dates_skipped, 0)  # default
        self.assertEqual(result.errors, [])  # default
        self.assertEqual(result.processing_time, 0.0)  # default

class TestIntegration(unittest.TestCase):
    """Интеграционные тесты"""
    
    @patch('yandex_metrica_refactored.clickhouse_connect')
    @patch('yandex_metrica_refactored.os.getenv')
    def test_clickhouse_manager_initialization(self, mock_getenv, mock_clickhouse):
        """Тест инициализации менеджера ClickHouse"""
        # Настройка моков
        mock_getenv.side_effect = lambda key, default=None: {
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_PORT': '9000',
            'CLICKHOUSE_USER': 'default',
            'CLICKHOUSE_PASSWORD': 'password',
            'CLICKHOUSE_DB': 'test_db'
        }.get(key, default)
        
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        # Импортируем и тестируем
        from yandex_metrica_refactored import ClickHouseManager
        
        manager = ClickHouseManager()
        
        # Проверяем вызовы
        mock_clickhouse.get_client.assert_called_once_with(
            host='localhost',
            port=9000,
            username='default',
            password='password',
            database='test_db',
            connect_timeout=30,
            send_receive_timeout=300
        )
        
        self.assertEqual(manager.client, mock_client)
    
    @patch('yandex_metrica_refactored.os.getenv')
    def test_yandex_metrika_client_initialization(self, mock_getenv):
        """Тест инициализации клиента Яндекс.Метрики"""
        mock_getenv.return_value = "test_token"
        
        from yandex_metrica_refactored import YandexMetrikaClient
        
        client = YandexMetrikaClient()
        
        self.assertEqual(client.access_token, "test_token")
        self.assertIsInstance(client.fields, list)
        self.assertGreater(len(client.fields), 0)
    
    @patch('yandex_metrica_refactored.os.getenv')
    def test_yandex_metrika_client_no_token(self, mock_getenv):
        """Тест инициализации клиента без токена"""
        mock_getenv.return_value = None
        
        from yandex_metrica_refactored import YandexMetrikaClient
        
        with self.assertRaises(ValueError) as context:
            YandexMetrikaClient()
        
        self.assertIn("Отсутствует токен YM_TOKEN", str(context.exception))

def run_performance_test():
    """Простой тест производительности нормализации"""
    print("Запуск теста производительности...")
    
    field_configs = ConfigManager.get_field_config()
    normalizer = DataNormalizer(field_configs)
    
    # Создаем тестовые данные
    test_row = [
        "12345", "true", "2024-01-15 10:30:00", "[1,2,3]", 
        "https://example.com", "67890", "wifi", "[100,200]",
        "[101,201]", "['2024-01-15 10:30:00']", "https://referer.com",
        "direct", "ru", "RU", "180", "desktop", "Apple", "iPhone",
        "iOS", "iOS 15", "Safari", "WebKit", "true", "true",
        "16:9", "24", "1", "portrait", "375", "667", "375", "667",
        "375", "667", "param1"
    ]
    
    import time
    start_time = time.time()
    
    # Нормализуем 10000 строк
    for i in range(10000):
        normalized = normalizer.normalize_row(test_row)
    
    end_time = time.time()
    
    print(f"Нормализовано 10000 строк за {end_time - start_time:.2f} секунд")
    print(f"Скорость: {10000 / (end_time - start_time):.0f} строк/сек")

if __name__ == '__main__':
    # Запуск unit тестов
    print("Запуск unit тестов...")
    unittest.main(argv=[''], exit=False, verbosity=2)
    
    # Запуск теста производительности
    print("\n" + "="*50)
    run_performance_test() 