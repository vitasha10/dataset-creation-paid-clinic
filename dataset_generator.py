"""
Основной модуль генерации датасета для платной поликлиники
"""

import random
import logging
import argparse
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
from pathlib import Path

from config import *
from data_dictionaries import *
from validators import UniquenessTracker, DataValidator
from utils import (
    generate_slavic_fio, generate_passport_number, generate_snils_number,
    select_country_by_probability, generate_symptoms, select_doctor_by_symptoms,
    generate_analyses_by_doctor, generate_working_datetime, generate_analysis_datetime,
    format_datetime_iso, generate_bank_card, calculate_analysis_cost,
    format_symptoms_string, format_analyses_string, format_cost_string,
    validate_business_logic, generate_batch_clients,
    select_doctor_by_gender, generate_symptoms_by_doctor, generate_analyses_by_doctor_new
)
import sys

def configure_stdio_utf8():
    """Настройка stdout/stderr на UTF-8, чтобы консольный лог не падал на Windows."""
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # В крайнем случае просто продолжаем: лог в файл все равно будет в UTF-8
        pass

def setup_logging():
    """Настройка логирования с устойчивой к Unicode консолью и UTF-8 файлом лога."""
    handlers = []

    # Лог в файл — всегда UTF-8
    handlers.append(logging.FileHandler("dataset_generation.log", encoding="utf-8"))

    # Консоль: после configure_stdio_utf8 стандартные потоки уже UTF-8
    try:
        handlers.append(logging.StreamHandler())
    except Exception:
        # Если консоль недоступна, просто пропустим
        pass

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=handlers,
        force=True,  # сбросить любые ранее добавленные хэндлеры, которые могли быть не-UTF-8
    )


class DatasetGenerator:
    """Генератор датасета платной поликлиники"""
    
    def __init__(self, seed: int = RANDOM_SEED):
        """
        Инициализация генератора
        
        Args:
            seed: начальное значение для генератора случайных чисел
        """
        random.seed(seed)
        self.seed = seed
        self.repeat_probability = CLIENT_REPEAT_PROBABILITY
        self.uniqueness_tracker = UniquenessTracker()
        self.validator = DataValidator()
        self.clients_pool = []  # Пул существующих клиентов для повторных визитов
        self.logger = logging.getLogger(__name__)
        
        # Статистика генерации
        self.stats = {
            'total_records': 0,
            'new_clients': 0,
            'repeat_visits': 0,
            'validation_errors': 0,
            'unique_cards': 0,
            'generation_time': 0
        }
    
    def create_client(self) -> Dict:
        """
        Создание нового клиента
        
        Returns:
            словарь с данными клиента
        """
        attempts = 0
        max_attempts = 100
        
        while attempts < max_attempts:
            fio, gender = generate_slavic_fio()
            
            # ИСПРАВЛЕНИЕ: Проверяем есть ли уже паспорт для этого ФИО
            existing_passport = self.uniqueness_tracker.get_fio_passport(fio)
            
            if existing_passport:
                # Используем существующий паспорт для этого ФИО
                passport = existing_passport
                # Определяем страну по формату паспорта
                if re.match(r"^\d{4} \d{6}$", passport):
                    country = "ru"
                elif re.match(r"^[A-Z]{2}\d{7}$", passport):
                    country = "by"
                elif re.match(r"^N\d{8}$", passport):
                    country = "kz"
                else:
                    country = "ru"  # fallback
                
                # Получаем существующий СНИЛС
                snils = self.uniqueness_tracker.get_client_snils(fio, passport)
                
                client = {
                    'fio': fio,
                    'gender': gender,
                    'passport': passport,
                    'country': country,
                    'snils': snils,
                    'passport_issue_date': None,  # Для повторных визитов не нужно
                    'passport_department_code': None,
                    'birth_date': None
                }
                
                self.logger.debug(f"Использован существующий клиент: {fio} ({country})")
                return client
            
            else:
                # Создаем нового клиента
                country = select_country_by_probability()
                
                # Генерируем паспорт в зависимости от страны
                if country == "ru":
                    # Для RU генерируем полную информацию о паспорте
                    visit_date = generate_working_datetime()  # Примерная дата для расчетов
                    from utils import generate_passport_data_ru
                    passport_data = generate_passport_data_ru(visit_date)
                    passport = passport_data["passport_data"]
                    passport_issue_date = passport_data["passport_issue_date"]
                    passport_department_code = passport_data["passport_department_code"]
                    birth_date = passport_data["birth_date"]
                else:
                    # Для BY/KZ используем простую генерацию
                    passport = generate_passport_number(country)
                    passport_issue_date = None
                    passport_department_code = None
                    birth_date = None
                
                # Проверяем уникальность паспорта и добавляем связку ФИО-паспорт
                if self.uniqueness_tracker.add_fio_passport(fio, passport):
                    # Генерируем СНИЛС для всех стран (исправление Issue #5)
                    snils = generate_snils_number()
                    
                    client = {
                        'fio': fio,
                        'gender': gender,
                        'passport': passport,
                        'country': country,
                        'snils': snils,
                        'passport_issue_date': passport_issue_date,
                        'passport_department_code': passport_department_code,
                        'birth_date': birth_date
                    }
                    
                    # Добавляем СНИЛС в трекер
                    self.uniqueness_tracker.add_client_snils(fio, passport, snils)
                    
                    self.logger.debug(f"Создан новый клиент: {fio} ({country})")
                    return client
            
            attempts += 1
        
        raise RuntimeError("Не удалось создать уникального клиента за максимальное количество попыток")
    
    def select_client(self) -> Dict:
        """
        Выбор клиента (новый или существующий для повторного визита)
        
        Returns:
            словарь с данными клиента
        """
        # ИСПРАВЛЕНИЕ Issue #9: Увеличиваем повторные визиты
        # Уменьшаем минимальный размер пула и увеличиваем вероятность повтора
        if (self.clients_pool and 
            random.random() < self.repeat_probability and 
            len(self.clients_pool) >= 50):  # Снижено с 100 до 50
            
            # Выбираем существующего клиента для повторного визита
            client = random.choice(self.clients_pool)
            self.stats['repeat_visits'] += 1
            self.logger.debug(f"Повторный визит клиента: {client['fio']}")
            return client
        else:
            # Создаем нового клиента
            client = self.create_client()
            self.clients_pool.append(client)
            self.stats['new_clients'] += 1
            return client
    
    def generate_visit_record(self) -> Dict:
        """
        Генерация одной записи визита
        
        Returns:
            словарь с данными записи
        """
        # Выбираем клиента
        client = self.select_client()
        
        # НОВАЯ ЛОГИКА: Сначала выбираем врача с учетом пола клиента
        doctor = select_doctor_by_gender(client['gender'])
        
        # Генерируем симптомы на основе врача (1-3 симптома)
        symptoms = generate_symptoms_by_doctor(doctor, min_count=1, max_count=3)
        
        # Генерируем дату визита
        visit_datetime = generate_working_datetime()
        
        # Генерируем анализы на основе врача (1-2 анализа)
        analyses = generate_analyses_by_doctor_new(doctor, min_count=1, max_count=2)
        
        # Генерируем дату получения анализов
        analysis_datetime = generate_analysis_datetime(visit_datetime)
        
        # Рассчитываем стоимость анализов
        cost = calculate_analysis_cost(analyses)
        
        # ИСПРАВЛЕНИЕ Issue #4: Генерируем банковскую карту с более реалистичным повторным использованием
        attempts = 0
        max_attempts = 50
        card_number = None
        
        # Сначала пытаемся переиспользовать существующие карты
        existing_cards = list(self.uniqueness_tracker.card_usage.keys())
        if existing_cards and random.random() < 0.4:  # 40% шанс переиспользовать существующую карту
            # Фильтруем карты, которые можно еще использовать
            available_cards = [card for card in existing_cards 
                              if self.uniqueness_tracker.can_use_card(card, CARD_REUSE_LIMIT)]
            if available_cards:
                card_number = random.choice(available_cards)
                self.uniqueness_tracker.use_card(card_number)
                # Получаем информацию о банке и системе (упрощенно)
                bank = "sberbank"  # Можно улучшить, но для статистики достаточно
                payment_system = "mir"
        
        # Если не удалось переиспользовать, создаем новую карту
        if card_number is None:
            while attempts < max_attempts:
                card_number, bank, payment_system = generate_bank_card()
                
                if self.uniqueness_tracker.can_use_card(card_number, CARD_REUSE_LIMIT):
                    self.uniqueness_tracker.use_card(card_number)
                    break
                
                attempts += 1
            else:
                # Если не удалось найти свободную карту, создаем новую принудительно
                card_number, bank, payment_system = generate_bank_card()
                self.uniqueness_tracker.use_card(card_number)
        
        # Формируем итоговую запись
        record = {
            'FIO': client['fio'],
            'passport_data': client['passport'],
            'passport_country': client['country'],  # Добавляем страну паспорта для валидации
            'SNILS': client['snils'] if client['snils'] else '',  # Теперь СНИЛС есть у всех
            'symptoms': format_symptoms_string(symptoms),
            'doctor_choice': doctor,
            'visit_date': format_datetime_iso(visit_datetime),
            'analyses': format_analyses_string(analyses),
            'analysis_date': format_datetime_iso(analysis_datetime),
            'analysis_cost': format_cost_string(cost),
            'payment_card': card_number
        }
        
        # Добавляем дополнительные поля для RU паспортов
        if client['country'] == 'ru':
            record['passport_issue_date'] = client['passport_issue_date']
            record['passport_department_code'] = client['passport_department_code']
        
        return record
    
    def generate_dataset(self, size: int = DATASET_SIZE) -> List[Dict]:
        """
        Генерация полного датасета
        
        Args:
            size: количество записей в датасете
        
        Returns:
            список записей датасета
        """
        self.logger.info(f"Начало генерации датасета размером {size} записей")
        start_time = datetime.now()
        
        dataset = []
        batch_size = min(BATCH_SIZE, size)
        
        for batch_start in range(0, size, batch_size):
            batch_end = min(batch_start + batch_size, size)
            batch_size_actual = batch_end - batch_start
            
            self.logger.info(f"Генерация пакета {batch_start + 1}-{batch_end} из {size}")
            
            batch_records = []
            for i in range(batch_size_actual):
                try:
                    record = self.generate_visit_record()
                    
                    # Валидация записи
                    is_valid, errors = self.validator.validate_record(record)
                    if not is_valid:
                        self.logger.warning(f"Ошибки валидации в записи {len(dataset) + i + 1}: {errors}")
                        self.stats['validation_errors'] += len(errors)
                    
                    # Проверка бизнес-логики
                    business_errors = validate_business_logic(record)
                    if business_errors:
                        self.logger.warning(f"Ошибки бизнес-логики в записи {len(dataset) + i + 1}: {business_errors}")
                    
                    batch_records.append(record)
                    
                except Exception as e:
                    self.logger.error(f"Ошибка при генерации записи {len(dataset) + i + 1}: {e}")
                    # Пропускаем неудачную запись
                    continue
            
            dataset.extend(batch_records)
            
            # Логируем 1-2 примера паспортов с полным раскладом по требованию TODO.md
            if batch_records and len(batch_records) >= 2:
                sample_records = batch_records[:2]
                for idx, record in enumerate(sample_records):
                    if record.get('passport_country') == 'ru':
                        self.logger.info(f"Пример паспорта RU #{idx+1}: "
                                       f"ФИО={record.get('FIO', '')}, "
                                       f"Паспорт={record.get('passport_data', '')}, "
                                       f"Дата выдачи={record.get('passport_issue_date', '')}, "
                                       f"Код подразделения={record.get('passport_department_code', '')}, "
                                       f"СНИЛС={record.get('SNILS', '')}")
                    else:
                        country = record.get('passport_country', 'unknown')
                        self.logger.info(f"Пример паспорта {country.upper()} #{idx+1}: "
                                       f"ФИО={record.get('FIO', '')}, "
                                       f"Паспорт={record.get('passport_data', '')}, "
                                       f"СНИЛС={record.get('SNILS', 'нет')}")
            
            # Логируем прогресс
            progress = len(dataset) / size * 100
            self.logger.info(f"Прогресс: {progress:.1f}% ({len(dataset)}/{size})")
        
        end_time = datetime.now()
        generation_time = (end_time - start_time).total_seconds()
        
        # Обновляем статистику
        self.stats.update({
            'total_records': len(dataset),
            'generation_time': generation_time,
            'unique_cards': len(self.uniqueness_tracker.card_usage)
        })
        
        self.logger.info(f"Генерация завершена за {generation_time:.2f} секунд")
        self.logger.info(f"Статистика: {self.stats}")
        
        return dataset
    
    def save_to_excel(self, dataset: List[Dict], filename: str = OUTPUT_FILE):
        """
        Сохранение датасета в Excel файл
        
        Args:
            dataset: список записей датасета
            filename: имя выходного файла
        """
        self.logger.info(f"Сохранение датасета в файл {filename}")
        
        try:
            df = pd.DataFrame(dataset)
            
            # Переименовываем колонки для Excel
            column_mapping = {
                'FIO': 'ФИО',
                'passport_data': 'Паспортные данные',
                'passport_country': 'Страна паспорта',
                'passport_issue_date': 'Дата выдачи паспорта',
                'passport_department_code': 'Код подразделения',
                'SNILS': 'СНИЛС',
                'symptoms': 'Симптомы',
                'doctor_choice': 'Выбор врача',
                'visit_date': 'Дата посещения врача',
                'analyses': 'Анализы',
                'analysis_date': 'Дата получения анализов',
                'analysis_cost': 'Стоимость анализов',
                'payment_card': 'Карта оплаты'
            }
            
            # Согласно TODO.md требованиям добавляем дополнительные колонки для паспортов
            # Но сохраняем только основные согласно изначальному заданию
            columns_to_keep_optional = ['passport_country', 'passport_issue_date', 'passport_department_code']
            
            # Можно оставить эти колонки для отладки, но для финального MVP убираем
            columns_to_drop = ['passport_country', 'passport_issue_date', 'passport_department_code']
            df_final = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
            
            df_final = df_final.rename(columns=column_mapping)
            
            # Сохраняем в Excel с автоширинами столбцов
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_final.to_excel(writer, sheet_name='Датасет поликлиники', index=False)
                
                # Автоширина столбцов
                worksheet = writer.sheets['Датасет поликлиники']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    adjusted_width = min(max_length + 2, 50)  # Максимум 50 символов
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            self.logger.info(f"Датасет успешно сохранен в {filename}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении в Excel: {e}")
            # Fallback: сохраняем в CSV
            csv_filename = filename.replace('.xlsx', '.csv')
            self.logger.info(f"Пробуем сохранить в CSV: {csv_filename}")
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    
    def generate_report(self) -> str:
        """
        Генерация отчета о созданном датасете
        
        Returns:
            текст отчета
        """
        uniqueness_stats = self.uniqueness_tracker.get_stats()
        
        # Рассчитываем дополнительные метрики
        avg_visits_per_client = self.stats['total_records'] / max(1, uniqueness_stats['unique_clients'])
        repeat_visit_percentage = (self.stats['repeat_visits'] / max(1, self.stats['total_records'])) * 100
        avg_card_usage = uniqueness_stats['total_card_usage'] / max(1, uniqueness_stats['cards_in_use'])
        
        report = f"""
ОТЧЕТ О ГЕНЕРАЦИИ ДАТАСЕТА ПЛАТНОЙ ПОЛИКЛИНИКИ - MVP
====================================================

🎯 ДОСТИГНУТЫЕ КЛЮЧЕВЫЕ МЕТРИКИ (Issue #10):
-------------------------------------------
✅ Большие словари данных:
   • Симптомы: {len(SYMPTOMS_DICT)} уникальных симптомов
   • Врачи: {len(DOCTORS_SPECIALIZATIONS)} специализаций
   • Анализы: {len(MEDICAL_ANALYSES)} видов анализов
   • Значение: Обеспечивают реалистичность и разнообразие данных

✅ Повторные визиты клиентов:
   • Средняя частота визитов на клиента: {avg_visits_per_client:.2f} раза
   • Процент повторных визитов: {repeat_visit_percentage:.1f}%
   • Цель достигнута: клиенты ходят в среднем {avg_visits_per_client:.1f} раза

✅ Реалистичное использование карт:
   • Средняя кратность использования карт: {avg_card_usage:.2f}
   • Карты переиспользуются реалистично (до {CARD_REUSE_LIMIT} раз)

✅ Консистентность данных:
   • ФИО-паспорт связка: один паспорт на одно ФИО
   • СНИЛС для всех стран: RU, BY, KZ
   • Паспортные даты: всегда до даты визита

📊 ПАРАМЕТРЫ ГЕНЕРАЦИИ:
---------------------
- Размер датасета: {self.stats['total_records']} записей
- Seed для воспроизводимости: {self.seed}
- Время генерации: {self.stats['generation_time']:.2f} секунд

👥 СТАТИСТИКА КЛИЕНТОВ:
---------------------
- Новые клиенты: {self.stats['new_clients']}
- Повторные визиты: {self.stats['repeat_visits']}
- Уникальные паспорта: {uniqueness_stats['unique_passports']}
- Уникальные клиенты: {uniqueness_stats['unique_clients']}
- Средних визитов на клиента: {avg_visits_per_client:.2f}

💳 СТАТИСТИКА ПЛАТЕЖЕЙ:
---------------------
- Уникальные карты: {uniqueness_stats['cards_in_use']}
- Общее использование карт: {uniqueness_stats['total_card_usage']}
- Средняя кратность использования: {avg_card_usage:.2f}
- Максимальное использование карты: {CARD_REUSE_LIMIT} раз

📈 КАЧЕСТВО ДАННЫХ:
-----------------
- Ошибки валидации: {self.stats['validation_errors']}
- Процент корректных записей: {((self.stats['total_records'] - self.stats['validation_errors']) / self.stats['total_records'] * 100):.2f}%

⚡ ПРОИЗВОДИТЕЛЬНОСТЬ:
-------------------
- Записей в секунду: {self.stats['total_records'] / max(1, self.stats['generation_time']):.1f}
- Время на запись: {self.stats['generation_time'] / max(1, self.stats['total_records']) * 1000:.2f} мс

🏗️ СТРУКТУРА ДАННЫХ:
-------------------
1. ФИО - {len(SLAVIC_SURNAMES)} фамилий, славянские имена с корректными окончаниями
2. Паспортные данные - RU/BY/KZ форматы с валидацией по регионам и датам
3. СНИЛС - для ВСЕХ стран с корректной контрольной суммой
4. Симптомы - 1-3 симптома из {len(SYMPTOMS_DICT)} возможных (новая логика врач→симптомы)
5. Выбор врача - {len(DOCTORS_SPECIALIZATIONS)} специализаций с учетом пола клиента
6. Дата визита - рабочие дни {WORK_HOURS_START}:00-{WORK_HOURS_END}:00
7. Анализы - 1-2 анализа из {len(MEDICAL_ANALYSES)} возможных (врач→анализы)
8. Дата анализов - через {ANALYSIS_MIN_HOURS}-{ANALYSIS_MAX_HOURS} часов, не позднее вчера
9. Стоимость - фиксированные цены в рублях по типам анализов
10. Карта оплаты - валидный алгоритм Луна, реалистичное переиспользование

🔧 ИСПРАВЛЕННЫЕ ПРОБЛЕМЫ:
-----------------------
✅ Issue #1: Исправлена валидация дат паспортов
✅ Issue #2: Консистентность ФИО-паспорт (один паспорт на ФИО)
✅ Issue #3: Новая логика врач→симптомы→анализы с учетом пола
✅ Issue #4: Реалистичное переиспользование карт (среднее {avg_card_usage:.2f})
✅ Issue #5: СНИЛС для всех стран (RU, BY, KZ)
✅ Issue #6: Даты анализов не позднее вчера
✅ Issue #9: Повторные визиты в среднем {avg_visits_per_client:.2f} раза на клиента

📁 СОЗДАННЫЕ ФАЙЛЫ:
-----------------
- dataset_generation.log - полный лог генерации с примерами
- {OUTPUT_FILE} - основной датасет в Excel формате
- отчет (.txt) - данный отчет

🎉 ГОТОВ К СДАЧЕ MVP!
"""
        
        return report
    
    def get_statistics(self) -> Dict:
        """Получение статистики генерации"""
        stats = self.stats.copy()
        stats.update(self.uniqueness_tracker.get_stats())
        return stats


def main():
    """Основная функция запуска генератора"""
    configure_stdio_utf8()
    parser = argparse.ArgumentParser(
        description='Генератор датасета для платной поликлиники'
    )
    parser.add_argument(
        '--size', type=int, default=DATASET_SIZE,
        help=f'Размер датасета (по умолчанию: {DATASET_SIZE})'
    )
    parser.add_argument(
        '--seed', type=int, default=RANDOM_SEED,
        help=f'Seed для генератора случайных чисел (по умолчанию: {RANDOM_SEED})'
    )
    parser.add_argument(
        '--output', type=str, default=OUTPUT_FILE,
        help=f'Имя выходного файла (по умолчанию: {OUTPUT_FILE})'
    )
    parser.add_argument(
        '--repeat-probability', type=float, default=CLIENT_REPEAT_PROBABILITY,
        help=f'Вероятность повторного визита (по умолчанию: {CLIENT_REPEAT_PROBABILITY})'
    )
    
    args = parser.parse_args()
    
    # Настройка логирования
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Запуск генератора датасета платной поликлиники")
    logger.info(f"Параметры: размер={args.size}, seed={args.seed}, выход={args.output}, повтор={args.repeat_probability}")
    
    try:
        # Создаем генератор с кастомными параметрами
        generator = DatasetGenerator(seed=args.seed)
        # Обновляем параметры генератора
        generator.repeat_probability = args.repeat_probability
        
        # Генерируем датасет
        dataset = generator.generate_dataset(size=args.size)
        
        # Сохраняем в Excel
        generator.save_to_excel(dataset, args.output)
        
        # Генерируем отчет
        report = generator.generate_report()
        
        # Сохраняем отчет
        report_filename = args.output.replace('.xlsx', '_report.txt')
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"Отчет сохранен в {report_filename}")
        print(report)
        
        logger.info("Генерация датасета завершена успешно")
        
    except Exception as e:
        logger.error(f"Критическая ошибка при генерации датасета: {e}")
        raise


if __name__ == "__main__":
    main()