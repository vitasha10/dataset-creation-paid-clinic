"""
Модуль методов обезличивания данных (переработанный согласно требованиям)
"""
import pandas as pd
import numpy as np
import random
import re
from typing import List, Dict
from datetime import datetime

# Импортируем конфигурацию из основного проекта
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import BANK_BINS, RU_REGIONS, PAYMENT_SYSTEMS_DISTRIBUTION
from data_dictionaries import SLAVIC_NAMES_MALE, SLAVIC_NAMES_FEMALE, DOCTORS_SPECIALIZATIONS


# ФЛАГ для переключения между отображением конкретного банка или платежной системы
# True - показывать конкретный банк (Сбербанк, ВТБ и т.д.)
# False - показывать только платежную систему (МИР, Visa, Mastercard)
USE_SPECIFIC_BANK_NAME = True


class DepersonalizationMethods:
    """Класс с методами обезличивания данных - предопределенные методы для каждого столбца"""
    
    def __init__(self, seed: int = 42):
        """
        Инициализация методов обезличивания
        
        Args:
            seed: начальное значение для генератора случайных чисел
        """
        random.seed(seed)
        np.random.seed(seed)
        
        # Создаем обратные маппинги для банков
        self.bin_to_bank = {}
        self.bin_to_payment_system = {}
        for bank, systems in BANK_BINS.items():
            for system, bins in systems.items():
                for bin_code in bins:
                    self.bin_to_bank[bin_code] = bank
                    self.bin_to_payment_system[bin_code] = system
        
        # Создаем маппинг серий паспортов к регионам
        self.series_to_region = {}
        for region_code, region_info in RU_REGIONS.items():
            # Первые 2 цифры серии = код региона
            self.series_to_region[f"{region_code:02d}"] = region_info["name"]
        
        # Создаем маппинг имен к полу
        self.male_names = set(SLAVIC_NAMES_MALE)
        self.female_names = set(SLAVIC_NAMES_FEMALE)
    
    def anonymize_fio(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ФИО → Пол (мужской/женский)
        Определяем пол по имени из списков SLAVIC_NAMES_MALE и SLAVIC_NAMES_FEMALE
        """
        df_copy = df.copy()
        
        def fio_to_gender(fio_str):
            if pd.isna(fio_str):
                return "Не указан"
            
            # Разбиваем ФИО на части
            parts = str(fio_str).strip().split()
            if len(parts) < 2:
                return "Не определен"
            
            # Имя - второй элемент (Фамилия Имя Отчество)
            name = parts[1]
            
            # Определяем пол по имени
            if name in self.male_names:
                return "Мужской"
            elif name in self.female_names:
                return "Женский"
            else:
                return "Не определен"
        
        df_copy['ФИО'] = df_copy['ФИО'].apply(fio_to_gender)
        return df_copy
    
    def anonymize_passport(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Паспорт → Регион (локальное обобщение)
        Извлекаем регион из серии паспорта
        """
        df_copy = df.copy()
        
        def passport_to_region(passport_str):
            if pd.isna(passport_str):
                return "Не указан"
            
            # Извлекаем первые 2 цифры серии (код региона)
            match = re.match(r'(\d{2})\d{2}\s+\d{6}', str(passport_str))
            if match:
                region_code = match.group(1)
                region_name = self.series_to_region.get(region_code, "Другой регион")
                return region_name
            
            # Для белорусских и казахских паспортов
            if re.match(r'[A-Z]{2}\d{7}', str(passport_str)):
                return "Беларусь"
            elif re.match(r'N\d{8}', str(passport_str)):
                return "Казахстан"
            
            return "Другой регион"
        
        df_copy['Паспортные данные'] = df_copy['Паспортные данные'].apply(passport_to_region)
        return df_copy
    
    def anonymize_snils(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        СНИЛС → Возрастная группа
        Извлекаем возраст на основе даты рождения в СНИЛС или используем случайное распределение
        """
        df_copy = df.copy()
        
        def snils_to_age_group(snils_str):
            if pd.isna(snils_str) or snils_str == '':
                return "Не указана"
            
            # Для упрощения используем случайное распределение по возрастным группам
            # В реальности СНИЛС не содержит прямой информации о возрасте
            # Поэтому используем равномерное распределение
            age_groups = [
                "18-25 лет",
                "26-35 лет", 
                "36-45 лет",
                "46-55 лет",
                "56-65 лет",
                "66+ лет"
            ]
            
            # Используем хеш от СНИЛС для стабильного распределения
            hash_val = hash(str(snils_str)) % len(age_groups)
            return age_groups[hash_val]
        
        df_copy['СНИЛС'] = df_copy['СНИЛС'].apply(snils_to_age_group)
        return df_copy
    
    def anonymize_symptoms(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Симптомы → Обобщение (локальное обобщение)
        Удаляем конкретные детали, оставляем только общие категории
        """
        df_copy = df.copy()
        
        def generalize_symptoms(symptoms_str):
            if pd.isna(symptoms_str) or symptoms_str == '':
                return "Не указаны"
            
            # Подсчитываем количество симптомов
            symptoms = str(symptoms_str).split(',')
            count = len(symptoms)
            
            # Обобщаем по количеству
            if count == 1:
                return "Один симптом"
            elif count <= 3:
                return "Несколько симптомов (2-3)"
            elif count <= 5:
                return "Несколько симптомов (4-5)"
            else:
                return "Множественные симптомы (6+)"
        
        df_copy['Симптомы'] = df_copy['Симптомы'].apply(generalize_symptoms)
        return df_copy
    
    def anonymize_doctor(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Выбор врача → Категория врача (локальное обобщение)
        Используем полный список из DOCTORS_SPECIALIZATIONS для минимизации "Другой специалист"
        """
        df_copy = df.copy()
        
        # Расширенный маппинг врачей к категориям на основе DOCTORS_SPECIALIZATIONS
        doctor_categories = {
            # Общая медицина
            'терапевт': 'Общая медицина',
            'педиатр': 'Общая медицина',
            'геронтолог': 'Общая медицина',
            
            # Кардио и сосуды
            'кардиолог': 'Кардиология и сосуды',
            'кардиохирург': 'Кардиология и сосуды',
            'сосудистый хирург': 'Кардиология и сосуды',
            'флеболог': 'Кардиология и сосуды',
            
            # Нервная система
            'невролог': 'Неврология',
            'нейрохирург': 'Неврология',
            'психиатр': 'Психическое здоровье',
            'психотерапевт': 'Психическое здоровье',
            'нарколог': 'Психическое здоровье',
            
            # Внутренние органы
            'гастроэнтеролог': 'Внутренние органы',
            'эндокринолог': 'Внутренние органы',
            'пульмонолог': 'Внутренние органы',
            'нефролог': 'Внутренние органы',
            'ревматолог': 'Внутренние органы',
            'гематолог': 'Внутренние органы',
            
            # Хирургия
            'хирург': 'Хирургия',
            'ортопед': 'Хирургия',
            'травматолог': 'Хирургия',
            'челюстно-лицевой хирург': 'Хирургия',
            'пластический хирург': 'Хирургия',
            'торакальный хирург': 'Хирургия',
            'онколог-хирург': 'Хирургия',
            'детский хирург': 'Хирургия',
            'проктолог': 'Хирургия',
            
            # Органы чувств
            'офтальмолог': 'Органы чувств',
            'лор': 'Органы чувств',
            
            # Кожа
            'дерматолог': 'Дерматология',
            'косметолог': 'Дерматология',
            'трихолог': 'Дерматология',
            'венеролог': 'Дерматология',
            
            # Репродуктивное здоровье
            'гинеколог': 'Репродуктивное здоровье',
            'уролог': 'Репродуктивное здоровье',
            'андролог': 'Репродуктивное здоровье',
            'сексолог': 'Репродуктивное здоровье',
            'маммолог': 'Репродуктивное здоровье',
            
            # Онкология
            'онколог': 'Онкология',
            
            # Инфекции и иммунология
            'инфекционист': 'Инфекционные заболевания',
            'иммунолог': 'Иммунология и аллергология',
            'аллерголог': 'Иммунология и аллергология',
            
            # Стоматология
            'стоматолог': 'Стоматология',
            'стоматолог-терапевт': 'Стоматология',
            'стоматолог-хирург': 'Стоматология',
            'ортодонт': 'Стоматология',
            'пародонтолог': 'Стоматология',
            
            # Реабилитация и терапия
            'физиотерапевт': 'Реабилитация',
            'мануальный терапевт': 'Реабилитация',
            'остеопат': 'Реабилитация',
            'рефлексотерапевт': 'Реабилитация',
            'спортивный врач': 'Реабилитация',
            
            # Другие
            'диетолог': 'Диетология',
            'неонатолог': 'Педиатрия',
        }
        
        df_copy['Выбор врача'] = df_copy['Выбор врача'].map(
            lambda x: doctor_categories.get(x, 'Другой специалист') if pd.notna(x) else 'Не указан'
        )
        return df_copy
    
    def anonymize_visit_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Дата посещения → Год и квартал (локальное обобщение)
        """
        df_copy = df.copy()
        
        def date_to_quarter(date_str):
            if pd.isna(date_str):
                return "Не указана"
            
            try:
                # Парсим дату в формате ISO
                if 'T' in str(date_str):
                    date_obj = datetime.fromisoformat(str(date_str).replace('+03:00', ''))
                    year = date_obj.year
                    quarter = (date_obj.month - 1) // 3 + 1
                    return f"{year}-Q{quarter}"
            except:
                pass
            
            return "Некорректная дата"
        
        df_copy['Дата посещения врача'] = df_copy['Дата посещения врача'].apply(date_to_quarter)
        return df_copy
    
    def anonymize_analyses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Анализы → Количество и категория (локальное обобщение)
        """
        df_copy = df.copy()
        
        def generalize_analyses(analyses_str):
            if pd.isna(analyses_str) or analyses_str == '':
                return "Не назначены"
            
            # Подсчитываем количество анализов
            analyses = str(analyses_str).split(',')
            count = len(analyses)
            
            # Обобщаем по количеству
            if count == 1:
                return "Один анализ"
            elif count == 2:
                return "Два анализа"
            elif count <= 4:
                return "Несколько анализов (3-4)"
            else:
                return "Множественные анализы (5+)"
        
        df_copy['Анализы'] = df_copy['Анализы'].apply(generalize_analyses)
        return df_copy
    
    def anonymize_analysis_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Дата получения анализов → Год и квартал (локальное обобщение)
        ЕДИНЫЙ ФОРМАТ с датой посещения: 2025-Q3
        """
        df_copy = df.copy()
        
        def date_to_quarter(date_str):
            if pd.isna(date_str):
                return "Не указана"
            
            try:
                # Парсим дату в формате ISO
                if 'T' in str(date_str):
                    date_obj = datetime.fromisoformat(str(date_str).replace('+03:00', ''))
                    year = date_obj.year
                    quarter = (date_obj.month - 1) // 3 + 1
                    return f"{year}-Q{quarter}"
            except:
                pass
            
            return "Некорректная дата"
        
        df_copy['Дата получения анализов'] = df_copy['Дата получения анализов'].apply(date_to_quarter)
        return df_copy
    
    def anonymize_cost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Стоимость → Диапазон (локальное обобщение + микро-агрегация)
        """
        df_copy = df.copy()
        
        def cost_to_range(cost_str):
            if pd.isna(cost_str):
                return "Не указана"
            
            # Извлекаем числовое значение
            match = re.search(r'(\d+)', str(cost_str))
            if match:
                cost = int(match.group(1))
                
                # Группируем по диапазонам
                if cost < 1000:
                    return "До 1000 руб."
                elif cost < 2000:
                    return "1000-2000 руб."
                elif cost < 3000:
                    return "2000-3000 руб."
                elif cost < 5000:
                    return "3000-5000 руб."
                elif cost < 7000:
                    return "5000-7000 руб."
                else:
                    return "7000+ руб."
            
            return "Не указана"
        
        df_copy['Стоимость анализов'] = df_copy['Стоимость анализов'].apply(cost_to_range)
        return df_copy
    
    def anonymize_card(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Карта → Название банка ИЛИ платежная система (извлечение из BIN-кода)
        Управляется флагом USE_SPECIFIC_BANK_NAME в начале модуля
        """
        df_copy = df.copy()
        
        def card_to_category(card_str):
            if pd.isna(card_str):
                return "Не указана"
            
            # Извлекаем первые 6 цифр (BIN)
            digits = re.sub(r'\D', '', str(card_str))
            if len(digits) >= 6:
                bin_code = digits[:6]
                
                if USE_SPECIFIC_BANK_NAME:
                    # Показываем конкретный банк
                    bank = self.bin_to_bank.get(bin_code, "Другой банк")
                    
                    # Переводим название банка
                    bank_names = {
                        'sberbank': 'Сбербанк',
                        'vtb': 'ВТБ',
                        'alfabank': 'Альфа-Банк',
                        'tinkoff': 'Тинькофф',
                        'gazprombank': 'Газпромбанк',
                        'raiffeisen': 'Райффайзенбанк'
                    }
                    
                    return bank_names.get(bank, 'Другой банк')
                else:
                    # Показываем только платежную систему
                    payment_system = self.bin_to_payment_system.get(bin_code, "Другая система")
                    
                    # Переводим название платежной системы
                    payment_system_names = {
                        'mir': 'МИР',
                        'visa': 'Visa',
                        'mastercard': 'Mastercard'
                    }
                    
                    return payment_system_names.get(payment_system, 'Другая система')
            
            return "Не указана"
        
        df_copy['Карта оплаты'] = df_copy['Карта оплаты'].apply(card_to_category)
        return df_copy
    
    def apply_anonymization(self, df: pd.DataFrame, selected_columns: List[str]) -> pd.DataFrame:
        """
        Применить обезличивание к выбранным столбцам
        
        Args:
            df: исходный датафрейм
            selected_columns: список столбцов для обезличивания
            
        Returns:
            обезличенный датафрейм
        """
        df_result = df.copy()
        
        # Применяем предопределенные методы для каждого столбца
        column_methods = {
            'ФИО': self.anonymize_fio,
            'Паспортные данные': self.anonymize_passport,
            'СНИЛС': self.anonymize_snils,
            'Симптомы': self.anonymize_symptoms,
            'Выбор врача': self.anonymize_doctor,
            'Дата посещения врача': self.anonymize_visit_date,
            'Анализы': self.anonymize_analyses,
            'Дата получения анализов': self.anonymize_analysis_date,
            'Стоимость анализов': self.anonymize_cost,
            'Карта оплаты': self.anonymize_card,
        }
        
        # Применяем методы только для выбранных столбцов
        for column in selected_columns:
            if column in column_methods:
                print(f"Обезличивание столбца: {column}")
                df_result = column_methods[column](df_result)
        
        return df_result
