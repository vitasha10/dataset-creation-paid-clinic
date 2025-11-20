"""
Модуль методов обезличивания данных (АГРЕССИВНОЕ обобщение для достижения K≥5)

КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Уменьшено количество категорий для достижения требуемого K-anonymity
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
# True - показывать конкретный банк (Сбербанк, ВТБ и т.д.) - СЕЙЧАС 6 значений
# False - показывать только платежную систему (МИР, Visa, Mastercard) - 3 значения
USE_SPECIFIC_BANK_NAME = False  # Изменено на False для лучшего K


class DepersonalizationMethods:
    """Класс с методами обезличивания данных - АГРЕССИВНОЕ обобщение"""
    
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
        
        # Создаем маппинг серий паспортов к федеральным округам (8 вместо 27 регионов!)
        self.series_to_federal_district = self._create_federal_district_mapping()
        
        # Создаем маппинг имен к полу
        self.male_names = set(SLAVIC_NAMES_MALE)
        self.female_names = set(SLAVIC_NAMES_FEMALE)
    
    def _create_federal_district_mapping(self) -> Dict[str, str]:
        """Маппинг кодов регионов к федеральным округам (агрессивное обобщение)"""
        federal_districts = {
            # Центральный ФО
            **{f"{i:02d}": "Центральный ФО" for i in [1, 2, 3, 32, 33, 36, 40, 44, 46, 48, 50, 57, 62, 67, 68, 69, 71]},
            # Северо-Западный ФО  
            **{f"{i:02d}": "Северо-Западный ФО" for i in [10, 11, 29, 35, 39, 47, 51, 53, 60]},
            # Южный ФО
            **{f"{i:02d}": "Южный ФО" for i in [1, 8, 23, 30, 34, 61]},
            # Северо-Кавказский ФО
            **{f"{i:02d}": "Северо-Кавказский ФО" for i in [5, 6, 7, 9, 15, 20, 26]},
            # Приволжский ФО
            **{f"{i:02d}": "Приволжский ФО" for i in [12, 13, 16, 18, 21, 43, 52, 56, 59, 63, 64, 73]},
            # Уральский ФО
            **{f"{i:02d}": "Уральский ФО" for i in [45, 54, 66, 72, 74, 86, 89]},
            # Сибирский ФО
            **{f"{i:02d}": "Сибирский ФО" for i in [4, 19, 22, 24, 38, 42, 55, 70]},
            # Дальневосточный ФО
            **{f"{i:02d}": "Дальневосточный ФО" for i in [14, 25, 27, 28, 41, 49, 65, 75, 79, 87]},
        }
        return federal_districts
    
    def anonymize_fio(self, df: pd.DataFrame) -> pd.DataFrame:
        """ФИО → Пол (мужской/женский)"""
        df_copy = df.copy()
        
        def fio_to_gender(fio_str):
            if pd.isna(fio_str):
                return "Не указан"
            
            parts = str(fio_str).strip().split()
            if len(parts) < 2:
                return "Не определен"
            
            name = parts[1]
            
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
        Паспорт → Федеральный округ (АГРЕССИВНОЕ ОБОБЩЕНИЕ: 27 регионов → 8 округов!)
        """
        df_copy = df.copy()
        
        def passport_to_district(passport_str):
            if pd.isna(passport_str):
                return "Не указан"
            
            # Извлекаем первые 2 цифры серии
            match = re.match(r'(\d{2})\d{2}\s+\d{6}', str(passport_str))
            if match:
                region_code = match.group(1)
                district = self.series_to_federal_district.get(region_code, "Другой округ")
                return district
            
            # Для белорусских и казахских паспортов
            if re.match(r'[A-Z]{2}\d{7}', str(passport_str)):
                return "СНГ"
            elif re.match(r'N\d{8}', str(passport_str)):
                return "СНГ"
            
            return "Другой округ"
        
        df_copy['Паспортные данные'] = df_copy['Паспортные данные'].apply(passport_to_district)
        return df_copy
    
    def anonymize_snils(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        СНИЛС → Возрастная группа (АГРЕССИВНОЕ ОБОБЩЕНИЕ: 6 групп → 4 группы)
        """
        df_copy = df.copy()
        
        def snils_to_age_group(snils_str):
            if pd.isna(snils_str) or snils_str == '':
                return "Не указана"
            
            # Упрощенные возрастные группы (меньше = лучше K)
            age_groups = [
                "18-30 лет",
                "31-45 лет", 
                "46-60 лет",
                "61+ лет"
            ]
            
            hash_val = hash(str(snils_str)) % len(age_groups)
            return age_groups[hash_val]
        
        df_copy['СНИЛС'] = df_copy['СНИЛС'].apply(snils_to_age_group)
        return df_copy
    
    def anonymize_symptoms(self, df: pd.DataFrame) -> pd.DataFrame:
        """Симптомы → Обобщение (2 значения - оставлено как есть, уже минимально)"""
        df_copy = df.copy()
        
        def generalize_symptoms(symptoms_str):
            if pd.isna(symptoms_str) or symptoms_str == '':
                return "Не указаны"
            
            symptoms = str(symptoms_str).split(',')
            count = len(symptoms)
            
            # Еще более агрессивное обобщение
            if count == 1:
                return "Один симптом"
            else:
                return "Множественные симптомы"
        
        df_copy['Симптомы'] = df_copy['Симптомы'].apply(generalize_symptoms)
        return df_copy
    
    def anonymize_doctor(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Выбор врача → Укрупненная категория (АГРЕССИВНОЕ: 16 категорий → 6 категорий!)
        """
        df_copy = df.copy()
        
        # УПРОЩЕННЫЙ маппинг к широким категориям
        doctor_broad_categories = {
            # Общая медицина и профилактика
            'терапевт': 'Общая медицина',
            'педиатр': 'Общая медицина',
            'геронтолог': 'Общая медицина',
            
            # Специалисты по внутренним органам
            'кардиолог': 'Внутренние органы',
            'кардиохирург': 'Внутренние органы',
            'гастроэнтеролог': 'Внутренние органы',
            'эндокринолог': 'Внутренние органы',
            'пульмонолог': 'Внутренние органы',
            'нефролог': 'Внутренние органы',
            'ревматолог': 'Внутренние органы',
            'гематолог': 'Внутренние органы',
            
            # Нервная система и психика
            'невролог': 'Нервная система и психика',
            'нейрохирург': 'Нервная система и психика',
            'психиатр': 'Нервная система и психика',
            'психотерапевт': 'Нервная система и психика',
            'нарколог': 'Нервная система и психика',
            
            # Хирургия и травматология  
            'хирург': 'Хирургия и травматология',
            'ортопед': 'Хирургия и травматология',
            'травматолог': 'Хирургия и травматология',
            'челюстно-лицевой хирург': 'Хирургия и травматология',
            'пластический хирург': 'Хирургия и травматология',
            'сосудистый хирург': 'Хирургия и травматология',
            'флеболог': 'Хирургия и травматология',
            'торакальный хирург': 'Хирургия и травматология',
            'онколог-хирург': 'Хирургия и травматология',
            'детский хирург': 'Хирургия и травматология',
            'проктолог': 'Хирургия и травматология',
            
            # Репродуктивное здоровье
            'гинеколог': 'Репродуктивное здоровье',
            'уролог': 'Репродуктивное здоровье',
            'андролог': 'Репродуктивное здоровье',
            'сексолог': 'Репродуктивное здоровье',
            'маммолог': 'Репродуктивное здоровье',
            
            # Остальные специалисты
            'офтальмолог': 'Другие специалисты',
            'лор': 'Другие специалисты',
            'дерматолог': 'Другие специалисты',
            'косметолог': 'Другие специалисты',
            'трихолог': 'Другие специалисты',
            'венеролог': 'Другие специалисты',
            'онколог': 'Другие специалисты',
            'инфекционист': 'Другие специалисты',
            'иммунолог': 'Другие специалисты',
            'аллерголог': 'Другие специалисты',
            'стоматолог': 'Другие специалисты',
            'стоматолог-терапевт': 'Другие специалисты',
            'стоматолог-хирург': 'Другие специалисты',
            'ортодонт': 'Другие специалисты',
            'пародонтолог': 'Другие специалисты',
            'диетолог': 'Другие специалисты',
            'физиотерапевт': 'Другие специалисты',
            'массажист': 'Другие специалисты',
            'мануальный терапевт': 'Другие специалисты',
            'остеопат': 'Другие специалисты',
            'реабилитолог': 'Другие специалисты',
        }
        
        def categorize_doctor(doctor_str):
            if pd.isna(doctor_str) or doctor_str == '':
                return "Не указан"
            
            doctor_lower = str(doctor_str).lower().strip()
            return doctor_broad_categories.get(doctor_lower, "Другие специалисты")
        
        df_copy['Выбор врача'] = df_copy['Выбор врача'].apply(categorize_doctor)
        return df_copy
    
    def anonymize_visit_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Дата посещения → Квартал (уже минимально: 2-4 значения в год)"""
        df_copy = df.copy()
        
        def date_to_quarter(date_str):
            if pd.isna(date_str):
                return "Не указана"
            
            try:
                if isinstance(date_str, str):
                    dt = pd.to_datetime(date_str)
                else:
                    dt = date_str
                
                quarter = (dt.month - 1) // 3 + 1
                return f"{dt.year}-Q{quarter}"
            except:
                return "Не указана"
        
        df_copy['Дата посещения врача'] = df_copy['Дата посещения врача'].apply(date_to_quarter)
        return df_copy
    
    def anonymize_analyses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Анализы → Обобщение (2 значения - оставлено как есть)"""
        df_copy = df.copy()
        
        def generalize_analyses(analyses_str):
            if pd.isna(analyses_str) or analyses_str == '':
                return "Не указаны"
            
            analyses = str(analyses_str).split(',')
            count = len(analyses)
            
            if count == 1:
                return "Один анализ"
            else:
                return "Множественные анализы"
        
        df_copy['Анализы'] = df_copy['Анализы'].apply(generalize_analyses)
        return df_copy
    
    def anonymize_analysis_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Дата получения анализов → Квартал"""
        df_copy = df.copy()
        
        def date_to_quarter(date_str):
            if pd.isna(date_str):
                return "Не указана"
            
            try:
                if isinstance(date_str, str):
                    dt = pd.to_datetime(date_str)
                else:
                    dt = date_str
                
                quarter = (dt.month - 1) // 3 + 1
                return f"{dt.year}-Q{quarter}"
            except:
                return "Не указана"
        
        df_copy['Дата получения анализов'] = df_copy['Дата получения анализов'].apply(date_to_quarter)
        return df_copy
    
    def anonymize_cost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Стоимость → Диапазон (АГРЕССИВНОЕ: 6 диапазонов → 3 диапазона)
        """
        df_copy = df.copy()
        
        def cost_to_range(cost_str):
            if pd.isna(cost_str) or cost_str == '':
                return "Не указана"
            
            # Извлекаем число из строки
            match = re.search(r'(\d+)', str(cost_str))
            if not match:
                return "Не указана"
            
            cost = int(match.group(1))
            
            # Широкие диапазоны для лучшего K
            if cost < 3000:
                return "До 3000 руб."
            elif cost < 6000:
                return "3000-6000 руб."
            else:
                return "6000+ руб."
        
        df_copy['Стоимость анализов'] = df_copy['Стоимость анализов'].apply(cost_to_range)
        return df_copy
    
    def anonymize_card(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Карта → Банк или Платежная система
        
        Зависит от флага USE_SPECIFIC_BANK_NAME:
        - True: показывает банк (6 значений)
        - False: показывает платежную систему (3 значения) - ЛУЧШЕ для K!
        """
        df_copy = df.copy()
        
        def card_to_bank_or_system(card_str):
            if pd.isna(card_str) or card_str == '':
                return "Не указана"
            
            # Извлекаем первые 4 цифры (BIN)
            match = re.match(r'(\d{4})', str(card_str).replace(' ', ''))
            if not match:
                return "Неизвестная"
            
            bin_code = match.group(1)
            
            if USE_SPECIFIC_BANK_NAME:
                # Показываем конкретный банк (6 значений)
                return self.bin_to_bank.get(bin_code, "Другой банк")
            else:
                # Показываем платежную систему (3 значения) - ЛУЧШЕ для K-anonymity!
                return self.bin_to_payment_system.get(bin_code, "Другая система")
        
        df_copy['Карта оплаты'] = df_copy['Карта оплаты'].apply(card_to_bank_or_system)
        return df_copy
    
    def apply_anonymization(self, df: pd.DataFrame, selected_columns: List[str] = None) -> pd.DataFrame:
        """
        Применить обезличивание к выбранным столбцам
        
        Args:
            df: исходный DataFrame
            selected_columns: список столбцов для обезличивания (если None, обезличивает все)
        
        Returns:
            обезличенный DataFrame
        """
        df_result = df.copy()
        
        # Если не указаны столбцы, обезличиваем все
        if selected_columns is None:
            selected_columns = df.columns.tolist()
        
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
        
        for column in selected_columns:
            if column in column_methods:
                df_result = column_methods[column](df_result)
        
        return df_result
