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
# True - показывать конкретный банк (Сбербанк, ВТБ и т.д.) - дает ~6 значений
# False - показывать только платежную систему (МИР, Visa, Mastercard) - дает ~3 значения
# РЕКОМЕНДУЕТСЯ: False для лучшего K-anonymity!
USE_SPECIFIC_BANK_NAME = False


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
        Паспорт → Макрорегион (АГРЕССИВНОЕ обобщение: 27 регионов → 3 макрорегиона!)
        Европейская часть / Азиатская часть / СНГ
        """
        df_copy = df.copy()
        
        # Европейская часть РФ (коды 01-99, большинство населения)
        european_russia = {'01','02','03','04','05','06','07','08','09','10','11','12','13','14','15',
                          '16','17','18','19','20','21','22','23','24','25','26','27','28','29','30',
                          '31','32','33','34','35','36','37','38','39','40','41','42','43','44','45',
                          '46','47','48','49','50','51','52','53','54','55','56','57','58','59','60',
                          '61','62','63','64','65','66','67','68','69','70','71','72','73','74','75',
                          '76','77','78','79'}
        
        # Азиатская часть РФ (коды после 80)
        asian_russia = {'80','81','82','83','84','85','86','87','88','89','90','91','92','93','94','95','96','97','98','99'}
        
        def passport_to_macroregion(passport_str):
            if pd.isna(passport_str):
                return "Не указан"
            
            # Извлекаем первые 2 цифры серии
            match = re.match(r'(\d{2})\d{2}\s+\d{6}', str(passport_str))
            if match:
                region_code = match.group(1)
                if region_code in european_russia:
                    return "Европейская часть РФ"
                elif region_code in asian_russia:
                    return "Азиатская часть РФ"
                else:
                    return "Европейская часть РФ"  # По умолчанию
            
            # Для белорусских и казахских паспортов
            if re.match(r'[A-Z]{2}\d{7}', str(passport_str)) or re.match(r'N\d{8}', str(passport_str)):
                return "СНГ"
            
            return "Европейская часть РФ"  # По умолчанию для остальных
        
        df_copy['Паспортные данные'] = df_copy['Паспортные данные'].apply(passport_to_macroregion)
        return df_copy
    
    def anonymize_snils(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        СНИЛС → Возрастная группа (АГРЕССИВНОЕ: 6 групп → 3 группы)
        """
        df_copy = df.copy()
        
        def snils_to_age_group(snils_str):
            if pd.isna(snils_str) or snils_str == '':
                return "Не указана"
            
            # Широкие возрастные группы для лучшего K
            age_groups = [
                "18-35 лет",   # Молодые
                "36-55 лет",   # Средний возраст
                "56+ лет"      # Пожилые
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
        Выбор врача → Широкая категория (АГРЕССИВНОЕ: 16 категорий → 4 категории!)
        """
        df_copy = df.copy()
        
        # ОЧЕНЬ ШИРОКИЙ маппинг к 4 основным категориям
        doctor_broad_categories = {
            # Общая медицина и терапия
            'терапевт': 'Общая медицина',
            'педиатр': 'Общая медицина',
            'геронтолог': 'Общая медицина',
            
            # Хирургия (ВСЕ хирурги)
            'хирург': 'Хирургия',
            'ортопед': 'Хирургия',
            'травматолог': 'Хирургия',
            'челюстно-лицевой хирург': 'Хирургия',
            'пластический хирург': 'Хирургия',
            'сосудистый хирург': 'Хирургия',
            'кардиохирург': 'Хирургия',
            'нейрохирург': 'Хирургия',
            'флеболог': 'Хирургия',
            'торакальный хирург': 'Хирургия',
            'онколог-хирург': 'Хирургия',
            'детский хирург': 'Хирургия',
            'проктолог': 'Хирургия',
            'стоматолог-хирург': 'Хирургия',
            
            # Узкие специалисты (ВСЕ узкие специалисты по органам и системам)
            'кардиолог': 'Узкие специалисты',
            'невролог': 'Узкие специалисты',
            'гастроэнтеролог': 'Узкие специалисты',
            'эндокринолог': 'Узкие специалисты',
            'пульмонолог': 'Узкие специалисты',
            'нефролог': 'Узкие специалисты',
            'ревматолог': 'Узкие специалисты',
            'гематолог': 'Узкие специалисты',
            'офтальмолог': 'Узкие специалисты',
            'лор': 'Узкие специалисты',
            'дерматолог': 'Узкие специалисты',
            'косметолог': 'Узкие специалисты',
            'трихолог': 'Узкие специалисты',
            'венеролог': 'Узкие специалисты',
            'онколог': 'Узкие специалисты',
            'инфекционист': 'Узкие специалисты',
            'иммунолог': 'Узкие специалисты',
            'аллерголог': 'Узкие специалисты',
            'гинеколог': 'Узкие специалисты',
            'уролог': 'Узкие специалисты',
            'андролог': 'Узкие специалисты',
            'сексолог': 'Узкие специалисты',
            'маммолог': 'Узкие специалисты',
            'психиатр': 'Узкие специалисты',
            'психотерапевт': 'Узкие специалисты',
            'нарколог': 'Узкие специалисты',
            'диетолог': 'Узкие специалисты',
            'неонатолог': 'Узкие специалисты',
            
            # Стоматология и реабилитация
            'стоматолог': 'Стоматология и реабилитация',
            'стоматолог-терапевт': 'Стоматология и реабилитация',
            'ортодонт': 'Стоматология и реабилитация',
            'пародонтолог': 'Стоматология и реабилитация',
            'физиотерапевт': 'Стоматология и реабилитация',
            'мануальный терапевт': 'Стоматология и реабилитация',
            'остеопат': 'Стоматология и реабилитация',
            'рефлексотерапевт': 'Стоматология и реабилитация',
            'спортивный врач': 'Стоматология и реабилитация',
        }
        
        def categorize_doctor(doctor_str):
            if pd.isna(doctor_str) or doctor_str == '':
                return "Не указан"
            
            doctor_lower = str(doctor_str).lower().strip()
            return doctor_broad_categories.get(doctor_lower, "Узкие специалисты")  # По умолчанию узкий специалист
        
        df_copy['Выбор врача'] = df_copy['Выбор врача'].apply(categorize_doctor)
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
        Стоимость → Диапазон (АГРЕССИВНОЕ: 6 диапазонов → 2 диапазона!)
        """
        df_copy = df.copy()
        
        def cost_to_range(cost_str):
            if pd.isna(cost_str):
                return "Не указана"
            
            # Извлекаем числовое значение
            match = re.search(r'(\d+)', str(cost_str))
            if match:
                cost = int(match.group(1))
                
                # Только 2 широких диапазона для лучшего K
                if cost < 4000:
                    return "До 4000 руб."
                else:
                    return "4000+ руб."
            
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
