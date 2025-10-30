"""
Модуль методов обезличивания данных
"""
import pandas as pd
import numpy as np
import random
import re
from typing import List, Dict, Tuple
from datetime import datetime, timedelta


class DepersonalizationMethods:
    """Класс с методами обезличивания данных"""
    
    def __init__(self, seed: int = 42):
        """
        Инициализация методов обезличивания
        
        Args:
            seed: начальное значение для генератора случайных чисел
        """
        random.seed(seed)
        np.random.seed(seed)
    
    def generalization_local(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Локальное обобщение - замена точных значений на более общие категории
        
        Args:
            df: датафрейм
            column: имя столбца для обобщения
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        if column == 'ФИО':
            # Обобщаем до инициалов
            df_copy[column] = df_copy[column].apply(
                lambda x: self._generalize_fio(x) if pd.notna(x) else x
            )
        elif column == 'Паспортные данные':
            # Обобщаем паспорт: оставляем только серию
            df_copy[column] = df_copy[column].apply(
                lambda x: self._generalize_passport(x) if pd.notna(x) else x
            )
        elif column == 'Дата посещения врача' or column == 'Дата получения анализов':
            # Обобщаем дату до месяца/года
            df_copy[column] = df_copy[column].apply(
                lambda x: self._generalize_date(x) if pd.notna(x) else x
            )
        elif column == 'Стоимость анализов':
            # Обобщаем стоимость до диапазона
            df_copy[column] = df_copy[column].apply(
                lambda x: self._generalize_cost(x) if pd.notna(x) else x
            )
        elif column == 'Карта оплаты':
            # Обобщаем карту до первых 6 цифр (BIN)
            df_copy[column] = df_copy[column].apply(
                lambda x: self._generalize_card(x) if pd.notna(x) else x
            )
        
        return df_copy
    
    def aggregation(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Агрегация - объединение записей в группы
        
        Args:
            df: датафрейм
            columns: список столбцов для агрегации
            
        Returns:
            агрегированный датафрейм
        """
        df_copy = df.copy()
        
        # Группируем по выбранным столбцам и заменяем значения на групповые
        if columns:
            for col in columns:
                if col in df_copy.columns:
                    # Для категориальных - оставляем как есть
                    # Для числовых - усредняем
                    if col == 'Стоимость анализов':
                        # Группируем по диапазонам стоимости
                        df_copy[col] = df_copy[col].apply(
                            lambda x: self._aggregate_cost(x) if pd.notna(x) else x
                        )
        
        return df_copy
    
    def perturbation(self, df: pd.DataFrame, column: str, noise_level: float = 0.1) -> pd.DataFrame:
        """
        Возмущение - добавление шума к числовым данным
        
        Args:
            df: датафрейм
            column: имя столбца для возмущения
            noise_level: уровень шума (процент от значения)
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        if column == 'Стоимость анализов':
            df_copy[column] = df_copy[column].apply(
                lambda x: self._add_noise_to_cost(x, noise_level) if pd.notna(x) else x
            )
        elif column in ['Дата посещения врача', 'Дата получения анализов']:
            df_copy[column] = df_copy[column].apply(
                lambda x: self._add_noise_to_date(x, days=3) if pd.notna(x) else x
            )
        
        return df_copy
    
    def microaggregation(self, df: pd.DataFrame, column: str, k: int = 3) -> pd.DataFrame:
        """
        Микро-агрегация - замена значений в малых группах на среднее
        
        Args:
            df: датафрейм
            column: имя столбца для микро-агрегации
            k: размер группы
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        if column == 'Стоимость анализов':
            # Сортируем и группируем по k записей
            values = df_copy[column].copy()
            numeric_values = []
            
            for v in values:
                if pd.notna(v):
                    # Извлекаем числовое значение
                    num = self._extract_numeric(str(v))
                    numeric_values.append(num)
                else:
                    numeric_values.append(None)
            
            # Микро-агрегация
            aggregated = []
            valid_indices = [i for i, v in enumerate(numeric_values) if v is not None]
            
            if valid_indices:
                sorted_valid = sorted(zip(valid_indices, [numeric_values[i] for i in valid_indices]), key=lambda x: x[1])
                
                for i in range(0, len(sorted_valid), k):
                    group = sorted_valid[i:i+k]
                    avg = sum([v for _, v in group]) / len(group)
                    for idx, _ in group:
                        aggregated.append((idx, avg))
                
                # Применяем агрегированные значения
                result_values = numeric_values.copy()
                for idx, avg_val in aggregated:
                    result_values[idx] = avg_val
                
                df_copy[column] = [f"{int(v)} руб." if v is not None else None for v in result_values]
        
        return df_copy
    
    def shuffling(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Перемешивание - случайная перестановка значений в столбце
        
        Args:
            df: датафрейм
            column: имя столбца для перемешивания
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        if column in df_copy.columns:
            # Перемешиваем значения
            values = df_copy[column].values.copy()
            np.random.shuffle(values)
            df_copy[column] = values
        
        return df_copy
    
    def pseudonymization(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Создание псевдонимов - замена идентификаторов на псевдослучайные значения
        
        Args:
            df: датафрейм
            column: имя столбца для псевдонимизации
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        if column == 'ФИО':
            # Создаем псевдонимы для ФИО
            unique_fio = df_copy[column].unique()
            pseudonym_map = {fio: f"Клиент_{i+1:05d}" for i, fio in enumerate(unique_fio) if pd.notna(fio)}
            df_copy[column] = df_copy[column].map(lambda x: pseudonym_map.get(x, x) if pd.notna(x) else x)
        elif column == 'СНИЛС':
            # Создаем псевдонимы для СНИЛС
            unique_snils = df_copy[column].unique()
            pseudonym_map = {snils: f"XXX-XXX-XXX {i%100:02d}" for i, snils in enumerate(unique_snils) if pd.notna(snils) and snils != ''}
            df_copy[column] = df_copy[column].map(lambda x: pseudonym_map.get(x, x) if pd.notna(x) and x != '' else x)
        elif column == 'Паспортные данные':
            # Создаем псевдонимы для паспортов
            unique_passport = df_copy[column].unique()
            pseudonym_map = {p: f"XXXX XXXXXX" for i, p in enumerate(unique_passport) if pd.notna(p)}
            df_copy[column] = df_copy[column].map(lambda x: pseudonym_map.get(x, x) if pd.notna(x) else x)
        elif column == 'Карта оплаты':
            # Маскируем карту
            df_copy[column] = df_copy[column].apply(
                lambda x: self._pseudonymize_card(x) if pd.notna(x) else x
            )
        
        return df_copy
    
    def masking(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Маскеризация - частичное скрытие данных
        
        Args:
            df: датафрейм
            column: имя столбца для маскирования
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        if column == 'ФИО':
            # Маскируем часть ФИО
            df_copy[column] = df_copy[column].apply(
                lambda x: self._mask_fio(x) if pd.notna(x) else x
            )
        elif column == 'СНИЛС':
            # Маскируем СНИЛС
            df_copy[column] = df_copy[column].apply(
                lambda x: self._mask_snils(x) if pd.notna(x) and x != '' else x
            )
        elif column == 'Паспортные данные':
            # Маскируем паспорт
            df_copy[column] = df_copy[column].apply(
                lambda x: self._mask_passport(x) if pd.notna(x) else x
            )
        elif column == 'Карта оплаты':
            # Маскируем карту
            df_copy[column] = df_copy[column].apply(
                lambda x: self._mask_card(x) if pd.notna(x) else x
            )
        
        return df_copy
    
    def suppression_local(self, df: pd.DataFrame, column: str, threshold: float = 0.1) -> pd.DataFrame:
        """
        Локальное подавление - удаление редких значений
        
        Args:
            df: датафрейм
            column: имя столбца для подавления
            threshold: порог частоты (значения встречающиеся реже будут подавлены)
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        if column in df_copy.columns:
            # Подсчитываем частоту значений
            value_counts = df_copy[column].value_counts()
            total = len(df_copy)
            
            # Определяем редкие значения
            rare_values = value_counts[value_counts / total < threshold].index
            
            # Подавляем редкие значения
            df_copy.loc[df_copy[column].isin(rare_values), column] = '*'
        
        return df_copy
    
    def attribute_removal(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Удаление атрибутов - полное удаление выбранных столбцов
        
        Args:
            df: датафрейм
            columns: список столбцов для удаления
            
        Returns:
            обновленный датафрейм
        """
        df_copy = df.copy()
        
        # Удаляем указанные столбцы
        columns_to_drop = [col for col in columns if col in df_copy.columns]
        if columns_to_drop:
            df_copy = df_copy.drop(columns=columns_to_drop)
        
        return df_copy
    
    # Вспомогательные методы
    
    def _generalize_fio(self, fio: str) -> str:
        """Обобщение ФИО до инициалов"""
        parts = fio.split()
        if len(parts) >= 3:
            return f"{parts[0]} {parts[1][0]}. {parts[2][0]}."
        elif len(parts) == 2:
            return f"{parts[0]} {parts[1][0]}."
        return fio
    
    def _generalize_passport(self, passport: str) -> str:
        """Обобщение паспорта до серии"""
        match = re.search(r'(\d{4})\s*\d{6}', str(passport))
        if match:
            return f"{match.group(1)} XXXXXX"
        return passport
    
    def _generalize_date(self, date_str: str) -> str:
        """Обобщение даты до месяца/года"""
        try:
            # Парсим дату в формате ISO
            if 'T' in str(date_str):
                date_obj = datetime.fromisoformat(str(date_str).replace('+03:00', ''))
                return date_obj.strftime('%Y-%m')
            return date_str
        except:
            return date_str
    
    def _generalize_cost(self, cost_str: str) -> str:
        """Обобщение стоимости до диапазона"""
        num = self._extract_numeric(str(cost_str))
        if num is not None:
            if num < 1000:
                return "0-1000 руб."
            elif num < 3000:
                return "1000-3000 руб."
            elif num < 5000:
                return "3000-5000 руб."
            else:
                return "5000+ руб."
        return cost_str
    
    def _generalize_card(self, card_str: str) -> str:
        """Обобщение карты до BIN (первые 6 цифр)"""
        digits = re.sub(r'\D', '', str(card_str))
        if len(digits) >= 6:
            return f"{digits[:6]} XXXX XXXX XXXX"
        return card_str
    
    def _aggregate_cost(self, cost_str: str) -> str:
        """Агрегация стоимости"""
        num = self._extract_numeric(str(cost_str))
        if num is not None:
            # Округляем до ближайших 500 рублей
            aggregated = round(num / 500) * 500
            return f"{aggregated} руб."
        return cost_str
    
    def _add_noise_to_cost(self, cost_str: str, noise_level: float) -> str:
        """Добавление шума к стоимости"""
        num = self._extract_numeric(str(cost_str))
        if num is not None:
            noise = np.random.normal(0, num * noise_level)
            noisy_value = max(0, int(num + noise))
            return f"{noisy_value} руб."
        return cost_str
    
    def _add_noise_to_date(self, date_str: str, days: int) -> str:
        """Добавление шума к дате"""
        try:
            if 'T' in str(date_str):
                # Парсим дату в формате ISO
                date_obj = datetime.fromisoformat(str(date_str).replace('+03:00', ''))
                noise_days = np.random.randint(-days, days + 1)
                new_date = date_obj + timedelta(days=noise_days)
                return new_date.strftime('%Y-%m-%dT%H:%M+03:00')
            return date_str
        except:
            return date_str
    
    def _extract_numeric(self, text: str) -> float:
        """Извлечение числового значения из текста"""
        match = re.search(r'(\d+(?:\.\d+)?)', str(text))
        if match:
            return float(match.group(1))
        return None
    
    def _pseudonymize_card(self, card_str: str) -> str:
        """Псевдонимизация карты"""
        return "XXXX XXXX XXXX XXXX"
    
    def _mask_fio(self, fio: str) -> str:
        """Маскирование ФИО"""
        parts = fio.split()
        if len(parts) >= 3:
            return f"{parts[0]} {parts[1][0]}*** {parts[2][0]}***"
        elif len(parts) == 2:
            return f"{parts[0]} {parts[1][0]}***"
        return fio
    
    def _mask_snils(self, snils: str) -> str:
        """Маскирование СНИЛС"""
        return "XXX-XXX-XXX XX"
    
    def _mask_passport(self, passport: str) -> str:
        """Маскирование паспорта"""
        return "XXXX XXXXXX"
    
    def _mask_card(self, card_str: str) -> str:
        """Маскирование карты (оставляем первые 4 и последние 4 цифры)"""
        digits = re.sub(r'\D', '', str(card_str))
        if len(digits) >= 16:
            return f"{digits[:4]} XXXX XXXX {digits[-4:]}"
        return "XXXX XXXX XXXX XXXX"
