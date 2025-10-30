"""
Модуль для оценки полезности данных
"""
import pandas as pd
import numpy as np
from typing import Dict, List
from scipy.stats import entropy


class DataUtilityEvaluator:
    """Класс для оценки полезности обезличенных данных"""
    
    def __init__(self):
        """Инициализация оценщика"""
        pass
    
    def calculate_kld(self, original_df: pd.DataFrame, anonymized_df: pd.DataFrame, 
                     columns: List[str] = None) -> Dict[str, float]:
        """
        Расчет расстояния Кульбака-Лейблера (KLD) между исходным и обезличенным датасетом
        
        KLD(P||Q) = Σ P(i) * log2(P(i)/Q(i))
        
        Args:
            original_df: исходный датафрейм
            anonymized_df: обезличенный датафрейм
            columns: список столбцов для анализа (если None, анализируем все)
            
        Returns:
            словарь с KLD для каждого столбца
        """
        if columns is None:
            # Анализируем все общие столбцы
            columns = [col for col in original_df.columns if col in anonymized_df.columns]
        else:
            # Фильтруем только существующие столбцы
            columns = [col for col in columns if col in original_df.columns and col in anonymized_df.columns]
        
        kld_results = {}
        
        for col in columns:
            try:
                kld = self._calculate_column_kld(original_df[col], anonymized_df[col])
                kld_results[col] = kld
            except Exception as e:
                # Если не удалось вычислить KLD для столбца, пропускаем
                kld_results[col] = None
        
        return kld_results
    
    def _calculate_column_kld(self, original_series: pd.Series, anonymized_series: pd.Series) -> float:
        """
        Расчет KLD для одного столбца
        
        Args:
            original_series: исходная серия
            anonymized_series: обезличенная серия
            
        Returns:
            значение KLD
        """
        # Получаем распределения
        p_dist = self._get_distribution(original_series)
        q_dist = self._get_distribution(anonymized_series)
        
        if not p_dist or not q_dist:
            return None
        
        # Объединяем все уникальные значения
        all_values = set(p_dist.keys()) | set(q_dist.keys())
        
        # Создаем выровненные распределения
        p_probs = []
        q_probs = []
        
        for value in all_values:
            p_prob = p_dist.get(value, 1e-10)  # Используем малое значение вместо 0
            q_prob = q_dist.get(value, 1e-10)
            
            p_probs.append(p_prob)
            q_probs.append(q_prob)
        
        # Нормализуем
        p_probs = np.array(p_probs)
        q_probs = np.array(q_probs)
        
        p_probs = p_probs / p_probs.sum()
        q_probs = q_probs / q_probs.sum()
        
        # Вычисляем KLD
        kld = self._kld_formula(p_probs, q_probs)
        
        return kld
    
    def _get_distribution(self, series: pd.Series) -> Dict:
        """
        Получить распределение вероятностей для серии
        
        Args:
            series: pandas Series
            
        Returns:
            словарь {значение: вероятность}
        """
        # Подсчитываем частоты
        value_counts = series.value_counts(dropna=False)
        total = len(series)
        
        # Вычисляем вероятности
        distribution = {}
        for value, count in value_counts.items():
            distribution[value] = count / total
        
        return distribution
    
    def _kld_formula(self, p: np.ndarray, q: np.ndarray) -> float:
        """
        Вычисление KLD по формуле
        
        KLD(P||Q) = Σ P(i) * log2(P(i)/Q(i))
        
        Args:
            p: распределение P
            q: распределение Q
            
        Returns:
            значение KLD
        """
        # Избегаем деления на ноль и логарифма от нуля
        epsilon = 1e-10
        p = np.where(p < epsilon, epsilon, p)
        q = np.where(q < epsilon, epsilon, q)
        
        # Вычисляем KLD
        kld = np.sum(p * np.log2(p / q))
        
        return kld
    
    def calculate_overall_kld(self, kld_results: Dict[str, float]) -> float:
        """
        Вычислить общее KLD как среднее по всем столбцам
        
        Args:
            kld_results: результаты KLD для каждого столбца
            
        Returns:
            среднее значение KLD
        """
        valid_kld = [v for v in kld_results.values() if v is not None]
        
        if not valid_kld:
            return None
        
        return np.mean(valid_kld)
    
    def evaluate_utility(self, original_df: pd.DataFrame, anonymized_df: pd.DataFrame,
                        columns: List[str] = None) -> Dict:
        """
        Комплексная оценка полезности данных
        
        Args:
            original_df: исходный датафрейм
            anonymized_df: обезличенный датафрейм
            columns: список столбцов для анализа
            
        Returns:
            словарь с результатами оценки
        """
        # Расчет KLD для каждого столбца
        kld_by_column = self.calculate_kld(original_df, anonymized_df, columns)
        
        # Общий KLD
        overall_kld = self.calculate_overall_kld(kld_by_column)
        
        # Оценка качества (интерпретация KLD)
        quality = self._interpret_kld(overall_kld)
        
        return {
            'kld_by_column': kld_by_column,
            'overall_kld': overall_kld,
            'quality_assessment': quality
        }
    
    def _interpret_kld(self, kld: float) -> str:
        """
        Интерпретация значения KLD
        
        Args:
            kld: значение KLD
            
        Returns:
            текстовая оценка качества
        """
        if kld is None:
            return "Невозможно оценить"
        
        if kld < 0.1:
            return "Отличная полезность (минимальные изменения)"
        elif kld < 0.5:
            return "Хорошая полезность (небольшие изменения)"
        elif kld < 1.0:
            return "Удовлетворительная полезность (умеренные изменения)"
        elif kld < 2.0:
            return "Низкая полезность (значительные изменения)"
        else:
            return "Очень низкая полезность (существенные изменения)"
