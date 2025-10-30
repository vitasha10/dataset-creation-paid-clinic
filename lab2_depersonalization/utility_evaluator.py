"""
Модуль для оценки полезности данных

ВАЖНО: KLD (Kullback-Leibler Divergence) применим только для атрибутов с ОБОБЩЕНИЕМ,
не для атрибутов с ЗАМЕНОЙ (где меняется смысл атрибута).
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from scipy.stats import entropy


class DataUtilityEvaluator:
    """Класс для оценки полезности обезличенных данных"""
    
    # Атрибуты с ОБОБЩЕНИЕМ (где KLD имеет смысл)
    GENERALIZATION_COLUMNS = [
        'Симптомы',
        'Выбор врача',
        'Дата посещения врача',
        'Анализы',
        'Дата получения анализов',
        'Стоимость анализов'
    ]
    
    # Атрибуты с ЗАМЕНОЙ (где KLD не применим, т.к. это другой атрибут)
    REPLACEMENT_COLUMNS = [
        'ФИО',  # Заменяется на пол
        'Паспортные данные',  # Заменяется на регион
        'СНИЛС',  # Заменяется на возраст
        'Карта оплаты'  # Заменяется на банк/систему
    ]
    
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
        
        ПРАВИЛЬНЫЙ ПОДХОД:
        - KLD считается только для атрибутов с ОБОБЩЕНИЕМ
        - Information Loss считается для всех атрибутов
        - Разные метрики для разных типов обезличивания
        
        Args:
            original_df: исходный датафрейм
            anonymized_df: обезличенный датафрейм
            columns: список столбцов для анализа (если None - все)
            
        Returns:
            словарь с детальными результатами оценки
        """
        if columns is None:
            columns = list(original_df.columns)
        
        # Определяем, какие столбцы относятся к обобщению, какие к замене
        gen_cols = [c for c in columns if c in self.GENERALIZATION_COLUMNS]
        repl_cols = [c for c in columns if c in self.REPLACEMENT_COLUMNS]
        
        # KLD только для обобщенных атрибутов (там, где это имеет смысл)
        kld_generalization = self.calculate_kld(original_df, anonymized_df, gen_cols)
        
        # Information Loss для всех атрибутов
        il_all = self.calculate_information_loss(original_df, anonymized_df, columns)
        
        # Общие метрики
        valid_kld = [v for v in kld_generalization.values() if v is not None]
        overall_kld = np.mean(valid_kld) if valid_kld else None
        
        valid_il = [v['information_loss'] for v in il_all.values() if v is not None]
        overall_il = np.mean(valid_il) if valid_il else None
        
        # Качественная оценка
        kld_quality = self._interpret_kld(overall_kld)
        il_quality = self._interpret_information_loss(overall_il) if overall_il else "Н/Д"
        
        return {
            # KLD для обобщенных атрибутов
            'kld_generalization': kld_generalization,
            'overall_kld_generalization': overall_kld,
            'kld_quality': kld_quality,
            
            # Information Loss для всех
            'information_loss': il_all,
            'overall_information_loss': overall_il,
            'il_quality': il_quality,
            
            # Для обратной совместимости (deprecated)
            'kld_by_column': kld_generalization,
            'overall_kld': overall_kld,
            'quality_assessment': kld_quality,
            
            # Метаданные
            'generalization_columns': gen_cols,
            'replacement_columns': repl_cols,
            'note': 'KLD считается только для обобщенных атрибутов. Для замененных атрибутов используйте Information Loss.'
        }
    
    def calculate_information_loss(self, original_df: pd.DataFrame, anonymized_df: pd.DataFrame,
                                  columns: List[str] = None) -> Dict[str, Dict]:
        """
        Расчет Information Loss для каждого столбца
        
        Information Loss = 1 - (unique_after / unique_before)
        
        Args:
            original_df: исходный датафрейм
            anonymized_df: обезличенный датафрейм
            columns: список столбцов для анализа
            
        Returns:
            словарь с метриками для каждого столбца
        """
        if columns is None:
            columns = [col for col in original_df.columns if col in anonymized_df.columns]
        
        il_results = {}
        
        for col in columns:
            try:
                unique_before = original_df[col].nunique()
                unique_after = anonymized_df[col].nunique()
                
                if unique_before > 0:
                    il = 1 - (unique_after / unique_before)
                    compression = unique_before / unique_after if unique_after > 0 else float('inf')
                else:
                    il = 0
                    compression = 1
                
                il_results[col] = {
                    'information_loss': il,
                    'unique_before': unique_before,
                    'unique_after': unique_after,
                    'compression_ratio': compression
                }
            except Exception as e:
                il_results[col] = None
        
        return il_results
    
    def _interpret_kld(self, kld: float) -> str:
        """
        Интерпретация значения KLD
        
        ВНИМАНИЕ: Эти пороги применимы только для ОБОБЩЕНИЯ категориальных данных.
        Для агрессивного обобщения (например, 1000 значений → 10 значений) 
        KLD может быть выше 10, и это НОРМАЛЬНО.
        
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
            return "Очень низкая полезность (агрессивное обобщение)"
    
    def _interpret_information_loss(self, il: float) -> str:
        """
        Интерпретация Information Loss
        
        Args:
            il: значение information loss (0-1)
            
        Returns:
            текстовая оценка
        """
        if il < 0.3:
            return "Низкая потеря информации"
        elif il < 0.6:
            return "Средняя потеря информации"
        elif il < 0.8:
            return "Высокая потеря информации"
        elif il < 0.95:
            return "Очень высокая потеря информации"
        else:
            return "Почти полная замена атрибута"
    
    def print_utility_report(self, utility_results: Dict) -> None:
        """
        Печать детального отчета об оценке полезности
        
        Args:
            utility_results: результаты evaluate_utility()
        """
        print("\n" + "="*80)
        print("ОЦЕНКА ПОЛЕЗНОСТИ ОБЕЗЛИЧЕННЫХ ДАННЫХ")
        print("="*80)
        
        # 1. KLD для обобщенных атрибутов
        print("\n1. KLD (KULLBACK-LEIBLER DIVERGENCE) - для обобщенных атрибутов:")
        print("-" * 80)
        print("   Применяется только к атрибутам, где сохраняется смысл (обобщение).")
        print("   НЕ применяется к замененным атрибутам (ФИО→Пол, СНИЛС→Возраст).\n")
        
        kld_gen = utility_results.get('kld_generalization', {})
        if kld_gen:
            for col, kld in kld_gen.items():
                if kld is not None:
                    print(f"   {col:<40} {kld:>10.6f}")
                else:
                    print(f"   {col:<40} {'N/A':>10}")
            
            overall_kld = utility_results.get('overall_kld_generalization')
            if overall_kld is not None:
                print(f"\n   {'СРЕДНИЙ KLD (обобщение):':<40} {overall_kld:>10.6f}")
                print(f"   {'Интерпретация:':<40} {utility_results.get('kld_quality', 'N/A')}")
        else:
            print("   Нет атрибутов с обобщением для расчета KLD.")
        
        # 2. Information Loss для всех атрибутов
        print("\n2. INFORMATION LOSS - для всех атрибутов:")
        print("-" * 80)
        print("   Показывает долю потерянной уникальной информации.\n")
        print(f"   {'Атрибут':<30} {'До':<10} {'После':<10} {'IL':<10} {'Сжатие':<12}")
        print("   " + "-" * 76)
        
        il_all = utility_results.get('information_loss', {})
        for col, metrics in il_all.items():
            if metrics:
                il = metrics['information_loss']
                before = metrics['unique_before']
                after = metrics['unique_after']
                comp = metrics['compression_ratio']
                
                print(f"   {col:<30} {before:<10,} {after:<10,} {il:>8.1%}  {comp:>10.1f}x")
        
        overall_il = utility_results.get('overall_information_loss')
        if overall_il is not None:
            print(f"\n   {'СРЕДНИЙ INFORMATION LOSS:':<30} {overall_il:>18.1%}")
            print(f"   {'Интерпретация:':<30} {utility_results.get('il_quality', 'N/A')}")
        
        # 3. Пояснения
        print("\n3. ИНТЕРПРЕТАЦИЯ РЕЗУЛЬТАТОВ:")
        print("-" * 80)
        print("""
   KLD (для обобщенных атрибутов):
   • < 0.5:  Малые изменения распределения
   • < 1.0:  Умеренные изменения
   • < 2.0:  Значительные изменения
   • > 2.0:  Агрессивное обобщение (ожидаемо для высокой K-anonymity)
   
   Information Loss:
   • < 30%:  Низкая потеря (много уникальных значений сохранено)
   • 30-60%: Средняя потеря
   • 60-80%: Высокая потеря
   • > 80%:  Очень высокая потеря (сильное обобщение или замена)
   
   ВАЖНО: Высокие значения метрик = сильное обезличивание = выше K-anonymity
          Это неизбежный компромисс между анонимностью и полезностью данных.
        """)
        
        print("="*80)
