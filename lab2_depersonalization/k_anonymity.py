import pandas as pd
from typing import List, Dict, Tuple
from collections import Counter

class KAnonymityCalculator:
    """Класс для расчета K-анонимности датасета"""

    def __init__(self):
        pass

    def _row_hash(self, df: pd.DataFrame, columns: List[str]) -> pd.Series:
        """
        Быстрый и компактный ключ группы: 64-битный хеш строки по всем столбцам.
        Учитывает NaN (как отдельное значение), порядок столбцов фиксирован.
        """
        # Векторный стабильный хеш от столбцов; работает быстро и не строит MultiIndex
        return pd.util.hash_pandas_object(df[columns], index=False)

    def calculate_k_anonymity(self, df: pd.DataFrame) -> Dict:
        """
        Расчет K-анонимности по ВСЕМ столбцам на основе row-hash.
        Возвращает только агрегаты и распределение, без тяжелых объектов.
        """
        if df is None or len(df) == 0:
            return {
                'k_values': [],
                'k_distribution': {},
                'min_k': None,
                'max_k': None,
                'avg_k': None,
                'unique_combinations': 0,
                'total_records': 0,
                'columns_used': []
            }

        all_columns = df.columns.tolist()

        # 1) Вычисляем хеш-ключ для каждой строки
        keys = self._row_hash(df, all_columns)

        # 2) Считаем частоты по ключу
        counts = keys.value_counts(dropna=False)

        # 3) k-values = значения частот, k-distribution = распределение этих частот
        k_values = counts.values.tolist()
        k_distribution = dict(Counter(k_values))

        min_k = min(k_values) if k_values else None
        max_k = max(k_values) if k_values else None
        avg_k = (sum(k_values) / len(k_values)) if k_values else None
        unique_combinations = len(counts)

        return {
            'k_values': k_values,
            'k_distribution': k_distribution,
            'min_k': min_k,
            'max_k': max_k,
            'avg_k': avg_k,
            'unique_combinations': unique_combinations,
            'total_records': len(df),
            'columns_used': all_columns
        }

    def get_top_bad_k_values(self, k_analysis: Dict, top_n: int = 5):
        k_distribution = k_analysis.get('k_distribution', {})
        total_records = k_analysis.get('total_records', 0)
        if not k_distribution or total_records == 0:
            return []
        sorted_k = sorted(k_distribution.items(), key=lambda x: x[0])
        top_k = sorted_k[:top_n]
        result = []
        for k_value, group_count in top_k:
            records_count = k_value * group_count
            percentage = (records_count / total_records) * 100
            result.append((k_value, records_count, percentage))
        return result

    def get_unique_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Возвращает строки с K=1 за один проход через хеш-ключи.
        Внимание: на очень больших наборах не вызывайте автоматически — это O(N) и съест память на срез.
        """
        if df is None or len(df) == 0:
            return pd.DataFrame()
        all_columns = df.columns.tolist()
        keys = self._row_hash(df, all_columns)
        counts = keys.value_counts(dropna=False)
        # map без выравнивания по индексу, затем mask по равенству 1
        mask = keys.map(counts).eq(1)
        return df[mask]

    def get_acceptable_k(self, dataset_size: int) -> int:
        """
        Возвращает требуемое минимальное K для датасета данного размера
        согласно требованиям задания
        """
        if dataset_size <= 51000:
            return 10
        elif dataset_size <= 105000:
            return 7
        elif dataset_size <= 260000:
            return 5
        else:
            # Для датасетов >260K используем минимальное значение
            return 5

    def check_k_threshold(self, k_analysis: Dict) -> Tuple[bool, int, int]:
        """
        Проверяет, соответствует ли датасет требованиям K-anonymity
        
        Returns:
            Tuple[bool, int, int]: (соответствует, минимальное_K, требуемое_K)
        """
        dataset_size = k_analysis.get('total_records', 0)
        min_k = k_analysis.get('min_k', 0)
        required_k = self.get_acceptable_k(dataset_size)
        if min_k is None:
            return False, 0, required_k
        return min_k >= required_k, min_k, required_k
    
    def get_k_quality_assessment(self, k_analysis: Dict) -> Dict:
        """
        Оценивает качество K-anonymity датасета и предоставляет детальные объяснения
        
        Returns:
            Dict с ключами:
            - quality_level: str ("Отлично"/"Хорошо"/"Удовлетворительно"/"Неудовлетворительно")
            - meets_requirement: bool
            - min_k: int
            - avg_k: float
            - required_k: int
            - k1_percent: float (процент строк с K=1)
            - explanation: str (детальное объяснение)
            - recommendation: str (рекомендации по улучшению)
        """
        min_k = k_analysis.get('min_k', 0)
        avg_k = k_analysis.get('avg_k', 0)
        total_records = k_analysis.get('total_records', 0)
        required_k = self.get_acceptable_k(total_records)
        
        # Процент строк с K=1
        k1_count = k_analysis.get('k_distribution', {}).get(1, 0) * 1
        k1_percent = (k1_count / total_records * 100) if total_records > 0 else 0
        
        # Оценка качества
        meets_requirement = min_k >= required_k
        
        if meets_requirement and k1_percent < 0.1:
            quality = "Отлично"
            explanation = f"Минимальное K ({min_k}) соответствует требованию (≥{required_k}), уникальных строк практически нет ({k1_percent:.2f}%). Датасет хорошо обезличен."
            recommendation = "Датасет готов к использованию. Дополнительные меры не требуются."
        elif avg_k >= required_k * 5 and k1_percent < 1.0:
            quality = "Хорошо"
            explanation = f"Хотя минимальное K ({min_k}) ниже требуемого ({required_k}), среднее K ({avg_k:.1f}) значительно выше, а уникальных строк очень мало ({k1_percent:.2f}%). Это приемлемо для реальных данных."
            recommendation = "Датасет можно использовать. Уникальные строки составляют менее 1%, что допустимо при высоком среднем K."
        elif avg_k >= required_k * 2:
            quality = "Удовлетворительно"
            explanation = f"Минимальное K ({min_k}) ниже требуемого ({required_k}), но среднее K ({avg_k:.1f}) вдвое выше требования. Уникальных строк {k1_percent:.2f}%."
            recommendation = "Рекомендуется дополнительное обобщение для уменьшения числа уникальных строк."
        else:
            quality = "Неудовлетворительно"
            explanation = f"Минимальное K ({min_k}) и среднее K ({avg_k:.1f}) ниже требуемого ({required_k}). Уникальных строк {k1_percent:.2f}%."
            recommendation = "Требуется агрессивное обобщение: уменьшите число категорий в каждом столбце."
        
        return {
            'quality_level': quality,
            'meets_requirement': meets_requirement,
            'min_k': min_k,
            'avg_k': avg_k,
            'required_k': required_k,
            'k1_percent': k1_percent,
            'explanation': explanation,
            'recommendation': recommendation
        }