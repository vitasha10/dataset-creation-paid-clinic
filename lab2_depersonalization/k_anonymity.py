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
        if dataset_size <= 51000:
            return 10
        elif dataset_size <= 105000:
            return 7
        elif dataset_size <= 260000:
            return 5
        else:
            return 5

    def check_k_threshold(self, k_analysis: Dict) -> Tuple[bool, int, int]:
        dataset_size = k_analysis.get('total_records', 0)
        min_k = k_analysis.get('min_k', 0)
        required_k = self.get_acceptable_k(dataset_size)
        if min_k is None:
            return False, 0, required_k
        return min_k >= required_k, min_k, required_k