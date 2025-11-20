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
        keys = self._row_hash(df, all_columns)
        counts = keys.value_counts(dropna=False)
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
        if df is None or len(df) == 0:
            return pd.DataFrame()
        all_columns = df.columns.tolist()
        keys = self._row_hash(df, all_columns)
        counts = keys.value_counts(dropna=False)
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

    def get_k_quality_assessment(self, k_analysis: Dict) -> Dict:
        min_k = k_analysis.get('min_k', 0)
        avg_k = k_analysis.get('avg_k', 0)
        total_records = k_analysis.get('total_records', 0)
        required_k = self.get_acceptable_k(total_records)
        k1_count = k_analysis.get('k_distribution', {}).get(1, 0) * 1
        k1_percent = (k1_count / total_records * 100) if total_records > 0 else 0
        meets_requirement = min_k >= required_k

        if meets_requirement and k1_percent < 0.1:
            quality = "Отлично"
            explanation = f"Минимальное K ({min_k}) соответствует требованию (≥{required_k}), уникальных строк практически нет ({k1_percent:.4f}%)."
            recommendation = "Датасет готов к использованию. Дополнительные меры не требуются."
        elif avg_k >= required_k * 5 and k1_percent < 1.0:
            quality = "Хорошо"
            explanation = f"Хотя минимальное K ({min_k}) ниже требуемого ({required_k}), среднее K ({avg_k:.1f}) значительно выше, а уникальных строк мало ({k1_percent:.2f}%)."
            recommendation = "Датасет можно использовать. При необходимости снизьте уникальность оставшихся строк."
        elif avg_k >= required_k * 2:
            quality = "Удовлетворительно"
            explanation = f"Минимальное K ({min_k}) ниже ({required_k}), но среднее ({avg_k:.1f}) достаточно высоко. Уникальных строк {k1_percent:.2f}%."
            recommendation = "Рекомендуется дополнительное обобщение для снижения числа уникальных строк."
        else:
            quality = "Неудовлетворительно"
            explanation = f"Минимальное K ({min_k}) и среднее K ({avg_k:.1f}) ниже требования ({required_k}). Уникальных строк {k1_percent:.2f}%."
            recommendation = "Требуется агрессивное обобщение."
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

    def filter_by_min_k(self, df: pd.DataFrame, min_k_keep: int = 8) -> Tuple[pd.DataFrame, Dict]:
        """
        Удаляет все строки, принадлежащие группам с размером < min_k_keep.
        По заданию: нужно убрать K от 1 до 7 => min_k_keep = 8.
        Returns:
            (filtered_df, stats)
            stats: {
                'original_rows': int,
                'new_rows': int,
                'removed_rows': int,
                'removed_percent': float,
                'threshold': int
            }
        """
        if df is None or len(df) == 0:
            return df, {
                'original_rows': 0,
                'new_rows': 0,
                'removed_rows': 0,
                'removed_percent': 0.0,
                'threshold': min_k_keep
            }
        all_columns = df.columns.tolist()
        keys = self._row_hash(df, all_columns)
        counts = keys.value_counts(dropna=False)
        group_sizes = keys.map(counts)
        mask = group_sizes >= min_k_keep
        filtered_df = df[mask].copy()
        original_rows = len(df)
        new_rows = len(filtered_df)
        removed_rows = original_rows - new_rows
        removed_percent = (removed_rows / original_rows * 100) if original_rows > 0 else 0.0
        stats = {
            'original_rows': original_rows,
            'new_rows': new_rows,
            'removed_rows': removed_rows,
            'removed_percent': removed_percent,
            'threshold': min_k_keep
        }
        return filtered_df, stats