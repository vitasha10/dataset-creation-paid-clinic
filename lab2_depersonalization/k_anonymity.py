"""
Модуль для расчета K-анонимности (переработанный)
K-анонимность считается по ВСЕМ столбцам датасета, а не только по выбранным
"""
import pandas as pd
from typing import List, Dict, Tuple
from collections import Counter


class KAnonymityCalculator:
    """Класс для расчета K-анонимности датасета"""
    
    def __init__(self):
        """Инициализация калькулятора"""
        pass
    
    def calculate_k_anonymity(self, df: pd.DataFrame) -> Dict:
        """
        Расчет K-анонимности для датасета
        ВАЖНО: K-anonymity считается по ВСЕМ столбцам датасета
        
        Args:
            df: датафрейм
            
        Returns:
            словарь с результатами анализа K-анонимности
        """
        if df is None or len(df) == 0:
            return {
                'k_values': [],
                'k_distribution': {},
                'min_k': None,
                'max_k': None,
                'avg_k': None,
                'unique_combinations': 0,
                'total_records': 0
            }
        
        # Используем ВСЕ столбцы для расчета K-anonymity
        all_columns = df.columns.tolist()
        
        # Группируем по всем столбцам
        grouped = df.groupby(all_columns, dropna=False).size()
        k_values = grouped.values.tolist()
        
        # Подсчитываем распределение K
        k_counter = Counter(k_values)
        k_distribution = dict(k_counter)
        
        # Статистика
        min_k = min(k_values) if k_values else None
        max_k = max(k_values) if k_values else None
        avg_k = sum(k_values) / len(k_values) if k_values else None
        unique_combinations = len(k_values)
        
        return {
            'k_values': k_values,
            'k_distribution': k_distribution,
            'min_k': min_k,
            'max_k': max_k,
            'avg_k': avg_k,
            'unique_combinations': unique_combinations,
            'total_records': len(df),
            'grouped': grouped,
            'columns_used': all_columns
        }
    
    def get_top_bad_k_values(self, k_analysis: Dict, top_n: int = 5) -> List[Tuple[int, int, float]]:
        """
        Получить топ N наименьших значений K с количеством и процентами
        
        Args:
            k_analysis: результат анализа K-анонимности
            top_n: количество топ значений
            
        Returns:
            список кортежей (k_value, count, percentage)
        """
        k_distribution = k_analysis.get('k_distribution', {})
        total_records = k_analysis.get('total_records', 0)
        
        if not k_distribution or total_records == 0:
            return []
        
        # Сортируем по значению K (от меньшего к большему)
        sorted_k = sorted(k_distribution.items(), key=lambda x: x[0])
        
        # Берем топ N
        top_k = sorted_k[:top_n]
        
        # Формируем результат с процентами
        result = []
        for k_value, group_count in top_k:
            # group_count - это количество ГРУПП с данным K
            # Количество записей = k_value * group_count
            records_count = k_value * group_count
            percentage = (records_count / total_records) * 100
            result.append((k_value, records_count, percentage))
        
        return result
    
    def get_unique_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Получить строки с K=1 (уникальные комбинации)
        Использует ВСЕ столбцы для определения уникальности
        
        Args:
            df: датафрейм
            
        Returns:
            датафрейм с уникальными строками
        """
        if df is None or len(df) == 0:
            return pd.DataFrame()
        
        # Используем все столбцы
        all_columns = df.columns.tolist()
        
        # Находим строки с уникальными комбинациями
        grouped = df.groupby(all_columns, dropna=False).size()
        unique_combinations = grouped[grouped == 1].index
        
        # Создаем маску для фильтрации
        mask = pd.Series(False, index=df.index)
        for combo in unique_combinations:
            if isinstance(combo, tuple):
                combo_mask = pd.Series(True, index=df.index)
                for i, col in enumerate(all_columns):
                    if pd.isna(combo[i]):
                        combo_mask &= df[col].isna()
                    else:
                        combo_mask &= (df[col] == combo[i])
                mask |= combo_mask
            else:
                # Одиночное значение
                if pd.isna(combo):
                    mask |= df[all_columns[0]].isna()
                else:
                    mask |= (df[all_columns[0]] == combo)
        
        return df[mask]
    
    def get_acceptable_k(self, dataset_size: int) -> int:
        """
        Определить приемлемое значение K для размера датасета
        
        Args:
            dataset_size: размер датасета
            
        Returns:
            приемлемое значение K
        """
        if dataset_size <= 51000:
            return 10
        elif dataset_size <= 105000:
            return 7
        elif dataset_size <= 260000:
            return 5
        else:
            return 5  # Для больших датасетов
    
    def check_k_threshold(self, k_analysis: Dict) -> Tuple[bool, int, int]:
        """
        Проверить, соответствует ли датасет приемлемому порогу K
        
        Args:
            k_analysis: результат анализа K-анонимности
            
        Returns:
            кортеж (соответствует, минимальное K, требуемое K)
        """
        dataset_size = k_analysis.get('total_records', 0)
        min_k = k_analysis.get('min_k', 0)
        
        required_k = self.get_acceptable_k(dataset_size)
        
        if min_k is None:
            return False, 0, required_k
        
        return min_k >= required_k, min_k, required_k
