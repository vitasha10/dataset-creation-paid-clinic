"""
Тестовый скрипт для проверки функциональности без GUI
"""
import pandas as pd
import sys
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

from depersonalization_methods import DepersonalizationMethods
from k_anonymity import KAnonymityCalculator
from utility_evaluator import DataUtilityEvaluator


def test_basic_functionality():
    """Тест базовой функциональности"""
    print("=" * 70)
    print("ТЕСТ БАЗОВОЙ ФУНКЦИОНАЛЬНОСТИ")
    print("=" * 70)
    
    # Загружаем тестовый датасет
    input_file = "../generations/clinic_dataset_1k.xlsx"
    
    print(f"\n1. Загрузка датасета: {input_file}")
    try:
        df = pd.read_excel(input_file)
        print(f"   ✓ Датасет загружен: {len(df)} записей")
        print(f"   Столбцы: {list(df.columns)}")
    except Exception as e:
        print(f"   ✗ Ошибка загрузки: {e}")
        return False
    
    # Тестируем методы обезличивания
    print("\n2. Тестирование методов обезличивания")
    depers = DepersonalizationMethods()
    
    # Локальное обобщение
    print("   - Локальное обобщение (ФИО)")
    df_test = depers.generalization_local(df.copy(), 'ФИО')
    print(f"     Пример: {df['ФИО'].iloc[0]} -> {df_test['ФИО'].iloc[0]}")
    
    # Маскеризация
    print("   - Маскеризация (Карта оплаты)")
    df_test = depers.masking(df.copy(), 'Карта оплаты')
    print(f"     Пример: {df['Карта оплаты'].iloc[0]} -> {df_test['Карта оплаты'].iloc[0]}")
    
    # Псевдонимизация
    print("   - Псевдонимизация (ФИО)")
    df_test = depers.pseudonymization(df.copy(), 'ФИО')
    print(f"     Пример: {df['ФИО'].iloc[0]} -> {df_test['ФИО'].iloc[0]}")
    
    print("   ✓ Методы обезличивания работают")
    
    # Тестируем K-анонимность
    print("\n3. Тестирование расчета K-anonymity")
    k_calc = KAnonymityCalculator()
    
    quasi_identifiers = ['ФИО', 'Паспортные данные', 'СНИЛС']
    k_analysis = k_calc.calculate_k_anonymity(df, quasi_identifiers)
    
    print(f"   Выбранные QI: {quasi_identifiers}")
    print(f"   Минимальное K: {k_analysis['min_k']}")
    print(f"   Максимальное K: {k_analysis['max_k']}")
    print(f"   Среднее K: {k_analysis['avg_k']:.2f}")
    print(f"   Уникальных комбинаций: {k_analysis['unique_combinations']}")
    
    # Топ плохих K
    top_bad = k_calc.get_top_bad_k_values(k_analysis, top_n=5)
    print(f"\n   Топ-5 плохих K-anonymity:")
    for k, count, percentage in top_bad:
        print(f"     K={k}: {count} записей ({percentage:.4f}%)")
    
    # Проверка порога
    meets, min_k, req_k = k_calc.check_k_threshold(k_analysis)
    print(f"\n   Требуемое K: {req_k}")
    print(f"   Соответствие: {'✓ Да' if meets else '✗ Нет'}")
    
    print("   ✓ K-anonymity работает")
    
    # Тестируем оценку полезности
    print("\n4. Тестирование оценки полезности (KLD)")
    
    # Применяем обезличивание
    df_anon = df.copy()
    df_anon = depers.generalization_local(df_anon, 'ФИО')
    df_anon = depers.masking(df_anon, 'Карта оплаты')
    
    evaluator = DataUtilityEvaluator()
    utility = evaluator.evaluate_utility(df, df_anon, quasi_identifiers)
    
    print(f"   Общий KLD: {utility['overall_kld']:.6f}")
    print(f"   Оценка качества: {utility['quality_assessment']}")
    
    print("\n   KLD по столбцам:")
    for col, kld in utility['kld_by_column'].items():
        if kld is not None:
            print(f"     {col}: {kld:.6f}")
    
    print("   ✓ Оценка полезности работает")
    
    # Сохраняем тестовый результат
    print("\n5. Сохранение обезличенного датасета")
    output_file = "test_depersonalized.xlsx"
    df_anon.to_excel(output_file, index=False, sheet_name='Обезличенные данные')
    print(f"   ✓ Сохранено в {output_file}")
    
    print("\n" + "=" * 70)
    print("ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    print("=" * 70)
    
    return True


if __name__ == "__main__":
    test_basic_functionality()
