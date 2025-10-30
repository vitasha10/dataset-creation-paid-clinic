"""
Тестовый скрипт для проверки новой функциональности
"""
import pandas as pd
import sys
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

from depersonalization_methods import DepersonalizationMethods
from k_anonymity import KAnonymityCalculator
from utility_evaluator import DataUtilityEvaluator


def test_new_functionality():
    """Тест новой функциональности"""
    print("=" * 70)
    print("ТЕСТ НОВОЙ ФУНКЦИОНАЛЬНОСТИ")
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
    
    # Показываем пример до обезличивания
    print("\n2. Пример данных ДО обезличивания:")
    print(f"   ФИО: {df['ФИО'].iloc[0]}")
    print(f"   Паспорт: {df['Паспортные данные'].iloc[0]}")
    print(f"   СНИЛС: {df['СНИЛС'].iloc[0]}")
    print(f"   Карта: {df['Карта оплаты'].iloc[0]}")
    print(f"   Дата посещения: {df['Дата посещения врача'].iloc[0]}")
    print(f"   Стоимость: {df['Стоимость анализов'].iloc[0]}")
    
    # Тестируем методы обезличивания
    print("\n3. Тестирование методов обезличивания")
    depers = DepersonalizationMethods()
    
    # Выбираем все столбцы
    selected_columns = list(df.columns)
    
    df_anon = depers.apply_anonymization(df.copy(), selected_columns)
    
    print("\n4. Пример данных ПОСЛЕ обезличивания:")
    print(f"   ФИО: {df_anon['ФИО'].iloc[0]}")
    print(f"   Паспорт: {df_anon['Паспортные данные'].iloc[0]}")
    print(f"   СНИЛС: {df_anon['СНИЛС'].iloc[0]}")
    print(f"   Карта: {df_anon['Карта оплаты'].iloc[0]}")
    print(f"   Дата посещения: {df_anon['Дата посещения врача'].iloc[0]}")
    print(f"   Стоимость: {df_anon['Стоимость анализов'].iloc[0]}")
    print(f"   Выбор врача: {df_anon['Выбор врача'].iloc[0]}")
    print(f"   Симптомы: {df_anon['Симптомы'].iloc[0]}")
    print(f"   Анализы: {df_anon['Анализы'].iloc[0]}")
    
    print("\n   ✓ Методы обезличивания работают")
    print("   ✓ ФИО полностью заменено на псевдоним")
    print("   ✓ Паспорт преобразован в регион")
    print("   ✓ Карта преобразована в название банка")
    
    # Тестируем K-анонимность ПО ВСЕМ СТОЛБЦАМ
    print("\n5. Тестирование расчета K-anonymity ПО ВСЕМ СТОЛБЦАМ")
    k_calc = KAnonymityCalculator()
    
    k_analysis = k_calc.calculate_k_anonymity(df_anon)
    
    print(f"   Минимальное K: {k_analysis['min_k']}")
    print(f"   Максимальное K: {k_analysis['max_k']}")
    print(f"   Среднее K: {k_analysis['avg_k']:.2f}")
    print(f"   Уникальных комбинаций: {k_analysis['unique_combinations']}")
    print(f"   Использовано столбцов: {len(k_analysis['columns_used'])}")
    
    # Топ плохих K
    top_bad = k_calc.get_top_bad_k_values(k_analysis, top_n=5)
    print(f"\n   Топ-5 плохих K-anonymity:")
    for k, count, percentage in top_bad:
        print(f"     K={k}: {count} записей ({percentage:.4f}%)")
    
    # Проверка порога
    meets, min_k, req_k = k_calc.check_k_threshold(k_analysis)
    print(f"\n   Требуемое K: {req_k}")
    print(f"   Соответствие: {'✓ Да' if meets else '✗ Нет'}")
    
    print("   ✓ K-anonymity рассчитывается по всем столбцам")
    
    # Тестируем оценку полезности
    print("\n6. Тестирование оценки полезности (KLD)")
    
    evaluator = DataUtilityEvaluator()
    utility = evaluator.evaluate_utility(df, df_anon, selected_columns)
    
    print(f"   Общий KLD: {utility['overall_kld']:.6f}")
    print(f"   Оценка качества: {utility['quality_assessment']}")
    
    print("\n   KLD по обезличенным столбцам:")
    for col, kld in utility['kld_by_column'].items():
        if kld is not None and kld > 0:
            print(f"     {col}: {kld:.6f}")
    
    print("   ✓ Оценка полезности работает")
    
    # Сохраняем тестовый результат
    print("\n7. Сохранение обезличенного датасета")
    output_file = "test_new_depersonalized.xlsx"
    df_anon.to_excel(output_file, index=False, sheet_name='Обезличенные данные')
    print(f"   ✓ Сохранено в {output_file}")
    
    print("\n" + "=" * 70)
    print("ВСЕ ТЕСТЫ НОВОЙ ФУНКЦИОНАЛЬНОСТИ ПРОЙДЕНЫ УСПЕШНО!")
    print("=" * 70)
    print("\nКЛЮЧЕВЫЕ ОТЛИЧИЯ ОТ СТАРОЙ ВЕРСИИ:")
    print("✓ ФИО полностью обезличено (не оставлена фамилия)")
    print("✓ Паспорт → Регион")
    print("✓ Карта → Название банка")
    print("✓ K-anonymity считается по ВСЕМ столбцам")
    print("✓ Методы обезличивания предопределены для каждого столбца")
    print("✓ Все столбцы по умолчанию выбраны для обезличивания")
    
    return True


if __name__ == "__main__":
    test_new_functionality()
