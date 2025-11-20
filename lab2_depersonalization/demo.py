"""
Демонстрационный скрипт использования Lab 2
"""
import pandas as pd
import sys
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

from depersonalization_methods import DepersonalizationMethods
from k_anonymity import KAnonymityCalculator
from utility_evaluator import DataUtilityEvaluator


def demo_full_workflow():
    """Полная демонстрация рабочего процесса обезличивания"""
    
    print("=" * 80)
    print(" " * 20 + "ДЕМОНСТРАЦИЯ LAB 2 - ОБЕЗЛИЧИВАНИЕ ДАННЫХ")
    print("=" * 80)
    print()
    
    # ========== ШАГ 1: ЗАГРУЗКА ДАТАСЕТА ==========
    print("ШАГ 1: ЗАГРУЗКА ДАТАСЕТА")
    print("-" * 80)
    
    input_file = "../generations/clinic_dataset_1k.xlsx"
    print(f"Загружаем: {input_file}")
    
    df_original = pd.read_excel(input_file)
    print(f"✓ Загружено {len(df_original)} записей")
    print(f"  Столбцы: {', '.join(df_original.columns)}")
    print()
    
    # ========== ШАГ 2: ВЫБОР КВАЗИ-ИДЕНТИФИКАТОРОВ ==========
    print("ШАГ 2: ВЫБОР КВАЗИ-ИДЕНТИФИКАТОРОВ")
    print("-" * 80)
    
    quasi_identifiers = ['ФИО', 'Паспортные данные', 'СНИЛС']
    print(f"Выбранные QI: {', '.join(quasi_identifiers)}")
    print()
    
    # ========== ШАГ 3: РАСЧЕТ K-ANONYMITY ДО ОБЕЗЛИЧИВАНИЯ ==========
    print("ШАГ 3: K-ANONYMITY ДО ОБЕЗЛИЧИВАНИЯ")
    print("-" * 80)
    
    k_calc = KAnonymityCalculator()
    k_before = k_calc.calculate_k_anonymity(df_original, quasi_identifiers)
    
    print(f"Минимальное K: {k_before['min_k']}")
    print(f"Максимальное K: {k_before['max_k']}")
    print(f"Среднее K: {k_before['avg_k']:.2f}")
    print(f"Уникальных комбинаций: {k_before['unique_combinations']}")
    
    meets, min_k, req_k = k_calc.check_k_threshold(k_before)
    print(f"\nТребуемое K: {req_k}")
    print(f"Соответствие: {'✓ Да' if meets else '✗ Нет (K={min_k} < {req_k})'}")
    
    print("\nТоп-5 плохих K-anonymity:")
    top_bad = k_calc.get_top_bad_k_values(k_before, top_n=5)
    for k, count, percentage in top_bad:
        print(f"  K={k}: {count} записей ({percentage:.4f}%)")
    
    unique_rows = k_calc.get_unique_rows(df_original, quasi_identifiers)
    print(f"\nУникальных строк (K=1): {len(unique_rows)}")
    print()
    
    # ========== ШАГ 4: ПРИМЕНЕНИЕ МЕТОДОВ ОБЕЗЛИЧИВАНИЯ ==========
    print("ШАГ 4: ПРИМЕНЕНИЕ МЕТОДОВ ОБЕЗЛИЧИВАНИЯ")
    print("-" * 80)
    
    depers = DepersonalizationMethods()
    df_anonymized = df_original.copy()
    
    print("Применяем методы:")
    
    # 1. Локальное обобщение для ФИО
    print("  1. Локальное обобщение (ФИО)")
    print(f"     До:  {df_original['ФИО'].iloc[0]}")
    df_anonymized = depers.generalization_local(df_anonymized, 'ФИО')
    print(f"     После: {df_anonymized['ФИО'].iloc[0]}")
    
    # 2. Маскеризация для паспорта
    print("  2. Маскеризация (Паспортные данные)")
    print(f"     До:  {df_original['Паспортные данные'].iloc[0]}")
    df_anonymized = depers.masking(df_anonymized, 'Паспортные данные')
    print(f"     После: {df_anonymized['Паспортные данные'].iloc[0]}")
    
    # 3. Маскеризация для СНИЛС
    print("  3. Маскеризация (СНИЛС)")
    print(f"     До:  {df_original['СНИЛС'].iloc[0]}")
    df_anonymized = depers.masking(df_anonymized, 'СНИЛС')
    print(f"     После: {df_anonymized['СНИЛС'].iloc[0]}")
    
    # 4. Маскеризация для карты
    print("  4. Маскеризация (Карта оплаты)")
    print(f"     До:  {df_original['Карта оплаты'].iloc[0]}")
    df_anonymized = depers.masking(df_anonymized, 'Карта оплаты')
    print(f"     После: {df_anonymized['Карта оплаты'].iloc[0]}")
    
    # 5. Обобщение для даты
    print("  5. Локальное обобщение (Дата посещения врача)")
    print(f"     До:  {df_original['Дата посещения врача'].iloc[0]}")
    df_anonymized = depers.generalization_local(df_anonymized, 'Дата посещения врача')
    print(f"     После: {df_anonymized['Дата посещения врача'].iloc[0]}")
    
    # 6. Обобщение для стоимости
    print("  6. Локальное обобщение (Стоимость анализов)")
    print(f"     До:  {df_original['Стоимость анализов'].iloc[0]}")
    df_anonymized = depers.generalization_local(df_anonymized, 'Стоимость анализов')
    print(f"     После: {df_anonymized['Стоимость анализов'].iloc[0]}")
    
    print()
    
    # ========== ШАГ 5: K-ANONYMITY ПОСЛЕ ОБЕЗЛИЧИВАНИЯ ==========
    print("ШАГ 5: K-ANONYMITY ПОСЛЕ ОБЕЗЛИЧИВАНИЯ")
    print("-" * 80)
    
    k_after = k_calc.calculate_k_anonymity(df_anonymized, quasi_identifiers)
    
    print(f"Минимальное K: {k_after['min_k']}")
    print(f"Максимальное K: {k_after['max_k']}")
    print(f"Среднее K: {k_after['avg_k']:.2f}")
    print(f"Уникальных комбинаций: {k_after['unique_combinations']}")
    
    meets, min_k, req_k = k_calc.check_k_threshold(k_after)
    print(f"\nТребуемое K: {req_k}")
    print(f"Соответствие: {'✓ Да' if meets else '✗ Нет (K={min_k} < {req_k})'}")
    
    print("\nТоп-5 плохих K-anonymity:")
    top_bad = k_calc.get_top_bad_k_values(k_after, top_n=5)
    for k, count, percentage in top_bad:
        print(f"  K={k}: {count} записей ({percentage:.4f}%)")
    
    unique_rows = k_calc.get_unique_rows(df_anonymized, quasi_identifiers)
    print(f"\nУникальных строк (K=1): {len(unique_rows)}")
    
    print("\nСРАВНЕНИЕ:")
    print(f"  До:  мин K = {k_before['min_k']}, среднее K = {k_before['avg_k']:.2f}")
    print(f"  После: мин K = {k_after['min_k']}, среднее K = {k_after['avg_k']:.2f}")
    print(f"  Улучшение: {'✓' if k_after['min_k'] >= k_before['min_k'] else '✗'}")
    print()
    
    # ========== ШАГ 6: ОЦЕНКА ПОЛЕЗНОСТИ ДАННЫХ (KLD) ==========
    print("ШАГ 6: ОЦЕНКА ПОЛЕЗНОСТИ ДАННЫХ (KLD)")
    print("-" * 80)
    
    evaluator = DataUtilityEvaluator()
    utility = evaluator.evaluate_utility(df_original, df_anonymized, quasi_identifiers)
    
    print("Расстояние Кульбака-Лейблера (KLD):")
    print(f"  Формула: KLD(P||Q) = Σ P(i) * log2(P(i)/Q(i))")
    print()
    
    print("KLD по столбцам:")
    for col, kld in utility['kld_by_column'].items():
        if kld is not None:
            print(f"  {col}: {kld:.6f}")
        else:
            print(f"  {col}: N/A")
    
    print()
    print(f"Общий KLD: {utility['overall_kld']:.6f}")
    print(f"Оценка качества: {utility['quality_assessment']}")
    
    print("\nИнтерпретация:")
    print("  • KLD < 0.1 : Отличная полезность")
    print("  • KLD < 0.5 : Хорошая полезность")
    print("  • KLD < 1.0 : Удовлетворительная полезность")
    print("  • KLD < 2.0 : Низкая полезность")
    print("  • KLD >= 2.0: Очень низкая полезность")
    print()
    
    # ========== ШАГ 7: СОХРАНЕНИЕ РЕЗУЛЬТАТА ==========
    print("ШАГ 7: СОХРАНЕНИЕ ОБЕЗЛИЧЕННОГО ДАТАСЕТА")
    print("-" * 80)
    
    output_file = "demo_depersonalized.xlsx"
    df_anonymized.to_excel(output_file, index=False, sheet_name='Обезличенные данные')
    print(f"✓ Обезличенный датасет сохранен: {output_file}")
    print()
    
    # ========== ИТОГОВАЯ СВОДКА ==========
    print("=" * 80)
    print(" " * 30 + "ИТОГОВАЯ СВОДКА")
    print("=" * 80)
    print()
    print(f"✓ Датасет обезличен с использованием 6 методов")
    print(f"✓ Исходный размер: {len(df_original)} записей")
    print(f"✓ Обезличенный размер: {len(df_anonymized)} записей")
    print(f"✓ K-anonymity: {k_before['min_k']} → {k_after['min_k']} (минимальное)")
    print(f"✓ Полезность данных (KLD): {utility['overall_kld']:.4f}")
    print(f"✓ Результат сохранен: {output_file}")
    print()
    print("=" * 80)
    print(" " * 25 + "ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
    print("=" * 80)


if __name__ == "__main__":
    demo_full_workflow()
