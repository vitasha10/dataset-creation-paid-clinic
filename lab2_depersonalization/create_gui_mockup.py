"""
Создание визуальной демонстрации GUI для документации
"""
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import Rectangle, FancyBboxPatch
import matplotlib.lines as mlines


def create_gui_mockup():
    """Создание макета GUI"""
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 140)
    ax.set_ylim(0, 100)
    ax.axis('off')
    
    # Заголовок окна
    title_bg = FancyBboxPatch((0, 95), 140, 5, 
                              boxstyle="round,pad=0.1", 
                              edgecolor='#2C3E50', 
                              facecolor='#34495E',
                              linewidth=2)
    ax.add_patch(title_bg)
    ax.text(70, 97.5, 'Лабораторная работа №2 - Обезличивание данных', 
            ha='center', va='center', fontsize=14, weight='bold', color='white')
    
    # Меню бар
    menu_bg = Rectangle((0, 91), 140, 3, facecolor='#ECF0F1', edgecolor='black')
    ax.add_patch(menu_bg)
    ax.text(5, 92.5, 'Файл', ha='left', va='center', fontsize=10, weight='bold')
    ax.text(15, 92.5, 'Операции', ha='left', va='center', fontsize=10, weight='bold')
    
    # Заголовки панелей
    header_bg = Rectangle((2, 85), 136, 4, facecolor='#BDC3C7', edgecolor='black')
    ax.add_patch(header_bg)
    
    ax.text(35, 87, 'Выберите квази-идентификаторы', 
            ha='center', va='center', fontsize=12, weight='bold')
    ax.text(105, 87, 'Топ плохих K-anonymity', 
            ha='center', va='center', fontsize=12, weight='bold')
    
    # Левая панель - квази-идентификаторы
    left_panel = FancyBboxPatch((2, 30), 66, 53, 
                                boxstyle="round,pad=0.3", 
                                edgecolor='#2C3E50', 
                                facecolor='white',
                                linewidth=2)
    ax.add_patch(left_panel)
    
    # Заголовок левой панели
    ax.text(35, 80, 'Квази-идентификаторы', 
            ha='center', va='center', fontsize=11, weight='bold')
    
    # Чекбоксы
    columns = [
        '☑ ФИО',
        '☑ Паспортные данные',
        '☑ СНИЛС',
        '☐ Симптомы',
        '☐ Выбор врача',
        '☐ Дата посещения врача',
        '☐ Анализы',
        '☐ Дата получения анализов',
        '☐ Стоимость анализов',
        '☐ Карта оплаты'
    ]
    
    y_pos = 75
    for col in columns:
        ax.text(6, y_pos, col, ha='left', va='center', fontsize=9)
        y_pos -= 4.2
    
    # Кнопки выбора
    select_btn = FancyBboxPatch((6, 32), 20, 3, 
                                boxstyle="round,pad=0.1", 
                                edgecolor='#2980B9', 
                                facecolor='#3498DB',
                                linewidth=1)
    ax.add_patch(select_btn)
    ax.text(16, 33.5, 'Выбрать все', ha='center', va='center', 
            fontsize=8, color='white', weight='bold')
    
    clear_btn = FancyBboxPatch((28, 32), 20, 3, 
                               boxstyle="round,pad=0.1", 
                               edgecolor='#E74C3C', 
                               facecolor='#E74C3C',
                               linewidth=1)
    ax.add_patch(clear_btn)
    ax.text(38, 33.5, 'Очистить', ha='center', va='center', 
            fontsize=8, color='white', weight='bold')
    
    # Правая панель - результаты K-anonymity
    right_panel = FancyBboxPatch((72, 30), 66, 53, 
                                 boxstyle="round,pad=0.3", 
                                 edgecolor='#2C3E50', 
                                 facecolor='white',
                                 linewidth=2)
    ax.add_patch(right_panel)
    
    # Заголовок правой панели
    ax.text(105, 80, 'Топ плохих K-anonymity', 
            ha='center', va='center', fontsize=11, weight='bold')
    
    # Результаты
    results_text = [
        '=' * 45,
        'РЕЗУЛЬТАТЫ K-ANONYMITY',
        '=' * 45,
        '',
        'Всего записей: 1000',
        'Уникальных комбинаций: 785',
        'Минимальное K: 1',
        'Максимальное K: 4',
        'Среднее K: 1.27',
        '',
        'Требуемое K для датасета: 10',
        '✗ Датасет НЕ соответствует требованиям',
        '',
        'ТОП-5 ПЛОХИХ K-ANONYMITY:',
        '-' * 45,
        'K=1: 605 записей (60.5000%)',
        'K=2: 300 записей (30.0000%)',
        'K=3: 75 записей (7.5000%)',
        'K=4: 20 записей (2.0000%)',
    ]
    
    y_pos = 77
    for line in results_text:
        if '=' in line or '-' in line:
            ax.text(76, y_pos, line[:35], ha='left', va='center', 
                   fontsize=6, family='monospace', color='#7F8C8D')
        elif 'РЕЗУЛЬТАТЫ' in line or 'ТОП-5' in line:
            ax.text(76, y_pos, line, ha='left', va='center', 
                   fontsize=7, weight='bold', family='monospace')
        else:
            ax.text(76, y_pos, line, ha='left', va='center', 
                   fontsize=7, family='monospace')
        y_pos -= 2.2
    
    # Нижняя панель - файлы
    files_bg = Rectangle((2, 20), 136, 8, facecolor='#ECF0F1', edgecolor='black', linewidth=1)
    ax.add_patch(files_bg)
    
    # Заголовки файлов
    ax.text(35, 26.5, 'Имя файла ввода', ha='center', va='center', 
            fontsize=11, weight='bold')
    ax.text(105, 26.5, 'Имя файла вывода', ha='center', va='center', 
            fontsize=11, weight='bold')
    
    # Поля ввода
    input_field = Rectangle((6, 22.5), 56, 2.5, facecolor='white', 
                           edgecolor='#95A5A6', linewidth=1)
    ax.add_patch(input_field)
    ax.text(8, 23.8, 'Dataset: clinic_dataset_1k.xlsx', ha='left', va='center', 
            fontsize=8, style='italic', color='#34495E')
    
    output_field = Rectangle((76, 22.5), 56, 2.5, facecolor='white', 
                            edgecolor='#95A5A6', linewidth=1)
    ax.add_patch(output_field)
    ax.text(78, 23.8, 'Depersonalization: output_depersonalized.xlsx', 
            ha='left', va='center', fontsize=8, style='italic', color='#34495E')
    
    # Кнопки выбора файлов
    input_btn = FancyBboxPatch((6, 21), 15, 1.5, 
                               boxstyle="round,pad=0.05", 
                               edgecolor='#16A085', 
                               facecolor='#1ABC9C',
                               linewidth=1)
    ax.add_patch(input_btn)
    ax.text(13.5, 21.75, 'Выбрать...', ha='center', va='center', 
            fontsize=7, color='white', weight='bold')
    
    output_btn = FancyBboxPatch((76, 21), 15, 1.5, 
                                boxstyle="round,pad=0.05", 
                                edgecolor='#16A085', 
                                facecolor='#1ABC9C',
                                linewidth=1)
    ax.add_patch(output_btn)
    ax.text(83.5, 21.75, 'Выбрать...', ha='center', va='center', 
            fontsize=7, color='white', weight='bold')
    
    # Строка состояния
    status_bar = Rectangle((2, 0), 136, 2.5, facecolor='#34495E', 
                          edgecolor='black', linewidth=1)
    ax.add_patch(status_bar)
    ax.text(5, 1.25, 'Статус: Готов к работе. Датасет загружен: 1000 записей', 
            ha='left', va='center', fontsize=9, color='white')
    
    # Легенда с описанием функциональности
    legend_bg = FancyBboxPatch((2, 3), 136, 15, 
                               boxstyle="round,pad=0.3", 
                               edgecolor='#8E44AD', 
                               facecolor='#F8F9FA',
                               linewidth=2, linestyle='--')
    ax.add_patch(legend_bg)
    
    ax.text(70, 16.5, 'ОСНОВНОЙ ФУНКЦИОНАЛ ПРОГРАММЫ', 
            ha='center', va='center', fontsize=11, weight='bold', color='#8E44AD')
    
    features = [
        '✓ Обезличивание: 9 методов (обобщение, агрегация, возмущение, микро-агрегация, и др.)',
        '✓ K-анонимность: расчет, топ-5 плохих значений, проверка порога',
        '✓ Уникальные строки: поиск записей с K=1',
        '✓ Оценка полезности: расстояние Кульбака-Лейблера (KLD)',
        '✓ Работа с XLSX: загрузка и сохранение датасетов',
    ]
    
    y_pos = 14
    for feature in features:
        ax.text(5, y_pos, feature, ha='left', va='center', fontsize=8, color='#2C3E50')
        y_pos -= 2
    
    plt.tight_layout()
    plt.savefig('lab2_gui_mockup.png', dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    print("✓ GUI макет сохранен: lab2_gui_mockup.png")
    plt.close()


if __name__ == "__main__":
    create_gui_mockup()
