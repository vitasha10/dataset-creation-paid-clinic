"""
Главное приложение для обезличивания данных с GUI на Tkinter (переработанное)
"""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
from pathlib import Path
import sys
import time

from depersonalization_methods import DepersonalizationMethods
from k_anonymity import KAnonymityCalculator
from utility_evaluator import DataUtilityEvaluator


class DepersonalizationApp:
    """Приложение для обезличивания данных"""
    
    # Столбцы датасета
    DATASET_COLUMNS = [
        'ФИО',
        'Паспортные данные',
        'СНИЛС',
        'Симптомы',
        'Выбор врача',
        'Дата посещения врача',
        'Анализы',
        'Дата получения анализов',
        'Стоимость анализов',
        'Карта оплаты'
    ]
    
    def __init__(self, root):
        """
        Инициализация приложения
        
        Args:
            root: корневое окно Tkinter
        """
        self.root = root
        self.root.title("Лабораторная работа №2 - Обезличивание данных (обновленная версия)")
        self.root.geometry("1200x800")
        
        # Данные
        self.original_df = None
        self.anonymized_df = None
        self.input_file = None
        self.output_file = None
        
        # Модули
        self.depers_methods = DepersonalizationMethods()
        self.k_calculator = KAnonymityCalculator()
        self.utility_evaluator = DataUtilityEvaluator()
        
        # Квази-идентификаторы (все по умолчанию выбраны)
        self.quasi_identifiers_vars = {}
        
        # Результаты k-anonymity
        self.k_analysis = None
        
        # Создаем GUI
        self.create_menu()
        self.create_widgets()
    
    def create_menu(self):
        """Создание верхнего меню"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # Меню Файл
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Файл", menu=file_menu)
        file_menu.add_command(label="Открыть датасет", command=self.load_dataset)
        file_menu.add_command(label="Сохранить датасет", command=self.save_dataset)
        file_menu.add_separator()
        file_menu.add_command(label="Выход", command=self.root.quit)
        
        # Меню Операции
        operations_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Операции", menu=operations_menu)
        operations_menu.add_command(label="Рассчитать K-anonymity", command=self.calculate_k_anonymity)
        operations_menu.add_command(label="Обезличить", command=self.depersonalize_dataset)
        operations_menu.add_separator()
        operations_menu.add_command(label="Оценить полезность данных", command=self.evaluate_utility)
    
    def create_widgets(self):
        """Создание виджетов GUI"""
        # Основной контейнер
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Настройка весов для растягивания
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)
        
        # Заголовки (две большие строки)
        header_frame = ttk.Frame(main_frame)
        header_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        header_frame.columnconfigure(0, weight=1)
        header_frame.columnconfigure(1, weight=1)
        
        left_header = ttk.Label(header_frame, text="Выберите квази-идентификаторы", 
                               font=('Arial', 14, 'bold'))
        left_header.grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        right_header = ttk.Label(header_frame, text="Топ плохих K-anonymity", 
                                font=('Arial', 14, 'bold'))
        right_header.grid(row=0, column=1, sticky=tk.W, padx=(10, 0))
        
        # Левая панель - выбор квази-идентификаторов
        left_frame = ttk.LabelFrame(main_frame, text="Квази-идентификаторы (методы предопределены)", padding="10")
        left_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 5))
        
        # Чекбоксы для квази-идентификаторов (ВСЕ ПО УМОЛЧАНИЮ ВЫБРАНЫ)
        for i, column in enumerate(self.DATASET_COLUMNS):
            var = tk.BooleanVar(value=True)  # ПО УМОЛЧАНИЮ ВСЕ ВЫБРАНЫ!
            self.quasi_identifiers_vars[column] = var
            cb = ttk.Checkbutton(left_frame, text=column, variable=var)
            cb.grid(row=i, column=0, sticky=tk.W, pady=2)
        
        # Кнопка выбора всех
        select_all_btn = ttk.Button(left_frame, text="Выбрать все", 
                                    command=self.select_all_qi)
        select_all_btn.grid(row=len(self.DATASET_COLUMNS), column=0, pady=(10, 0), sticky=tk.W)
        
        # Кнопка очистки
        clear_btn = ttk.Button(left_frame, text="Очистить", 
                              command=self.clear_all_qi)
        clear_btn.grid(row=len(self.DATASET_COLUMNS)+1, column=0, pady=(5, 0), sticky=tk.W)
        
        # Правая панель - результаты K-anonymity
        right_frame = ttk.LabelFrame(main_frame, text="Топ плохих K-anonymity", padding="10")
        right_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(5, 0))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(0, weight=1)
        
        # Текстовое поле для отображения результатов
        self.k_results_text = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, 
                                                        width=40, height=20)
        self.k_results_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Нижняя панель - файлы ввода/вывода
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        bottom_frame.columnconfigure(1, weight=1)
        bottom_frame.columnconfigure(3, weight=1)
        
        # Заголовок для файлов
        files_header_frame = ttk.Frame(bottom_frame)
        files_header_frame.grid(row=0, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 5))
        files_header_frame.columnconfigure(0, weight=1)
        files_header_frame.columnconfigure(1, weight=1)
        
        input_header = ttk.Label(files_header_frame, text="Имя файла ввода", 
                                font=('Arial', 12, 'bold'))
        input_header.grid(row=0, column=0, sticky=tk.W)
        
        output_header = ttk.Label(files_header_frame, text="Имя файла вывода", 
                                 font=('Arial', 12, 'bold'))
        output_header.grid(row=0, column=1, sticky=tk.W, padx=(20, 0))
        
        # Поле ввода для входного файла
        ttk.Label(bottom_frame, text="Dataset:").grid(row=1, column=0, sticky=tk.W, padx=(0, 5))
        self.input_file_var = tk.StringVar()
        input_entry = ttk.Entry(bottom_frame, textvariable=self.input_file_var, width=40)
        input_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 5))
        
        # Кнопка выбора входного файла
        input_btn = ttk.Button(bottom_frame, text="Выбрать...", 
                              command=self.select_input_file)
        input_btn.grid(row=2, column=1, sticky=tk.W, pady=(5, 0))
        
        # Поле ввода для выходного файла
        ttk.Label(bottom_frame, text="Depersonalization:").grid(row=1, column=2, 
                                                                sticky=tk.W, padx=(20, 5))
        self.output_file_var = tk.StringVar()
        output_entry = ttk.Entry(bottom_frame, textvariable=self.output_file_var, width=40)
        output_entry.grid(row=1, column=3, sticky=(tk.W, tk.E))
        
        # Кнопка выбора выходного файла
        output_btn = ttk.Button(bottom_frame, text="Выбрать...", 
                               command=self.select_output_file)
        output_btn.grid(row=2, column=3, sticky=tk.W, pady=(5, 0))
        
        # Строка состояния
        self.status_var = tk.StringVar(value="Готов к работе. K-anonymity считается по ВСЕМ столбцам датасета.")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, 
                              relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
    
    def select_all_qi(self):
        """Выбрать все квази-идентификаторы"""
        for var in self.quasi_identifiers_vars.values():
            var.set(True)
    
    def clear_all_qi(self):
        """Очистить все квази-идентификаторы"""
        for var in self.quasi_identifiers_vars.values():
            var.set(False)
    
    def get_selected_qi(self):
        """Получить список выбранных квази-идентификаторов"""
        selected = []
        for column, var in self.quasi_identifiers_vars.items():
            if var.get():
                selected.append(column)
        return selected
    
    def select_input_file(self):
        """Выбор входного файла"""
        filename = filedialog.askopenfilename(
            title="Выберите входной файл",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if filename:
            self.input_file_var.set(filename)
            self.load_dataset(filename)
    
    def select_output_file(self):
        """Выбор выходного файла"""
        filename = filedialog.asksaveasfilename(
            title="Выберите выходной файл",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if filename:
            self.output_file_var.set(filename)
    
    def load_dataset(self, filename=None):
        """Загрузка датасета с оптимизацией для больших файлов"""
        try:
            if filename is None:
                filename = filedialog.askopenfilename(
                    title="Выберите входной файл",
                    filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
                )
            
            if not filename:
                return
            
            self.status_var.set("Загрузка датасета... Пожалуйста, подождите...")
            self.root.update()
            
            start_time = time.time()
            
            # Загружаем файл с оптимизацией
            # Используем chunksize для больших файлов
            print(f"Загрузка файла: {filename}")
            self.original_df = pd.read_excel(filename, engine='openpyxl')
            self.anonymized_df = self.original_df.copy()
            self.input_file = filename
            
            load_time = time.time() - start_time
            print(f"Файл загружен за {load_time:.2f} сек")
            
            # Обновляем поле ввода
            self.input_file_var.set(filename)
            
            # Проверяем столбцы
            missing_columns = [col for col in self.DATASET_COLUMNS 
                             if col not in self.original_df.columns]
            
            if missing_columns:
                messagebox.showwarning(
                    "Предупреждение",
                    f"В датасете отсутствуют столбцы:\n" + "\n".join(missing_columns)
                )
            
            self.status_var.set(f"Датасет загружен: {len(self.original_df)} записей за {load_time:.2f} сек")
            messagebox.showinfo("Успех", f"Датасет загружен успешно!\nЗаписей: {len(self.original_df)}\nВремя: {load_time:.2f} сек")
            
        except Exception as e:
            self.status_var.set("Ошибка загрузки")
            messagebox.showerror("Ошибка", f"Не удалось загрузить файл:\n{str(e)}")
    
    def save_dataset(self):
        """Сохранение обезличенного датасета"""
        try:
            if self.anonymized_df is None:
                messagebox.showwarning("Предупреждение", "Нет данных для сохранения")
                return
            
            # Получаем имя выходного файла
            output_file = self.output_file_var.get()
            
            if not output_file:
                output_file = filedialog.asksaveasfilename(
                    title="Сохранить обезличенный датасет",
                    defaultextension=".xlsx",
                    filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
                )
            
            if not output_file:
                return
            
            self.status_var.set("Сохранение датасета... Пожалуйста, подождите...")
            self.root.update()
            
            start_time = time.time()
            
            # Сохраняем
            self.anonymized_df.to_excel(output_file, index=False, sheet_name='Обезличенные данные', engine='openpyxl')
            
            save_time = time.time() - start_time
            
            self.output_file = output_file
            self.output_file_var.set(output_file)
            
            self.status_var.set(f"Датасет сохранен: {output_file} за {save_time:.2f} сек")
            messagebox.showinfo("Успех", f"Датасет сохранен успешно!\n{output_file}\nВремя: {save_time:.2f} сек")
            
        except Exception as e:
            self.status_var.set("Ошибка сохранения")
            messagebox.showerror("Ошибка", f"Не удалось сохранить файл:\n{str(e)}")
    
    def calculate_k_anonymity(self):
        """Расчет K-анонимности по ВСЕМ столбцам"""
        try:
            if self.anonymized_df is None:
                messagebox.showwarning("Предупреждение", "Сначала загрузите датасет")
                return
            
            self.status_var.set("Расчет K-anonymity по ВСЕМ столбцам... Пожалуйста, подождите...")
            self.root.update()
            
            start_time = time.time()
            
            # Рассчитываем K-анонимность по ВСЕМ столбцам
            self.k_analysis = self.k_calculator.calculate_k_anonymity(self.anonymized_df)
            
            calc_time = time.time() - start_time
            
            # Отображаем результаты
            self.display_k_results()
            
            self.status_var.set(f"K-anonymity рассчитан за {calc_time:.2f} сек (по всем {len(self.anonymized_df.columns)} столбцам)")
            
        except Exception as e:
            self.status_var.set("Ошибка расчета")
            messagebox.showerror("Ошибка", f"Не удалось рассчитать K-anonymity:\n{str(e)}")

    # Фрагмент: только безопасные изменения в отображении результатов.
    def display_k_results(self):
        if self.k_analysis is None:
            return

        self.k_results_text.delete(1.0, tk.END)
        output = []
        output.append("=" * 50)
        output.append("РЕЗУЛЬТАТЫ K-ANONYMITY")
        output.append("(расчет по ВСЕМ столбцам)")
        output.append("=" * 50)
        output.append("")
        output.append(f"Всего записей: {self.k_analysis['total_records']}")
        output.append(f"Уникальных комбинаций: {self.k_analysis['unique_combinations']}")
        output.append(f"Минимальное K: {self.k_analysis['min_k']}")
        output.append(f"Максимальное K: {self.k_analysis['max_k']}")
        avg_k = self.k_analysis['avg_k']
        output.append(f"Среднее K: {avg_k:.2f}" if avg_k is not None else "Среднее K: N/A")
        output.append("")

        meets_threshold, min_k, required_k = self.k_calculator.check_k_threshold(self.k_analysis)
        output.append(f"Требуемое K для датасета: {required_k}")
        output.append("✓ Датасет соответствует требованиям (K >= {0})".format(required_k) if meets_threshold
                      else "✗ Датасет НЕ соответствует требованиям (K = {0} < {1})".format(min_k, required_k))
        output.append("")

        output.append("ТОП-5 ПЛОХИХ K-ANONYMITY:")
        output.append("-" * 50)
        top_bad_k = self.k_calculator.get_top_bad_k_values(self.k_analysis, top_n=5)
        if top_bad_k:
            for k_value, count, percentage in top_bad_k:
                output.append(f"K={k_value}: {count} записей ({percentage:.4f}%)")
        else:
            output.append("Нет данных")
        output.append("")

        # Без материализации df на больших наборах: берем число K=1 из распределения
        k1_groups = self.k_analysis['k_distribution'].get(1, 0)
        k1_records = k1_groups  # 1 * group_count
        output.append(f"УНИКАЛЬНЫЕ СТРОКИ (K=1): {k1_records}")
        if self.k_analysis['total_records'] > 0:
            output.append(f"Процент от общего: {(k1_records / self.k_analysis['total_records'] * 100):.2f}%")

        # Если очень нужно показать сами строки, делайте это только на малых датасетах:
        # if self.k_analysis['total_records'] <= 10000:
        #     unique_rows = self.k_calculator.get_unique_rows(self.anonymized_df)
        #     output.append("-" * 50)
        #     output.append(f"Первые 20 уникальных строк:")
        #     output.append(unique_rows.head(20).to_string(index=False))

        self.k_results_text.insert(1.0, "\n".join(output))

    def depersonalize_dataset(self):
        """Обезличивание датасета с предопределенными методами"""
        try:
            if self.original_df is None:
                messagebox.showwarning("Предупреждение", "Сначала загрузите датасет")
                return
            
            # Получаем выбранные столбцы
            selected_columns = self.get_selected_qi()
            
            if not selected_columns:
                messagebox.showwarning("Предупреждение", 
                                     "Выберите хотя бы один столбец для обезличивания")
                return
            
            self.status_var.set("Применение методов обезличивания... Пожалуйста, подождите...")
            self.root.update()
            
            start_time = time.time()
            
            # Копируем оригинальный датасет
            self.anonymized_df = self.original_df.copy()
            
            # Применяем предопределенные методы
            self.anonymized_df = self.depers_methods.apply_anonymization(
                self.anonymized_df, selected_columns
            )
            
            anon_time = time.time() - start_time
            
            self.status_var.set(f"Обезличивание завершено за {anon_time:.2f} сек")
            messagebox.showinfo("Успех", 
                              f"Методы обезличивания применены успешно!\n"
                              f"Обезличено столбцов: {len(selected_columns)}\n"
                              f"Время: {anon_time:.2f} сек")
            
            # Автоматически пересчитываем K-anonymity
            self.calculate_k_anonymity()
            
        except Exception as e:
            self.status_var.set("Ошибка обезличивания")
            messagebox.showerror("Ошибка", f"Не удалось применить методы:\n{str(e)}")
    
    def evaluate_utility(self):
        """Оценка полезности данных"""
        try:
            if self.original_df is None or self.anonymized_df is None:
                messagebox.showwarning("Предупреждение", 
                                     "Загрузите датасет и примените обезличивание")
                return
            
            self.status_var.set("Оценка полезности данных...")
            self.root.update()
            
            # Получаем выбранные квази-идентификаторы
            selected_qi = self.get_selected_qi()
            
            # Оцениваем полезность
            utility_results = self.utility_evaluator.evaluate_utility(
                self.original_df, self.anonymized_df, selected_qi
            )
            
            # Показываем результаты
            self.show_utility_results(utility_results)
            
            self.status_var.set("Оценка полезности завершена")
            
        except Exception as e:
            self.status_var.set("Ошибка оценки")
            messagebox.showerror("Ошибка", f"Не удалось оценить полезность:\n{str(e)}")
    
    def show_utility_results(self, utility_results):
        """Отображение результатов оценки полезности"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Оценка полезности данных")
        dialog.geometry("700x600")
        
        # Заголовок
        ttk.Label(dialog, text="Оценка полезности обезличенных данных", 
                 font=('Arial', 14, 'bold')).pack(pady=10)
        
        # Текстовое поле для результатов
        text = scrolledtext.ScrolledText(dialog, wrap=tk.WORD, width=80, height=30)
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Формируем вывод
        output = []
        output.append("=" * 70)
        output.append("РАССТОЯНИЕ КУЛЬБАКА-ЛЕЙБЛЕРА (KLD)")
        output.append("=" * 70)
        output.append("")
        output.append("KLD измеряет различие между распределениями данных.")
        output.append("Чем меньше значение, тем больше сохранена полезность данных.")
        output.append("")
        output.append("Формула: KLD(P||Q) = Σ P(i) * log2(P(i)/Q(i))")
        output.append("")
        output.append("-" * 70)
        
        # KLD по столбцам
        output.append("KLD ПО СТОЛБЦАМ:")
        output.append("-" * 70)
        
        kld_by_column = utility_results.get('kld_by_column', {})
        for column, kld in kld_by_column.items():
            if kld is not None:
                output.append(f"{column}: {kld:.6f}")
            else:
                output.append(f"{column}: N/A")
        
        output.append("")
        output.append("-" * 70)
        
        # Общий KLD
        overall_kld = utility_results.get('overall_kld')
        if overall_kld is not None:
            output.append(f"ОБЩИЙ KLD: {overall_kld:.6f}")
        else:
            output.append("ОБЩИЙ KLD: N/A")
        
        output.append("")
        
        # Качественная оценка
        quality = utility_results.get('quality_assessment', 'N/A')
        output.append(f"ОЦЕНКА КАЧЕСТВА: {quality}")
        output.append("")
        
        # Интерпретация
        output.append("ИНТЕРПРЕТАЦИЯ:")
        output.append("• KLD < 0.1 : Отличная полезность")
        output.append("• KLD < 0.5 : Хорошая полезность")
        output.append("• KLD < 1.0 : Удовлетворительная полезность")
        output.append("• KLD < 2.0 : Низкая полезность")
        output.append("• KLD >= 2.0: Очень низкая полезность")
        
        # Выводим в текстовое поле
        text.insert(1.0, "\n".join(output))
        text.config(state=tk.DISABLED)
        
        # Кнопка закрытия
        ttk.Button(dialog, text="Закрыть", command=dialog.destroy).pack(pady=10)


def main():
    """Главная функция"""
    root = tk.Tk()
    app = DepersonalizationApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
