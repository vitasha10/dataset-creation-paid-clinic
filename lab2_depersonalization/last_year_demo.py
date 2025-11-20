import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox,scrolledtext
import time

df_global = None
df_original = None  # Для хранения оригинального DataFrame
stats = None
columns = ['Магазин', 'Координаты', 'Дата и время', 'Товар', 'Производитель',
           'Номер карты', 'Количество', 'Цена',
           ]

bad_locations = set(['Технополис Политехнический' ,'Красные Зори' ,'Технопарк Мебельная' ,'Глобус Джус' ,'Туристическое агентство Глобус' ,'Технопарк Рот Фронт' ,'Приневский технопарк' ,'СберМаркет' ,'Издательство Утконос' ,'Технопарк Боровая' ,'Технопарк Арсенал' ,'Азбука Вкуса' ,'Звезда' ,'Павловский коммунальный технопарк' ,'Промышленный Парк на Дерибасовской' ,'Hiker' ,'Промышленный Парк' ,'Ленполиграфмаш' ,'Машиностроительный технопарк Гагарин' ,'Технопарк Санкт-Петербурга' ,'Индустриальный парк Greenstate' ,'Технопарк Мариенбург' ,'Технопарк Новикова' ,'Рандеву' ,'Технопарк' ,'PiterGSM' ,'Street Beat Kids' ,'Технопарк Нарвский'])

item_dict = { # 10 unique categories -> micro agreggation
    'платье': 'одежда',
    'куртка': 'одежда',
    'свитер': 'одежда',
    'пальто': 'одежда',
    'костюм': 'одежда',
    'толстовка': 'одежда',
    'футболка': 'одежда',
    'кеды': 'одежда',
    'шапка': 'одежда',
    'спортивные брюки': 'одежда',
    'кроссовки': 'одежда',
    'ботинки': 'одежда',
    'рубашка': 'одежда',
    'джинсы': 'одежда',
    'шорты': 'одежда',
    'юбка': 'одежда',
    'галстук': 'одежда',
    'поло': 'одежда',
    'джемпер': 'одежда',
    'пюре': 'еда готовая',
    'сок': 'еда готовая',
    'йогурт': 'молочный продукт',
    'кефир': 'молочный продукт',
    'молоко': 'молочный продукт',
    'творог': 'молочный продукт',
    'масло': 'молочный продукт',
    'мясо': 'мясные продукты',
    'курица': 'мясные продукты',
    'индейка': 'мясные продукты',
    'специи': 'еда готовая',
    'кетчуп': 'еда готовая',
    'майонез': 'еда готовая',
    'шоколад': 'еда готовая',
    'конфеты': 'еда готовая',
    'суп': 'еда готовая',
    'консервы': 'еда готовая',
    'чипсы': 'еда готовая',
    'батончики': 'еда готовая',
    'овсянка': 'еда готовая',
    'колонки': 'фото/видео/аудио техника',
    'наушники': 'фото/видео/аудио техника',
    'наушники беспроводные': 'фото/видео/аудио техника',
    'усилитель': 'фото/видео/аудио техника',
    'телевизор': 'бытовая техника',
    'ноутбук': 'компьютерная техника',
    'планшет': 'компьютерная техника',
    'смартфон': 'компьютерная техника',
    'пылесос': 'бытовая техника',
    'микроволновка': 'бытовая техника',
    'камера': 'фото/видео/аудио техника',
    'проигрыватель': 'фото/видео/аудио техника',
    'рюкзак': 'одежда',
    'ремень': 'одежда',
    'супы': 'еда готовая',
    'сметана': 'молочный продукт',
    'крекеры': 'еда готовая',
    'творог обезжиренный': 'молочный продукт',
    'напитки': 'еда готовая',
    'соус': 'еда готовая',
    'холодильник': 'бытовая техника',
    'хлопья': 'еда готовая',
    'фотоаппарат': 'фото/видео/аудио техника',
    'умные часы': 'фото/видео/аудио техника',
    'часы': 'фото/видео/аудио техника',
    'мышь': 'компьютерная техника',
    'монитор': 'компьютерная техника',
    'фитнес-трекер': 'фото/видео/аудио техника',
    'мюсли': 'еда готовая',
    'бекон': 'мясные продукты',
    'колбаса': 'мясные продукты',
    'крупы': 'еда готовая',
    'газированные напитки': 'еда готовая',
    'гарнитура': 'фото/видео/аудио техника',
    'снеки': 'еда готовая',
    'саундбар': 'фото/видео/аудио техника',
    'вода': 'еда готовая',
    'кепка': 'одежда',
    'процессор': 'компьютерная техника',
    'консервированные овощи': 'еда готовая',
    'десерты': 'еда готовая',
    'проектор': 'компьютерная техника',
    'молоко 1.5%': 'молочный продукт',
    'туфли': 'одежда',
    'видеокарта': 'компьютерная техника',
    'соки': 'еда готовая',
    'приправы': 'еда готовая',
    'компьютер': 'компьютерная техника',
    'носки': 'одежда',
    'сыр': 'молочный продукт',
    'колбасы': 'мясные продукты',
    'утюг': 'бытовая техника',
    'сосиски': 'мясные продукты',
    'плеер': 'фото/видео/аудио техника',
    'роутер': 'компьютерная техника',
    'брюки': 'одежда',
    'молоко 3.2%': 'молочный продукт',
    'адаптер': 'компьютерная техника',
    'модем': 'компьютерная техника',
    'объектив': 'фото/видео/аудио техника',
    'клавиатура': 'компьютерная техника',
    'ряженка': 'молочный продукт',
    'лампа': 'бытовая техника',
    'зарядное устройство': 'компьютерная техника',
    'лапша': 'еда готовая',
    'квас': 'еда готовая',
    'пуховик': 'одежда',
    'материнская плата': 'компьютерная техника',
    'горчица': 'еда готовая',
    'соусы': 'еда готовая',
    'макароны': 'еда готовая',
    'спортивная куртка': 'одежда',
    'датчик сердечного ритма': 'фото/видео/аудио техника',
    'пиво': 'еда готовая',
    'вермишель': 'еда готовая',
    'бейсболка': 'одежда',
    'стиральная машина': 'бытовая техника',
    'мясные продукты': 'мясные продукты',
    'штатив': 'фото/видео/аудио техника'
}

def read_csv(file_name, delimiter=';') -> pd.DataFrame:
    df_csv = pd.read_csv(file_name, sep=delimiter)
    return df_csv

# заменяет название товара на одну из категорий (11 штук)
def item_mask(df, col):
    out = []
    for item,general_category in df[[col,'Магазин']].values:
        if item in item_dict:
            out.append(item_dict[item])
        else:
            print(item)
            out.append(general_category)
    df[col] = out
    return df

# заменяет название бренда на общую категорию (clothes food electronics)
def brand_mask(df, col):
    out = []
    for brand,general_category in df[[col,'Магазин']].values:
        out.append(general_category)
    df[col] = out
    return df

# оставляет только первые 4 цифры карты
def card_mask(df, col):
    # Определение диапазонов для MASTERCARD и VISA
    mastercard_prefixes = ['51', '52', '53', '54', '55']  # основные префиксы MASTERCARD
    visa_prefixes = ['4']  # основной префикс для VISA

    def mask_card(card_number):
        card_str = str(card_number)
        first_four = card_str[:4]

        # Определение платежной системы по первым цифрам
        if first_four[:2] in mastercard_prefixes:
            # Для MASTERCARD объединяем
            return first_four + "X" * 12
        elif first_four[:1] in visa_prefixes:
            # Для VISA объединяем
            return first_four + "X" * 12
        else:
            # Для других карт оставляем только первые 4 цифры
            return first_four + "X" * 12

    # Применение маскирования к каждому значению столбца
    df[col] = df[col].apply(mask_card)

    return df

# если у сети магазинов с одним названием много точек по городу, то меняет их общие координаты на коордианыт одного магазина
def coordinates_mask_alternative(df, col,location_stores):
    output = []
    for coordinates,shop_name in df[[col,'Магазин']].values:
        if shop_name in bad_locations:
            output.append('59.742865, 30.472608')
        elif shop_name in location_stores:
            output.append(location_stores[shop_name])
        else:
            output.append('59.933146, 30.437312')

    df[col] = output
    return df
# сохраняет только год покупки
def date_mask(df, col):
    mask = []
    for data in df[col]:
        date = data[:8] + "XXTXX" + ":" + "XX"
        mask.append(date)
    df[col] = mask
    return df

# возваращет категорию магазина (например М.Видео -> electronics, Zara -> clothes)
def get_shop_category(df, shop_name):
    # Находим строку, где название магазина совпадает с переданным
    category = df.loc[df['shop_name'] == shop_name, 'category_type']

    if not category.empty:
        return category.values[0]  # Возвращаем первый найденный тип продукта
    else:
        return 'None'

# заменяет название магазина на общую категорию (clothes food electronics)
def shop_mask(df,col):
    df_shops = pd.read_csv('input_data/shop_locations.csv', sep=';',names=['category_type','shop_name', 'coordinates'])
    out = []
    for shop_name in df[col]:
        out.append(get_shop_category(df_shops,shop_name))
    df[col] = out
    return df

# заменяет точное количество купленного товара на обощение
def amount_mask(df,col):
    out = []
    for amount in df[col]:
        if(amount == 1):
            out.append('one')
        elif(amount <= 4):
            out.append('some')
        else:
            out.append('a lot')
    df[col] = out
    return df

# заменяет точную цену купленного товара на обощение
def price_mask(df,col):
    out = []
    for price,amount in df[[col,'Количество']].values:
        price /= amount
        if price < 5000:
            out.append('< 5000')
        elif price < 10000:
            out.append('< 10000')
        elif price >= 10000:
            out.append('>= 10000')
    df[col] = out
    return df

def even_stronger_anonymization(df,selected_attributes):
    # Проверяем количество строк
    if len(df) < 30000:
        # 1. Агрегация даты до квартала года
        if 'Дата и время' in selected_attributes:
            df['Дата и время'] = df['Дата и время'].apply(lambda dt: f"{dt[:4]}-Q{(int(dt[5:7])-1)//3+1}" if isinstance(dt, str) and len(dt) >= 7 else dt)

        if 'Координаты' in selected_attributes:
            # 2. Округление координат до целого числа
            df['Координаты'] = df['Координаты'].apply(lambda coord: ', '.join([f"{round(float(x))}" for x in coord.split(", ")]) if coord != "- , -" else "- , -")

        if 'Товар' in selected_attributes:
            # 3. Упрощение категорий товаров
            df['Товар'] = df['Товар'].apply(lambda item: 'продукты питания' if item in ['еда готовая', 'йогурт', 'ряженка', 'творог обезжиренный','мясные продукты']
            else 'электроника' if item in ['фото/видео/аудио техника','бытовая техника', 'компьютерная техника']
            else 'прочее')

        if 'Производитель' in selected_attributes:
            # 4. Упрощение производителей
            df['Производитель'] = df['Производитель'].apply(lambda prod: 'прочее')

        if 'Номер карты' in selected_attributes:
            # 5. Еще более жесткая маскировка карт
            df['Номер карты'] = df['Номер карты'].apply(lambda card: str(card)[:0] + "X" * 16)

        if 'Цена' in selected_attributes:
            # 6. Сгруппировать цены и количество
            df['Цена'] = df['Цена'].apply(lambda price:
                                          '<10000' if '<5000' in str(price) or '10000' in str(price)
                                          else '>= 10000')
        if 'Количество' in selected_attributes:
            df['Количество'] = df['Количество'].apply(lambda qty: '-') #if 'some' in str(qty) else 'много')

    return df


def add_or_update_k_anonymity_column(df, quasi_identifiers):
    """
    Функция для добавления или обновления k-anonymity к каждой строке датасета
    на основе квазиидентификаторов.

    :param df: DataFrame
    :param quasi_identifiers: Список квазиидентификаторов
    :return: DataFrame с добавленным или обновленным столбцом 'k-anonymity'
    """
    # Группируем по квазиидентификаторам и считаем количество строк в каждой группе
    grouped = df.groupby(quasi_identifiers).size().reset_index(name='k-anonymity')

    # Объединяем исходный DataFrame с группированным, чтобы добавить или обновить столбец 'k-anonymity'
    if 'k-anonymity' in df.columns:
        # Обновляем существующий столбец 'k-anonymity'
        df = df.drop(columns=['k-anonymity']).merge(grouped, on=quasi_identifiers, how='left')
    else:
        # Добавляем новый столбец 'k-anonymity'
        df = df.merge(grouped, on=quasi_identifiers, how='left')

    return df.sort_values(by='k-anonymity')


def remove_worst_k_anonymity_rows(df, max_percent=0.05):
    """
    Функция для удаления строк с самым низким показателем k-anonymity,
    но не более max_percent от общего количества строк.
    Возваращет отсортированный по возрастанию датасет

    :param df: DataFrame с добавленным столбцом 'k-anonymity'
    :param max_percent: Максимальный процент строк для удаления (по умолчанию 5%)
    :return: DataFrame с удалёнными строками
    """
    # Определяем количество строк, которые нужно удалить (максимум 5%)
    n_rows_to_remove = int(len(df) * max_percent)

    # Сортируем DataFrame по столбцу 'k-anonymity' в порядке возрастания
    df_sorted = df.sort_values(by='k-anonymity')

    # Удаляем первые n строк с самым низким k-anonymity
    df_trimmed = df_sorted.iloc[n_rows_to_remove:]

    return df_trimmed

def get_store_coordinates(csv_file):
    # Чтение CSV файла с использованием ';' в качестве разделителя
    df = pd.read_csv(csv_file, delimiter=';', header=None, names=['category', 'store', 'coordinates'])

    # Создание словаря для хранения результата
    store_coordinates = {}

    # Проходим по уникальным названиям магазинов
    for store in df['store'].unique():
        # Получаем любые координаты для текущего магазина
        coords = df[df['store'] == store]['coordinates'].iloc[0]
        # Добавляем в словарь
        store_coordinates[store] = coords

    return store_coordinates


def k_anonymity_statistics(df, k_threshold=7):
    """
    Функция для подсчета количества строк, k-anonymity которых меньше указанного порога,
    а также для вычисления среднего, медианного, максимального и минимального значений k-anonymity.

    :param df: DataFrame с добавленным столбцом 'k-anonymity'
    :param k_threshold: Пороговое значение для подсчета строк (по умолчанию 7)
    :return: Словарь с количеством строк с k-anonymity меньше порога и статистикой
    """
    # Подсчет количества строк с k-anonymity меньше порога
    count_below_threshold = (df['k-anonymity'] < k_threshold).sum()

    # Вычисление статистических значений
    mean_k = df['k-anonymity'].mean()
    median_k = df['k-anonymity'].median()
    max_k = df['k-anonymity'].max()
    min_k = df['k-anonymity'].min()

    # Возвращаем статистику и количество строк
    stats = {
        'count_below_threshold': count_below_threshold,
        'percent_below_threshold': (str( round(((count_below_threshold/len(df))*100),1) ) + '%'),
        'mean_k': mean_k,
        'median_k': median_k,
        'max_k': max_k,
        'min_k': min_k,
        'number of rows': len(df),
    }

    return stats

def anonimize(df, selected_attributes):
    global stats
    """
    Анонимизация на основе выбранных квази-идентификаторов.
    
    :param df: DataFrame
    :param selected_attributes: Список выбранных атрибутов (квазиидентификаторов)
    :return: Анонимизированный DataFrame
    """

    # Загрузка данных для анонимизации координат магазинов
    location_stores = get_store_coordinates('input_data/shop_locations.csv')

    # Проверяем каждый атрибут и запускаем соответствующую функцию анонимизации
    if 'Координаты' in selected_attributes:
        print('Начата работа над Коордианатами')
        coordinates_mask_alternative(df, 'Координаты', location_stores=location_stores)

    if 'Дата и время' in selected_attributes:
        print('Начата работа над Датой и Временем')
        date_mask(df, 'Дата и время')

    if 'Магазин' in selected_attributes:
        print('Начата работа над Магазинами')
        shop_mask(df, 'Магазин')

    if 'Номер карты' in selected_attributes:
        print('Начата работа над Картами')
        card_mask(df, 'Номер карты')

    if 'Цена' in selected_attributes:
        print('Начата работа над Ценой')
        price_mask(df, 'Цена')

    if 'Количество' in selected_attributes:
        print('Начата работа над Количеством')
        amount_mask(df, 'Количество')  # Всегда после price_mask

    if 'Товар' in selected_attributes:
        print('Начата работа над Товаром')
        item_mask(df, 'Товар')

    if 'Производитель' in selected_attributes:
        print('Начата работа над Производителем')
        brand_mask(df, 'Производитель')

    quasi_identifiers = ['Магазин','Координаты','Номер карты','Количество', 'Цена','Товар','Производитель'] #

    df = even_stronger_anonymization(df,selected_attributes)
    df = add_or_update_k_anonymity_column(df,quasi_identifiers)
    df = remove_worst_k_anonymity_rows(df, max_percent=0.05)
    return df


def savefile(dataframe):
    # Открываем окно для выбора пути сохранения файла
    file_path = filedialog.asksaveasfilename(defaultextension=".csv",
                                             filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                                             title="Сохранить файл как")
    if file_path:
        try:
            # Сохраняем DataFrame в выбранном месте
            dataframe.to_csv(file_path, sep=';', index=False)
            messagebox.showinfo("Успех", f"Файл успешно сохранен: {file_path}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сохранить файл: {e}")
    else:
        messagebox.showinfo("Отмена", "Сохранение файла отменено.")


def load_file(file_label, attribute_vars):
    global df_global  # Используем глобальную переменную
    global df_original
    # Открытие диалогового окна для выбора файла
    file_path = filedialog.askopenfilename(
        filetypes=[("CSV файлы", "*.csv")], title="Выберите CSV файл"
    )

    if not file_path:
        return  # Если файл не выбран, возвращаемся

    try:
        # Попытка загрузить CSV файл
        df_original= pd.read_csv(file_path, sep=';')
        df_global= pd.read_csv(file_path, sep=';')
        # Проверка количества колонок и наличия заголовков
        required_columns = ["Магазин", "Координаты", "Дата и время", "Товар", "Производитель", "Номер карты", "Количество", "Цена"]

        if list(df_global.columns) != required_columns:
            messagebox.showerror("Ошибка", "Файл должен содержать следующие колонки: " + ', '.join(required_columns))
            df_global = None  # Обнуляем глобальный DataFrame в случае ошибки
            df_original = None
            return

        # Обновление метки с информацией о выбранном файле
        file_label.config(text=f"Выбран файл: {file_path}")

        # Устанавливаем квази-идентификаторы
        for attribute in df_global.columns:
            if attribute in attribute_vars:
                attribute_vars[attribute].set(1)

    except Exception as e:
        messagebox.showerror("Ошибка", f"Ошибка при загрузке файла: {str(e)}")
        df_global = None
        df_original = None

def show_results(df):
    head_df = df.head(7)
    result_window = tk.Toplevel()  # Создаем новое окно
    result_window.title("Результаты анонимизации")
    result_window.geometry("800x400")

    # Создаем текстовое поле с прокруткой
    text_area = scrolledtext.ScrolledText(result_window, wrap=tk.WORD, font=("Arial", 12))
    text_area.pack(expand=True, fill='both')

    # Преобразуем DataFrame в строку и вставляем в текстовое поле
    text = '\n\n'
    stats = k_anonymity_statistics(df)

    for i in stats.keys():
        text += (i + ' : ' + str(stats[i]) + '\n')
    text_area.insert(tk.END, head_df.to_string(index=False))
    text_area.insert(tk.END,text)
    text_area.configure(state='disabled')  # Делаем текстовое поле только для чтения

    load_button = tk.Button(result_window, text="Сохранить CSV файл", command=lambda: savefile(df), font=("Arial", 12))
    load_button.pack(pady=10)


def process_file(attribute_vars):
    global df_global, df_original
    if df_global is not None:
        # Проверяем, что df_original не None
        if df_original is not None:
            # Восстанавливаем DataFrame в исходное состояние
            df_global = df_original.copy()

            # Собираем выбранные атрибуты
            selected_attributes = [attr for attr, var in attribute_vars.items() if var.get() == 1]
            print(selected_attributes)
            # Передаем выбранные атрибуты в функцию анонимизации
            df_global = anonimize(df_global, selected_attributes)
            print(df_global.head())
            show_results(df=df_global)
        else:
            messagebox.showerror("Ошибка", "Оригинальные данные не загружены.")
    else:
        messagebox.showerror("Ошибка", "Файл не был загружен.")

def interface():
    root = tk.Tk()
    root.title("Обезличиватель датасета")
    root.geometry("500x600")

    greeting_label = tk.Label(root, text="Добро пожаловать! Выберите квази-идентификаторы:", font=("Arial", 14))
    greeting_label.pack(pady=20)

    file_label = tk.Label(root, text="", font=("Arial", 12))
    file_label.pack(pady=10)

    # Добавляем кнопку для загрузки файла
    load_button = tk.Button(root, text="Загрузить CSV файл", command=lambda: load_file(file_label, attribute_vars), font=("Arial", 12))
    load_button.pack(pady=10)

    attribute_vars = {}

    # Переменные для чекбоксов
    columns = ["Магазин", "Координаты", "Дата и время", "Товар", "Производитель", "Номер карты", "Количество", "Цена"]

    for attribute in columns:
        var = tk.IntVar(value=0)
        attribute_vars[attribute] = var
        checkbox = tk.Checkbutton(root, text=attribute, variable=var, font=("Arial", 12))
        checkbox.pack(anchor=tk.W)

    # Кнопка для обработки файла
    process_button = tk.Button(root, text="Обработать файл", command=lambda: process_file(attribute_vars), font=("Arial", 12))
    process_button.pack(pady=10)

    root.mainloop()




if __name__ == '__main__':
    # TODO : shop_name, item_name, brand_name, amount, price

    df = read_csv(file_name='input_data/all_types_1k.csv',delimiter=';')
    interface()