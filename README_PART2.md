# README PART 2: Инструкции по использованию и примеры

## Быстрый старт

### Установка зависимостей
```bash
pip install -r requirements.txt
```

### Базовая генерация датасета
```bash
# Генерация стандартного датасета (50,000 записей)
python dataset_generator.py

# Генерация с кастомным размером
python dataset_generator.py --size 10000

# Генерация с фиксированным seed для воспроизводимости
python dataset_generator.py --size 1000 --seed 42

# Генерация с кастомным именем файла
python dataset_generator.py --output my_clinic_data.xlsx
```

## Подробные параметры командной строки

### Основные параметры:
- `--size N` - Размер датасета в записях (по умолчанию: 50000)
- `--seed N` - Seed для генератора случайных чисел (по умолчанию: 42)
- `--output FILE` - Имя выходного Excel файла (по умолчанию: clinic_dataset.xlsx)
- `--repeat-probability X` - Вероятность повторного визита клиента (по умолчанию: 0.25)

### Примеры использования:

#### 1. Генерация маленького тестового датасета
```bash
python dataset_generator.py --size 100 --output test.xlsx --seed 123
```

#### 2. Генерация большого датасета с высокой долей повторных визитов
```bash
python dataset_generator.py --size 100000 --repeat-probability 0.4 --output large_dataset.xlsx
```

#### 3. Генерация для исследований (воспроизводимый результат)
```bash
python dataset_generator.py --size 50000 --seed 2024 --output research_data.xlsx
```

## Структура выходных файлов

### Основной датасет (.xlsx)
Содержит следующие колонки:
1. **ФИО** - Полное имя клиента (славянские имена)
2. **Паспортные данные** - Номер паспорта (RU/BY/KZ)
3. **СНИЛС** - Страховой номер с валидной контрольной суммой
4. **Симптомы** - Список симптомов через запятую (1-10 штук)
5. **Выбор врача** - Специализация врача
6. **Дата посещения врача** - ISO 8601 формат с часовым поясом
7. **Анализы** - Список назначенных анализов (1-5 штук)
8. **Дата получения анализов** - Через 24-72 часа после визита
9. **Стоимость анализов** - Сумма в рублях
10. **Карта оплаты** - 16-значный номер карты с алгоритмом Луна

### Отчет о генерации (_report.txt)
Содержит:
- Параметры генерации
- Статистику клиентов и визитов
- Метрики качества данных
- Показатели производительности
- Информацию о структуре данных

### Лог генерации (dataset_generation.log)
Детальный лог процесса генерации с:
- Временными метками
- Предупреждениями о валидации
- Ошибками и их исправлениями
- Статистикой по пакетам

## Примеры использования данных

### 1. Анализ в Python с pandas
```python
import pandas as pd

# Загрузка датасета
df = pd.read_excel('clinic_dataset.xlsx')

# Базовая статистика
print(f"Всего записей: {len(df)}")
print(f"Уникальных клиентов: {df['СНИЛС'].nunique()}")
print(f"Повторных визитов: {len(df) - df['СНИЛС'].nunique()}")

# Топ-10 самых частых симптомов
symptoms_flat = df['Симптомы'].str.split(', ').explode()
top_symptoms = symptoms_flat.value_counts().head(10)
print("Топ-10 симптомов:")
print(top_symptoms)

# Распределение по врачам
doctor_dist = df['Выбор врача'].value_counts()
print("Распределение по врачам:")
print(doctor_dist.head())

# Анализ стоимости
df['Стоимость_число'] = df['Стоимость анализов'].str.replace(' руб.', '').astype(int)
print(f"Средняя стоимость: {df['Стоимость_число'].mean():.2f} руб.")
print(f"Медианная стоимость: {df['Стоимость_число'].median():.2f} руб.")
```

### 2. Проверка качества данных
```python
# Проверка уникальности паспортов
duplicate_passports = df['Паспортные данные'].duplicated().sum()
print(f"Дублирующихся паспортов: {duplicate_passports}")

# Проверка формата дат
from datetime import datetime
def validate_iso_date(date_str):
    try:
        datetime.fromisoformat(date_str.replace('+03:00', ''))
        return True
    except:
        return False

valid_visit_dates = df['Дата посещения врача'].apply(validate_iso_date).sum()
valid_analysis_dates = df['Дата получения анализов'].apply(validate_iso_date).sum()

print(f"Валидных дат визитов: {valid_visit_dates}/{len(df)}")
print(f"Валидных дат анализов: {valid_analysis_dates}/{len(df)}")
```

### 3. Визуализация данных
```python
import matplotlib.pyplot as plt
import seaborn as sns

# Распределение по времени визитов
df['Дата_визита'] = pd.to_datetime(df['Дата посещения врача'].str.replace('+03:00', ''))
df['Час_визита'] = df['Дата_визита'].dt.hour

plt.figure(figsize=(12, 6))
plt.subplot(1, 2, 1)
df['Час_визита'].hist(bins=range(9, 19), alpha=0.7)
plt.title('Распределение визитов по часам')
plt.xlabel('Час дня')
plt.ylabel('Количество визитов')

# Топ-15 врачей
plt.subplot(1, 2, 2)
top_doctors = df['Выбор врача'].value_counts().head(15)
top_doctors.plot(kind='barh')
plt.title('Топ-15 врачей по количеству визитов')
plt.xlabel('Количество визитов')

plt.tight_layout()
plt.show()
```

## Настройка параметров генерации

### Модификация config.py
Для изменения базовых параметров отредактируйте `config.py`:

```python
# Изменение размера датасета по умолчанию
DATASET_SIZE = 100000

# Изменение рабочих часов поликлиники
WORK_HOURS_START = 8  # с 8:00
WORK_HOURS_END = 20   # до 20:00

# Добавление субботы как рабочего дня
WORK_DAYS = [0, 1, 2, 3, 4, 5]  # Пн-Сб

# Изменение вероятности повторных визитов
CLIENT_REPEAT_PROBABILITY = 0.35  # 35%

# Изменение часового пояса
TIMEZONE = "+05:00"  # Екатеринбургское время
```

### Добавление новых банков
```python
# В config.py добавьте новый банк
BANKS_DISTRIBUTION = {
    "sberbank": 0.35,
    "vtb": 0.18,
    "alfabank": 0.15,
    "tinkoff": 0.12,
    "gazprombank": 0.08,
    "raiffeisen": 0.07,
    "new_bank": 0.05  # Новый банк
}

# Добавьте BIN-коды нового банка
BANK_BINS = {
    # ... существующие банки
    "new_bank": {
        "mir": ["220015"],
        "visa": ["498765"],
        "mastercard": ["554433"]
    }
}
```

### Расширение словарей данных
Отредактируйте `data_dictionaries.py` для добавления:

```python
# Новые симптомы
ADDITIONAL_SYMPTOMS = [
    "новый симптом 1",
    "новый симптом 2",
    # ...
]
SYMPTOMS_DICT.extend(ADDITIONAL_SYMPTOMS)

# Новые специализации врачей
NEW_SPECIALIZATIONS = [
    "новая специализация 1",
    "новая специализация 2"
]
DOCTORS_SPECIALIZATIONS.extend(NEW_SPECIALIZATIONS)

# Связь новых симптомов с врачами
SYMPTOM_DOCTOR_MAPPING.update({
    "новая специализация 1": ["новый симптом 1", "головная боль"],
    "новая специализация 2": ["новый симптом 2", "усталость"]
})
```

## Решение проблем

### Частые ошибки и решения

#### 1. "ModuleNotFoundError: No module named 'pandas'"
```bash
pip install pandas openpyxl xlsxwriter
```

#### 2. "Memory Error при генерации большого датасета"
Уменьшите размер пакета в config.py:
```python
BATCH_SIZE = 500  # Вместо 1000
```

#### 3. "Слишком много ошибок валидации"
Это нормально для синтетических данных. Типичный процент корректных записей: 75-85%.

#### 4. "Файл не открывается в Excel"
Попробуйте открыть в LibreOffice Calc или Google Sheets. Если проблема persist, сгенерируйте CSV:
```python
# В dataset_generator.py измените save_to_excel на:
df.to_csv(filename.replace('.xlsx', '.csv'), index=False, encoding='utf-8-sig')
```

### Оптимизация производительности

#### Для очень больших датасетов (500k+ записей):
1. Увеличьте размер пакета:
```python
BATCH_SIZE = 5000
```

2. Отключите подробное логирование:
```python
LOG_LEVEL = "WARNING"
```

3. Используйте SSD диск для выходных файлов

4. Выделите больше RAM:
```python
MAX_MEMORY_MB = 2048
```

### Кастомизация выходного формата

#### Добавление новых колонок:
В `dataset_generator.py`, метод `generate_visit_record()`:
```python
record = {
    # ... существующие поля
    'phone_number': generate_phone_number(),  # Новое поле
    'email': generate_email(client['fio']),   # Новое поле
}
```

#### Изменение формата дат:
```python
# В utils.py, функция format_datetime_iso():
def format_datetime_iso(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y %H:%M")  # Российский формат
```

## Интеграция с другими системами

### Экспорт в базу данных
```python
import sqlite3
import pandas as pd

# Загрузка и экспорт в SQLite
df = pd.read_excel('clinic_dataset.xlsx')
conn = sqlite3.connect('clinic_data.db')
df.to_sql('visits', conn, if_exists='replace', index=False)
conn.close()
```

### Создание API endpoint
```python
from flask import Flask, jsonify
import pandas as pd

app = Flask(__name__)
df = pd.read_excel('clinic_dataset.xlsx')

@app.route('/api/patients/<snils>')
def get_patient_visits(snils):
    patient_visits = df[df['СНИЛС'] == snils].to_dict('records')
    return jsonify(patient_visits)

@app.route('/api/stats/doctors')
def get_doctor_stats():
    stats = df['Выбор врача'].value_counts().to_dict()
    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True)
```

### ETL пайплайн
```python
# airflow_dag.py - пример DAG для Apache Airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'clinic-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'clinic_dataset_generation',
    default_args=default_args,
    description='Generate synthetic clinic dataset',
    schedule_interval='@weekly',
    catchup=False
)

generate_task = BashOperator(
    task_id='generate_dataset',
    bash_command='cd /path/to/generator && python dataset_generator.py --size 50000',
    dag=dag
)

validate_task = BashOperator(
    task_id='validate_dataset',
    bash_command='cd /path/to/generator && python validate_output.py',
    dag=dag
)

generate_task >> validate_task
```

## Дополнительные возможности

### Создание тематических подвыборок
```python
# Создание датасета только для кардиологии
df_cardio = df[df['Выбор врача'] == 'кардиолог'].copy()
df_cardio.to_excel('cardio_only.xlsx', index=False)

# Создание датасета дорогих процедур (>5000 руб)
df['Стоимость_число'] = df['Стоимость анализов'].str.replace(' руб.', '').astype(int)
df_expensive = df[df['Стоимость_число'] > 5000].copy()
df_expensive.to_excel('expensive_procedures.xlsx', index=False)
```

### Анонимизация данных
```python
# Замена ФИО на ID
df['Пациент_ID'] = 'PAT_' + df.index.astype(str).str.zfill(6)
df_anon = df.drop(['ФИО', 'Паспортные данные', 'СНИЛС'], axis=1)
df_anon.to_excel('anonymous_dataset.xlsx', index=False)
```

### Создание отчетов
```python
# Генерация бизнес-отчета
def generate_business_report(df):
    report = {
        'period': f"{df['Дата_визита'].min()} - {df['Дата_визита'].max()}",
        'total_visits': len(df),
        'unique_patients': df['СНИЛС'].nunique(),
        'total_revenue': df['Стоимость_число'].sum(),
        'avg_cost_per_visit': df['Стоимость_число'].mean(),
        'top_symptoms': df['Симптомы'].str.split(', ').explode().value_counts().head(5).to_dict(),
        'doctor_workload': df['Выбор врача'].value_counts().to_dict()
    }
    return report

# Экспорт отчета в JSON
import json
report = generate_business_report(df)
with open('business_report.json', 'w', encoding='utf-8') as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
```

## Лицензия и использование

Этот генератор датасета создан для образовательных и исследовательских целей. Сгенерированные данные являются полностью синтетическими и не содержат реальной персональной информации.

### Использование в коммерческих проектах
Разрешено с указанием авторства и источника.

### Модификация и распространение
Разрешены при сохранении уведомления об авторских правах.

### Ответственность
Автор не несет ответственности за использование сгенерированных данных в продакшн-системах без должной валидации.