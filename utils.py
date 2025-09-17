"""
Вспомогательные утилиты для генерации датасета
"""

import random
import string
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Optional
from data_dictionaries import (
    SLAVIC_SURNAMES, SLAVIC_NAMES_MALE, SLAVIC_NAMES_FEMALE,
    SLAVIC_PATRONYMICS_MALE, SLAVIC_PATRONYMICS_FEMALE,
    SYMPTOMS_DICT, DOCTORS_SPECIALIZATIONS, MEDICAL_ANALYSES,
    SYMPTOM_DOCTOR_MAPPING, DOCTOR_ANALYSIS_MAPPING
)
from config import (
    WORK_HOURS_START, WORK_HOURS_END, WORK_DAYS, TIMEZONE,
    BANK_BINS, BANKS_DISTRIBUTION, PAYMENT_SYSTEMS_DISTRIBUTION,
    PASSPORT_FORMATS, PASSPORT_COUNTRIES_DISTRIBUTION,
    ANALYSIS_MIN_HOURS, ANALYSIS_MAX_HOURS
)


def generate_slavic_fio() -> Tuple[str, str]:
    """
    Генерация славянского ФИО
    
    Returns:
        (full_name, gender) где gender = 'M' или 'F'
    """
    gender = random.choice(['M', 'F'])
    surname = random.choice(SLAVIC_SURNAMES)
    
    if gender == 'M':
        name = random.choice(SLAVIC_NAMES_MALE)
        patronymic = random.choice(SLAVIC_PATRONYMICS_MALE)
    else:
        name = random.choice(SLAVIC_NAMES_FEMALE)
        patronymic = random.choice(SLAVIC_PATRONYMICS_FEMALE)
        # Добавляем женские окончания к фамилиям при необходимости
        if surname.endswith('ов') or surname.endswith('ин') or surname.endswith('ский'):
            if surname.endswith('ов'):
                surname = surname[:-2] + 'ова'
            elif surname.endswith('ин'):
                surname = surname[:-2] + 'ина'
            elif surname.endswith('ский'):
                surname = surname[:-3] + 'ская'
    
    full_name = f"{surname} {name} {patronymic}"
    return full_name, gender


def generate_passport_number(country: str = "ru") -> str:
    """
    Генерация номера паспорта для указанной страны
    
    Args:
        country: код страны (ru, by, kz)
    
    Returns:
        номер паспорта в соответствующем формате
    """
    if country == "ru":
        series = random.randint(1000, 9999)
        number = random.randint(100000, 999999)
        return f"{series} {number}"
    
    elif country == "by":
        prefix = random.choice(PASSPORT_FORMATS["by"]["prefixes"])
        number = random.randint(1000000, 9999999)
        return f"{prefix}{number}"
    
    elif country == "kz":
        number = random.randint(10000000, 99999999)
        return f"N{number}"
    
    else:
        raise ValueError(f"Неподдерживаемая страна: {country}")


def generate_snils_number() -> str:
    """
    Генерация номера СНИЛС с корректной контрольной суммой
    
    Returns:
        номер СНИЛС в формате XXX-XXX-XXX YY
    """
    # Генерируем 9 цифр
    digits = [random.randint(0, 9) for _ in range(9)]
    
    # Вычисляем контрольную сумму
    control_sum = 0
    for i, digit in enumerate(digits):
        control_sum += digit * (9 - i)
    
    # Определяем контрольное число
    if control_sum < 100:
        control_number = control_sum
    elif control_sum in [100, 101]:
        control_number = 0
    else:
        control_sum = control_sum % 101
        control_number = 0 if control_sum == 100 else control_sum
    
    # Форматируем
    digits_str = ''.join(map(str, digits))
    formatted = f"{digits_str[:3]}-{digits_str[3:6]}-{digits_str[6:9]} {control_number:02d}"
    
    return formatted


def select_country_by_probability() -> str:
    """
    Выбор страны паспорта согласно вероятностному распределению
    
    Returns:
        код страны
    """
    rand = random.random()
    cumulative = 0
    
    for country, prob in PASSPORT_COUNTRIES_DISTRIBUTION.items():
        cumulative += prob
        if rand <= cumulative:
            return country
    
    return "ru"  # fallback


def generate_symptoms(min_count: int = 1, max_count: int = 10) -> List[str]:
    """
    Генерация списка симптомов
    
    Args:
        min_count: минимальное количество симптомов
        max_count: максимальное количество симптомов
    
    Returns:
        список симптомов
    """
    count = random.randint(min_count, max_count)
    # Используем weighted choice для более реалистичного распределения
    weights = [100 if i < 1000 else 50 if i < 3000 else 10 for i in range(len(SYMPTOMS_DICT))]
    return random.choices(SYMPTOMS_DICT, weights=weights, k=count)


def select_doctor_by_symptoms(symptoms: List[str]) -> str:
    """
    Выбор врача на основе симптомов с учетом вероятности
    
    Args:
        symptoms: список симптомов
    
    Returns:
        специализация врача
    """
    # Подсчет совпадений симптомов с каждым врачом
    doctor_scores = {}
    
    for doctor, doctor_symptoms in SYMPTOM_DOCTOR_MAPPING.items():
        score = 0
        for symptom in symptoms:
            if any(doc_symptom in symptom for doc_symptom in doctor_symptoms):
                score += 1
        doctor_scores[doctor] = score
    
    # Если есть совпадения, выбираем врача с наибольшим score
    if any(score > 0 for score in doctor_scores.values()):
        max_score = max(doctor_scores.values())
        best_doctors = [doc for doc, score in doctor_scores.items() if score == max_score]
        return random.choice(best_doctors)
    
    # Если совпадений нет, идем к терапевту (с 70% вероятностью) или случайному врачу
    if random.random() < 0.7:
        return "терапевт"
    else:
        return random.choice(DOCTORS_SPECIALIZATIONS)


def generate_analyses_by_doctor(doctor: str, symptoms: List[str], 
                               min_count: int = 1, max_count: int = 5) -> List[str]:
    """
    Генерация анализов на основе специализации врача и симптомов
    
    Args:
        doctor: специализация врача
        symptoms: список симптомов
        min_count: минимальное количество анализов
        max_count: максимальное количество анализов
    
    Returns:
        список анализов
    """
    count = random.randint(min_count, max_count)
    
    # Получаем типичные анализы для данного врача
    doctor_analyses = DOCTOR_ANALYSIS_MAPPING.get(doctor, [])
    
    # Добавляем общие анализы
    common_analyses = ["общий анализ крови", "общий анализ мочи"]
    
    # Формируем список кандидатов
    candidates = doctor_analyses + common_analyses
    
    # Если кандидатов мало, добавляем случайные
    if len(candidates) < count:
        additional = random.choices(MEDICAL_ANALYSES, k=count - len(candidates))
        candidates.extend(additional)
    
    # Выбираем уникальные анализы
    selected = []
    while len(selected) < count and candidates:
        analysis = random.choice(candidates)
        if analysis not in selected:
            selected.append(analysis)
        candidates = [a for a in candidates if a != analysis]
    
    return selected


def generate_working_datetime(start_date: datetime = None, 
                            end_date: datetime = None) -> datetime:
    """
    Генерация даты и времени в рабочие дни и часы
    
    Args:
        start_date: начальная дата (по умолчанию - 30 дней назад)
        end_date: конечная дата (по умолчанию - сегодня)
    
    Returns:
        дата и время в рабочий период
    """
    if start_date is None:
        start_date = datetime.now() - timedelta(days=30)
    if end_date is None:
        end_date = datetime.now()
    
    # Генерируем случайную дату в диапазоне
    date_range = (end_date - start_date).days
    random_days = random.randint(0, date_range)
    target_date = start_date + timedelta(days=random_days)
    
    # Проверяем, что это рабочий день
    while target_date.weekday() not in WORK_DAYS:
        target_date += timedelta(days=1)
        if target_date > end_date:
            target_date = start_date
            while target_date.weekday() not in WORK_DAYS:
                target_date += timedelta(days=1)
    
    # Генерируем рабочее время
    work_hour = random.randint(WORK_HOURS_START, WORK_HOURS_END - 1)
    work_minute = random.choice([0, 15, 30, 45])  # Кратно 15 минутам
    
    return target_date.replace(hour=work_hour, minute=work_minute, second=0, microsecond=0)


def generate_analysis_datetime(visit_datetime: datetime) -> datetime:
    """
    Генерация даты получения анализов (через 24-72 часа после визита)
    
    Args:
        visit_datetime: дата и время визита
    
    Returns:
        дата и время получения анализов
    """
    # Случайное количество часов в диапазоне 24-72
    hours_later = random.randint(ANALYSIS_MIN_HOURS, ANALYSIS_MAX_HOURS)
    analysis_datetime = visit_datetime + timedelta(hours=hours_later)
    
    # Убеждаемся, что это рабочий день и время
    attempts = 0
    max_attempts = 10
    
    while attempts < max_attempts:
        # Проверяем рабочий день
        if analysis_datetime.weekday() not in WORK_DAYS:
            # Переносим на следующий рабочий день
            days_to_add = 1
            while (analysis_datetime + timedelta(days=days_to_add)).weekday() not in WORK_DAYS:
                days_to_add += 1
            analysis_datetime = analysis_datetime + timedelta(days=days_to_add)
        
        # Корректируем время на рабочие часы
        if analysis_datetime.hour < WORK_HOURS_START:
            analysis_datetime = analysis_datetime.replace(hour=WORK_HOURS_START, minute=0)
        elif analysis_datetime.hour >= WORK_HOURS_END:
            # Переносим на следующий рабочий день, начало рабочего времени
            analysis_datetime = analysis_datetime.replace(hour=WORK_HOURS_START, minute=0)
            analysis_datetime += timedelta(days=1)
        
        # Проверяем что итоговая дата не превышает лимит в 72 часа
        hours_diff = (analysis_datetime - visit_datetime).total_seconds() / 3600
        if hours_diff <= ANALYSIS_MAX_HOURS:
            break
            
        # Если превышает лимит, выбираем более раннюю дату
        hours_later = random.randint(ANALYSIS_MIN_HOURS, min(60, ANALYSIS_MAX_HOURS))
        analysis_datetime = visit_datetime + timedelta(hours=hours_later)
        attempts += 1
    
    return analysis_datetime


def format_datetime_iso(dt: datetime) -> str:
    """
    Форматирование даты в ISO 8601 с часовым поясом
    
    Args:
        dt: объект datetime
    
    Returns:
        строка в формате ISO 8601
    """
    return dt.strftime(f"%Y-%m-%dT%H:%M{TIMEZONE}")


def generate_bank_card() -> Tuple[str, str, str]:
    """
    Генерация номера банковской карты
    
    Returns:
        (card_number, bank, payment_system)
    """
    # Выбираем банк по вероятности
    rand = random.random()
    cumulative = 0
    selected_bank = "sberbank"
    
    for bank, prob in BANKS_DISTRIBUTION.items():
        cumulative += prob
        if rand <= cumulative:
            selected_bank = bank
            break
    
    # Выбираем платежную систему по вероятности
    rand = random.random()
    cumulative = 0
    selected_system = "mir"
    
    for system, prob in PAYMENT_SYSTEMS_DISTRIBUTION.items():
        cumulative += prob
        if rand <= cumulative:
            selected_system = system
            break
    
    # Получаем BIN код
    bin_codes = BANK_BINS[selected_bank][selected_system]
    bin_code = random.choice(bin_codes)
    
    # Генерируем остальные 10 цифр
    remaining_digits = ''.join([str(random.randint(0, 9)) for _ in range(9)])
    
    # Формируем номер без контрольной цифры
    partial_number = bin_code + remaining_digits
    
    # Вычисляем контрольную цифру по алгоритму Луна
    def luhn_checksum(card_num):
        total = 0
        reverse_digits = card_num[::-1]
        
        for i, char in enumerate(reverse_digits):
            digit = int(char)
            
            if i % 2 == 1:  # Каждая вторая цифра справа
                digit *= 2
                if digit > 9:
                    digit = digit // 10 + digit % 10
            
            total += digit
        
        return total % 10
    
    # Находим контрольную цифру
    for check_digit in range(10):
        test_number = partial_number + str(check_digit)
        if luhn_checksum(test_number) == 0:
            full_number = test_number
            break
    else:
        # Fallback если что-то пошло не так
        full_number = partial_number + "0"
    
    # Форматируем с пробелами
    formatted_number = f"{full_number[:4]} {full_number[4:8]} {full_number[8:12]} {full_number[12:16]}"
    
    return formatted_number, selected_bank, selected_system


def calculate_analysis_cost(analyses: List[str]) -> int:
    """
    Расчет стоимости анализов
    
    Args:
        analyses: список анализов
    
    Returns:
        общая стоимость в рублях
    """
    from config import ANALYSIS_COSTS, COST_RANGES
    
    total_cost = 0
    
    for analysis in analyses:
        if analysis in ANALYSIS_COSTS:
            base_cost = ANALYSIS_COSTS[analysis]
        else:
            # Определяем тип анализа и используем диапазон
            analysis_lower = analysis.lower()
            if any(keyword in analysis_lower for keyword in ['кровь', 'крови']):
                base_cost = random.randint(*COST_RANGES['кровь'])
            elif any(keyword in analysis_lower for keyword in ['моча', 'мочи']):
                base_cost = random.randint(*COST_RANGES['моча'])
            elif 'мазок' in analysis_lower:
                base_cost = random.randint(*COST_RANGES['мазок'])
            elif any(keyword in analysis_lower for keyword in ['рентген', 'рентгена']):
                base_cost = random.randint(*COST_RANGES['рентген'])
            elif 'узи' in analysis_lower:
                base_cost = random.randint(*COST_RANGES['узи'])
            elif any(keyword in analysis_lower for keyword in ['мрт', 'томография']):
                base_cost = random.randint(*COST_RANGES['мрт'])
            elif 'кт' in analysis_lower:
                base_cost = random.randint(*COST_RANGES['кт'])
            else:
                base_cost = random.randint(500, 3000)  # Базовый диапазон
        
        # Добавляем случайное отклонение ±20%
        variation = random.uniform(0.8, 1.2)
        total_cost += int(base_cost * variation)
    
    return total_cost


def weighted_choice(items: List, weights: List) -> any:
    """
    Выбор элемента с учетом весов
    
    Args:
        items: список элементов
        weights: список весов
    
    Returns:
        выбранный элемент
    """
    return random.choices(items, weights=weights, k=1)[0]


def generate_batch_clients(batch_size: int) -> List[Dict]:
    """
    Генерация пакета уникальных клиентов
    
    Args:
        batch_size: размер пакета
    
    Returns:
        список словарей с данными клиентов
    """
    clients = []
    used_passports = set()
    
    for _ in range(batch_size):
        attempts = 0
        while attempts < 100:  # Максимум 100 попыток
            fio, gender = generate_slavic_fio()
            country = select_country_by_probability()
            passport = generate_passport_number(country)
            
            if passport not in used_passports:
                used_passports.add(passport)
                snils = generate_snils_number()
                
                clients.append({
                    'fio': fio,
                    'gender': gender,
                    'passport': passport,
                    'country': country,
                    'snils': snils
                })
                break
            
            attempts += 1
    
    return clients


def format_symptoms_string(symptoms: List[str]) -> str:
    """Форматирование списка симптомов в строку"""
    return ", ".join(symptoms)


def format_analyses_string(analyses: List[str]) -> str:
    """Форматирование списка анализов в строку"""
    return ", ".join(analyses)


def format_cost_string(cost: int) -> str:
    """Форматирование стоимости в строку"""
    return f"{cost} руб."


def validate_business_logic(record: Dict) -> List[str]:
    """
    Проверка бизнес-логики записи
    
    Args:
        record: словарь с данными записи
    
    Returns:
        список ошибок бизнес-логики
    """
    errors = []
    
    # Проверяем что дата анализов после даты визита
    try:
        visit_str = record.get('visit_date', '')
        analysis_str = record.get('analysis_date', '')
        
        if visit_str and analysis_str:
            # Убираем часовой пояс для парсинга
            visit_clean = visit_str.replace(TIMEZONE, '')
            analysis_clean = analysis_str.replace(TIMEZONE, '')
            
            visit_dt = datetime.fromisoformat(visit_clean)
            analysis_dt = datetime.fromisoformat(analysis_clean)
            
            if analysis_dt <= visit_dt:
                errors.append("Дата анализов должна быть после даты визита")
            
            diff_hours = (analysis_dt - visit_dt).total_seconds() / 3600
            if diff_hours < ANALYSIS_MIN_HOURS or diff_hours > ANALYSIS_MAX_HOURS:
                errors.append(f"Анализы должны быть получены через {ANALYSIS_MIN_HOURS}-{ANALYSIS_MAX_HOURS} часов, получено: {diff_hours:.1f}")
    
    except Exception as e:
        errors.append(f"Ошибка проверки дат: {e}")
    
    return errors