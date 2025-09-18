"""
Валидаторы для проверки форматов и уникальности данных
"""

import re
from typing import Dict, Set, Tuple, Optional


def validate_passport_format(passport: str, country: str) -> bool:
    """
    Проверка формата паспорта в зависимости от страны
    
    Args:
        passport: номер паспорта
        country: код страны (ru, by, kz)
    
    Returns:
        True если формат корректен
    """
    patterns = {
        "ru": r"^\d{4} \d{6}$",  # 1234 123456
        "by": r"^[A-Z]{2}\d{7}$",  # AB1234567
        "kz": r"^N\d{8}$"  # N12345678
    }
    
    if country not in patterns:
        return False
    
    return bool(re.match(patterns[country], passport))


def validate_snils_format(snils: str) -> bool:
    """
    Проверка формата СНИЛС и контрольной суммы
    
    Args:
        snils: номер СНИЛС в формате XXX-XXX-XXX YY
    
    Returns:
        True если формат и контрольная сумма корректны
    """
    # Проверяем формат
    if not re.match(r"^\d{3}-\d{3}-\d{3} \d{2}$", snils):
        return False
    
    # Извлекаем цифры
    digits = snils.replace("-", "").replace(" ", "")
    if len(digits) != 11:
        return False
    
    # Проверяем контрольную сумму
    number_part = digits[:9]
    control_sum = int(digits[9:11])
    
    calculated_sum = 0
    for i, digit in enumerate(number_part):
        calculated_sum += int(digit) * (9 - i)
    
    if calculated_sum < 100:
        return calculated_sum == control_sum
    elif calculated_sum == 100 or calculated_sum == 101:
        return control_sum == 0
    else:
        calculated_sum = calculated_sum % 101
        if calculated_sum == 100:
            return control_sum == 0
        return calculated_sum == control_sum


def validate_card_number(card_number: str) -> bool:
    """
    Проверка номера банковской карты по алгоритму Луна
    
    Args:
        card_number: номер карты
    
    Returns:
        True если номер корректен
    """
    # Убираем пробелы
    card_clean = card_number.replace(" ", "")
    
    # Проверяем что только цифры и длина 16
    if not card_clean.isdigit() or len(card_clean) != 16:
        return False
    
    # Алгоритм Луна
    total = 0
    reverse_digits = card_clean[::-1]
    
    for i, char in enumerate(reverse_digits):
        digit = int(char)
        
        if i % 2 == 1:  # Каждая вторая цифра
            digit *= 2
            if digit > 9:
                digit = digit // 10 + digit % 10
        
        total += digit
    
    return total % 10 == 0


def validate_iso_datetime(datetime_str: str) -> bool:
    """
    Проверка формата ISO 8601 с часовым поясом
    
    Args:
        datetime_str: строка даты-времени
    
    Returns:
        True если формат корректен
    """
    # Паттерн ISO 8601 с часовым поясом
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}[+-]\d{2}:\d{2}$"
    return bool(re.match(pattern, datetime_str))


def validate_working_hours(hour: int, minute: int, work_start: int = 9, work_end: int = 18) -> bool:
    """
    Проверка что время находится в рабочих часах
    
    Args:
        hour: час (0-23)
        minute: минута (0-59)  
        work_start: начало рабочего дня
        work_end: конец рабочего дня
    
    Returns:
        True если время рабочее
    """
    if hour < work_start or hour >= work_end:
        return False
    return True


def validate_working_day(weekday: int, work_days: list = None) -> bool:
    """
    Проверка что день является рабочим
    
    Args:
        weekday: день недели (0=Monday, 6=Sunday)
        work_days: список рабочих дней
    
    Returns:
        True если день рабочий
    """
    if work_days is None:
        work_days = [0, 1, 2, 3, 4]  # Пн-Пт
    
    return weekday in work_days


class UniquenessTracker:
    """
    Класс для отслеживания уникальности значений
    """
    
    def __init__(self):
        self.passports: Set[str] = set()
        self.snils_by_client: Dict[Tuple[str, str], str] = {}  # (fio, passport) -> snils
        self.card_usage: Dict[str, int] = {}  # card_number -> usage_count
        
    def is_passport_unique(self, passport: str) -> bool:
        """Проверка уникальности паспорта"""
        return passport not in self.passports
    
    def add_passport(self, passport: str) -> bool:
        """Добавление паспорта в отслеживание"""
        if self.is_passport_unique(passport):
            self.passports.add(passport)
            return True
        return False
    
    def get_client_snils(self, fio: str, passport: str) -> Optional[str]:
        """Получение СНИЛС клиента"""
        client_key = (fio, passport)
        return self.snils_by_client.get(client_key)
    
    def add_client_snils(self, fio: str, passport: str, snils: str) -> bool:
        """Добавление СНИЛС клиента"""
        client_key = (fio, passport)
        if client_key not in self.snils_by_client:
            self.snils_by_client[client_key] = snils
            return True
        return False
    
    def can_use_card(self, card_number: str, limit: int = 5) -> bool:
        """Проверка можно ли использовать карту"""
        current_usage = self.card_usage.get(card_number, 0)
        return current_usage < limit
    
    def use_card(self, card_number: str) -> bool:
        """Использование карты (увеличение счетчика)"""
        if self.can_use_card(card_number):
            self.card_usage[card_number] = self.card_usage.get(card_number, 0) + 1
            return True
        return False
    
    def get_stats(self) -> Dict[str, int]:
        """Получение статистики уникальности"""
        return {
            "unique_passports": len(self.passports),
            "unique_clients": len(self.snils_by_client), 
            "cards_in_use": len(self.card_usage),
            "total_card_usage": sum(self.card_usage.values())
        }


class DataValidator:
    """
    Главный класс для валидации всех данных
    """
    
    def __init__(self):
        self.uniqueness = UniquenessTracker()
        self.errors = []
    
    def validate_record(self, record: dict) -> Tuple[bool, list]:
        """
        Полная валидация записи датасета
        
        Args:
            record: словарь с данными записи
        
        Returns:
            (is_valid, errors_list)
        """
        errors = []
        
        # Проверка ФИО
        fio = record.get("FIO", "")
        if not fio or len(fio.split()) != 3:
            errors.append("ФИО должно содержать фамилию, имя и отчество")
        
        # Проверка паспорта
        passport = record.get("passport_data", "")
        country = record.get("passport_country", "ru")
        if not validate_passport_format(passport, country):
            errors.append(f"Неверный формат паспорта для страны {country}")
        
        # Проверка СНИЛС
        snils = record.get("SNILS", "")
        if not validate_snils_format(snils):
            errors.append("Неверный формат или контрольная сумма СНИЛС")
        
        # Проверка даты визита
        visit_date = record.get("visit_date", "")
        if not validate_iso_datetime(visit_date):
            errors.append("Неверный формат даты визита")
        
        # Проверка даты анализов
        analysis_date = record.get("analysis_date", "")
        if not validate_iso_datetime(analysis_date):
            errors.append("Неверный формат даты анализов")
        
        # Проверка номера карты
        card_number = record.get("payment_card", "")
        if not validate_card_number(card_number):
            errors.append("Неверный номер банковской карты")
        
        # Проверка стоимости
        cost_str = record.get("analysis_cost", "")
        if not cost_str.endswith(" руб."):
            errors.append("Стоимость должна быть указана в рублях")
        
        return len(errors) == 0, errors
    
    def validate_uniqueness(self, record: dict) -> Tuple[bool, list]:
        """
        Проверка уникальности данных в записи
        
        Args:
            record: словарь с данными записи
        
        Returns:
            (is_valid, errors_list)  
        """
        errors = []
        
        # Проверка уникальности паспорта
        passport = record.get("passport_data", "")
        if not self.uniqueness.is_passport_unique(passport):
            # Это может быть повторный визит того же клиента
            pass  # Не ошибка
        
        # Проверка лимита использования карты
        card_number = record.get("payment_card", "")
        if not self.uniqueness.can_use_card(card_number):
            errors.append("Превышен лимит использования банковской карты")
        
        return len(errors) == 0, errors
    
    def get_validation_stats(self) -> dict:
        """Получение статистики валидации"""
        stats = self.uniqueness.get_stats()
        stats["total_errors"] = len(self.errors)
        return stats


def validate_symptoms_count(symptoms_list: list, max_count: int = 10) -> bool:
    """Проверка количества симптомов"""
    return 1 <= len(symptoms_list) <= max_count


def validate_analyses_count(analyses_list: list, max_count: int = 5) -> bool:
    """Проверка количества анализов"""
    return 1 <= len(analyses_list) <= max_count


def validate_cost_format(cost_str: str) -> Tuple[bool, Optional[int]]:
    """
    Проверка формата стоимости
    
    Args:
        cost_str: строка стоимости
    
    Returns:
        (is_valid, amount)
    """
    if not cost_str.endswith(" руб."):
        return False, None
    
    try:
        amount_str = cost_str.replace(" руб.", "").replace(" ", "")
        amount = int(amount_str)
        return amount > 0, amount
    except ValueError:
        return False, None


def validate_doctor_specialization(specialization: str, valid_specializations: list) -> bool:
    """Проверка корректности специализации врача"""
    return specialization in valid_specializations


def validate_symptom(symptom: str, valid_symptoms: list) -> bool:
    """Проверка корректности симптома"""
    return symptom in valid_symptoms


def validate_analysis(analysis: str, valid_analyses: list) -> bool:
    """Проверка корректности анализа"""
    return analysis in valid_analyses