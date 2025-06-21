import pytest
import csv
import os
from csv_processor.main import (
    read_csv,
    parse_condition,
    apply_filter,
    apply_aggregation,
    apply_order_by
)


@pytest.fixture
def sample_csv(tmp_path):
    """Фикстура создает тестовый CSV файл"""
    csv_data = [
        {"name": "Alice", "age": "30", "salary": "5000", "department": "HR"},
        {"name": "Bob", "age": "25", "salary": "4000", "department": "IT"},
        {"name": "Charlie", "age": "35", "salary": "6000", "department": "IT"}
    ]
    csv_path = tmp_path / "test.csv"
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["name", "age", "salary", "department"])
        writer.writeheader()
        writer.writerows(csv_data)
    return csv_path


@pytest.fixture
def empty_csv(tmp_path):
    """Фикстура создает пустой CSV файл"""
    csv_path = tmp_path / "empty.csv"
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["name", "age"])
        writer.writeheader()
    return csv_path


def test_read_csv_success(sample_csv):
    """Тест успешного чтения CSV"""
    data, headers = read_csv(sample_csv)
    assert len(data) == 3
    assert headers == ["name", "age", "salary", "department"]
    assert data[0]["name"] == "Alice"


def test_read_csv_file_not_found():
    """Тест обработки отсутствующего файла"""
    with pytest.raises(FileNotFoundError):
        read_csv("nonexistent_file.csv")


def test_parse_condition():
    """Тест разбора условий"""
    assert parse_condition("age>25") == ("age", ">", "25")
    assert parse_condition("name=Alice") == ("name", "=", "Alice")
    assert parse_condition("salary<=5000") == ("salary", "<=", "5000")

    with pytest.raises(ValueError):
        parse_condition("invalid_condition")


@pytest.mark.parametrize("condition,expected_count", [
    ("age>25", 2),  # Alice (30) и Charlie (35)
    ("age<30", 1),  # Bob (25)
    ("age=25", 1),  # Bob
    ("name=Alice", 1),
    ("department=IT", 2),
    ("salary>=5000", 2)
])
def test_apply_filter(sample_csv, condition, expected_count):
    """Параметризованный тест фильтрации"""
    data, _ = read_csv(sample_csv)
    filtered = apply_filter(data, condition)
    assert len(filtered) == expected_count


def test_apply_filter_empty_data(empty_csv):
    """Тест фильтрации с пустыми данными"""
    data, _ = read_csv(empty_csv)
    filtered = apply_filter(data, "age>20")
    assert len(filtered) == 0


@pytest.mark.parametrize("operation,expected", [
    ("avg", 5000.0),  # (5000 + 4000 + 6000) / 3
    ("min", 4000.0),
    ("max", 6000.0),
    ("median", 5000.0)  # После сортировки: 4000, 5000, 6000
])
def test_apply_aggregation(sample_csv, operation, expected):
    """Тест агрегационных функций"""
    data, _ = read_csv(sample_csv)
    result = apply_aggregation(data, f"salary={operation}")
    assert result[0]["value"] == expected


def test_apply_aggregation_errors(sample_csv):
    """Тест обработки ошибок агрегации"""
    data, _ = read_csv(sample_csv)

    # Нечисловая колонка
    with pytest.raises(ValueError, match="Нельзя агрегировать"):
        apply_aggregation(data, "name=avg")

    # Неизвестная операция
    with pytest.raises(ValueError, match="Неизвестная операция"):
        apply_aggregation(data, "salary=unknown")


def test_apply_order_by_asc(sample_csv):
    """Тест сортировки по возрастанию"""
    data, _ = read_csv(sample_csv)
    sorted_data = apply_order_by(data, "age=asc")
    assert [d["name"] for d in sorted_data] == ["Bob", "Alice", "Charlie"]


def test_apply_order_by_desc(sample_csv):
    """Тест сортировки по убыванию"""
    data, _ = read_csv(sample_csv)
    sorted_data = apply_order_by(data, "age=desc")
    assert [d["name"] for d in sorted_data] == ["Charlie", "Alice", "Bob"]


def test_apply_order_by_text(sample_csv):
    """Тест сортировки текстовых значений"""
    data, _ = read_csv(sample_csv)
    sorted_data = apply_order_by(data, "name=asc")
    assert [d["name"] for d in sorted_data] == ["Alice", "Bob", "Charlie"]


def test_apply_order_by_errors(sample_csv):
    """Тест ошибок сортировки"""
    data, _ = read_csv(sample_csv)

    # Неизвестный порядок
    with pytest.raises(ValueError, match="Неизвестный порядок"):
        apply_order_by(data, "age=invalid")

    # Несуществующая колонка
    with pytest.raises(KeyError):
        apply_order_by(data, "invalid_column=asc")