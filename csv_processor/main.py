import argparse
import csv
from pathlib import Path
from typing import List, Dict, Tuple, Union, Callable, Any
from tabulate import tabulate


def read_csv(file_path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    """Читает CSV-файл и возвращает данные + заголовки."""
    if not Path(file_path).exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return list(reader), reader.fieldnames


def parse_condition(condition: str) -> Tuple[str, str, str]:
    """Разбирает условие вида 'column>value' на (колонка, оператор, значение)."""
    operators = ['>=', '<=', '=', '>', '<']
    for op in operators:
        if op in condition:
            column, value = condition.split(op, maxsplit=1)
            return column.strip(), op, value.strip()
    raise ValueError(f"Некорректное условие: {condition}")


def apply_filter(
        data: List[Dict[str, str]],
        condition: str
) -> List[Dict[str, str]]:
    """Фильтрует данные по условию (>, <, =)."""
    column, operator, value = parse_condition(condition)

    filtered_data = []
    for row in data:
        row_value = row[column]
        try:
            # Числовое сравнение
            row_num, val_num = float(row_value), float(value)
            if operator == '>':
                condition_met = row_num > val_num
            elif operator == '<':
                condition_met = row_num < val_num
            elif operator == '=':
                condition_met = row_num == val_num
            elif operator == '>=':
                condition_met = row_num >= val_num
            elif operator == '<=':
                condition_met = row_num <= val_num
        except ValueError:
            # Текстовое сравнение
            if operator == '=':
                condition_met = row_value == value
            elif operator == '>':
                condition_met = row_value > value
            elif operator == '<':
                condition_met = row_value < value
            else:
                raise ValueError(f"Оператор {operator} не поддерживается для текста")
        if condition_met:
            filtered_data.append(row)

    return filtered_data


# Словарь агрегационных функций
AGGREGATIONS = {
    'avg': lambda x: sum(x) / len(x),
    'min': min,
    'max': max,
    'median': lambda x: sorted(x)[len(x) // 2] if x else 0,
}


def apply_aggregation(
        data: List[Dict[str, str]],
        condition: str  # Формат: "column=operation" (например, "salary=avg")
) -> List[Dict[str, Union[str, float]]]:
    """Применяет агрегацию (avg, min, max, median) к данным."""
    if '=' not in condition:
        raise ValueError("Условие агрегации должно быть в формате 'column=operation'")

    column, operation = condition.split('=', maxsplit=1)
    column, operation = column.strip(), operation.strip()

    if operation not in AGGREGATIONS:
        raise ValueError(f"Неизвестная операция агрегации: {operation}")

    try:
        values = [float(row[column]) for row in data]
    except ValueError:
        raise ValueError(f"Нельзя агрегировать нечисловую колонку: {column}")

    result = AGGREGATIONS[operation](values)
    return [{'operation': operation, 'column': column, 'value': result}]


def apply_order_by(
        data: List[Dict[str, str]],
        condition: str  # Формат: "column=order" (например, "age=desc")
) -> List[Dict[str, str]]:
    """Сортирует данные по колонке (asc или desc)."""
    SORT_ORDERS = {
        'asc': lambda x: x,
        'desc': lambda x: -x if isinstance(x, (int, float)) else x[::-1],
    }
    if '=' not in condition:
        raise ValueError("Условие сортировки должно быть в формате 'column=order'")

    column, order = condition.split('=', maxsplit=1)
    column, order = column.strip(), order.strip()

    if order not in SORT_ORDERS:
        raise ValueError(f"Неизвестный порядок сортировки: {order}")

    try:
        return sorted(data, key=lambda x: SORT_ORDERS[order](float(x[column])))
    except ValueError:
        return sorted(data, key=lambda x: SORT_ORDERS[order](x[column]))


def main(): # pragma: no cover
    """Точка входа для CLI."""
    parser = argparse.ArgumentParser(description='Обработка CSV-файлов.')
    parser.add_argument('file', help='Путь к CSV-файлу')
    parser.add_argument('--where', help='Фильтрация (например, "age>25")')
    parser.add_argument('--aggregate', help='Агрегация (например, "salary=avg")')
    parser.add_argument('--order-by', help='Сортировка (например, "name=desc")')

    args = parser.parse_args()

    try:
        data, headers = read_csv(args.file)

        if args.where:
            data = apply_filter(data, args.where)

        if args.order_by:
            data = apply_order_by(data, args.order_by)

        if args.aggregate:
            result = apply_aggregation(data, args.aggregate)
            print(tabulate(result, headers="keys", tablefmt="grid"))
        else:
            print(tabulate(data, headers=headers, tablefmt="grid"))

    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == '__main__':
    main()