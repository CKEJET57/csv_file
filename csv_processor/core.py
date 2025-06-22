import argparse
import csv
from pathlib import Path
from typing import List, Dict, Tuple, Union, Callable, Any
from tabulate import tabulate


def read_csv(file_path: str) -> Tuple[List[Dict[str, Union[str, float]]], List[str]]:
    """Читает CSV-файл с автоматическим преобразованием чисел"""
    if not Path(file_path).exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise ValueError("CSV файл не содержит заголовков")

        data = []
        for row in reader:
            processed_row = {}
            for key, value in row.items():
                # Пытаемся преобразовать в число, если возможно
                try:
                    processed_row[key] = float(value)
                except ValueError:
                    processed_row[key] = value.strip() if value else value
            data.append(processed_row)

        return data, reader.fieldnames


def parse_condition(condition: str) -> Tuple[str, str, str]:
    """Разбирает условие вида 'column>value' на (колонка, оператор, значение)"""
    operators = ['>=', '<=', '=', '>', '<']
    for op in operators:
        if op in condition:
            column, value = condition.split(op, maxsplit=1)
            return column.strip(), op, value.strip()
    raise ValueError(f"Некорректное условие: {condition}")


def apply_filter(
        data: List[Dict[str, Union[str, float]]],
        condition: str
) -> List[Dict[str, Union[str, float]]]:
    """Фильтрует данные по условию с автоматическим определением типов"""
    column, operator, value = parse_condition(condition)

    filtered_data = []
    for row in data:
        row_value = row.get(column)
        if row_value is None:
            continue

        # Пытаемся сравнить как числа
        try:
            row_num = float(row_value) if not isinstance(row_value, (int, float)) else row_value
            val_num = float(value)

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
            else:
                condition_met = False

        except (ValueError, TypeError):
            # Если не получилось - сравниваем как строки
            str_row = str(row_value)
            str_val = str(value)

            if operator == '=':
                condition_met = str_row == str_val
            elif operator == '>':
                condition_met = str_row > str_val
            elif operator == '<':
                condition_met = str_row < str_val
            else:
                condition_met = False

        if condition_met:
            filtered_data.append(row)

    return filtered_data


AGGREGATIONS = {
    'avg': lambda x: sum(x) / len(x),
    'min': min,
    'max': max,
    'median': lambda x: sorted(x)[len(x) // 2] if x else 0,
    'sum': sum,
    'count': len
}


def apply_aggregation(
        data: List[Dict[str, Union[str, float]]],
        condition: str
) -> List[Dict[str, Union[str, float]]]:
    """Применяет агрегацию к данным"""
    if '=' not in condition:
        raise ValueError("Условие агрегации должно быть в формате 'column=operation'")

    column, operation = condition.split('=', maxsplit=1)
    column, operation = column.strip(), operation.strip()

    if operation not in AGGREGATIONS:
        raise ValueError(f"Неизвестная операция агрегации: {operation}. Доступные: {', '.join(AGGREGATIONS.keys())}")

    try:
        values = [float(row[column]) if not isinstance(row[column], (int, float)) else row[column]
                  for row in data if column in row]
    except (ValueError, TypeError):
        raise ValueError(f"Нельзя агрегировать нечисловую колонку: {column}")

    if not values:
        return [{'operation': operation, 'column': column, 'value': None}]

    result = AGGREGATIONS[operation](values)
    return [{'operation': operation, 'column': column, 'value': result}]


SORT_ORDERS = {
    'asc': lambda x: x,
    'desc': lambda x: -x if isinstance(x, (int, float)) else str(x)[::-1],
}


def apply_order_by(
        data: List[Dict[str, Union[str, float]]],
        condition: str
) -> List[Dict[str, Union[str, float]]]:
    """Сортирует данные по колонке"""
    if '=' not in condition:
        raise ValueError("Условие сортировки должно быть в формате 'column=order'")

    column, order = condition.split('=', maxsplit=1)
    column, order = column.strip(), order.strip()

    if order not in SORT_ORDERS:
        raise ValueError(f"Неизвестный порядок сортировки: {order}. Используйте 'asc' или 'desc'")

    try:
        return sorted(data, key=lambda x: SORT_ORDERS[order](x.get(column, 0)))
    except TypeError:
        return sorted(data, key=lambda x: SORT_ORDERS[order](str(x.get(column, ''))))


def cli():
    """Точка входа для командной строки"""
    parser = argparse.ArgumentParser(
        description='Обработчик CSV файлов с фильтрацией, агрегацией и сортировкой',
        epilog='Примеры:\n'
               '  python -m csv_processor data.csv --where "price>500"\n'
               '  python -m csv_processor data.csv --aggregate "rating=avg"\n'
               '  python -m csv_processor data.csv --order-by "name=desc"'
    )
    parser.add_argument('file', help='Путь к CSV файлу')
    parser.add_argument('--where', help='Условие фильтрации (например, "price>500")')
    parser.add_argument('--aggregate', help='Агрегация (например, "rating=avg")')
    parser.add_argument('--order-by', help='Сортировка (например, "name=desc")')

    args = parser.parse_args()

    try:
        # Чтение и обработка данных
        data, headers = read_csv(args.file)

        if args.where:
            data = apply_filter(data, args.where)

        if args.order_by:
            data = apply_order_by(data, args.order_by)

        # Вывод результатов
        if args.aggregate:
            result = apply_aggregation(data, args.aggregate)
            print(tabulate(result, headers="keys", tablefmt="grid", floatfmt=".2f"))
        else:
            print(tabulate(data, headers=headers, tablefmt="grid", floatfmt=".2f"))

    except Exception as e:
        print(f"\033[91mОшибка:\033[0m {e}")


if __name__ == '__main__':
    cli()
