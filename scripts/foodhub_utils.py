import csv

def read_csv(filepath):
    """
    Reads CSV file and returns list of dictionaries
    """
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return list(reader)
    except FileNotFoundError:
        print(f"[ERROR] File not found: {filepath}")
        return []


def validate_not_null(data, column):
    """
    Checks null or empty values in a column
    """
    null_count = 0

    for row in data:
        if row.get(column) in [None, ""]:
            null_count += 1

    return {
        'column': column,
        'null_count': null_count,
        'valid': null_count == 0
    }


def count_duplicates(data, key_column):
    """
    Counts duplicate values in key column
    """
    seen = set()
    duplicates = 0

    for row in data:
        key = row.get(key_column)
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    return duplicates


def log_summary(table_name, row_count, null_report, dup_count):
    """
    Logs formatted summary
    """
    print(f"[FoodHub] {table_name} | rows: {row_count} | nulls in {null_report['column']}: {null_report['null_count']} | duplicates: {dup_count}")


if __name__ == "__main__":
    data = read_csv("../data/orders.csv")

    null_report = validate_not_null(data, "order_id")
    dup_count = count_duplicates(data, "order_id")

    log_summary("orders", len(data), null_report, dup_count)