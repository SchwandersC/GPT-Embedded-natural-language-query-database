# sql_executor.py

import re
from pyspark.sql.functions import expr, when, col, lit
from pyspark.sql import Row
from dateutil import parser

def custom_sort_merge_join(rdd1, rdd2):
    result = []
    iterator1, iterator2 = iter(rdd1), iter(rdd2)
    current1, current2 = next(iterator1, None), next(iterator2, None)

    while current1 is not None and current2 is not None:
        key1, value1 = current1
        key2, value2 = current2

        if key1 < key2:
            current1 = next(iterator1, None)
        elif key1 > key2:
            current2 = next(iterator2, None)
        else:
            result.append((key1, value1, value2))
            current2 = next(iterator2, None)

    return result


def parse_where_string(where_string):
    column_pattern = re.compile(r'\b\.([A-Za-z_]+)\b')
    condition_pattern = re.compile(r'([<>]=?|=)')
    value_pattern = re.compile(r"'([^']*)'|\"([^\"]*)\"|(\d+)")
    junction_pattern = re.compile(r'\b(AND|OR)\b')

    columns = column_pattern.findall(where_string)
    conditions = condition_pattern.findall(where_string)
    values_match = value_pattern.findall(where_string)
    values = [val[0] or val[1] or int(val[2]) for val in values_match]
    junctions = junction_pattern.findall(where_string)

    return columns, conditions, values, junctions


def execute_sql(spark, sql):
    if sql.strip().upper().startswith("INSERT"):
        print("\n[INFO] INSERT functionality should be handled in main.py separately.")
        return

    elif sql.strip().upper().startswith("UPDATE"):
        table_name = re.search(r"UPDATE\s+(\w+)", sql).group(1)
        set_clause = re.search(r"SET\s+(.*?)\s+WHERE", sql).group(1)
        where_clause = re.search(r"WHERE\s+(.*)", sql).group(1)

        file_path = f"{table_name}.csv"
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        column_values = {k.strip(): v.strip() for k, v in (pair.split("=") for pair in set_clause.split(","))}
        conditions = [cond.strip() for cond in where_clause.split("AND")]
        where_dict = {k.strip(): v.strip().strip(';') for k, v in (cond.split("=") for cond in conditions)}

        for column, value in column_values.items():
            df = df.withColumn(column, when(
                (col(k) == lit(v) for k, v in where_dict.items()), lit(value)).otherwise(col(column)))

        df.toPandas().to_csv(file_path, index=False)
        print("[SUCCESS] Update complete.")

    elif sql.strip().upper().startswith("DELETE"):
        table_name = re.search(r"DELETE\s+FROM\s+(\w+)", sql).group(1)
        condition = re.search(r'\bWHERE\b\s*(.*?);?$', sql).group(1).strip()

        file_path = f"{table_name}.csv"
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        df = df.filter(~expr(condition))
        df.toPandas().to_csv(file_path, index=False)
        print("[SUCCESS] Delete complete.")

    elif sql.strip().upper().startswith("SELECT"):
        # Very basic SELECT support for a single table (no JOIN or GROUP BY yet)
        match = re.search(r"FROM\s+(\w+)", sql, re.IGNORECASE)
        table_name = match.group(1)
        file_path = f"{table_name}.csv"

        df = spark.read.csv(file_path, header=True, inferSchema=True)
        df.createOrReplaceTempView(table_name)

        result = spark.sql(sql)
        result.show(truncate=False)

    else:
        print("[ERROR] Unsupported or malformed SQL. Please rephrase.")
