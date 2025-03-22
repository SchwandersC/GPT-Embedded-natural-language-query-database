import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, expr

# Function to process UPDATE queries
def process_update(sql_query):
    table_name_pattern = r"UPDATE\s+(\w+)"
    set_row_pattern = r"SET\s+(.*?)\s+WHERE\s+(.*)"
    
    table_name_match = re.search(table_name_pattern, sql_query)
    set_row_match = re.search(set_row_pattern, sql_query)
    
    if not table_name_match or not set_row_match:
        print("Error: Invalid UPDATE query format.")
        return

    table_name = table_name_match.group(1)
    set_row_text = set_row_match.group(1)
    where_clause = set_row_match.group(2)

    # Process SET clause into a dictionary of column-value pairs
    columns_and_values = [pair.strip() for pair in set_row_text.split(",")]
    column_values = {}
    for pair in columns_and_values:
        column, value = [item.strip() for item in pair.split("=")]
        column_values[column] = value

    # Process WHERE clause into a dictionary of conditions
    where_conditions = [condition.strip() for condition in where_clause.split("AND")]
    where_dict = {}
    for condition in where_conditions:
        column, value = [item.strip() for item in condition.split("=")]
        where_dict[column] = value

    spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()
    file_path = f"{table_name}.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    update_script = "df"
    for column, value in column_values.items():
        update_script += f".withColumn('{column}', when("
        conditions = []
        for where_column, where_value in where_dict.items():
            if where_value.endswith(';'):
                where_value = where_value[:-1]
            conditions.append(f"(col({repr(where_column)}) == lit({where_value}))")
        update_script += f"{' & '.join(conditions)}, lit({value})).otherwise(col({repr(column)})))"
    
    updated_df = eval(update_script)
    print("updated")
    updated_df.toPandas().to_csv(file_path, index=False)
    spark.stop()


# Function to process DELETE queries
def process_delete(sql_query):
    table_pattern = r"DELETE\s+FROM\s+(\w+)"
    table_match = re.search(table_pattern, sql_query)
    if not table_match:
        print("Error: Invalid DELETE query format.")
        return
    table_name = table_match.group(1)
    
    condition_pattern = r'\bWHERE\b\s*(.*?);'
    condition_match = re.search(condition_pattern, sql_query)
    if not condition_match:
        print("Error: DELETE query missing condition.")
        return
    condition = condition_match.group(1).strip()

    spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()
    file_path = f"{table_name}.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    filtered_df = df.filter(~expr(condition))
    filtered_df.toPandas().to_csv(file_path, index=False)
    print("Deleted")
    spark.stop()


# Helper function to parse WHERE clauses in SELECT queries
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


# Function to process SELECT queries
def process_select(sql_query):
    spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()
    data = None

    if "JOIN" not in sql_query:
        table_name = None
        query_lines = sql_query.split('\n')
        for line in query_lines:
            if 'FROM' in line:
                match = re.search(r'FROM\s+(\w+)(?:\s+(\w+))?\s*$', line)
                if match:
                    table_name = match.group(1)
                    break
        if not table_name:
            print("Error: No table specified in SELECT query.")
            spark.stop()
            return

        file_path = f"{table_name}.csv"
        data = spark.read.csv(file_path, header=True, inferSchema=True).rdd

    else:
        words = sql_query.split()
        joins = sum(1 for word in words if word == 'JOIN')
        query_lines = sql_query.split('\n')
        table_dict = {}
        join_pairs = []

        for line in query_lines:
            if 'FROM' in line:
                match = re.search(r'FROM\s+(\w+)(?:\s+(\w+))?\s*$', line)
                if match:
                    table_name = match.group(1)
                    alias = match.group(2) if match.group(2) else table_name
                    table_dict[alias] = table_name
            elif 'JOIN' in line:
                match = re.search(r'JOIN\s+(\w+)(?:\s+(\w+))?\s+ON', line)
                if match:
                    table_name = match.group(1)
                    alias = match.group(2) if match.group(2) else table_name
                    table_dict[alias] = table_name
                pattern = r'\.\s*([a-zA-Z_][a-zA-Z0-9_]*)'
                join_vals = re.findall(pattern, line)
                join_pairs.append(join_vals)

        rdd_list = []
        join_num = 1

        def custom_sort_merge_join(left, right):
            # Placeholder for custom join logic.
            return []

        for alias, table in table_dict.items():
            file_path = f'{table}.csv'
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            if not rdd_list:
                left_columns = df.columns
                index = join_pairs[0][0]
                new_rdd = df.rdd.map(lambda x: (x[index], x)).sortByKey()
                rdd_list.append(new_rdd)
            else:
                index = join_pairs[join_num - 1][1]
                right_columns = df.columns
                new_rdd = df.rdd.map(lambda x: (x[index], x)).sortByKey()
                joined_data = custom_sort_merge_join(rdd_list[-1].collect(), new_rdd.collect())
                print("joined")
                index_list = [index]
                script_str = 'spark.sparkContext.parallelize(joined_data).flatMap(lambda x: [(x[0]'
                left_used_cols = []
                right_used_cols = []
                for col in right_columns:
                    if col not in index_list:
                        script_str += f", x[1][\"{col}\"]"
                        right_used_cols.append(col)
                for col in left_columns:
                    if col not in index_list and col not in right_columns:
                        script_str += f", x[2][\"{col}\"]"
                        left_used_cols.append(col)
                script_str += "]) )"
                rdd_rows = eval(script_str)
                df_result = spark.createDataFrame(rdd_rows, index_list + right_used_cols + left_used_cols)
                if join_num < len(join_pairs):
                    print("still going")
                    left_columns = [col for col in left_columns if col != join_pairs[join_num - 1][0]]
                    index = join_pairs[join_num - 1][0]
                    new_rdd = df_result.rdd.map(lambda x: (x[index], x)).sortByKey()
                    join_num += 1
                    rdd_list.append(new_rdd)
                else:
                    data = df_result.rdd

    # Process additional clauses: WHERE, GROUP BY, ORDER BY, HAVING, LIMIT, SELECT
    where_string = ''
    group_string = ''
    order_string = ''
    having_string = ''
    select_string = ''
    limit = 100
    query_lines = sql_query.split('\n')

    for line in query_lines:
        if 'WHERE' in line:
            columns, conditions, values, junctions = parse_where_string(line)
            new_conditions = ['==' if con == '=' else con for con in conditions]
            where_string = '.filter(lambda r: '
            for i, (col, con, val) in enumerate(zip(columns, new_conditions, values)):
                val_str = f"'{val}'" if isinstance(val, str) else str(val)
                where_string += f'r.{col} {con} {val_str}'
                if i < len(junctions):
                    where_string += f" {junctions[i].lower()} "
            where_string += ")"
        if "GROUP BY" in line:
            pattern = re.compile(r'\.\s*([A-Za-z_]+)')
            match = pattern.search(line)
            if match:
                grouper = match.group(1)
            if "COUNT" in sql_query:
                group_string = f'.map(lambda x: (x["{grouper.upper()}"], 1)).reduceByKey(lambda x, y: x+y)'
            elif "MAX" in sql_query:
                group_string = f'.map(lambda x: (x["{grouper.upper()}"], x[1])).groupByKey().mapValues(max)'
            elif "MIN" in sql_query:
                group_string = f'.map(lambda x: (x["{grouper.upper()}"], x[1])).groupByKey().mapValues(min)'
            elif "AVG" in sql_query:
                group_string = f'.groupBy(lambda x: (x["{grouper.upper()}"], x[1])).mapValues(lambda values: (sum(v[2] for v in values), len(values))).mapValues(lambda sum_count: sum_count[0] / sum_count[1])'
            elif "SUM" in sql_query:
                group_string = f'.map(lambda x: (x["{grouper.upper()}"], x[1])).groupByKey().mapValues(lambda values: sum(v[2] for v in values))'
        if "ORDER BY" in line:
            words = line.split()
            last_word = words[-1].rstrip(';').lower()
            desc = False if last_word == "desc" else True
            order_string = f'.sortBy(lambda x: x[1], ascending={desc})'
        if "HAVING" in line:
            columns, conditions, values, junctions = parse_where_string(line)
            if conditions[0] == '=':
                conditions[0] = '=='
            having_string = f'.filter(lambda r: r[1] {conditions[0]} {values[0]})'
        if "LIMIT" in line:
            vals = line.split()
            num = vals[-1].rstrip(';').lower()
            limit = int(num)
        if "SELECT" in line:
            matches = re.findall(r'\.\s*([\w_]+)', line)
            select_string = '.map(lambda x: '
            if len(matches) > 1:
                select_string += '('
                for val in matches:
                    select_string += f'x.{val.upper()}'
                select_string += ')'
            else:
                select_string += f'x.{matches[0].upper()})'

    if group_string != '':
        select_string = ''

    final_script = 'data' + where_string + group_string + having_string + order_string + select_string + f'.take({limit})'
    result = eval(final_script)
    print(result)
    spark.stop()


# Main dispatch function to decide which query to process
def process_sql_query(sql_query):
    if "UPDATE" in sql_query:
        process_update(sql_query)
    elif "DELETE" in sql_query:
        process_delete(sql_query)
    elif "SELECT" in sql_query:
        process_select(sql_query)
    else:
        print("Error: Unsupported SQL query type.")


# Optional: if you want to test this module directly, you can include:
if __name__ == "__main__":
    sql_query = input("Enter your SQL query: ")
    process_sql_query(sql_query)
