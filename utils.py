# utils.py
import re
from pyspark.sql import Row
from pyspark.sql.functions import when, col, lit
from dateutil import parser

# Sort-merge join for sorted, partitioned RDDs
def custom_sort_merge_join(rdd2, rdd1):
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

# Send a query to OpenAI to get a SQL statement
def determine_user_query(client, query):
    response = client.completions.create(
        model="gpt-3.5-turbo-instruct",
        prompt=query,
        max_tokens=200,
        temperature=0.1,
        stop=None
    )
    return response.choices[0].text.strip()

# Convert input string to appropriate Python type
def cast_value(val, dtype):
    if dtype == 'int':
        return int(val)
    elif dtype == 'date':
        return parser.parse(val)
    elif dtype == 'double':
        return float(val)
    return val
