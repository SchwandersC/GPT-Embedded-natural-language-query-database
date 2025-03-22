# main.py
from pyspark.sql import SparkSession
from config import client
from utils import determine_user_query
from retrieval import setup_retriever
from schema import DB_SCHEMA
from sql_processor import process_sql_query

def generate_prompt(nl_query, examples):
    return f"""
    Given an input question, use sql syntax to generate a sql query by choosing 
    one or multiple of the following tables.

    <table_schema>
    {DB_SCHEMA}
    </table_schema>

    Pay careful attention to the non nullable columns. If needed, auto generate these values if not specified by a user. Also keep in mind all columns are fully capitalized.

    Example Queries:
    <example>{examples[0]}</example>
    <example>{examples[1]}</example>
    <example>{examples[2]}</example>
    <example>{examples[3]}</example>

    Please provide the SQL query for this question:
    Question: {nl_query}
    Query:
    """

def main():
    retriever = setup_retriever()
    spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()

    while True:
        nl_query = input("DB> ")
        if nl_query.lower() == "exit":
            print("Bye!")
            break

        docs = retriever.get_relevant_documents(nl_query)
        examples = [doc.page_content for doc in docs[:4]]
        prompt = generate_prompt(nl_query, examples)

        sql = determine_user_query(client, prompt)
        print("\nGenerated SQL:\n", sql)
        process_sql_query(sql

if __name__ == "__main__":
    main()
