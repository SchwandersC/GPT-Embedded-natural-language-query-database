#Loading in Libraries
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import when, col
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from pyspark.sql.types import DateType
import re
from dateutil import parser
from operator import itemgetter
from langchain.prompts import ChatPromptTemplate
from langchain.chat_models import ChatOpenAI
from langchain.embeddings import OpenAIEmbeddings
from langchain.schema.output_parser import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough, RunnableLambda
from langchain.vectorstores import FAISS
from langchain.document_loaders import TextLoader
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores import Chroma
from openai import OpenAI



#Open AI API key
key ='insert here'


client = OpenAI(api_key=key)
import warnings
warnings.filterwarnings("ignore")

#Creating a function to perform join on sorted and partioned rdds
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


# Function to take user query and generate SQL
def determine_user_query(query):
    

    # Provide the feedback to ChatGPT for analysis
    response = client.completions.create(
    model="gpt-3.5-turbo-instruct",
    prompt = query, #from user query and prompt
    max_tokens=200,  # set for enough tokens to generate query
    temperature=0.1,  # Adjusted to remain conservative
    stop=None)

    # Extract generated query from the response
    generated_sql = response.choices[0].text.strip()
    
    return generated_sql



#loading in pre selected query examples. Storing in vector database of gpt embeddings. Creating retriever for later use
raw_documents = TextLoader('551_sql_query_examples.txt').load()
text_splitter = CharacterTextSplitter(chunk_size=250, chunk_overlap=0)
documents = text_splitter.split_documents(raw_documents)
db = FAISS.from_documents(documents, OpenAIEmbeddings(openai_api_key = key))
retriever = db.as_retriever()



#defining database schema for use later in prompt
db_schema = """CREATE TABLE leagues (
    LEAGUE_ID INT PRIMARY KEY,
    LEAGUE_NAME VARCHAR(255) NOT NULL
);

CREATE TABLE teams (
    LEAGUE_ID INT,
    TEAM_ID INT PRIMARY KEY,
    MIN_YEAR INT,
    MAX_YEAR INT,
    ABBREVIATION VARCHAR(10),
    NICKNAME VARCHAR(255) NOT NULL,
    YEARFOUNDED INT,
    CITY VARCHAR(255) NOT NULL,
    ARENA VARCHAR(255),
    ARENACAPACITY INT,
    OWNER VARCHAR(255),
    GENERALMANAGER VARCHAR(255),
    HEADCOACH VARCHAR(255),
    DLEAGUEAFFILIATION VARCHAR(255),
    FOREIGN KEY (LEAGUE_ID) REFERENCES leagues(LEAGUE_ID)
);

CREATE TABLE players (
    PLAYER_NAME VARCHAR(255) NOT NULL,
    TEAM_ID INT,
    PLAYER_ID INT PRIMARY KEY,
    SEASON INT NOT NULL,
    FOREIGN KEY (TEAM_ID) REFERENCES teams(TEAM_ID)
);

CREATE TABLE games (
    GAME_DATE_EST DATE,
    GAME_ID INT PRIMARY KEY,
    GAME_STATUS_TEXT VARCHAR(255),
    HOME_TEAM_ID INT NOT NULL,
    VISITOR_TEAM_ID INT NOT NULL,
    SEASON INT NOT NULL,
    TEAM_ID_home INT NOT NULL,
    PTS_home INT,
    FG_PCT_home DECIMAL(5, 2),
    FT_PCT_home DECIMAL(5, 2),
    FG3_PCT_home DECIMAL(5, 2),
    AST_home INT,
    REB_home INT,
    TEAM_ID_away INT,
    PTS_away INT,
    FG_PCT_away DECIMAL(5, 2),
    FT_PCT_away DECIMAL(5, 2),
    FG3_PCT_away DECIMAL(5, 2),
    AST_away INT,
    REB_away INT,
    HOME_TEAM_WINS BOOLEAN,
    FOREIGN KEY (HOME_TEAM_ID) REFERENCES teams(TEAM_ID),
    FOREIGN KEY (VISITOR_TEAM_ID) REFERENCES teams(TEAM_ID)
);

CREATE TABLE ranking (
    TEAM_ID INT,
    LEAGUE_ID INT,
    SEASON_ID INT NOT NULL,
    STANDINGSDATE DATE,
    CONFERENCE VARCHAR(255),
    TEAM VARCHAR(255) NOT NULL,
    G INT,
    W INT,
    L INT,
    W_PCT DECIMAL(5, 3),
    HOME_RECORD VARCHAR(255),
    ROAD_RECORD VARCHAR(255),
    FOREIGN KEY (TEAM_ID) REFERENCES teams(TEAM_ID),
    FOREIGN KEY (LEAGUE_ID) REFERENCES leagues(LEAGUE_ID)
);

CREATE TABLE games_details (
    GAME_ID INT,
    TEAM_ID INT,
    TEAM_ABBREVIATION VARCHAR(10),
    TEAM_CITY VARCHAR(255),
    PLAYER_ID INT,
    PLAYER_NAME VARCHAR(255),
    NICKNAME VARCHAR(255),
    START_POSITION VARCHAR(10),
    COMMENT VARCHAR(255),
    MIN INT,
    FGM INT,
    FGA INT,
    FG_PCT DECIMAL(5, 2),
    FG3M INT,
    FG3A INT,
    FG3_PCT DECIMAL(5, 2),
    FTM INT,
    FTA INT,
    FT_PCT DECIMAL(5, 2),
    OREB INT,
    DREB INT,
    REB INT,
    AST INT,
    STL INT,
    BLK INT,
    TO INT,
    PF INT,
    PTS INT,
    PLUS_MINUS INT,
    FOREIGN KEY (GAME_ID) REFERENCES games(GAME_ID),
    FOREIGN KEY (TEAM_ID) REFERENCES teams(TEAM_ID),
    FOREIGN KEY (PLAYER_ID) REFERENCES players(PLAYER_ID)
);
"""

nl_query = ''
#beginning the database querying
while nl_query != "exit":
    nl_query = input("DB> ")
    if nl_query.lower() == 'exit': #stop the querying if they say exit
        print("Bye!")

    #if user wants to insert data they must first type in the word insert
    elif nl_query.lower() == 'insert':
        table_name = input("which table do you want to insert into?: ")
        #table name is then extracted
        file_path = f"{table_name}.csv"
        spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()

        df = spark.read.csv(file_path, header=True, inferSchema = True)
        new_row = []
        column_data_types = df.dtypes
        # each column is iterated through and its data type are shown. The user is asked to insert a value for each.
        for column_name, data_type in column_data_types:
            new_val = input(data_type + ", " + column_name + ": ")
            #data types are converted if needed
            if data_type == 'int':
                new_val = int(new_val)
            elif data_type == 'date':
                new_val = parser.parse(new_val)
            elif data_type == "double":
                new_val = float(new_val)
            new_row.append(new_val)
        new_row = Row(new_row)
        new_df = spark.createDataFrame(new_row, schema = df.schema)
        # Append the new row of data to the existing dataframe
        result_df = df.union(new_df)
        #overwrite the dataframe
        result_df.toPandas().to_csv(file_path, index = False)

        spark.stop()

    else: #if the user does not type in exit or insert they go through the query parsing process
        docs = db.similarity_search(nl_query)
        examples =[]
        #most similar queries from prior examples are extracted base on similarity
        for num in range(0,4):
            examples.append(docs[num].page_content)

        #template for the gpt embeddings to generate content
        template = f"""Given an input question, use sql syntax to generate a sql query by choosing 
        one or multiple of the following tables.
    
        For this Problem you can use the following table Schema:
        <table_schema>
        {db_schema} 
        </table_schema>
    
        Pay careful attention to the non nullable columns. If needed, auto generate these values if not specified by a user. Also keep in mind all columns are fully capitalized.
    
        Below are four example Questions and the corresponding Queries. 
        <example>
        {examples[0]}
        </example>
        <example>
        {examples[1]}
        </example>
        <example>
        {examples[2]}
        </example>
        <example>
        {examples[3]}
        </example>
    
        Please provide the SQL query for this question: 
        Question:{nl_query}
        Query: """

        #calling previosuly defined function using the made template
        sql_query = determine_user_query(template)
        #print(sql_query)

        # Conditional statements to identify what type of query was generated
        if sql_query.find("UPDATE") != -1:

            #setting the patterns to extract table and where condition information
            table_name_pattern = r"UPDATE\s+(\w+)"
            set_row_pattern = r"SET\s+(.*?)\s+WHERE\s+(.*)"
    

            table_name_match = re.search(table_name_pattern, sql_query)
            set_row_match = re.search(set_row_pattern, sql_query)

            table_name = table_name_match.group(1)
            set_row_text = set_row_match.group(1)
            where_clause = set_row_match.group(2)

            columns_and_values = [pair.strip() for pair in set_row_text.split(",")]
            column_values = {}

            #iterating through and adding each column name and value to a dictionary
            for pair in columns_and_values:
                column, value = [item.strip() for item in pair.split("=")]
                column_values[column] = value
    
            where_conditions = [condition.strip() for condition in where_clause.split("AND")]
    
            # Create a dictionary to store the conditions in the where clause
            where_dict = {}
            for condition in where_conditions:
                column, value = [item.strip() for item in condition.split("=")]
                where_dict[column] = value

            spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()
    
            #load in table
            file_path = f"{table_name}.csv"
            df = spark.read.csv(file_path, header=True, inferSchema=True)

            # create a script with all table names, values, and conditions
            update_script = "df"
            for column, value in column_values.items():
                update_script += f".withColumn('{column}', when("
                conditions = []
                #create format for how to implement the where clauses
                for where_column, where_value in where_dict.items():
                    if where_value[-1] == ';':
                        where_value = where_value[:-1]
                    conditions.append(f"(col({repr(where_column)}) == lit({where_value}))")
            column = repr(column)
            #append all conditions and column names into a script
            update_script += f"{' & '.join(conditions)}, lit({value})).otherwise(col({column})))"
            #run script
            updated_df = eval(update_script)
            print("updated")
            #overwrite to the original file
            updated_df.toPandas().to_csv(file_path, index = False)

            spark.stop()

        elif sql_query.find("DELETE") != -1: #if its a deletion
    
            pattern = r"DELETE\s+FROM\s+(\w+)"
            match = re.search(pattern, sql_query)
            table_name = match.group(1)
            condition_pattern = r'\bWHERE\b\s*(.*?);'
# Search for the pattern in the SQL statement
            new_match = re.search(condition_pattern, sql_query)

    # Extract the condition from the matched group
            condition = new_match.group(1).strip()
            spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()
    
            #get the table as a df
            file_path = f"{table_name}.csv"
            df = spark.read.csv(file_path, header=True, inferSchema=True)

            #filter the df to not have the condition
            filtered_df = df.filter(~expr(condition))

            #overwrite the file
            filtered_df.toPandas().to_csv(file_path, index = False)
            print("Deleted")    
            spark.stop()
    
        elif sql_query.find("SELECT") != -1: #If its a select query
            # Initialize a Spark session
            spark = SparkSession.builder.appName("SQL_QUERY").getOrCreate()

            # If the query does not have a join
            if  "JOIN" not in sql_query:
                query_lines = sql_query.split('\n')
                #looking through each line in the query
                for line in query_lines:
                    # taking information in the from statemen
                    if 'FROM' in line:
                        match = re.search(r'FROM\s+(\w+)(?:\s+(\w+))?\s*$', line)
                        if match:
                            table_name = match.group(1)
                            alias = match.group(2)
                            #checking if the table had an alias. If not set it as table name
                            alias = alias if alias else table_name
                file_path = f'{table_name}.csv'

                data = spark.read.csv(file_path, header=True, inferSchema = True)            
                #load in table as an rdd
                data = data.rdd

            #if there is a join
            else:
                words = sql_query.split()

                # Initialize a counter
                joins = 0

                # Iterate through the words and count how many joins there will be
                for word in words:
                    if word == 'JOIN':
                        joins += 1


                # Split the SQL query into lines
                query_lines = sql_query.split('\n')


                table_dict = {}
                join_pairs = []

                #look through each line
                for line in query_lines:

                    if 'FROM' in line:
                        match = re.search(r'FROM\s+(\w+)(?:\s+(\w+))?\s*$', line)
                        if match:
                            #find table names and aliases
                            table_name = match.group(1)
                            alias = match.group(2)
                            alias = alias if alias else table_name
                            table_dict[alias] = table_name


                    elif 'JOIN' in line:
                        match = re.search(r'JOIN\s+(\w+)(?:\s+(\w+))?\s+ON', line)
                        if match:
                            #find table name and aliases
                            table_name = match.group(1)
                            alias = match.group(2)
                            alias = alias if alias else table_name
                            table_dict[alias] = table_name


                        pattern = r'\.\s*([a-zA-Z_][a-zA-Z0-9_]*)'
                        join_vals = re.findall(pattern, line)
                # Find the columns to be joined on
                        join_pairs.append(join_vals)    
                rdd_list = []
                join_num = 1

                # iterate through each table in the dictionary
                for x, y in table_dict.items():
                    #read in the value
                    file_path = f'{y}.csv'
                    df = spark.read.csv(file_path, header=True, inferSchema = True)

                    #if this is the first df in the list
                    if len(rdd_list) == 0:
                        #get column names
                        left_columns = df.columns

                        index = f"{join_pairs[0][0]}"
                        #create an rdd with the specified index based upon the join pairs discovered in parsing
                        new_rdd = df.rdd.map(lambda x: (x[index], x))
                        new_rdd = new_rdd.sortByKey()
                        #add sorted rdd to list
                        rdd_list.append(new_rdd)
                    else: #if not the first instance

                        #find index
                        index = f'{join_pairs[join_num -1][1]}'
                        right_columns = df.columns

                        new_rdd = df.rdd.map(lambda x: (x[index], x))
                        new_rdd = new_rdd.sortByKey()

                        # join the new rdd with the other in the list. Partition them first.
                        joined_data = custom_sort_merge_join(rdd_list[-1].collect(),new_rdd.collect())
                        print("joined")
                        index = [index]

               
                        #create a string to convert the joined data back into a df with the proper columns
                        string = 'spark.sparkContext.parallelize(joined_data).flatMap(lambda x: [(x[0]'
                        left_used_cols = []
                        right_used_cols = []
                        #Go back through all columns used and append them to the script in the proper manner
                        for col in right_columns:
                           if col not in index:
                               string = string + f', x[1][\"{col}\"]'
                               right_used_cols.append(col)
                        for col in left_columns:
                            if col not in index:
                                if col not in right_columns:
                                    string = string + f', x[2][\"{col}\"]'
                                    left_used_cols.append(col)
                        string = string + ")])"

                        # run the script to get the rows and then make a dataframe
                        rdd_rows = eval(string)
                        df_result = spark.createDataFrame(rdd_rows, index+ right_used_cols+left_used_cols)

                        #If there are more joins to happen
                        if join_num < len(join_pairs):
                            print("still going")
                            #get columns of joined data
                            left_columns = [x for x in left_columns if x != join_pairs[join_num-1][0]]
                            index = f'{join_pairs[join_num -1][0]}'
                            #go through process of adding the joined data to the list of rdds
                            new_rdd = df_result.rdd.map(lambda x: (x[index], x))
                            joined_data = new_rdd.sortByKey()
                            join_num = join_num +1
                            rdd_list.append(joined_data)
                        else:
                            #Take final joined data
                            data = df_result.rdd

            #function for parsing a where string and getting the right values and condition
            def parse_where_string(where_string):
                # Define regular expressions for extracting columns, conditions, values, and junctions
                column_pattern = re.compile(r'\b\.([A-Za-z_]+)\b')
                condition_pattern = re.compile(r'([<>]=?|=)')
                value_pattern = re.compile(r"'([^']*)'|\"([^\"]*)\"|(\d+)")
                junction_pattern = re.compile(r'\b(AND|OR)\b')

                # Extract columns, conditions, and values
                columns = column_pattern.findall(where_string)
                conditions = condition_pattern.findall(where_string)
                values_match = value_pattern.findall(where_string)
                # Combine the matched groups from the value_pattern
                values = [val[0] or val[1] or int(val[2]) for val in values_match]

                # Extract junctions
                junctions = junction_pattern.findall(where_string)

                return columns, conditions, values, junctions

            #initialize variables for use in rdd operations
            where_string = ''
            aggregate_string = ''
            order_string = ''
            having_string = ''
            group_string = ''
            select_string = ''
            limit = 100
            query_lines = sql_query.split('\n')

            #for each line in the query
            for line in query_lines:

                #If its a where clause run the function
                if 'WHERE' in line:
                    columns, conditions, values, junctions = parse_where_string(line)
                    new_conditions=[]
                    for con in conditions: #If there was an equal sign in the sql query turn it into pythonic form
                        if con == '=':
                            con = '=='
                        new_conditions.append(con)

                    i = 0 #create a count to know if all conditions have been included
                    where_string = '.filter(lambda r: '
                    for col,con,val in zip(columns, new_conditions, values):
                        if isinstance(val, str): #create where clause string
                            val = f"'{val}'"
                        where_string = where_string + f'r.{col} {con} {val}'
                        if i < len(junctions): #if not at the limit yet append the statment

                            where_string = where_string + " " + junctions[i].lower() + " "
                        i = i+1
                    where_string = where_string + ")"

                if "GROUP BY" in line:
                    pattern = re.compile(r'\.\s*([A-Za-z_]+)')

                    match = pattern.search(line)

                    # Extract the value if there is a match
                    if match:
                        grouper = match.group(1)
                    #look through the entire query and see if there is an aggregation. Use format for specified action
                    if "COUNT" in sql_query:
                        group_string = f'.map(lambda x: (x[\"{grouper.upper()}\"], 1)).reduceByKey(lambda x, y: x+y)'
                    elif "MAX" in sql_query:
                        group_string = f'.map(lambda x: (x[\"{grouper.upper()}\"], x[1])).groupByKey().mapValues(max)'
                    elif "MIN" in sql_query:
                        group_string = f'.map(lambda x: (x[\"{grouper.upper()}\"], x[1])).groupByKey().mapValues(min)'
                    elif "AVG" in sql_query:
                        group_string = f'.groupBy(lambda x: (x[\"{grouper.upper()}\"], x[1])).mapValues(lambda values: (sum(v[2] for v in values), len(values))).mapValues(lambda sum_count: sum_count[0] / sum_count[1])'
                    elif "SUM" in sql_query:
                        group_string = f'.map(lambda x: (x[\"{grouper.upper()}\"], x[1])).groupByKey().mapValues(lambda values: sum(v[2] for v in values))'



                if "ORDER BY" in line:
                    words = line.split()

            # Remove trailing characters, such as semicolons
                    last_word = words[-1].rstrip(';').lower()

                    desc = True
            # Check if the last word is "desc"
                    if last_word == "desc":
                        desc = False
                    order_string = f'.sortBy(lambda x: x[1], ascending={desc})'

                if "HAVING" in line:
                    columns, conditions, values, junctions = parse_where_string(line)
                    if conditions[0] == '=':
                        conditions[0] = '=='
                    # create the filter for having
                    having_string = f'.filter(lambda r: r[1] {conditions[0]} {values[0]})'

                if "LIMIT" in line: #changes limit
                    vals = line.split()
                    num = vals[-1].rstrip(';').lower()
                    limit = int(num)

                if "SELECT" in line: #map certain values from select statment
                    matches = re.findall(r'\.\s*([\w_]+)', line)
                    select_string = '.map(lambda x: '
                    if len(matches) > 1:
                        
                        select_string = select_string + '('
                        for val in matches: # for each value found use it in the map
                            select_string =  select_string + f'x.{val.upper()}'
                        select_string = select_string + '))'
                    else: #if theres just 1 thing in the select use this format
                        select_string = select_string + f'x.{matches[0].upper()})'

            #If there's group by then ignore the select to deal with aggregation
            if group_string != '':
                select_string = ''
               
            # create script with all components
            final = 'data'+where_string + group_string + having_string +order_string + select_string + f'.take({limit})'

            #show desired output
            print(eval(final))
            spark.stop()

        #If a sql query can't be produced
        else: 
            print("Error: Please rephrase your query")
