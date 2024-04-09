import json
import logging
import os

from dotenv import load_dotenv
import pandas as pd
import pyodbc

from azure_configuration import initialize_azure_ad_app
from fetch_planner_data import (
    fetch_categories_name,
    fetch_planner_data,
    fetch_planner_buckets
)
from process_planner_data import (
    extract_info_createdBy,
    extract_values_application,
    extract_values_user
)

# Configure the logger
logging.basicConfig(level=logging.INFO)

# Create a logger
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def main():

    database_user = os.getenv("DATABASE_USER")
    database_password = os.getenv("DATABASE_PASSWORD")
    database_host = os.getenv("DATABASE_HOST")
    database_port = os.getenv("DATABASE_PORT")
    database_name = os.getenv("DATABASE_NAME")

    if not all(
        [database_user,
         database_password,
         database_host,
         database_port,
         database_name]
    ):
        logger.error("Database configuration values are missing.")
        return

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    tenant_id = os.getenv("TENANT_ID")
    plan_id = os.getenv("PLAN_ID")

    if not all(
        [client_id,
         client_secret,
         tenant_id,
         plan_id]
    ):
        logger.error("Azure details values are missing")
        return

    AUTHORITY = f'https://login.microsoftonline.com/{tenant_id}'

    SCOPES = [
        'https://graph.microsoft.com/.default'
    ]

    app, token_result = initialize_azure_ad_app(client_id,
                                                client_secret,
                                                AUTHORITY,
                                                SCOPES
                                                )

    if "access_token" in token_result:
        logger.info("Token acquired successfully")
    else:
        logger.error("Error acquiring token: %s - %s", token_result.get('error'), token_result.get('error_description'))

    tasks_normalized = fetch_planner_data(app, token_result, plan_id)
    bucket_name_mapping = fetch_planner_buckets(app, token_result, plan_id)
    categories_name_mapping = fetch_categories_name(app, token_result, plan_id)

    df = pd.DataFrame(tasks_normalized["value"])

    df.insert(0, 'id', df.pop('id'))
    df['bucket_name'] = df['bucketId'].map(bucket_name_mapping)
    df.columns = [col.replace('.', '_').replace('@', '').replace('-', '_') for col in df.columns]

    df[['created_by_user', 'created_by_application']] = df['createdBy'].apply(lambda x: pd.Series(extract_info_createdBy(x)))
    df['created_by_user_displayname'], df['created_by_user_id'] = zip(*df['created_by_user'].apply(lambda x: extract_values_user(x)))
    df['created_by_application_displayname'], df['created_by_application_id'] = zip(*df['created_by_application'].apply(lambda x: extract_values_application(x)))
    df.drop(['createdBy', 'created_by_user', 'created_by_application'], axis=1, inplace=True)

    df = df.rename(columns={'appliedCategories': 'Categories', 'assignments': 'Assignments'})
    dict_columns = [col for col in df.columns if isinstance(df[col].iloc[0], dict) or col == 'id']
    unique_buckets = df[['bucketId', 'bucket_name']].drop_duplicates()

    server = database_host
    database = database_name
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ',' + str(database_port) + ';DATABASE=' + database + ';Trusted_Connection=yes;')
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE Buckets (bucketId NVARCHAR(255) PRIMARY KEY, bucket_name VARCHAR(MAX))")

    # Insert unique pairs of bucketId and bucket_name into the new table
    for _, row in unique_buckets.iterrows():
        cursor.execute("INSERT INTO Buckets (bucketId, bucket_name) VALUES (?, ?)", (row['bucketId'], row['bucket_name']))

    df.drop('bucket_name', axis=1, inplace=True)

    for col in dict_columns:
        if col in ["id", "created_by_user", "created_by_application"]:
            continue

        cursor.execute(f"CREATE TABLE {col} (id NVARCHAR(255), category VARCHAR(255))")

        for index, row in df.iterrows():
            for key, value in row[col].items():
                if value:  # Check if the value is True
                    cursor.execute(f"INSERT INTO {col} (id, category) VALUES (?, ?)", (str(row['id']), key))

    columns_to_exclude = ['Categories']
    columns_to_iterate = [col for col in df.columns if col not in columns_to_exclude]
    datatypes = df.dtypes

    create_table_query = 'CREATE TABLE Planner_data_raw ('

    for column, datatype in zip(columns_to_iterate, datatypes):
        if column == 'bucket_name':
            continue
        sql_datatype = 'VARCHAR(MAX)' if datatype == 'object' else 'FLOAT' if datatype == 'float64' else 'INT'
        create_table_query += f'\n{column} {sql_datatype},'

    create_table_query = create_table_query[:-1]
    create_table_query += '\n)'

    cursor.execute(create_table_query)
    conn.commit()

    df.drop('Categories', axis=1, inplace=True)

    for index, row in df.iterrows():
        # Convert dictionary values to JSON strings
        processed_row = [json.dumps(value) if isinstance(value, dict) else value for value in row]

        insert_query = f'''
                    INSERT INTO Planner_data_raw({', '.join(df.columns)})
                    VALUES ({', '.join('?' for _ in range(len(df.columns)))})
                    '''

        cursor.execute(insert_query, processed_row)
        conn.commit()

    # Create a temporary table to hold the category list
    temp_table_name = 'TempCategoryTable'
    cursor.execute(f"CREATE TABLE {temp_table_name} (category NVARCHAR(255), category_name NVARCHAR(255))")

    # Insert the category list into the temporary table
    for category, category_name in categories_name_mapping:
        cursor.execute(f"INSERT INTO {temp_table_name} (category, category_name) VALUES (?, ?)", (category, category_name))

    # Update the Categories table to add the category_name column
    cursor.execute(f"ALTER TABLE Categories ADD category_name NVARCHAR(255)")

    # Update the Categories table to populate the category_name column using a JOIN operation
    cursor.execute(f"UPDATE Categories SET Categories.category_name = Temp.category_name FROM Categories INNER JOIN {temp_table_name} AS Temp ON Categories.category = Temp.category")

    # Drop the temporary table
    cursor.execute(f"DROP TABLE {temp_table_name}")
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
