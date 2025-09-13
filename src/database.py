import urllib
from sqlalchemy import create_engine,text
from sqlalchemy.sql import quoted_name
from typing import List
from datetime import datetime
from model import PipelineInfo

def get_connection_string(host:str,database_name:str,user:str,password:str,port:str=1433)->str:

    driver = "{ODBC Driver 17 for SQL Server}"

    odbc_str = f"DRIVER={driver};SERVER={host};PORT={port};UID={user};DATABASE={database_name};PWD={password}"

    connection_string = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"

    return connection_string

def test_connection(connection_str:str)->bool:
    """
    Test whether we can connect to database
    """
    try:
        engine = create_engine(connection_str)

        with engine.connect() as connection:
            return True
    except :
        return False

def create_schema(connection_str:str,schema_name:str)->bool:

    sql = f"""

        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema_name}')
        BEGIN
            EXEC('CREATE SCHEMA {schema_name}');
        END

    """

    engine = create_engine(connection_str)

    try:
        with engine.connect() as connection:
            connection.execute(statement=text(sql))
            connection.commit()
            return True
    except:
        return False


def create_table(connection_str:str,schema_name:str,table_name:str)->bool:

    sql = f"""

        IF NOT EXISTS 
        (
            SELECT TOP 1 * 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table_name}'
        )
        BEGIN
            CREATE TABLE {schema_name}.{table_name}
            (
                pipeline_name varchar(300),
                activity_name varchar(300),
                dataset_name varchar(300),
                dataset_type varchar(300),
                linked_service_name varchar(300),
                linked_service_type varchar(300),
                created_date datetime2,
                updated_date datetime2

            );
        END

    """

    engine = create_engine(connection_str)

    try:
        with engine.connect() as connection:
            connection.execute(statement=text(sql))
            connection.commit()
            return True
    except:
        return False

def insert_pipeline_row(connection_str: str, schema_name: str, table_name: str, row)->bool:
    engine = create_engine(connection_str)

    try:
        with engine.connect() as connection:

            sql = f"""
                INSERT INTO {schema_name}.{table_name} 
                (
                    pipeline_name,
                    activity_name,
                    dataset_name,
                    dataset_type,
                    linked_service_name,
                    linked_service_type,
                    created_date,
                    updated_date
                ) 
                VALUES 
                (
                    '{row[0]}',
                    '{row[1]}',
                    '{row[2]}', 
                    '{row[3]}',
                    '{row[4]}', 
                    '{row[5]}',
                    '{row[6]}',
                    '{row[7]}'
                )
            """

            result = connection.execute(text(sql)) 
            connection.commit()

            return True
    except:
        return False

def insert_pipeline_data(connection_str: str, schema_name: str, table_name: str, pipelines: List[PipelineInfo]) -> bool:
    """
    Insert flattened pipeline data into SQL Server table.
    Returns number of rows inserted.
    """
    # Flatten the nested structure into list of tuples

    rows = []

    for pipeline in pipelines:
        for activity in pipeline.activities:
            for dataset in activity.datasets:
                row = (
                    pipeline.name,
                    activity.name,
                    dataset.dataset_name,
                    dataset.dataset_type,
                    dataset.linked_service_name,
                    dataset.linked_service_type,
                    str(datetime.now()),  
                    str(datetime.now())
                )
                rows.append(row)

    if len(rows)==0:
        return True

    engine = create_engine(connection_str)

    for row in rows:
        if not insert_pipeline_row(connection_str=connection_str, schema_name=schema_name, table_name=table_name, row=row):
            return False

    
    return True
