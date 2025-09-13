from config import \
(
    OUTPUT_FILE_PATH,\
    HOST_NAME,\
    DATABASE_NAME,\
    PORT,\
    USER_NAME,\
    PASSWORD,\
    SCHEMA,\
    TABLE
)
from database import get_connection_string,test_connection,create_schema,create_table,insert_pipeline_data
import logging
from model import PipelineInfo
from pydantic import ValidationError

logger = logging.getLogger("add-pipeline-info")

logger.setLevel(logging.DEBUG)

# Prevent propagation to root logger (avoids duplicate logs if used in packages)
logger.propagate = False

formatter = logging.Formatter(
    fmt='%(asctime)s | %(levelname)-8s | %(name)-15s | %(lineno)-3d | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S.%fZ'  # ISO 8601 with microseconds → Z for UTC
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)


def main()->int:

    connection_str = get_connection_string(host=HOST_NAME,\
    database_name=DATABASE_NAME,\
    user=USER_NAME,\
    password=PASSWORD,\
    port=PORT)


    if not test_connection(connection_str=connection_str):
        logger.info("Cannot connect to the database.Please check the database configuration")
        return -1
    else:
        logger.info("Connection to database success")

    if not create_schema(connection_str=connection_str,schema_name=SCHEMA):
        logger.info(f"Cannot create the schema:{SCHEMA}")
        return -1
    else:
        logger.info(f"Schema {SCHEMA} created or if exist")

    if not create_table(connection_str=connection_str,schema_name=SCHEMA,table_name=TABLE):
        logger.info(f"Cannot create the table:{SCHEMA}.{TABLE}")
        return -1
    else:
        logger.info(f"Table {SCHEMA}.{TABLE} created or if exist")

    pipelines:List[PipelineInfo] = list()

    try:

        pipelines = PipelineInfo.from_json_file(OUTPUT_FILE_PATH)

        logger.info(f"Loaded pipeline data from : {OUTPUT_FILE_PATH}")

    except ValidationError as e:
        
        logger.info(f"Invalid file format : {OUTPUT_FILE_PATH}")
        return -1

    except FileNotFoundError:
        
        logger.info(f"Cannot find the {OUTPUT_FILE_PATH} file.")
        return -1
    

    if not insert_pipeline_data(connection_str=connection_str,\
    schema_name=SCHEMA,\
    table_name=TABLE,\
    pipelines=pipelines):
        logger.info(f"Cannot insert data into table:{SCHEMA}.{TABLE}")
        return -1
    
    logger.info(f"Inserted pipeline into table:{SCHEMA}.{TABLE}")

if __name__=="__main__":
    main()  