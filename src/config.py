from decouple import config

OUTPUT_FILE_PATH = config("OUTPUT_FILE_PATH",cast=str)

HOST_NAME = config("HOST_NAME",cast=str)

DATABASE_NAME = config("DATABASE_NAME",cast=str)

PORT = config("PORT",cast=int)

USER_NAME = config("USER_NAME",cast=str)

PASSWORD = config("PASSWORD",cast=str)

SCHEMA = config("SCHEMA",cast=str)

TABLE = config("TABLE",cast=str)