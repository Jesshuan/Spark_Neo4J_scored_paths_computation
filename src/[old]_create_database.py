import psycopg2


import os


HOST_POSTGRES = "172.17.0.1"

POSTGRES_PORT = "5433"

POSTGRES_DATABASE = "postgres"

POSTGRES_TABLE = "insee_value"

POSTGRES_USER = "postgres"

POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]


# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host= HOST_POSTGRES,
    port=POSTGRES_PORT,
    database=POSTGRES_DATABASE,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

curr = conn.cursor()

curr.execute("CREATE TABLE IF NOT EXISTS insee_value (\
             insee INT PRIMARY KEY, \
             value INT \
             )")

conn.commit()
curr.close()
conn.close()