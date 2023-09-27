from variables.connection_variables import HOST_DB, DB_PORT, DB_USER, DB_NAME, DB_PASSWORD

import psycopg2

import pandas as pd

# POSTGRES Variables


def get_all_tables():

    conn = psycopg2.connect(
    host = HOST_DB,
    port = DB_PORT,
    database = DB_NAME,
    user= DB_USER,
    password = DB_PASSWORD)

    get_table_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

    cur = conn.cursor()

    cur.execute(get_table_query)

    list_results = cur.fetchall()

    conn.commit()
    cur.close()
    conn.close()

    return [table[0] for table in list_results]


def create_table(table_name):

    conn = psycopg2.connect(
    host = HOST_DB,
    port = DB_PORT,
    database = DB_NAME,
    user= DB_USER,
    password = DB_PASSWORD)

    curr = conn.cursor()

    print(f"Table {table_name} doesn't exist, so we must generate it on database....")

    curr.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (\
                insee INT PRIMARY KEY, \
                value INT \
                )")
    
    print("Done.")

    conn.commit()
    curr.close()
    conn.close()

def take_a_snapshot_db(table_name):

    conn = psycopg2.connect(
    host = HOST_DB,
    port = DB_PORT,
    database = DB_NAME,
    user= DB_USER,
    password = DB_PASSWORD)

    sql = f"SELECT * FROM {table_name};"

    df = pd.read_sql_query(sql, conn)

    conn.close()

    return df


def update_to_database(df, table_name):


# --- Postgres Database Connection --- #

# Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
    host = HOST_DB,
    port = DB_PORT,
    database = DB_NAME,
    user= DB_USER,
    password = DB_PASSWORD)


    # Création d'un curseur
    cur = conn.cursor()

    print(df)

# Boucle pour insérer chaque ligne du DataFrame
    for _, row in df.iterrows():
        insee = int(row['insee'])
        value = int(row['value'])

        # Requête d'insertion personnalisée
        insert_query = f"INSERT INTO {table_name} (insee, value) VALUES ('{insee}', {value}) ON CONFLICT (insee) DO UPDATE SET value = {table_name}.value + excluded.value;"

        # Exécution de la requête
        cur.execute(insert_query)

# Valider les modifications et fermer la connexion
    conn.commit()
    cur.close()
    conn.close()
