from variables.connection_variables import HOST_DB, DB_PORT, DB_USER, DB_NAME, DB_PASSWORD

import psycopg2

import pandas as pd

from datetime import datetime

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

def create_meta_table(meta_table_name):

    conn = psycopg2.connect(
    host = HOST_DB,
    port = DB_PORT,
    database = DB_NAME,
    user= DB_USER,
    password = DB_PASSWORD)

    curr = conn.cursor()

    print(f"Table {meta_table_name} doesn't exist, so we must generate it on database....")

    curr.execute(f"CREATE TABLE IF NOT EXISTS {meta_table_name} (\
                id SERIAL PRIMARY KEY, \
                upsert_date TIMESTAMP WITH TIME ZONE, \
                contributor VARCHAR(50), \
                algo_start_date TIMESTAMP WITH TIME ZONE, \
                batch_nb INT, \
                mode VARCHAR(50), \
                init_batch_size INT, \
                update_batch_size INT \
                );")
    
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




def update_to_database(df, table_name, meta_table_name, date_update, contributor, date, batch_nb, mode, len_batch, len_df_filter):


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

# Boucle pour insérer chaque ligne du DataFrame
    for _, row in df.iterrows():
        insee = int(row['insee'])
        value = int(row['value'])

        # Requête d'insertion personnalisée
        insert_query = f"INSERT INTO {table_name} (insee, value) VALUES ('{insee}', {value}) ON CONFLICT (insee) DO UPDATE SET value = {table_name}.value + excluded.value;"

        # Exécution de la requête
        cur.execute(insert_query)

    date_ts = datetime.timestamp(date)

    insert_query_2 = f"INSERT INTO {meta_table_name} \
                    (upsert_date, contributor, algo_start_date, batch_nb, mode, init_batch_size, update_batch_size) \
                    VALUES \
                    (CURRENT_TIMESTAMP, '{contributor}', '{date}', {batch_nb}, '{mode}', {len_batch}, {len_df_filter});"

    # Exécution de la requête
    cur.execute(insert_query_2)

# Valider les modifications et fermer la connexion
    conn.commit()
    cur.close()
    conn.close()


"""

def append_to_meta_table(meta_table_name, date_update, contributor, date, batch_nb, mode, len_batch):


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


    # Requête d'insertion personnalisée
    insert_query = f"INSERT INTO {meta_table_name} \
                    (upsert_date, contributor, algo_start_date, batch_nb, mode, batch_size) \
                    VALUES \
                    ({date_update}, {contributor}, {date}, {batch_nb}, {mode}, {len_batch});"

    # Exécution de la requête
    cur.execute(insert_query)

# Valider les modifications et fermer la connexion
    conn.commit()
    cur.close()
    conn.close()

    """

