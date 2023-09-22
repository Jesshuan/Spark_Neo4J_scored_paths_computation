import os

import psycopg2

# POSTGRES Variables

HOST_POSTGRES = "172.17.0.1"

POSTGRES_PORT = "5433"

POSTGRES_DATABASE = "postgres"

POSTGRES_TABLE = "insee_value"

POSTGRES_USER = "postgres"

POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]



def update_to_database(df):


# --- Postgres Database Connection --- #

# Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
    host= HOST_POSTGRES,
    port=POSTGRES_PORT,
    database=POSTGRES_DATABASE,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD)


    # Création d'un curseur
    cur = conn.cursor()

# Boucle pour insérer chaque ligne du DataFrame
    for _, row in df.iterrows():
        insee = row['insee']
        value = row['value']

        # Requête d'insertion personnalisée
        insert_query = f"INSERT INTO {POSTGRES_TABLE} (insee, value) VALUES ('{insee}', {value}) ON CONFLICT (insee) DO UPDATE SET value = {POSTGRES_TABLE}.value + excluded.value;"

        # Exécution de la requête
        cur.execute(insert_query)

# Valider les modifications et fermer la connexion
    conn.commit()
    cur.close()
    conn.close()
