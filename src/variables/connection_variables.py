import os

# Here, user must define connection variables for Neo4J and the database.

# The passwords were supplied by environment variables (when the docker container was built).


# ----- NEO4J CONNECTION VARIABLES ------ #

NEO4J_HOST = "bolt://host.docker.internal:7687"

NEO4J_USER = "neo4j"

NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]



# ----- DATABASE CONNECTION VARIABLES ------ #

HOST_DB = "172.17.0.1"

DB_PORT = "5433"

DB_NAME = "postgres"

DB_USER = "postgres"

DB_PASSWORD = os.environ["DB_PASSWORD"]
