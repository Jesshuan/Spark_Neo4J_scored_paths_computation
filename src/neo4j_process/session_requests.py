import pandas as pd

import numpy as np

import time

from neo4j import GraphDatabase, basic_auth

from neo4j_process.cypher_requests import get_all_communes_with_properties_cypher, check_projections_list, projection_graph, shortest_path_request, get_cities_path_from_path_list

from variables.hyperparameters import NEO4J_BATCH_SIZE


from variables.connection_variables import NEO4J_HOST, NEO4J_USER, NEO4J_PASSWORD


# ---- Neo4J session functions ---- #

# (All the Neo4j requests sub-fuctions are placed in the "cypher_requests" script)


def driver_generation():

    driver = GraphDatabase.driver(
                                    NEO4J_HOST,
                                    auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
    
    return driver


# We need a batch function to iterate in a list with a specific Neo4j batch size
def batch(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]



def get_all_communes_with_properties_session(properties_list):

    print("Driver generation for neo4j...")

    driver = driver_generation()

    print("... ok.")

    with driver.session() as session:

        print(f"Neo4j : get all communes with this properties list {properties_list}... ")

        result = session.execute_read(get_all_communes_with_properties_cypher, properties_list)

        print("Request done.")

    driver.close()

    return pd.DataFrame(result).fillna(0)



def check_or_regenerate_projection(experiment_name):

    # Function to generate a graph projection which will be used by the shortest path algorithm.
    # The graph name will be the name of the experiment.
    
    print("Check Neo4j graph projection...")
       
    driver = driver_generation()

    with driver.session() as session:
        
        result_list = session.execute_read(check_projections_list)

        if experiment_name not in result_list[0]["list"]:

            print("Projection not founded for this experiment... Generation...")
                   
            result = session.execute_write(projection_graph, experiment_name)

            print(f"Graph projection {result[0]['graphName']} generated !...")

        else:
            print("Graph projection found for this experiment ! (No regeneration)...")
            
        driver.close()




def get_cities_path_session(df_to_calc):

    # Function used by the "aggregator" script.
    # Compute a list of all cities nearly to a list of paths (road_points)
    # With a batch processing.

    print("Conversion into records for Neo4j request...")

    props_list = df_to_calc.to_dict('records')

    driver = driver_generation()

    print(f"Neo4J batch size : {NEO4J_BATCH_SIZE}.")

    with driver.session() as session:

        start_time = time.time()
        
        result_paths_list = []
        
        for props_batch in batch(props_list, NEO4J_BATCH_SIZE):
                                    
            print("neo4j request...")

            start_time = time.time()

            result = session.execute_read(get_cities_path_from_path_list, props_batch)

            print(f"Neo4J request done in {np.around(time.time() - start_time, 2)} sec.")
            print(f"{len(result)} results.")

            result_paths_list.extend(result)
            
        driver.close()

        print(f"Neo4J request done")

    return result_paths_list




def compute_shortest_paths_session(df_to_calc, experiment_name):

    # Function used by the "path calculator" script.
    # Compute a list of all cities nearly to a list of paths (road_points)
    # With a batch processing.

    print("Conversion into records for Neo4j request...")

    props_list = df_to_calc.to_dict('records')

    driver = driver_generation()

    print(f"Neo4J batch size : {NEO4J_BATCH_SIZE}.")

    with driver.session() as session:

        start_time = time.time()
        
        result_paths_list = []
        
        for props_batch in batch(props_list, NEO4J_BATCH_SIZE):
                                    
            print(f"Mini-batch neo4j( extract) : {props_batch[0:2]}")
                                    
            print("neo4j request...")

            start_time = time.time()

            result = session.execute_read(shortest_path_request, props_batch, experiment_name)

            print(f"Neo4J request done in {np.around(time.time() - start_time, 2)} sec.")
            print(f"{len(result)} results.")

            result_paths_list.extend(result)
            
        driver.close()

        print(f"Neo4J request done")

    return result_paths_list




