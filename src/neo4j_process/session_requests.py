import pandas as pd

from neo4j import GraphDatabase, basic_auth

from neo4j_process.cypher_requests import get_all_communes_with_properties_cypher, check_projections_list, projection_graph, shortest_path_request

from variables.hyperparameters import NEO4J_BATCH_SIZE


from variables.connection_variables import NEO4J_HOST, NEO4J_USER, NEO4J_PASSWORD

def driver_generation():

    driver = GraphDatabase.driver(
                                    NEO4J_HOST,
                                    auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
    
    return driver



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

    return pd.DataFrame(result)


def check_or_regenerate_projection(experiment_name):
    
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



def compute_shortest_paths_session(df_to_calc, experiment_name):

    print("Conversion into records for Neo4j request...")

    props_list = df_to_calc.to_dict('records')

    print("Driver generation for neo4j...")

    driver = driver_generation()

    print(f"Neo4J batch size : {NEO4J_BATCH_SIZE}.")

    with driver.session() as session:
        
        result_paths_list = []
        
        for props_batch in batch(props_list, NEO4J_BATCH_SIZE):
                                    
            print(f"Mini-batch neo4j( extract) : {props_batch[0:2]}")
                                    
            print("neo4j request...")

            result = session.execute_write(shortest_path_request, props_batch, experiment_name)

            result_paths_list.extend(result)
            
        driver.close()

    return result_paths_list




