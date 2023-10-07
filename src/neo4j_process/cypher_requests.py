
# ---- Cypher request functions on Neo4J ---- #



def check_projections_list(tx):

    query = "CALL gds.graph.list() YIELD graphName RETURN collect(graphName) as list"

    result = tx.run(query)
    return result.data()



def projection_graph(tx, name_graph):

    # Function to generate a graph projection for the algo shortest path
    # We just need ROAD_POINT nodes and RELATED edge.
    # The weighted property will be "travel_time" during the shortest path processing
    # Orientation "NATURAL" allows us to keep a sense with the roads

    query = "CALL gds.graph.project( \
    '" + name_graph + "', \
    { \
        ROAD_POINT: {properties: ['x', 'y']} \
            }, \
    { \
        RELATED: {properties:'travel_time', orientation : 'NATURAL'} \
        }) \
    YIELD graphName, nodeProjection,  nodeCount, relationshipProjection, relationshipCount \
    RETURN graphName, nodeProjection,  nodeCount, relationshipProjection, relationshipCount"

    result = tx.run(query)
    return result.data()


def remove_projection_graph(tx, name_graph):

    query = "CALL gds.graph.drop( \
                '" + name_graph + "', \
                False) \
                YIELD graphName \
                RETURN graphName"
    
    result = tx.run(query)
    return result.data()


def get_all_communes_with_coords(tx):

    query = "MATCH (n:CITY) RETURN n.insee as insee, n.x as x, n.y as y"

    result = tx.run(query)
    return result.data()


def get_all_communes_with_properties_cypher(tx, properties_list):

    # A custom query to get all insee code with a specific list of properties
    # Query used by the two programs

    return_instruction = ",".join(["n." + arg + " as " + arg for arg in properties_list])

    query = "MATCH (n:CITY) RETURN " + return_instruction

    result = tx.run(query)
    return result.data()


def get_cities_path_from_path_list(tx, props_list):

    # A query to get all the cities which are "NEARLY_TO" all the road_points of a path
    # This query is used by the "agregator" script

    query = "UNWIND $props_list AS map \
                MATCH (c:CITY)-[NEARLY_TO]->(p:ROAD_POINT) \
                WHERE ID(p) IN map.path_truncated \
                RETURN map.source as source, \
                      map.target as target, \
                      apoc.coll.toSet(collect(c.insee)) AS cities_path "
    
    result = tx.run(query, props_list = props_list)
    return result.data()


def shortest_path_request(tx, props_list, graph_proj_name):

    # Our favorite centra query...
    # With two city nodes (source and target), we get the two associated road_point ("VERY_NEARLY_TO" relationships)
    # And compute the shortest path algorithm between these two nodes, with a weight property as "travel-time"
    # We return all the path, source, target, and list of incremental cost (of travel time) during the travel

    query = "UNWIND $props_list AS map \
            MATCH (source:CITY {insee: map.source})-[:VERY_NEARLY_TO]-(p_s:ROAD_POINT) \
            MATCH (target:CITY {insee: map.target})-[:VERY_NEARLY_TO]-(p_t:ROAD_POINT) \
                CALL gds.shortestPath.dijkstra.stream( \
                '" + graph_proj_name + "', \
                    { \
                    sourceNode: p_s, \
                    targetNode: p_t, \
                    relationshipWeightProperty: 'travel_time' \
                        }) \
            YIELD nodeIds, totalCost, costs \
            RETURN map.source as source, \
                    map.target as target, \
                    nodeIds as path, \
                    totalCost, \
                    costs"
    
    
    result = tx.run(query, props_list = props_list)
    return result.data()