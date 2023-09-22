
def check_projections_list(tx):

    query = "CALL gds.graph.list() YIELD graphName RETURN collect(graphName) as list"

    result = tx.run(query)
    return result.data()

#        CITY: {properties: ['insee', 'x', 'y']}, \
# NEARLY_TO: {properties:'travel_time', orientation : 'UNDIRECTED'}, \

def projection_graph(tx, name_graph):

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

    return_instruction = ",".join(["n." + arg + " as " + arg for arg in properties_list])

    query = "MATCH (n:CITY) RETURN " + return_instruction

    result = tx.run(query)
    return result.data()


def shortest_path_request(tx, props_list, graph_proj_name):

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
            CALL { \
                    WITH nodeIds \
                    MATCH (c:CITY)-[NEARLY_TO]->(p:ROAD_POINT) \
                    WHERE ID(p) IN nodeIds \
                    RETURN apoc.coll.toSet(collect(c.insee)) AS path \
                } \
                RETURN map.source as source, \
                    map.target as target, \
                    path, \
                    totalCost, \
                    costs"
    
    
    result = tx.run(query, props_list = props_list)
    return result.data()