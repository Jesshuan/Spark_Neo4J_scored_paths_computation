
from spark_process.spark_and_memory_paths import spark_get_a_df_memory_paths_selection_from_batch

from spark_process.spark_filters import spark_filter_cut_begining

from spark_process.spark_aggregation import spark_aggregation

from neo4j_process.session_requests import get_cities_path_session

import time

import numpy as np




def aggregate_a_batch(batch_list, mode, weighted_feats_df = None):

    # Function for aggregate the batch, take a batch list from the buffer of th experiment and return a pandas dataframe

    print(f" **** --- AGGREGATOR PROCESS in Mode : {mode} ---- ***")

    print(f"Aggregation of a batch of initial size {len(batch_list)}...")

    print("batch extract :")

    print(batch_list[0:5])

    print("Spark job : get the batch selection from the memory paths...")

    start_time = time.time()

    # with the batch list, we take all the paths already computed in the stock (memory paths)
    # and assign each (source, target) with its path

    df_spark_batch = spark_get_a_df_memory_paths_selection_from_batch(batch_list)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    print("Spark job : filter the paths with a cut...")

    start_time = time.time()

    # Call filter application (spark function)

    df_filter = spark_filter_cut_begining(df_spark_batch)

    df_spark_batch.unpersist()

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    len_df_filter = len(df_filter)

    print("--- Neo4j REQUEST ---")

    # With the path of road points, we must deduce the cities nearly to the road points 
    # We must request Neo4J

    results_path_lists = get_cities_path_session(df_filter)

    # -- Aggegration --
    # We call the spark function, for each mode case
    # If mode == "weigted" there is no weigting at this step (the weighting have been processed before)
    # If mode == "equiprobable", the weighteing takes place now !...

    start_time = time.time()

    print("--- SPARK AGGREGATION ---")

    if mode=="weighted":

        df_result = spark_aggregation(results_path_lists, mode, len_df_filter)

    elif mode == "equiprobable":

        df_result = spark_aggregation(results_path_lists, mode, len_df_filter, weighted_feats_df = weighted_feats_df)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    print("Extract of the aggregated result :")

    print(df_result.head())

    # return the pandas dataframe result of the aggregation with all cities and all scores

    return df_result, len_df_filter
