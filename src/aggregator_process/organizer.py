
from spark_process.spark_and_memory_paths import spark_get_a_df_memory_paths_selection_from_batch

from spark_process.spark_filters import spark_filter_cut_begining

from spark_process.spark_aggregation import spark_aggregation

from neo4j_process.session_requests import get_cities_path_session

import time

import numpy as np




def aggregate_a_batch(batch_list, mode, weighted_feats_df = None):

    print(f" **** --- AGGREGATOR PROCESS in Mode : {mode} ---- ***")

    print(f"Aggregation of a batch of size {len(batch_list)}...")

    print("batch extract :")

    print(batch_list[0:5])

    print("Spark job : get the batch selection from the memory paths...")

    start_time = time.time()

    df_spark_batch = spark_get_a_df_memory_paths_selection_from_batch(batch_list)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    print("Spark job : filter the paths with a cut...")

    start_time = time.time()

    df_filter = spark_filter_cut_begining(df_spark_batch)

    df_spark_batch.unpersist()

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    len_df_filter = len(df_filter)

    print("--- Neo4j REQUEST ---")

    results_path_lists = get_cities_path_session(df_filter)

    start_time = time.time()

    print("--- SPARK AGGREGATION ---")

    if mode=="weighted":

        df_result = spark_aggregation(results_path_lists, mode, len_df_filter)

    elif mode == "equiprobable":

        df_result = spark_aggregation(results_path_lists, mode, len_df_filter, weighted_feats_df = weighted_feats_df)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    print("Extract of the aggregated result :")

    print(df_result.head())

    return df_result
