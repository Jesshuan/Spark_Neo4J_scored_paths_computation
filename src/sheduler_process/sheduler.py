import random

import pandas as pd

import numpy as np

import time

import os

from variables.hyperparameters import NB_DRAWS, RATIO_MIN_DRAWS

from variables.memory_path import MEMORY_PATHS_FOLDER

from spark_process.spark_functions import spark_comparison_to_df_memory_paths, spark_append_to_memory_paths

from neo4j_process.session_requests import compute_shortest_paths_session



def sheduler_batch_weighted_mode(source_list, target_list, experiment_name):

    print("--------------------------------------")
    print("### Sheduler : prepare a batch source/target in mode 'Weighted' ###")
    print("--------------------------------------")

    print("Source/target randomly choosen....")

    source_list = [random.choice(source_list) for i in range(NB_DRAWS)]
    target_list = [random.choice(target_list) for i in range(NB_DRAWS)]

    df_batch = pd.DataFrame({"source" : source_list, "target" : target_list})

    print("...done.")

    if os.path.exists(MEMORY_PATHS_FOLDER):
        if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

            print("There were paths already computed... ")
            print("Comparison by a spark process :")

            start_time = time.time()

            df_to_calc, df_preprocess = spark_comparison_to_df_memory_paths(df_batch)

            print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

            print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

            print(f"Nb. (source/target) already computed : {len(df_preprocess)}.")

        else:

            print("No existing pre-computed paths folder detected...")

            df_to_calc = df_batch

            print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

    else:

        print("No existing pre-computed paths folder detected...")

        df_to_calc = df_batch

        print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

    print("Neo4J Computation ....")

    result_paths_list = compute_shortest_paths_session(df_to_calc, experiment_name)

    print("Results compiled.")

    start_time = time.time()

    spark_append_to_memory_paths(result_paths_list)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    return df_batch.values




def sheduler_batch_equiprobable_mode(source_list, target_list, experiment_name):

    source_list = [random.choice(source_list) for i in range(NB_DRAWS)]
    target_list = [random.choice(target_list) for i in range(NB_DRAWS)]

    df_batch = pd.DataFrame({"source" : source_list, "target" : target_list})

    if os.path.exists(MEMORY_PATHS_FOLDER):
        if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

            print("There were paths already computed... ")
            print("Comparison by a spark process :")

            start_time = time.time()

            df_to_calc, df_preprocess = spark_comparison_to_df_memory_paths(df_batch)

            print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

            print(f"Nb. (source/target) never computed : {len(df_to_calc)}.")

            print(f"Nb. (source/target) already computed : {len(df_preprocess)}.")

            len_df_calc = len(df_to_calc)

            if len_df_calc < RATIO_MIN_DRAWS*NB_DRAWS:

                stock_batch = len_df_calc

                denominator = len_df_calc

                new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

                while stock_batch < RATIO_MIN_DRAWS*NB_DRAWS:

                    print("Minimun required batch not reached.")

                    print(f"New draws with additional draws : {new_nb_draws} draws.")

                    source_list = [random.choice(source_list) for i in range(new_nb_draws)]
                    target_list = [random.choice(target_list) for i in range(new_nb_draws)]

                    df_batch = pd.DataFrame({"source" : source_list, "target" : target_list})

                    start_time = time.time()

                    partial_df_to_calc, df_preprocess = spark_comparison_to_df_memory_paths(df_batch)

                    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

                    print(f"Nb. (source/target) never computed : {len(partial_df_to_calc)}.")

                    print(f"Nb. (source/target) already computed : {len(df_preprocess)}.")

                    part_len_df_calc = len(partial_df_to_calc)

                    stock_batch += part_len_df_calc

                    df_to_calc = pd.concat([df_to_calc, partial_df_to_calc], axis=0)

                    denominator += part_len_df_calc/2

                    new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

            df_to_calc = df_to_calc.iloc[:NB_DRAWS,:]

        else:
            print("No existing pre-computed paths folder detected...")

            df_to_calc = df_batch

            print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")


    else:

        print("No existing pre-computed paths folder detected...")

        df_to_calc = df_batch

        print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

    
    print(f"Neo4J Computation .... Nb draws : {len(df_to_calc)}")

    result_paths_list = compute_shortest_paths_session(df_to_calc, experiment_name)

    print("Results compiled.")

    start_time = time.time()

    spark_append_to_memory_paths(result_paths_list)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    return df_to_calc.values
