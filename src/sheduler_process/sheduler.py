import random

import pandas as pd

import numpy as np

import time

import os

from variables.hyperparameters import NB_DRAWS, RATIO_MIN_DRAWS

from variables.memory_path import MEMORY_PATHS_FOLDER

from spark_process.spark_and_memory_paths import spark_comparison_to_df_memory_paths, spark_append_to_memory_paths

from neo4j_process.session_requests import compute_shortest_paths_session



def generate_a_batch(source_list, target_list, nb_draws):

    source_list = [random.choice(source_list) for i in range(nb_draws)]
    target_list = [random.choice(target_list) for i in range(nb_draws)]

    return pd.DataFrame({"source" : source_list, "target" : target_list})


def comparison_with_messages(df_batch):

    start_time = time.time()

    df_to_calc, df_preprocess = spark_comparison_to_df_memory_paths(df_batch)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    print(f"Nb. (source/target) never computed : : {len(df_to_calc)}.")

    print(f"Nb. (source/target) already computed : {len(df_preprocess)}.")

    return df_to_calc



def no_comparison_no_memory_batch(df_batch):

    print("No existing pre-computed paths folder detected...")

    df_to_calc = df_batch

    print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

    return df_to_calc




def sheduler_batch(source_list, target_list, experiment_name, mode):

    print("--------------------------------------")
    print(f"### Sheduler : prepare a batch source/target in mode : {mode} ###")
    print("--------------------------------------")

    print("Source/target randomly choosen....")

    df_batch = generate_a_batch(source_list, target_list, NB_DRAWS)

    print("...done.")

    if os.path.exists(MEMORY_PATHS_FOLDER):
        if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

            print("There were paths already computed... ")
            print("Comparison by a spark process :")

            df_to_calc = comparison_with_messages(df_batch)

            if mode == "equiprobable":

                len_df_calc = len(df_to_calc)

                if len_df_calc < RATIO_MIN_DRAWS*NB_DRAWS:

                    stock_batch = len_df_calc

                    denominator = len_df_calc

                    new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

                    while stock_batch < RATIO_MIN_DRAWS*NB_DRAWS:

                        print("Minimun required batch not reached.")

                        print(f"New draws with additional draws : {new_nb_draws} draws.")

                        df_batch = generate_a_batch(source_list, target_list, new_nb_draws)

                        partial_df_to_calc = comparison_with_messages(df_batch)

                        part_len_df_calc = len(partial_df_to_calc)

                        print(f"New_Stock = (old_stock) {stock_batch} + (add) {part_len_df_calc} = {stock_batch + part_len_df_calc}")

                        stock_batch += part_len_df_calc

                        df_to_calc = pd.concat([df_to_calc, partial_df_to_calc], axis=0)

                        denominator += part_len_df_calc/2

                        new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

                print (f"Reduction to nb_draws : {NB_DRAWS}")

                df_to_calc = df_to_calc.iloc[:NB_DRAWS,:]

        else:

            df_to_calc = no_comparison_no_memory_batch(df_batch)

    else:

        df_to_calc = no_comparison_no_memory_batch(df_batch)


    print(f"Neo4J Computation .... with batchs : {len(df_to_calc)}")

    result_paths_list = compute_shortest_paths_session(df_to_calc, experiment_name)

    print("Results compiled.")

    start_time = time.time()

    spark_append_to_memory_paths(result_paths_list)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    if mode == "equiprobable":

        return df_to_calc.values
    
    elif mode == "weighted":

        return df_batch.values


"""

def sheduler_batch_equiprobable_mode(source_list, target_list, experiment_name):

    df_batch = generate_a_batch(NB_DRAWS)

    if os.path.exists(MEMORY_PATHS_FOLDER):
        if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

            print("There were paths already computed... ")
            print("Comparison by a spark process :")

            df_to_calc, df_preprocess = comparison_with_messages(df_batch)

            len_df_calc = len(df_to_calc)

            if len_df_calc < RATIO_MIN_DRAWS*NB_DRAWS:

                stock_batch = len_df_calc

                denominator = len_df_calc

                new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

                while stock_batch < RATIO_MIN_DRAWS*NB_DRAWS:

                    print("Minimun required batch not reached.")

                    print(f"New draws with additional draws : {new_nb_draws} draws.")

                    df_batch = generate_a_batch(new_nb_draws)

                    partial_df_to_calc, df_preprocess = comparison_with_messages(df_batch)

                    part_len_df_calc = len(partial_df_to_calc)

                    print(f"New_Stock = (old_stock) {stock_batch} + (add) {part_len_df_calc} = {stock_batch + part_len_df_calc}")

                    stock_batch += part_len_df_calc

                    df_to_calc = pd.concat([df_to_calc, partial_df_to_calc], axis=0)

                    denominator += part_len_df_calc/2

                    new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

            df_to_calc = df_to_calc.iloc[:NB_DRAWS,:]

        else:
            df_to_calc = no_comparison_no_memory_batch(df_batch)


    else:

        df_to_calc = no_comparison_no_memory_batch(df_batch)

    
    print(f"Neo4J Computation .... Nb draws : {len(df_to_calc)}")

    result_paths_list = compute_shortest_paths_session(df_to_calc, experiment_name)

    print("Results compiled.")

    start_time = time.time()

    spark_append_to_memory_paths(result_paths_list)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    return df_to_calc.values

    
"""