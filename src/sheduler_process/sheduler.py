import random

import pandas as pd

import os

from variables.hyperparameters import NB_DRAWS

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

    #if os.path.exists(MEMORY_PATHS_FOLDER):
    if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

        print("There were paths already computed... ")
        print("Comparison by a spark process :")

        df_to_calc, df_preprocess = spark_comparison_to_df_memory_paths(df_batch)

        print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

        print(f"Nb. (source/target) already computed : {len(df_preprocess)}.")

    else:

        print("No existing pre-computed paths folder detected...")

        df_to_calc = df_batch

        print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

    print("Neo4J Computation ....")

    result_paths_list = compute_shortest_paths_session(df_to_calc, experiment_name)

    print("Results compiled.")

    spark_append_to_memory_paths(result_paths_list)

    return df_batch.values




def sheduler_batch_equiprobable_mode(mode, source_list, target_list):

    stock = 0


    while (stock!=NB_DRAWS):

        source = [random.choice(source_list) for i in range(NB_DRAWS)]
        target = [random.choice(target_list) for i in range(NB_DRAWS)]


    return []


        