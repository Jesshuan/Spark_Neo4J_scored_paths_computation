import random

import pandas as pd

import numpy as np

import time

import os

from variables.hyperparameters import NB_DRAWS, RATIO_MIN_DRAWS

from variables.memory_path import MEMORY_PATHS_FOLDER

from spark_process.spark_and_memory_paths import spark_comparison_to_df_memory_paths, spark_append_to_memory_paths

from neo4j_process.session_requests import compute_shortest_paths_session


#----- Sheduler functions -----#

# The sheduler is the main function of the path calculator program,
# an orchestrator program which :
# - generates, during a batch, a list of (source, target) to be calculated,
# - compares this forecast batch with the large list of (source, target) already calculated (by spark processing),
# - then sends this batch to Neo4j (shortest path algorithm)
# - and finally stores all the paths calculated in the memory disk.

# In the case of the "weighted" mode, we divide the path already calculated and the one that has never been calculated into two blocks of data,
# and, after calculation, we join the two parts to send the batch to the buffer list (in reality, we send the original batch before the cut). This treatment maintains a logical distribution of the most important cities,
# as sources or destinations...
# A more complex process is used if the mode is "equiprobable": the path already calculated must not be recalculated
# and keeping it in a batch makes no sense for any "distribution". So, if the batch has a large proportion of
# tuple already calculated, we draw a new set of tuples (source, target) and add it to the first draw, etc... until we have a good
# until we have an adequate quantity of (source, target). The user can define the correct proportion using the RATIO_MIN_DRAWS parameter. (default: 1)



# --- Sub_functions --- #

def generate_a_batch(source_list, target_list, nb_draws):

    # Generate a random batch with a random fuction, from the provided sources and targets lists

    source_list = [random.choice(source_list) for i in range(nb_draws)]
    target_list = [random.choice(target_list) for i in range(nb_draws)]

    return pd.DataFrame({"source" : source_list, "target" : target_list})


def comparison_with_messages(df_batch, delta_table):

    # To compare the provided batch with all the paths already computed by the program (Delta spark file "memory path")
    # The delta spark file is provided as a delta spark table

    start_time = time.time()

    df_to_calc, df_preprocess = spark_comparison_to_df_memory_paths(df_batch, delta_table)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")

    print(f"Nb. (source/target) never computed : : {len(df_to_calc)}.")

    print(f"Nb. (source/target) already computed : {len(df_preprocess)}.")

    return df_to_calc



def no_comparison_no_memory_batch(df_batch):

    # In case of memory empty...
    # We don't have any comparison to do... so the batch is the good batch and we must process all the batch.

    print("No existing pre-computed paths folder detected...")

    df_to_calc = df_batch

    print(f"Nb. (source/target) we have to compute : {len(df_to_calc)}.")

    return df_to_calc




def sheduler_batch(source_list, target_list, experiment_name, mode, delta_table):

    print("--------------------------------------")
    print(f"### Sheduler : prepare a batch source/target in mode : {mode} ###")
    print("--------------------------------------")

    print("Source/target randomly choosen....")

    df_batch = generate_a_batch(source_list, target_list, NB_DRAWS)

    print("...done.")

    # If "Memory path" folder exists, we must compare the batch with all the pre-computed paths.

    if os.path.exists(MEMORY_PATHS_FOLDER):
        if len(os.listdir(MEMORY_PATHS_FOLDER)) != 0:

            print("There were paths already computed... ")
            print("Comparison by a spark process :")

            df_to_calc = comparison_with_messages(df_batch, delta_table)

            # if mode is "weighted", the next part is not effective.
            # if mode is "equiprobable", we must to generate a new more little batch,
            # to look what is the proportion of paths never calculated,
            #  and add it to the original batch... etc, in a loop.

            if mode == "equiprobable":

                len_df_calc = len(df_to_calc)

                if len_df_calc < RATIO_MIN_DRAWS*NB_DRAWS:

                    stock_batch = len_df_calc

                    denominator = len_df_calc

                    # The initial formula to resize the new batch draw :

                    # ---!!!----

                    new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

                    # ---!!!----

                    while stock_batch < RATIO_MIN_DRAWS*NB_DRAWS:

                        print("Minimun required batch not reached.")

                        print(f"New draws with additional draws : {new_nb_draws} draws.")

                        df_batch = generate_a_batch(source_list, target_list, new_nb_draws)

                        partial_df_to_calc = comparison_with_messages(df_batch, delta_table)

                        part_len_df_calc = len(partial_df_to_calc)

                        print(f"New_Stock = (old_stock) {stock_batch} + (add) {part_len_df_calc} = {stock_batch + part_len_df_calc}")

                        stock_batch += part_len_df_calc

                        df_to_calc = pd.concat([df_to_calc, partial_df_to_calc], axis=0)

                        denominator += part_len_df_calc/2

                        # An other version of the same formula to resize the new batch draw, with the new denominator :

                        new_nb_draws = int(NB_DRAWS*((RATIO_MIN_DRAWS*NB_DRAWS)/denominator - 0.9))

                print (f"Reduction to nb_draws : {NB_DRAWS}")

                df_to_calc = df_to_calc.iloc[:NB_DRAWS,:]

        else:

            df_to_calc = no_comparison_no_memory_batch(df_batch)

    else:

        df_to_calc = no_comparison_no_memory_batch(df_batch)


    print(f"Neo4J Computation .... with batchs : {len(df_to_calc)}")

    result_paths_list = compute_shortest_paths_session(df_to_calc, experiment_name)

    print(f"{len(result_paths_list)} results compiled.")

    start_time = time.time()

    #  ---- Spark Append to memory paths ---

    spark_append_to_memory_paths(result_paths_list, delta_table)

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")


    if mode == "equiprobable": # if mode is "equiprobable" we send the df_to_calc
                                    # (final batch of paths that have only just been calculated)

        return df_to_calc.values
    
    elif mode == "weighted": # if mode is "weighted" we send the initial batch (paths already computed + paths that have only just been calculated)

        return df_batch.values

