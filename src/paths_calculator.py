import sys

import os


from user_interface.mode_experiment import mode_experiment_paths_computer

from list_management.st_provider import provide_source_target_lists

from list_management.buffer_and_historical_manager import append_to_buffer_batches, append_to_historical_batches

from sheduler_process.sheduler import sheduler_batch

from neo4j_process.session_requests import check_or_regenerate_projection

from spark_process.spark_and_memory_paths import spark_get_delta_table_from_memory_paths

from variables.memory_path import MEMORY_PATHS_FOLDER

from variables.hyperparameters import CLEAN_CACHE_MEMORY_INTERVAL

from list_management.memory_path import check_and_clean_memory_path


if __name__ == "__main__":

    mode, experiment_name = mode_experiment_paths_computer(sys.argv[1:])

    source_list, target_list = provide_source_target_lists(mode, experiment_name)

    check_or_regenerate_projection(experiment_name)

    batch_nb = 1

    print("")
    print("---- PATH COMPUTATION ON NEO4J -----")
    print("")

    already_delta = "not yet"

    if os.path.exists(MEMORY_PATHS_FOLDER + "_delta_log"):
        delta_table = spark_get_delta_table_from_memory_paths()
        already_delta = "many"
    else:
        delta_table = None
    
    while True:

        if already_delta=="once":
            delta_table = spark_get_delta_table_from_memory_paths()
        

        print(f" -> Batch number : {batch_nb}.")

        batch_st_list = sheduler_batch(source_list, target_list, experiment_name, mode, delta_table)

        if already_delta == "not yet":

            already_delta = "once"

        elif already_delta == "once":

            already_delta = "many"

        if batch_nb%CLEAN_CACHE_MEMORY_INTERVAL + 1 == CLEAN_CACHE_MEMORY_INTERVAL:
            check_and_clean_memory_path()

        append_to_buffer_batches(batch_st_list, experiment_name, mode)

        append_to_historical_batches(batch_st_list, experiment_name, mode)

        batch_nb+=1





                 
