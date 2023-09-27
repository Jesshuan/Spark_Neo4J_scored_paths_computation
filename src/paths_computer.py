import sys


from user_interface.mode_experiment import mode_experiment_paths_computer

from list_management.st_provider import provide_source_target_lists

from list_management.buffer_and_historical_manager import append_to_buffer_batches, append_to_historical_batches

from sheduler_process.sheduler import sheduler_batch

from neo4j_process.session_requests import check_or_regenerate_projection




if __name__ == "__main__":

    mode, experiment_name = mode_experiment_paths_computer(sys.argv[1:])

    source_list, target_list = provide_source_target_lists(mode, experiment_name)

    check_or_regenerate_projection(experiment_name)

    batch_nb = 1

    print("")
    print("---- PATH COMPUTATION ON NEO4J -----")
    print("")
    
    while True:

        print(f" -> Batch number : {batch_nb}.")

        batch_st_list = sheduler_batch(source_list, target_list, experiment_name, mode)

        append_to_buffer_batches(batch_st_list, experiment_name, mode)

        append_to_historical_batches(batch_st_list, experiment_name, mode)

        batch_nb+=1





                 
