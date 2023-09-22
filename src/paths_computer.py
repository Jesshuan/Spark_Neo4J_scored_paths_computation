import sys


from user_interface.mode_experiment import mode_experiment_paths_computer

from list_management.st_provider import provide_source_target_lists

from sheduler_process.sheduler import sheduler_batch_equiprobable_mode, sheduler_batch_weighted_mode

from neo4j_process.session_requests import check_or_regenerate_projection




#This only happens when mymodule.py is called directly:
if __name__ == "__main__":

    mode, experiment_name = mode_experiment_paths_computer(sys.argv[1:])

    source_list, target_list = provide_source_target_lists(mode, experiment_name)

    check_or_regenerate_projection(experiment_name)


    batch_nb = 1
    
    while True:

        print(f" -> Batch number : {batch_nb}.")

        if mode == "equiprobable":

            batch_st_list = sheduler_batch_equiprobable_mode(source_list, target_list, experiment_name)

        elif mode == "weighted":

            batch_st_list = sheduler_batch_weighted_mode(source_list, target_list, experiment_name)

        



        batch_nb+=1





                 
