import sys

import time

from datetime import datetime

from user_interface.mode_experiment import mode_experiment_spark_agregator

from variables.hyperparameters import COLUMNS_LIST_FOR_WEIGHTING

from neo4j_process.session_requests import get_all_communes_with_properties_session

from list_management.buffer_and_historical_manager import transfert_historical_to_buffer, read_head_buffer_batches, pop_head_buffer_batches

from aggregator_process.organizer import aggregate_a_batch

from database_process.db_interactions import transfer_to_db_and_snapshot

from database_process.db_save_process import save_df_to_csv


if __name__ == "__main__":

    mode, experiment_name, fast_recomp, exp_fr = mode_experiment_spark_agregator(sys.argv[1:])

    date = str(datetime.now()).split(".")[0].replace(" ","_")

    if mode == "equiprobable":

        print("Equiprobable mode : Neo4j request to get features for weighting process during all future aggregations....")

        weighted_feats_df = get_all_communes_with_properties_session(list(set(["insee"] + COLUMNS_LIST_FOR_WEIGHTING)))

        print("Extract :")
        print(weighted_feats_df.head())

    
    if fast_recomp:

        transfert_historical_to_buffer(experiment_name, exp_fr, mode)

    print("")
    print("---- PATHS AGGREGATION WITH SPARK -----")
    print("")

    batch_nb = 1

    while True:

        print(f" -> Batch number : {batch_nb}.")

        batch_list = read_head_buffer_batches(experiment_name, mode)

        if batch_list is None:

            print("waiting for a new batch...")

            time.sleep(5)

            continue

        if mode=="weighted":

            df_result = aggregate_a_batch(batch_list, mode)

        elif mode=="equiprobable":

            df_result = aggregate_a_batch(batch_list, mode, weighted_feats_df)

        df_table_saved = transfer_to_db_and_snapshot(df_result, experiment_name)

        save_df_to_csv(df_table_saved, experiment_name, date, batch_nb)

        pop_head_buffer_batches(experiment_name, mode)

        batch_nb += 1

        
        



