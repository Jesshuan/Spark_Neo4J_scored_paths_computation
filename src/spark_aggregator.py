import sys


from user_interface.mode_experiment import mode_experiment_spark_agregator

from variables.hyperparameters import COLUMNS_LIST_FOR_WEIGHTING

from neo4j_process.session_requests import get_all_communes_with_properties_session

from list_management.buffer_and_historical_manager import transfert_historical_to_buffer, read_head_buffer_batches, pop_head_buffer_batches

from spark_process.spark_and_memory_paths import spark_get_a_df_memory_paths_selection_from_batch

from spark_process.spark_filters import spark_filter_short_travels, spark_filter_cut_begining

from spark_process.spark_aggregation import spark_aggregation

from pyspark.sql.functions import expr

if __name__ == "__main__":

    mode, experiment_name, fast_recomp, exp_fr = mode_experiment_spark_agregator(sys.argv[1:])


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

        batch_list = read_head_buffer_batches(experiment_name, mode)

        if batch_list is None:

            break

        print(f"Aggregation of a batch of size {len(batch_list)}...")

        print("batch extract :")

        print(batch_list[0:5])

        df_spark_batch = spark_get_a_df_memory_paths_selection_from_batch(batch_list)

        #df_spark_batch = spark_filter_short_travels(df_spark_batch)

        #df_spark_batch = spark_filter_cut_begining(df_spark_batch)

        print(df_spark_batch.withColumn("Len_costs",expr("size(costs)")).withColumn("Len_path",expr("size(path)")).take(1))

        if mode == "equiprobable":

            df_result = spark_aggregation(df_spark_batch.take(1), mode, weighted_feats_df = weighted_feats_df)

        elif mode == "weighted":

            df_result = spark_aggregation(df_spark_batch, mode)


        print(df_result)

        print("---")

        print(df_result.loc[df_result['path'].isnull(),:])

        print("---")

        print(df_result.loc[df_result['costs'].isnull(),:])



