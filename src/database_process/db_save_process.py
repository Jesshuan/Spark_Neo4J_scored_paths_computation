import os

import pandas as pd

from variables.memory_path import DB_SNAPSHOTS_FOLDER



def save_df_to_csv(df_table_saved, experiment_name, date, batch_nb):

    length_df = len(df_table_saved)

    print("Saving snapshot of database...")

    if os.path.exists(DB_SNAPSHOTS_FOLDER + experiment_name):

        print("Some existing snapshots folder was found for this experiment...")

    else:

        print("No folder found for this experiment. Generation !")

        os.makedirs(DB_SNAPSHOTS_FOLDER + experiment_name)

    filename = "db_snapshot_exp_" + experiment_name + "_" + date + "_" + str(batch_nb)

    df_table_saved.to_csv(DB_SNAPSHOTS_FOLDER + experiment_name + "/" + filename, sep=",")

    print(f"Saving of the dataframe of length {length_df} done.")


