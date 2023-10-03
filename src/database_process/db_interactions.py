from variables.hyperparameters import CONTRIBUTOR

from database_process.db_requests import get_all_tables, create_table, create_meta_table, update_to_database, take_a_snapshot_db


from datetime import datetime

def create_table_or_check(table_n, tables_list):

    if table_n not in tables_list:

        print(f"Table {table_n} generation...")

        create_table(table_n)

        print("Verification")

        tables_list = get_all_tables()

        if table_n not in tables_list:

            raise Exception("Sorry, but there is a problem with the table generation on the database....")
        



def transfer_to_db_and_snapshot(df_result, experiment_name, date, batch_nb, mode, len_batch, len_df_filter):

    table_name = experiment_name

    meta_table_name = "meta_" + experiment_name

    tables_list = get_all_tables()

    if table_name not in tables_list:

        print(f"Table {table_name} generation...")

        create_table(table_name)


    if meta_table_name not in tables_list:

        print(f"Table {meta_table_name} generation...")

        create_meta_table(meta_table_name)


    print("Verifications")

    tables_list = get_all_tables()

    if table_name not in tables_list or meta_table_name not in tables_list:

        raise Exception("Sorry, but there is a problem with tables generation on the database....")



    print("--- UPDATE TO DATABASE ---")
    print("Export results to the table of the experiment in the database and add infos to meta-table...")

    date_update = datetime.now()

    update_to_database(df_result, table_name, meta_table_name, date_update, CONTRIBUTOR, date, batch_nb, mode, len_batch, len_df_filter)

    print("done.")

    print("Take a snapshot of the db for archiving.")

    df_saved = take_a_snapshot_db(table_name)

    print('Snapshot saved.')

    print("Add infos to the meta table on db.")


    return df_saved


