from database_process.db_requests import get_all_tables, create_table, update_to_database, take_a_snapshot_db


def transfer_to_db_and_snapshot(df_result, experiment_name):

    table_name = experiment_name

    tables_list = get_all_tables()


    if table_name not in tables_list:

        create_table(table_name)

        print("Verification")

        tables_list = get_all_tables()

        if table_name not in tables_list:

            raise Exception("Sorry, but there is a problem with the table generation on the database....")


    print("--- UPDATE TO DATABASE ---")
    print("Export results to the table of the experiment in the database...")
    
    update_to_database(df_result, table_name)

    print("done.")

    df_saved = take_a_snapshot_db(table_name)

    print(df_saved)

    return df_saved


