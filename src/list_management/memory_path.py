import os

import time

import numpy as np

from variables.memory_path import MEMORY_PATHS_FOLDER

from spark_process.spark_and_memory_paths import spark_recompact_file


# A special function to compact the memory paths file in parquet format.
# And after that, to clean obsolete files in the memory path.
# Built in relation to the delta spark paradigm and the folder "_delta_log"
# which manage the history of the merged, overwrite files, etc...

def check_and_clean_memory_path():

    print("Check cache of memory file folder (delta spark format)...")

    # Read all files in "_delta_log"

    path = MEMORY_PATHS_FOLDER + "_delta_log/"

    if not os.path.exists(path):

        print("No path memory folder found.")

        return
    
    # --- COMPACTION --- #
    
    # Deta spark file compaction

    start_time = time.time()

    spark_recompact_file()

    print(f"Spark job done in {np.around(time.time() - start_time, 2)} sec.")


    # --- CLEANING --- #

     # Dectect the last json file written in the folder delta

    list_json_files = os.listdir(path)

    key_start = '00'

    key_end = '.json'

    tupple_list = []

    for file in list_json_files:
        if file.startswith(key_start) and file.endswith(key_end):
            _, _, _, _, _, _, _, _, _, ctime = os.stat(path + "/" + file)
            tupple_list.append((file, ctime))

    tupple_list.sort(key = lambda x: x[1])

    json_latest = tupple_list[-1][0]

    # Read the json file and parse it :


    with open(path + '/' + json_latest, "r") as f:
        jsonContent = f.read()

    # Security, verify the json file is a checkpoint of the last compaction :
    if jsonContent.split('"operation":"')[1].split('",')[0]!= "WRITE":
        print("Last json file in the delta folder not recognized... It is a WRITE operation for compaction ?")
        print("Conpaction aborted.")
        return

    json_term_list = jsonContent.split("{")

    # Analyse important files parquet to keep :

    kept_file_list = []

    for i, term in enumerate(json_term_list):

        if term == '"add":' or term == '"remove":':
            next_term_file = json_term_list[i+1].split('"path":"')[1].split('","')[0]
            kept_file_list.append(next_term_file)

    print("Necessary files we have to keep :")

    print("\n".join(kept_file_list))

    # Return to the delta folder and collect all the files :

    list_files = os.listdir(MEMORY_PATHS_FOLDER)

    key_start = "part"

    # Reduce the list to the partion files ("part-...") :

    part_list_file = [file for file in list_files if file.startswith(key_start)]

    # Remove all the partition files not in the protected list :

    deleted_files = []

    for file in part_list_file:
        if file not in kept_file_list:
            os.remove(MEMORY_PATHS_FOLDER + file)
            deleted_files.append(file)

    print("Files deleted :")

    print("\n".join(deleted_files))

    print("--- Memory path cache cleaned ! ---")


