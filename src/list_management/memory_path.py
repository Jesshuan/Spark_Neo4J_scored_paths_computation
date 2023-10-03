import os

import time

from variables.memory_path import MEMORY_PATHS_FOLDER


def check_and_clean_memory_path():

    print("Check cache of memory file folder (delta spark format)...")

    # Read all files in "_delta_log"

    path = MEMORY_PATHS_FOLDER + "_delta_log/"

    try:

        list_json_files = os.listdir(path)

    except:

        print("No path memory folder found.")

        return
    
    # Dectect the last json file written in the folder delta

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





"""

    list_files = os.listdir(MEMORY_PATHS_FOLDER)

    if len(list_files) > MIN_MEMORY_PATH_NB_FILES:

        clean_middle_old_files('.part', list_files)

        list_files = os.listdir(MEMORY_PATHS_FOLDER)

        clean_middle_old_files('part', list_files)

        print("--- Cache cleaned !")





def clean_middle_old_files(key_string, list_files):

    tupple_list = []
    path = MEMORY_PATHS_FOLDER + "/"

    for file in list_files:
        if file.startswith(key_string):
            _, _, _, _, _, _, _, _, mtime, _ = os.stat(path + file)
            tupple_list.append((file, mtime))

    tupple_list.sort(key = lambda x: x[1])

    len_t_l = len(tupple_list)

    for tup_file in tupple_list[0:int(len_t_l*RATIO_CLEAN_CACHE_MEMORY)]:

        os.remove(path + tup_file[0])

        print(f"delete : {tup_file[0]}")

"""