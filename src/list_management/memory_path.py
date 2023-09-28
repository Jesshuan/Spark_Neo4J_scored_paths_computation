import os

import time

from variables.memory_path import MEMORY_PATHS_FOLDER

from variables.hyperparameters import MIN_MEMORY_PATH_NB_FILES


def clean_middle_old_files(key_string, list_files):

    tupple_list = []
    path = MEMORY_PATHS_FOLDER + "/"

    for file in list_files:
        if file.startswith(key_string):
            _, _, _, _, _, _, _, _, mtime, _ = os.stat(path + file)
            tupple_list.append((file, mtime))

    tupple_list.sort(key = lambda x: x[1])

    len_t_l = len(tupple_list)

    for tup_file in tupple_list[0:len_t_l//2]:

        os.remove(path + tup_file[0])

        print(f"delete : {tup_file[0]}")


def check_and_clean_memory_path():

    print("Check cache of memory file folder (delta spark format)...")

    list_files = os.listdir(MEMORY_PATHS_FOLDER)

    if len(list_files) > MIN_MEMORY_PATH_NB_FILES:

        clean_middle_old_files('.part', list_files)

        list_files = os.listdir(MEMORY_PATHS_FOLDER)

        clean_middle_old_files('part', list_files)

        print("--- Cache cleaned !")