
import pandas as pd

import os

import sys

import pickle

import numpy as np

import random

from neo4j_process.session_requests import get_all_communes_with_properties_session

from variables.hyperparameters import SOURCE_FILTER_COL, SOURCE_FILTER_VALUE, TARGET_FILTER_COL, TARGET_FILTER_VALUE, SOURCE_DIVIDE_FACTOR, TARGET_DIVIDE_FACTOR

from variables.memory_path import SOURCE_TARGET_LIST_FOLDER


# --- Sub-Sub-Functions definition for two modes ----

def equiprobable_list_generation(df, col_weighted, value_filter):

    df_red = df.loc[df[col_weighted] >= value_filter, "insee"]

    print(f"Length of the list after filtering : {len(df_red)}")

    return df_red.tolist()


def weighted_distribution_list_generation(df, col_weighted, value_filter, divide_factor):

    df_red = df.loc[df[col_weighted] >= value_filter, :]

    result_list = []

    cities_list = df_red['insee'].tolist()

    prop_list = df_red[col_weighted].tolist()

    length_df = len(df_red)

    print(f"Length of the list after filtering : {length_df}")

    prop_max = 0

    for i in range(length_df):

        insee = cities_list[i]

        try:

            prop_score = int(np.round(prop_list[i]*divide_factor,0)) + 1

            if prop_score > prop_max:
                prop_max = prop_score

            for j in range(prop_score):
                result_list.append(insee)

        except:
            pass

        if i%10000==0 and i!=0:
            print(f"{i} cities done...")

    print(f"Length of the list after processing the weighted distribution mode : {len(result_list)}")
    print(f" Occurence max of a city : {prop_max}")
    
    return result_list



# --- Sub-Function ----

def source_target_generation(mode):

    print("--------------------------------------")
    print("### Source/target list generation ###")
    print("--------------------------------------")



    # 1 -- Request to Neo4j to get all communes --

    properties_list = ["insee", SOURCE_FILTER_COL, TARGET_FILTER_COL]

    df_communes = get_all_communes_with_properties_session(properties_list)

    print("Extract of dataframe result :" )

    print(df_communes.head())

    # 2 -- Apply filters and convertion to lists --

    if mode=="equiprobable":

        print("Mode provided is 'equiprobable' so we generate list just applying filters...")

        print("Source list generation :")

        source_list = equiprobable_list_generation(df_communes, SOURCE_FILTER_COL, SOURCE_FILTER_VALUE)

        print("Target list generation :")

        target_list = equiprobable_list_generation(df_communes, TARGET_FILTER_COL, TARGET_FILTER_VALUE)


    elif mode=="weighted":

        print("Mode provided is 'weighted' so we generate new lists, after a first step of filtering, with a probability of occurence in function of the provided properties...")

        print("Source list generation :")
        
        source_list = weighted_distribution_list_generation(df_communes, SOURCE_FILTER_COL, SOURCE_FILTER_VALUE, SOURCE_DIVIDE_FACTOR)

        print("Target list generation :")

        target_list = weighted_distribution_list_generation(df_communes, TARGET_FILTER_COL, TARGET_FILTER_VALUE, TARGET_DIVIDE_FACTOR)

    else:

        print("Error. Mode not knew...")
        sys.exit(2)

    print("Final shuffling...")
    
    random.shuffle(source_list)

    random.shuffle(target_list)

    print("...done.")


    return source_list, target_list



# --- Main function ---- #

def provide_source_target_lists(mode, experiment_name):


    if os.path.exists(SOURCE_TARGET_LIST_FOLDER + experiment_name):

        print("Some existing lists were found for this experiment !...")

        try:

            with open(SOURCE_TARGET_LIST_FOLDER + experiment_name + '/st_list.pkl', 'rb') as f:
                st_list = pickle.load(f)


        except:

            print("...but is it an empty folder ?...")

            source_list, target_list = source_target_generation(mode)

            os.makedirs(SOURCE_TARGET_LIST_FOLDER + experiment_name)

            st_list = {
                "mode" : mode,
                "source_list" : source_list,
                "target_list" : target_list
            }

            with open(SOURCE_TARGET_LIST_FOLDER + experiment_name + '/st_list.pkl', 'wb') as f:
                pickle.dump(st_list, f)


        mode_provided = st_list["mode"]

        if mode_provided != mode:
            print("Sorry, but mode you've provided is different from the experiment's mode...")
            sys.exit(2)
            
        else:

            source_list = st_list["source_list"]
            target_list = st_list["target_list"]

    else:

        source_list, target_list = source_target_generation(mode)

        os.makedirs(SOURCE_TARGET_LIST_FOLDER + experiment_name)

        st_list = {
                "mode" : mode,
                "source_list" : source_list,
                "target_list" : target_list
            }

        with open(SOURCE_TARGET_LIST_FOLDER + experiment_name + '/st_list.pkl', 'wb') as f:
            pickle.dump(st_list, f)

    return source_list, target_list

