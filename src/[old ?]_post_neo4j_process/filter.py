# Binary Search in python

import pandas as pd


def binarySearch(array, x, low, high):

    if high >= low:

        mid = low + (high - low)//2

        # If found at mid, then return it
        if array[mid] == x:
            return mid

        # Search the left half
        elif array[mid] > x:
            return binarySearch(array, x, low, mid-1)

        # Search the right half
        else:
            return binarySearch(array, x, mid + 1, high)

    else:
        return low


def filter_begining(df, minimal_duration):

    costs_list = df["costs"].tolist()
    collection_list = df["collection"].tolist()

    new_collection = []

    df_len = len(df)

    for i in range(df_len):

        communes_array = collection_list[i]

        len_communes_array = len(communes_array)

        costs_array = costs_list[i]

        len_costs_array = len(costs_array)

        index_min = binarySearch(costs_array, minimal_duration, 0, len_costs_array - 1)

        ratio = index_min / len_costs_array

        index_begin = int(ratio*len_communes_array)

        new_collection.append(communes_array[index_begin:])

    df["collection"] = new_collection

    df.drop(["costs"], axis=1, inplace=True)

    return df