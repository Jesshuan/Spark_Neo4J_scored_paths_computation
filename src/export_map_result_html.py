
import plotly.express as px

import numpy as np

import sys

import os

import pandas as pd

from user_interface.mode_experiment import experiment_export_map

from database_process.db_requests import take_a_snapshot_db

from neo4j_process.session_requests import get_all_communes_with_properties_session

from variables.memory_path import MAP_RESULT_FOLDER

if __name__ == "__main__":

    experiment_name = experiment_export_map(sys.argv[1:])

    print("Take the result from db...")

    df_value = take_a_snapshot_db(experiment_name)

    print("Neo4j request to get all cities coordinates...")

    df_communes = get_all_communes_with_properties_session(['insee','x', 'y'])

    print("Computation... (merge and normalization)...")

    df_join_score = df_value.merge(df_communes, how="left", on="insee")

    df_join_score["ln_value"] = df_join_score["value"].apply(lambda v : np.log(v))

    print("Plotly figure generation... with open-street-map layer and log scaling")

    fig = px.scatter_mapbox(df_join_score, lat="y", lon="x", mapbox_style="open-street-map", color_continuous_scale="reds", color="ln_value", hover_name="insee")
 
    print("Export to html format...")

    if not os.path.exists(MAP_RESULT_FOLDER):

        os.makedirs(MAP_RESULT_FOLDER)

        os.chmod(MAP_RESULT_FOLDER, 0o777)

        print("Folder Viz created...")

    filepath = MAP_RESULT_FOLDER + "viz_" + experiment_name + ".html"

    fig.write_html(filepath)

    os.chmod(filepath, 0o777)

    print("Export !")


