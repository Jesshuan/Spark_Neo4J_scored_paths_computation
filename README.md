# Modelling tourist flows with shortest paths computation
## between source / destination cities
## with a Neo4j graph and a spark aggregation

This project is linked to the DataForGood project E-Carto_Mobile, the aim of which is to offer a web platform that can be used to visualise national (mainland France) requirements for electric vehicle charging points.

Description of the global project here : https://dataforgood.notion.site/volution-des-infrastructures-de-recharge-pour-les-v-hicules-lectriques-e6dc33916a96435ea068a630a92c9799
[in french]

The git of the global project : https://github.com/dataforgoodfr/batch11_e_cartomobile#data-for-good---batch-11---e-cartomobile

And the website mock-up : https://github.com/Thopiax/e-cartomobile-ui

----

### Scoring the needs of charging points and score 2 importance

As part of the project's small R&D team, we're looking to "score" the needs of every town and village in France in terms of charging points for users' daily journeys, tourist trips, etc...

This modelling research has led us to divide these types of needs into 3 or 4 categories, which are updated in this methodology : https://dataforgood.notion.site/M-thodologie-83832573d23f484f960db4e77561ac14

In this methodology, an important category of score is the score linked to the tourism flow :

Most users need to recharge their electric vehicle during a long journey (more than two hours) to a tourist destination, in summer or winter, during a break. Apart from this, most users simply recharge their vehicles at home.
This score, called score 2 in the methodology, therefore requires the ability to model tourist flows throughout France, in order to identify the communes closest to the points where these flows pass through.


### General principle of the score 2 continuous generation :

The principle is as follows:

Firstly, a graph was constructed using Neo4J technology, based on an aggregation of data taken from Open Street Map, coupled with various other sources (data.gouv for the geometry of the communes, for example).

In this graph, a road network (made up of "ROAD_POINTS" nodes and "RELATED" relationships / roads) is connected to the communes ("CITY" nodes) thanks to the prior generation of new "NEARLY_TO" relationships which make it possible to identify, for a given road point, the communes less than 5km away.
[To recreate the graph, it's here: https://github.com/dataforgoodfr/batch11_e_cartomobile/tree/main/Neo4j_container]

Next, this code, which runs inside a Spark docker container, is used to generate score batches that will be incremented continously in a database using the process illustrated below:

![global schema](/illustr/common_schema.jpg "Global schema").


The process is divided into two scripts that can be run independently, within the spark container.

The ***"paths_calculator.py" script***:

- launches a request to the Neo4j graph to retrieve the updated list of communes with their geocode (code named "INSEE code" in France).

- randomly selects source/destination pairs from this list and builds up a batch of paths to calculate (example: 10,000 source/destination pairs).

- compares the batch data with an existing memory of previously calculated paths ("Memory paths storage", which we store in the form of a file in delta spark format).

- for paths not yet calculated, launches a request to the Neo4j graph to calculate the shortest path (Dijkstra algorithm) for each source/destination pair.

- retrieves, for each pair, the path travelled in the road network (via road points), as well as the associated costs (in travel time) along the route, and the overall cost.

- commit all the batch of source/destination pair to a buffer list (queue) for the second script. This buffer list is stored as a pickle object.

- commit also this batch to a "historical" list, which is a list that infinitly extends, contrary to the "buffer" list. This historic can be used by a specific "fast recomputation" mode by the second script.

The second sript, **"spark_aggregator.py" script**:

- launches a request to the Neo4j graph to retrieve the updated list of communes with their geocode (code named "INSEE code" in France) but also differents associated necessaries properties aggregated by town (tourism features, number of registered electric cars...) for the specific mode "equiprobable" (see below the difference between the two mode).

- interrogates the "buffer" list provided by the "paths calculator" script and take, one by one, a batch of source/destination pair.

- take the associated path of each source/destination pair in the "memory paths" storage (delta spark file format)

- with this path of road points, lanches a new neo4J request to the graph to collect all the cities (towns) which are nearly to these road points nodes...

- with the result of all the collection of towns in a dataframe, explodes it and aggregates it to score each apparition of a town

- sends the final result (the aggregated scores) to the database by a "upsert" process (new value = old value + score for each town)

- (and take a snapshot of the database and save it, for any specific studies)

----

### Launching the Spark (and database) container(s)

Git clone this project !...

Before building and launching the container, you must verify the docker-compose file :

- If you don't need a test database (external database), you can comment or remove the two service postgres-db and postgres-admin.

- Please adjust your ports numbers if necessary (I use the port 5433 for the test database, because my port 5432 is already used by a postgres service, you can correct it to 5432.)

- All the sensitive data (passwords) is transmitted via environment variables.
You need to create an ".env" file in the same folder than the docker-compose file and write the passwords for the Neo4j instances and the database in it, like this :

```

NEO4J_PASSWORD = "[password_for_neo4j]"

DB_PASSWORD="[password_for_my_db]"

```

This .env file will be interpreted when the container is built.

Add other environment variables if you don't want it to be shared on a git account like this one. Or adopt another method like docker secrets, if the database allows it.

-  In the docker-compose file, on the spark-worker service chapter, adjust the memory and processors size of the spark components : SPARK_WORKER_MEMORY, SPARK_EXECUTOR_MEMORY, SPARK_WORKER_CORES.
Number of spark-worker will be mutiplied during the building and launching. Therefore, please calculate this parameters in function of your local ressources but also you desired number of spark worker.


-> **Build and launch the containers** :

```
docker-compose up --scale spark-worker=2
```

...for example, if you want two spark worker (default : 1)


In a terminal, to see if the container(s) work:

```
docker ps
```

### Change parameters

All the scripts and module are synchronized between your local interface and the container envionement.
So, you can change all parameters you want in the "variables" folder :

- connections variables

- memory path (but don't change it except in the case of restructuring the code architecture)

- **hyperparameters** :

in this file, please change your contributor name. (This contributor name will be used to identify who is sending which batch in the monitoring of calculated batches, if a database and an experiment are shared between several contributors)

Check all the others parameters and don't change the default paramaters if you don't immediately understand the purpose of each parameter...

Parameters like the Filters Parameters and the Weighting Parameters (for "equiprobable" mode) must remain fixed for a single experiment. (share your hyperparameters between contributors if you share the same experiment!)


### Launching the two scripts

Now, to launch the two script, the preferable option is to enter within the spark container. (Visualisation is a better thing when the script are processing !...)

The main (executor) spark container has a name ending in "...spark" and not "...spark-worker".

Locate this executor spark container by its ID with ```docker ps```.

And do :

```
docker exec -it [CONTAINER_ID] sh
```

Once inside, you can do "cd work/src" etc... to find the two scripts.

-> You can firstly launch the script "paths_calculator.py".

```
python paths_calculator.py
```

Please specify the name of your experiment and the mode with these options:

```
python paths_calculator.py -e my_experiment_name -m equiprobable
```

or ```-m weighted``` in case of "weigthed" mode.

(By default, the mode is "equiprobable" and the experiment name will be a string composed with the actual date and the word "experiment_" )


-> You can secondly, in parallel, launch the script "spark_aggregator.py".

```
python spark_aggregator.py
```

Please specify the name of your experiment and the mode with these options:

```
python spark_aggregator -e my_experiment_name -m equiprobable
```

or ```-m weighted``` in case of "weigthed" mode.

(By default, the mode is "equiprobable" and the experiment name will be a string composed with the actual date and the word "experiment_" )

The mode shared between the two script, with the same experiment name, must be the same (otherwise it will generate an error)

A third option can be optionnaly used in case of "fast recomputation" for this script :

```
python spark_aggregator -e my_experiment_name -m weighted -f old_experiment_name
```

This option means that the aggregator script, before any other batch, will start by retrieving all the history of another experiment and quickly recalculate all the batches (in the same order) of this other (and old) experiment, before waiting for new batches from the current experiment.
This "fast recomputation" mode is very useful for trying out, for example, other weighting laws in the "equiprobable" mode.

----

### Difference between the two mode "weighted" and "equiprobable" :

Here, we explain the difference between the two mode "weighted" and "equiprobable".

In both cases, the idea is to weight the scores obtained by the towns and villages, by virtue of their presence on the routes, by their relative importance in relation to the problem.
A path, and the towns that make it up, is more important (more likely to be used) if the source town has more registered electric vehicles. Similarly, it will also be weighted more heavily (and so will the towns that make it up) if the destination town has a higher tourism score (for example, the "number of visitor points" feature).

In the **"weighted" mode**, this weighting is carried out upstream, when the initial lists of possible sources and targets are built.

Firstly, a filter can be applied to remove from the list source towns that do not have enough registered electric vehicles (e.g. < 5) or destination towns that do not have enough tourist points of interest (e.g. < 2).
The script then builds its list of sources according to the importance of a feature. A town with only 5 registered vehicles will appear only 1 time in the source list, for example. Whereas a town with, say, 200 electric vehicles will appear 20 times (2000 times 0.01, because there is a parameter to reduce this impact to avoid lists that are too large).
Similarly, for the list of destinations, a town with just 10 tourist points of interest will appear, for example, 1 time, whereas a town with 180 will appear 18 times (a reduction factor of 0.1).

In this way, when the script then takes its source/destination pairs at random from the lists constructed, it follows a distribution of the relative importance of each path, in the eyes of the problem.

This mode therefore "gets to the point" by having a greater chance of calculating the important paths, and it is therefore possible that it will converge more quickly towards a solution for score 2.


![weighted mode schema](/illustr/weighted_mode_schema.jpg "Weighted mode schema")

In **"equiprobable" mode**, the weighting is not upstream but downstream.

As in the previous mode, when the source and destination base lists are compiled, filters can be applied to remove cities that do not have enough electric vehicles as sources, or cities that do not have enough touristic points of interest as destinations. However, apart from this filter, all source and destination cities will be represented in the basic list in the same proportion.

So the paths are calculated regardless of any weighting. In this way, all the possible paths are fairly represented, and their representation is balanced over time, even if it seems to converge less quickly towards a possible global solution for score 2.

Then, just before the aggregation phase, the important features of the sources and destinations that are to be used to weight the importance of the paths are requested in the graph. Aggregation takes into account a weighting law between these features that has been entered by the user. 

The advantage of this mode is that a solution can be calculated without weighting initially, and then a weighting rule can be provided afterwards.

![equiprobable mode schema](/illustr/equiprobable_mode_schema.jpg "Equiprobable mode schema")

The differences between the two modes force the two scripts to use different join and data path logics, as can be seen in the two diagrams above.
The main implementation difference is that in the "weighted" mode, the paths already calculated in the batch and those yet to be calculated are kept and added to be supplied to the aggregator script, in order to preserve the distribution of source lists and upstream weighted targets. This mode is based on a progressive incrementation of important paths (and the cities associated with these paths and scored), where redundancy reflects importance.
In "equiprobable" mode, the paths already calculated in the batch (already present in memory) are of no interest, and are replaced, within a loop, by new paths to be calculated. The aim of this mode is to achieve exhaustive computation of all possible source/target combinations, because the importance of each path (and of the cities associated with these paths and scored) is only deduced after the fact when the weighting for each path is calculated.


### Map Vizualisation :

![example map France with paths](/illustr/carte_france_paths.jpg "Example map France with paths")

In addition to the results stored in your database (or the test database supplied as a container), it is possible to launch a third script, from the "src" folder, which launches a joint request to your database and the Neo4j graph (to retrieve the coordinates of each city) and then uses this to generate a map with Plotly express.
This map is automatically saved in html format in a "viz" folder.
Simply run the python script like this:

```
python export_map_result_html.py -e my_experiment_name
```

Make sure you supply an existing experiment name.


Moreover, you can, of course, export your tables stored in the database, whether they be tables containing the cities that have been marked or meta-tables listing the progress of batch shipments for each experiment.









