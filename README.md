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

This modelling research has led us to divide these types of needs into 3 or 4 categories, which are updated in this methodology : (link)

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


### Difference between the two mode "weighted" and "equiprobable" :

Here, we explain the difference between the two mode "weighted" and "equiprobable".

In both cases, the idea is to weight the scores obtained by the towns and villages, by virtue of their presence on the routes, by their relative importance in relation to the problem.
A path, and the towns that make it up, is more important (more likely to be used) if the source town has more registered electric vehicles. Similarly, it will also be weighted more heavily (and so will the towns that make it up) if the destination town has a higher tourism score (for example, the "number of visitor points" feature).


![weighted mode schema](/illustr/weighted_mode_schema.jpg "Weighted mode schema").

In the **"weighted" mode**, this weighting is carried out upstream, when the initial lists of possible sources and targets are built.
Firstly, a filter can be applied to remove from the list source towns that do not have enough registered electric vehicles (e.g. < 5) or destination towns that do not have enough tourist points of interest (e.g. < 2).
The script then builds its list of sources according to the importance of a feature. A town with only 5 registered vehicles will appear only 1 time in the source list, for example. Whereas a town with, say, 200 electric vehicles will appear 20 times (2000 times 0.01, because there is a parameter to reduce this impact to avoid lists that are too large).
Similarly, for the list of destinations, a town with just 10 tourist points of interest will appear, for example, 1 time, whereas a town with 180 will appear 18 times (a reduction factor of 0.1).
In this way, when the script then takes its source/destination pairs at random from the lists constructed, it follows a distribution of the relative importance of each path, in the eyes of the problem.
This mode therefore "gets to the point" by having a greater chance of calculating the important paths, and it is therefore possible that it will converge more quickly towards a solution for score 2.

![equiprobable mode schema](/illustr/equiprobable_mode_schema.jpg "Equiprobable mode schema").

In **"equiprobable" mode**, the weighting is not upstream but downstream.
As in the previous mode, when the source and destination base lists are compiled, filters can be applied to remove cities that do not have enough electric vehicles as sources, or cities that do not have enough touristic points of interest as destinations. However, apart from this filter, all source and destination cities will be represented in the basic list in the same proportion.
So the paths are calculated regardless of any weighting.






