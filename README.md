### Modelling tourist flows with shortest paths computation between source / destination cities
### With a Neo4j graph and a spark aggregation

This project is linked to the DataForGood project E-Carto_Mobile, the aim of which is to offer a web platform that can be used to visualise national (mainland France) requirements for electric vehicle charging points.

Description of the global project here : https://dataforgood.notion.site/volution-des-infrastructures-de-recharge-pour-les-v-hicules-lectriques-e6dc33916a96435ea068a630a92c9799
[in french]

The git of the global project : https://github.com/dataforgoodfr/batch11_e_cartomobile#data-for-good---batch-11---e-cartomobile

And the website mock-up : https://github.com/Thopiax/e-cartomobile-ui

----

#### Scoring the needs of charging points and score 2 importance

As part of the project's small R&D team, we're looking to "score" the needs of every town and village in France in terms of charging points for users' daily journeys, tourist trips, etc...

This modelling research has led us to divide these types of needs into 3 or 4 categories, which are updated in this methodology : (link)

An important category of score is the score linked to the tourism flow :

Most users need to recharge their electric vehicle during a long journey (more than two hours) to a tourist destination, in summer or winter, during a break. Apart from this, most users simply recharge their vehicles at home.
This score, called score 2 in the methodology, therefore requires the ability to model tourist flows throughout France, in order to identify the communes closest to the points where these flows pass through.


#### General principle of the score 2 continuous generation :

The principle is as follows:

Firstly, a graph was constructed using Neo4J technology, based on an aggregation of data taken from Open Street Map, coupled with various other sources (data.gouv for the geometry of the communes, for example). In this graph, a road network (made up of "ROAD_POINTS" nodes and "RELATED" roads/relationships) is connected to the communes ("CITY" meouds) thanks to the prior generation of new "NEARLY_TO" relationships which make it possible to identify, for a given road point, the communes less than 5km away.
[To recreate the graph, click here: ]

Next, this code, which runs inside a Spark docker container, is used to generate score batches that will be incremented in a database using the process illustrated below:

![global schema](/illustr/common_schema.jpg "Global schema").




