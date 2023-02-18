# Twitter-Simulation
Comparing different database models' efficiency in a Twitter simulation.

The project is essentially a study in noSQL efficency. Its is to compare different database models' performance on a Twitter simulation. The api 
simulates posting tweets and loading a given user's home timeline, so the preformance metrics are tweets posted per second and timelines loaded 
per second. 

Currently, this repo only contains code for an sql database, The next database to be added will be a Redis database. Eventually it will also include a 
Slick database, a neo4j database, a MongoDB database, and others. 

As these databases are added, a dashboard will be constructed to compare their performances.
