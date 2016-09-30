Introduction
------------

This application can be used to import chat documents from MongoDB into an ElasticSearch index. The aim is to process
all historical data in the database, creating indices and mappings to make it as easy as possible to start indexing
the IRC chat logs.

Work has been completed to add support for indexing channel documents in addition to chat documents. This should allow
channels to be searched, autocompleted, topics to be indexed etc.

Thanks to bendem, the chats are processed using a thread pool with each thread pulling batches of documents from the
database cursor and then creating a single request of many documents to send to ElasticSearch. Importing around 4
million documents takes about 5 minutes with this approach.

In September of 2016, work began on updating the system to work with ElasticSearch v2.

Building
--------

We use Maven to build the project and handle dependencies. To compile the code, run the tests and create an "uber-JAR"
with batteries (shaded dependencies) included, execute the `mvn` command in the project's root directory.

Usage
-----

To (re)create the necessary indices and import all data from a MongoDB instance, run the following command:

`java -Xmx4G -jar target/mongotoelastic-1.1.0-SNAPSHOT.jar --mongo-db "korobi" --reconfigure-indexes`
