   # Working with Hive tables using Spark

we can use the Spark execution engine to run Hive queries. In this notebook, we look at examples on how to read data from a Hive table and how to write data back into a Hive table.

__Notebook setup__

When using PySpark kernel notebooks on HDInsight, there is no need to create a SparkContext or a SparkSession; a SparkSession which has the SparkContext is created for you automatically when we run the first code cell, and we'll be able to see the progress printed. The contexts are created with the following variable names:

   #### SparkSession (spark)
To run the cells below, place the cursor in the cell and then press __SHIFT + ENTER__.

   #### Reading data from Hive
To start with, let's first see what we have in our Hive store. Run the snippet below. At a minimum this should return a table called hivesampletable. hivesampletable is a default table that is included with the Spark HDInsight cluster.

%%sql

SHOW TABLES

You can then create dataframe from the hivesampletable. The snippet below creates a dataframe that you can perform any dataframe operation on. This dataframe contains all the data in the hivesampletable.

   ## hivesampletabledf is a dataframe

hivesampletabledf = spark.table('hivesampletable')

Instead, if you want to run a query on the table and return only the results as a __Spark dataframe__, you can do so with the .sql() method. In the snippet below, hivesampletablequerydf is a __dataframe__ that only contains the data returned by the __SQL query__.

hivesampletablequerydf = spark.sql("""
SELECT clientid, querytime, deviceplatform, querydwelltime 
FROM hivesampletable 

WHERE state = 'Washington' AND devicemake = 'Microsoft' AND querydwelltime > 15
""")

hivesampletablequerydf.show()

   ## Writing data into Hive

If you have a dataframe that was created with a SparkSession and you want to persist that data to Hive, you can create a table and then insert the dataframe into the table:

%%sql -q

CREATE TABLE IF NOT EXISTS hivesampletablecopypy ( 
                    clientid string, 
                    querytime string, 
                    market string, 
                    deviceplatform string,
                    devicemake string,
                    devicemodel string,
                    state string, 
                    country string,
                    querydwelltime double,
                    sessionid bigint,
                    sessionpagevieworder bigint )

__from pyspark.sql import DataFrameWriter__

dfw = DataFrameWriter(hivesampletabledf)

dfw.insertInto('hivesampletablecopypy', overwrite=True)

Call dfw.insertInto('hivesampletablecopy', overwrite=False) __to keep any data already in the table__.
