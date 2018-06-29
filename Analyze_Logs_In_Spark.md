   # Analyze logs in Spark using a custom library

This notebook demonstrates how to analyze log data using a custom library with Spark on HDInsight. The custom library we use is a Python library called iislogparser.py. 
*This library is already included on the Spark cluster at 

/HdiSamples/HdiSamples/WebsiteLogSampleData/iislogparser.py*.

   #### Notebook setup

When using __PySpark__ kernel notebooks on HDInsight, there is no need to create a __SparkContext or a SparkSession__; a SparkSession which has the SparkContext is created for you automatically when you run the first code cell, and you'll be able to see the progress printed. The contexts are created with the following variable names:

__SparkSession (spark)__
To run the cells below, place the cursor in the cell and then press **SHIFT + ENTER**.

   ##### Save raw data as an RDD
Start with importing some types that are going to be used later in this sample.

from pyspark.sql import Row

from pyspark.sql.types import *

Create an **RDD** using the sample log data already available on the cluster. we can access the data in the default storage account associated with the cluster at \HdiSamples\HdiSamples\WebsiteLogSampleData\SampleLog\909f2b.log.

logs = spark.sparkContext.textFile('wasb:///HdiSamples/HdiSamples/WebsiteLogSampleData/SampleLog/909f2b.log')

Retrieve a sample log set to verify that the previous step completed successfully.

logs.take(5)

   ### Analyze log data using a custom Python library

In the output above, the first couple lines include the header information and each remaining line matches the schema described in that header. Parsing such logs could be complicated. So, we use a custom Python library __(iislogparser.py)__ that makes parsing such logs much easier.

However, this is not a Python library that we can install with Pip, and it is not in the PYTHONPATH, we cannot use it by using an import statement like import iislogparser. To use this library, we must distribute it to all the worker nodes.

The first step in doing that is to copy it over to the default storage account associated with the cluster. Let us assume you copy it over to wasb:///HdiSamples/HdiSamples/WebsiteLogSampleData/iislogparser.py. You must then run the following snippet to distribute the library to all worker nodes in the Spark cluster.

spark.sparkContext.addPyFile('wasb:///HdiSamples/HdiSamples/WebsiteLogSampleData/iislogparser.py')
iislogparser provides a function parse_log_line that returns None if a log line is a header row, and returns an instance of the LogLine class if it encounters a log line. Use the LogLine class to extract only the log lines from the RDD:

def parse_line(l):
    import iislogparser
    return iislogparser.parse_log_line(l)
logLines = logs.map(parse_line).filter(lambda p: p is not None).cache()
Retrieve a couple of extracted log lines to verify that the step completed successfully.

logLines.take(2)
The LogLine class, in turn, has some useful methods, like is_error(), which returns whether a log entry has an error code. Use this to compute the number of errors in the extracted log lines, and then log all the errors to a different file.

errors = logLines.filter(lambda p: p.is_error())
numLines = logLines.count()
numErrors = errors.count()
print('There are %d errors and %d log entries' % (numErrors, numLines))
errors.map(lambda p: str(p)).saveAsTextFile('wasb:///HdiSamples/HdiSamples/WebsiteLogSampleData/SampleLog/909f2b-2.log')
If you want to isolate the cause of requests that run for a long time, you might want to find the files that take the most time to serve on average. The snippet below retrieves the top 25 resources that took most time to serve a request.

def avgTimeTakenByKey(rdd):
    return rdd.combineByKey(lambda line: (line.time_taken, 1),
                            lambda x, line: (x[0] + line.time_taken, x[1] + 1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1]))\
              .map(lambda x: (x[0], float(x[1][0]) / float(x[1][1])))
    
avgTimeTakenByKey(logLines.map(lambda p: (p.cs_uri_stem, p))).top(25, lambda x: x[1])
You can also present this information in the form of plot. As a first step to create a plot, let us first create a temporary table AverageTime. The table groups the logs by time to see if there were any unusual latency spikes at any particular time.

avgTimeTakenByMinute = avgTimeTakenByKey(logLines.map(lambda p: (p.datetime.minute, p))).sortByKey()
schema = StructType([StructField('Minutes', IntegerType(), True),
                     StructField('Time', FloatType(), True)])
                     
avgTimeTakenByMinuteDF = spark.createDataFrame(avgTimeTakenByMinute, schema)
avgTimeTakenByMinuteDF.registerTempTable('AverageTime')
You can then run the following SQL query to get all the records in the AverageTime table. You can run SQL queries using the %%sql magic. The -o averagetime argument will persist the output of the SQL query as a Pandas dataframe, with the name averagetime on the Jupyter server.

%%sql -o averagetime
SELECT * FROM AverageTime
If you want to construct your own custom visualization of the data, you can pull the results of a SQL query out of the cluster and onto the Jupyter server and generate a plot with Matplotlib, which is a library used to construct a visualization of the data. You can use %%local magic in cases where you want your code snippets to run locally. You can always use regular Python code with the %%local magic.

%%local
%matplotlib inline
import matplotlib.pyplot as plt

plt.plot(averagetime['Minutes'], averagetime['Time'], marker='o', linestyle='--')
plt.xlabel('Time (min)')
plt.ylabel('Average time taken for request (ms)')
With the help of Spark, Matplotlib, and a custom library, it becomes fairly easy to generate insights into the contents of a log file.
