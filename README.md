   # Azure HDInsight

* Features available with the new kernels
Apache Spark cluster on HDInsight (Linux) includes Jupyter notebooks that you can use to test your applications.
By default Jupyter notebook comes with a Python2 kernel.
A kernel is a program that runs and interprets your code.
HDInsight Spark clusters provide four additional kernels that you can use with the Jupyter notebook.

These are:
* PySpark - for applications written in Python. PySpark kernel exposes the Spark programming model to Python.
* PySpark3 - for applications written in Python3. PySpark3 kernel exposes the Spark programming model to Python3.
* Spark - for applications written in Scala.
* SparkR - for applications written in R.
How do I use the new kernels?
Create a notebook with the new kernels. Click New, and then click PySpark, PySpark3, Spark or SparkR. 
Create notebooks with new kernels
This should open a new notebook with the kernel you selected.

   ## Notebook setup

When using PySpark kernel notebooks on HDInsight, there is no need to create a SparkContext or a SparkSession; a SparkSession which has the SparkContext is created for you automatically when you run the first code cell, and you'll be able to see the progress printed. The contexts are created with the following variable names:

   ## SparkSession (spark)
To run the cells below, place the cursor in the cell and then press __SHIFT + ENTER__.


Everytime you run a cell, your web browser window title will show a (Busy) status along with the notebook title. You will also see a solid circle next to the PySpark text in the top-right corner. After the job completes, this will change to a hollow circle.

   #### Status of a Jupyter notebook job

   ##### PySpark magics
The PySpark kernel provides some predefined “magics”, which are special commands that you can call with %% (e.g. %%MAGIC ). The magic command must be the first word in a code cell and allow for multiple lines of content. You can’t put comments before a cell magic.

For more information on magics, see here.

Help magic (%%help)
This magic gives you information about the different supported magics in PySpark kernel and the usage for each.

   ## %%help

Session information __(%%info)__
Livy is an open source REST server for Spark. When you execute a code cell in a PySpark notebook, it creates a Livy session to execute your code. You can use the %%info magic to display the current Livy session information.

   ## %%info

Session configuration __(%%configure)__
Use the %%configure magic to configure new or existing Livy sessions.

If a session is already running, you can change the configuration by using the -f argument with %%configure magic. This will delete the current session and recreate it with the applied configurations. If you don't provide the -f argument, an error will be displayed and no configuration changes will be applied.

If you haven't already started the session, then the -f argument is not mandatory. Even if you use it with a session that you are just creating, it will not delete any currently running sessions.

These are some session attributes that can be used for configuration
__"name":__ Name of the application
__"driverMemory":__ Memory for driver (e.g. 1000M, 2G)
__"executorMemory":__ Memory for executor (e.g. 1000M, 2G)
For more attributes for session configuration see the Livy documentation.

__TIP:__ The application name should start with remotesparkmagics to allow sessions to get automatically cleaned up if an error happened. If you provide a name that does not start with remotesparkmagics it will not result in an error but the cleanup won't occur.

__%%configure -f__ 
{"name":"remotesparkmagics-sample", "executorMemory": "4G", "executorCores":4}
Automatic visualization of queries
The Pyspark kernel automatically visualizes the output of SQL (HiveQL) queries. You are given the option to choose between several different types of visualizations:

Table
Pie
Line
Area
Bar

__TIP:__ When you perform a SQL query, the number of rows of data that will be included in the result data set will be limited by default to 2500 rows.

__SQL magic (%%sql)__
The HDInsight PySpark kernel supports easy inline HiveQL queries against the SparkSession. The (-o VARIABLE_NAME) argument persists the output of the SQL query as a Pandas dataframe on the Jupyter server (e.g. -o query1 in the example below). This means it'll be available in the local mode which will be explained later. The output will be automatically visualized after you run the cell below.

__%%sql -o query1__
SELECT clientid, querytime, deviceplatform, querydwelltime 
FROM hivesampletable
WHERE state = 'Washington' AND devicemake = 'Microsoft' 
In addition to -o, a number of other configuration parameters for SQL queries are available (as described in the %%help output above). 

   #### These include:
-q. This causes the magic to return None, turning off visualizations for that cell. If you don't want to auto-visualize the content of a cell and just want to capture it as a dataframe, then use -q -o VARIABLE. If you want to turn off visualizations without capturing the results (e.g. for running a SQL query with side effects, like a CREATE TABLE statement), just use -q without specifying a -o argument.

Remember that the kernel by default limits the output of a SQL query to 2500 rows. If you want to adjust this default behavior, use these options.

-m METHOD, where METHOD is either take or sample (defaults to take). If the method is take, the kernel will take elements from the top of the result data set; if the method is sample, the kernel will randomly sample elements of the data set according to -r, described below.

-r FRACTION, where FRACTION is some floating-point number between 0.0 and 1.0. If the sample method for the SQL query is sample, then the kernel will randomly sample this fraction of the elements of the result set for you; e.g. if you run a SQL query with the arguments 

-m sample -r 0.01, then 1% of the result rows will be randomly sampled.

-n MAXROWS, where MAXROWS is some integer. The kernel will limit the number of output rows to MAXROWS. If MAXROWS is a negative number such as -1, then the number of rows in the result set will not be limited.

__WARNING:__ Be careful with the -n option, as it is possible to cause the Jupyter server to run out of memory if you try to collect too many result rows. We recommend only using -n -1 if you are certain that the result dataset will not be too large.

__As a final example__, this SQL query randomly samples 10% of the rows in the hivesampletable, limits the size of the result set to 500, and saves the output into a dataframe called query2 without doing any visualizations.

%%sql -q -m sample -r 0.1 -n 500 -o query2 
SELECT * FROM hivesampletable
Running locally (%%local)
You can use the %%local magic to run your code locally on the Jupyter server, which is the headnode of the HDInsight cluster. Here's a typical use case for this scenario.

__By default__, the output of any code snippet that you run from a Jupyter notebook is available within the context of the session that is persisted on the worker nodes. However, if you want to save a trip to the worker nodes for every computation, and all the data that you need for your computation is available locally on the Jupyter server node (which is the headnode), you can use the %%local magic to run the code snippet on the Jupyter server. Typically, you would use %%local magic in conjunction with the %%sql magic with -o parameter. The -o parameter would persist the output of the SQL query locally and then %%local magic would trigger the next set of code snippet to run locally against the output of the SQL queries that is persisted locally.

__TIPS:__

Working against locally persisted data is especially useful when you want the flexibility of using visualization libraries such as matplotlib. If you work directly against the data on the remote worker nodes, the output received through Livy is always text that limits the options of visual representation.

Remember that SQL queries, by default, limit the number of result rows to 2500 -- and that it is possible to get Out of Memory errors if you accidentally collect too many results from your SQL query. Therefore, if your dataset is large, it is considered a best practice to do your computation or number-crunching on the cluster or in the SQL query rather than in local mode.

When you use %%local all subsequent lines in the cell will be executed locally. The code in the cell must be valid Python code.

This code block prints the length of the result set of the dataframe query2 -- remember, we used the -n 500 option to limit the size of the dataframe to 500.

%%local  
print(len(query2))
Session logs (%%logs)
You can get the logs of your current Livy session to debug any issues you encounter. From the logs, you can retrieve the YARN application id and then use the YARN UI to look at the YARN logs for that application id. Yarn is the resource manager for Spark. URL for Yarn UI is ** https://<clusterdnsname>.azurehdinsight.net/yarnui **. For instructions on how to access the YARN UI for the cluster, see Access YARN application logs on Linux-based HDInsight.

%%logs
   #### Delete session (%%delete)
Use '%%delete -f -s <session #>' to delete a session given its session ID. Note that you cannot delete the session that is initiated for the kernel itself. Another notebook might go into an error state, since notebooks are supposed to manage sessions by themselves, and all work will be lost on that session.

   #### Sessions cleanup (%%cleanup)
Use '%%cleanup -f' magic to delete all of the sessions for this cluster, including this notebook's session. The force flag -f is mandatory.

