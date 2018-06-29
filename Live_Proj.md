   # Importing Required Libraries

      import pandas as pd
      import pandas as pd

      from pyspark.sql.functions import *
      from pyspark.sql.types import *

   #  Importing Data

      DATA = spark.read.csv("wasb:///DATA.csv")

  ###  Number of Rows

      DATA.count()

  ###  Type of Data

      type(DATA)

      <class 'pyspark.sql.dataframe.DataFrame'>

  ###  Transform to sql table

      DATA.createOrReplaceTempView("pp")

      %%sql

      select * from pp

  ###  Drop NA Values

      DATA= DATA.na.drop(subset=["_c3"])

      DATA.count()

  ###  Extracting datetime from "_c2"

      cef = substring_index(DATA._c2, ' ', 1)

      DATA = DATA.withColumn("Day", cef)

      cef = substring_index(DATA._c2, ' ', -1)

      DATA = DATA.withColumn("Year", cef)

      cef = substring_index(DATA._c2, ' ', -2)

      DATA = DATA.withColumn("Time", cef)

      cef = substring_index(DATA.Time, ' ', 1)

      DATA = DATA.withColumn("Time", cef)

      cef = substring_index(DATA._c2, ' ', 2)

      DATA = DATA.withColumn("Month", cef)

      cef = substring_index(DATA.Month, ' ', -1)

      DATA = DATA.withColumn("Month", cef)

      cef = substring_index(DATA._c2, ' ', 3)

      DATA = DATA.withColumn("Date", cef)

      cef = substring_index(DATA.Date, ' ', -1)

      DATA = DATA.withColumn("Date", cef)

      DATA.show(5)


      DATA = DATA.drop("_c5", "_c7", "_c9", "_c2")
      DATA = DATA.drop("_c5", "_c7", "_c9", "_c2")
                                        
  ###  Extracting Column1 from "_c1"

      ss = locate('rn=', DATA._c1, 1)
      ee = locate('cid=', DATA._c1, 1)

      DATA = DATA.withColumn('ss',ss )
      DATA = DATA.withColumn('ee',ee )

      ssn = DATA.ss + 3

      DATA = DATA.withColumn("ssn" , ssn)

      diff = DATA.ee - DATA.ssn

      DATA = DATA.withColumn("Diff" , diff)

      DATA = DATA.withColumn('Column1', DATA['_c1'].substr(DATA.ssn, DATA.Diff))

      DATA.show(5)

*only showing top 5 rows*
                                                  

  ###  Extracting Column2 from "_c1"

      ss = locate('cid=', DATA._c1, 1)
      ee = locate('eid=', DATA._c1, 1)

      DATA = DATA.withColumn('ss',ss )
      DATA = DATA.withColumn('ee',ee )

      ssn = DATA.ss + 4

      DATA = DATA.withColumn("ssn" , ssn)

      diff = DATA.ee - DATA.ssn

      DATA = DATA.withColumn("Diff" , diff)

      DATA = DATA.withColumn('Column2', DATA['_c1'].substr(DATA.ssn, DATA.Diff))

      CHN_DATA.show(5)

*only showing top 5 rows*

      cef = substring_index(CHN_DATA._c1, 'eid=', -1)

      CHN_DATA = CHN_DATA.withColumn("abc", cef)

####    Drop the column

      DATA = DATA.drop("_c0","_c1")

 ###   Converting all strings to lowercase

      DATA = DATA.withColumn("col_name", lower(col("col_name")));

  ###  Extracting last word from column
    
      cef = substring_index(DATA._c10, 'layer run-time id:', -1)

      DATA = DATA.withColumn("Layer_Run_TimeID", cef)

  ###  Rename a column

      DATA = DATA.withColumnRenamed("_c3", "New_Column_Name")

  ###  Print Column Name 

      DATA.printSchema()
    
   ###    Subset/Filter Data using conditions
    
      DATA = DATA.where(col("EventID").isin({"5145"}))

  ##  Write Processed Data as CSV file in Blob Storage

      DATA.coalesce(1).write.csv("wasb:///DATA.csv" , header = True , mode="append")

