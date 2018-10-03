  ### NoteBook Setup
  
      SparkSession (spark)
      SHIFT + ENTER
      
#### PySpark magics

      info	%%info
      cleanup	%%cleanup -f
      delete	%%delete -f -s 0
      logs	%%logs
      configure	%%configure -f {"executorMemory": "1000M", "executorCores": 4}
      spark	%%spark -o df   df = spark.read.parquet('...
      sql	%%sql -o tables -q  SHOW TABLES
      local	%%local   a = 1

  
