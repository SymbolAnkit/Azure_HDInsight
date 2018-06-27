                                                  1. Importing Libraries

import pandas as pd
import pandas as pd

from pyspark.sql.functions import *
from pyspark.sql.types import *

                                                  2. Importing Data

CHN_DATA = spark.read.csv("wasb:///CHN_ARC_LOG1.csv")
CHN_DATA = spark.read.csv("wasb:///CHN_ARC_LOG1.csv")

CHN_DATA.count()
CHN_DATA.count()
9990036

type(CHN_DATA)
<class 'pyspark.sql.dataframe.DataFrame'>

CHN_DATA.createOrReplaceTempView("pp")

%%sql
select * from pp
Type:
Table
Pie
Scatter
Line
Area
Bar
_c0	_c1	_c2	_c3	_c4	_c5	_c6	_c7	_c8	_c9	_c10
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050036 cid=76 eid=4	Tue Mar 06 00:00:33 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050037 cid=76 eid=4	Tue Mar 06 00:00:33 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5158_Microsoft-Windows-Secu...	rn=879050038 cid=76 eid=4	Tue Mar 06 00:00:33 2018	5158	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050039 cid=76 eid=4	Tue Mar 06 00:00:33 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5158_Microsoft-Windows-Secu...	rn=879050040 cid=76 eid=4	Tue Mar 06 00:00:33 2018	5158	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050041 cid=76 eid=4	Tue Mar 06 00:00:33 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050042 cid=76 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050043 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050044 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050045 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050046 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050047 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050048 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050049 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050050 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050051 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050052 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050053 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050054 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050055 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050056 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_4634_Microsoft-Windows-Secu...	rn=879050057 cid=1112 eid=548	Tue Mar 06 00:00:34 2018	4634	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Logoff	NaN	An account was logged off. Subject: Security...
%NICWIN-4-Security_5158_Microsoft-Windows-Secu...	rn=879050058 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5158	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050059 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050060 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050061 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050062 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050063 cid=88 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
%NICWIN-4-Security_4634_Microsoft-Windows-Secu...	rn=879050064 cid=1112 eid=548	Tue Mar 06 00:00:34 2018	4634	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Logoff	NaN	An account was logged off. Subject: Security...
%NICWIN-4-Security_5156_Microsoft-Windows-Secu...	rn=879050065 cid=68 eid=4	Tue Mar 06 00:00:34 2018	5156	Microsoft-Windows-Security-Auditing	NaN	Audit Success	BLRSSPCITIDC.CITIPRO.HCLT.COM	Filtering Platform Connection	NaN	The Windows Filtering Platform has permitted a...
...	...	...	...	...	...	...	...	...	...	...
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1058377...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1058377...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
Mar 06 2018 05:28:45: %ASA-4-106023: Deny tcp ...	0x0]	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1058377...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094949...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094869...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094879...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094749...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094876...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094813...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094888...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094893...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094890...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1058377...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094893...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094910...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094896...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1058377...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094918...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
%ASA-6-302016: Teardown UDP connection 1094917...	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
2500 rows × 11 columns

                                                  3. Extracting and Structuring Features
                                                  1. Dropping Rows with No EventID("_c3")

CHN_DATA= CHN_DATA.na.drop(subset=["_c3"])

CHN_DATA.count()
1237528
                                                  2. Extracting datetime from "_c2"

cef = substring_index(CHN_DATA._c2, ' ', 1)
CHN_DATA = CHN_DATA.withColumn("Day", cef)
cef = substring_index(CHN_DATA._c2, ' ', 1)
CHN_DATA = CHN_DATA.withColumn("Day", cef)

cef = substring_index(CHN_DATA._c2, ' ', -1)
CHN_DATA = CHN_DATA.withColumn("Year", cef)

cef = substring_index(CHN_DATA._c2, ' ', -2)
CHN_DATA = CHN_DATA.withColumn("Time", cef)

cef = substring_index(CHN_DATA.Time, ' ', 1)
CHN_DATA = CHN_DATA.withColumn("Time", cef)

cef = substring_index(CHN_DATA._c2, ' ', 2)
CHN_DATA = CHN_DATA.withColumn("Month", cef)
cef = substring_index(CHN_DATA._c2, ' ', 2)
CHN_DATA = CHN_DATA.withColumn("Month", cef)

cef = substring_index(CHN_DATA.Month, ' ', -1)
CHN_DATA = CHN_DATA.withColumn("Month", cef)

cef = substring_index(CHN_DATA._c2, ' ', 3)
CHN_DATA = CHN_DATA.withColumn("Date", cef)

-1
cef = substring_index(CHN_DATA.Date, ' ', -1)
CHN_DATA = CHN_DATA.withColumn("Date", cef)

CHN_DATA.show(5)
+--------------------+--------------------+--------------------+----+--------------------+----+-------------+--------------------+--------------------+----+--------------------+---+----+--------+-----+----+
|                 _c0|                 _c1|                 _c2| _c3|                 _c4| _c5|          _c6|                 _c7|                 _c8| _c9|                _c10|Day|Year|    Time|Month|Date|
+--------------------+--------------------+--------------------+----+--------------------+----+-------------+--------------------+--------------------+----+--------------------+---+----+--------+-----+----+
|%NICWIN-4-Securit...|rn=879050036 cid=...|Tue Mar 06 00:00:...|5156|Microsoft-Windows...|null|Audit Success|BLRSSPCITIDC.CITI...|Filtering Platfor...|null|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|
|%NICWIN-4-Securit...|rn=879050037 cid=...|Tue Mar 06 00:00:...|5156|Microsoft-Windows...|null|Audit Success|BLRSSPCITIDC.CITI...|Filtering Platfor...|null|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|
|%NICWIN-4-Securit...|rn=879050038 cid=...|Tue Mar 06 00:00:...|5158|Microsoft-Windows...|null|Audit Success|BLRSSPCITIDC.CITI...|Filtering Platfor...|null|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|
|%NICWIN-4-Securit...|rn=879050039 cid=...|Tue Mar 06 00:00:...|5156|Microsoft-Windows...|null|Audit Success|BLRSSPCITIDC.CITI...|Filtering Platfor...|null|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|
|%NICWIN-4-Securit...|rn=879050040 cid=...|Tue Mar 06 00:00:...|5158|Microsoft-Windows...|null|Audit Success|BLRSSPCITIDC.CITI...|Filtering Platfor...|null|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|
+--------------------+--------------------+--------------------+----+--------------------+----+-------------+--------------------+--------------------+----+--------------------+---+----+--------+-----+----+
only showing top 5 rows

CHN_DATA = CHN_DATA.drop("_c5", "_c7", "_c9", "_c2")
CHN_DATA = CHN_DATA.drop("_c5", "_c7", "_c9", "_c2")
                                        3. Extracting EventRecordID from "_c1"

ss = locate('rn=', CHN_DATA._c1, 1)
ee = locate('cid=', CHN_DATA._c1, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 3
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('EventRecordID', CHN_DATA['_c1'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(5)
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+
|                 _c0|                 _c1| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+
|%NICWIN-4-Securit...|rn=879050036 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|  1| 14|  4|  10|   879050036 |
|%NICWIN-4-Securit...|rn=879050037 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|  1| 14|  4|  10|   879050037 |
|%NICWIN-4-Securit...|rn=879050038 cid=...|5158|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|  1| 14|  4|  10|   879050038 |
|%NICWIN-4-Securit...|rn=879050039 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|  1| 14|  4|  10|   879050039 |
|%NICWIN-4-Securit...|rn=879050040 cid=...|5158|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06|  1| 14|  4|  10|   879050040 |
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+
only showing top 5 rows
                                                  4. Extracting ThreadID from "_c1"

ThreadID
ss = locate('cid=', CHN_DATA._c1, 1)
ee = locate('eid=', CHN_DATA._c1, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 4
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('ThreadID', CHN_DATA['_c1'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(5)
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+
|                 _c0|                 _c1| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+
|%NICWIN-4-Securit...|rn=879050036 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050036 |     76 |
|%NICWIN-4-Securit...|rn=879050037 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050037 |     76 |
|%NICWIN-4-Securit...|rn=879050038 cid=...|5158|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050038 |     76 |
|%NICWIN-4-Securit...|rn=879050039 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050039 |     76 |
|%NICWIN-4-Securit...|rn=879050040 cid=...|5158|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050040 |     76 |
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+
only showing top 5 rows

cef = substring_index(CHN_DATA._c1, 'eid=', -1)
CHN_DATA = CHN_DATA.withColumn("ExecutionProcessID", cef)

CHN_DATA.show(5)
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+
|                 _c0|                 _c1| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+
|%NICWIN-4-Securit...|rn=879050036 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050036 |     76 |                 4|
|%NICWIN-4-Securit...|rn=879050037 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050037 |     76 |                 4|
|%NICWIN-4-Securit...|rn=879050038 cid=...|5158|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050038 |     76 |                 4|
|%NICWIN-4-Securit...|rn=879050039 cid=...|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050039 |     76 |                 4|
|%NICWIN-4-Securit...|rn=879050040 cid=...|5158|Microsoft-Windows...|Audit Success|Filtering Platfor...|The Windows Filte...|Tue|2018|00:00:33|  Mar|  06| 14| 21| 18|   3|   879050040 |     76 |                 4|
+--------------------+--------------------+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+
only showing top 5 rows

CHN_DATA = CHN_DATA.drop("_c0","_c1")
                                                  5. Extracting ProcessID from "_c10"

CHN_DATA = CHN_DATA.withColumn("_c10", lower(col("_c10")));

ProcessID
ss = locate('process id:', CHN_DATA._c10, 1)
ee = locate('application name', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 11
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('ProcessID', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06| 87|103| 98|   5|   879050036 |     76 |                 4|      4  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06| 87|103| 98|   5|   879050037 |     76 |                 4|      4  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+
only showing top 2 rows
                                                  6. Extracting ApplicationName from "_c10"

ApplicationName
ss = locate('application name:', CHN_DATA._c10, 1)
ee = locate('network information:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 17
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('ApplicationName', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|103|129|120|   9|   879050036 |     76 |                 4|      4  |       system  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|103|129|120|   9|   879050037 |     76 |                 4|      4  |       system  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+
only showing top 2 rows
                                                  7. Extracting Direction from "_c10"

Direction
ss = locate('direction:', CHN_DATA._c10, 1)
ee = locate('source address:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 10
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('Direction', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|151|172|161|  11|   879050036 |     76 |                 4|      4  |       system  |  inbound  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|151|172|161|  11|   879050037 |     76 |                 4|      4  |       system  |  inbound  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+
only showing top 2 rows
                                                  8. Extracting SourceAddress from "_c10"

SourceAddress
ss = locate('source address:', CHN_DATA._c10, 1)
ee = locate('source port:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 15
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('SourceAddress', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|172|205|187|  18|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|172|205|187|  18|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+
only showing top 2 rows
                                                  9. Extracting SourcePort from "_c10"

SourcePort
ss = locate('source port:', CHN_DATA._c10, 1)
ee = locate('destination address:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 12
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('SourcePort', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|SourcePort|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|205|224|217|   7|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|205|224|217|   7|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+
only showing top 2 rows
                                                  10. Extracting DestinationAddress from "_c10"

DestinationAddress
ss = locate('destination address:', CHN_DATA._c10, 1)
ee = locate('destination port:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 20
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('DestinationAddress', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|SourcePort|DestinationAddress|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|224|260|244|  16|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |   10.117.192.90  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|224|261|244|  17|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |  10.117.192.158  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+
only showing top 2 rows
                                                  11. Extracting DestinationPort from "_c10"

DestinationPort
ss = locate('destination port:', CHN_DATA._c10, 1)
ee = locate('protocol:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 17
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('DestinationPort', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|SourcePort|DestinationAddress|DestinationPort|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|260|284|277|   7|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |   10.117.192.90  |          137  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|261|285|278|   7|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |  10.117.192.158  |          137  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+
only showing top 2 rows
                                                  12. Extracting Protocol from "_c10"

Protocol
ss = locate('protocol:', CHN_DATA._c10, 1)
ee = locate('filter information:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 9
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('Protocol', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|SourcePort|DestinationAddress|DestinationPort|Protocol|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|284|299|293|   6|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |   10.117.192.90  |          137  |    17  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|285|300|294|   6|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |  10.117.192.158  |          137  |    17  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+
only showing top 2 rows
                                                  13. Extracting Filter_Run_TimeID from "_c10"

Filter_Run_TimeID
ss = locate('filter run-time id:', CHN_DATA._c10, 1)
ee = locate('layer name:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 19
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('Filter_Run_TimeID', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|SourcePort|DestinationAddress|DestinationPort|Protocol|Filter_Run_TimeID|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|320|348|339|   9|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |   10.117.192.90  |          137  |    17  |         520340  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|321|349|340|   9|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |  10.117.192.158  |          137  |    17  |         520340  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+
only showing top 2 rows
                                                  14. Extracting LayerName from "_c10"

LayerName
ss = locate('layer name:', CHN_DATA._c10, 1)
ee = locate('layer run-time id:', CHN_DATA._c10, 1)
​
​
​
CHN_DATA = CHN_DATA.withColumn('ss',ss )
CHN_DATA = CHN_DATA.withColumn('ee',ee )
​
ssn = CHN_DATA.ss + 11
​
CHN_DATA = CHN_DATA.withColumn("ssn" , ssn)
​
diff = CHN_DATA.ee - CHN_DATA.ssn
​
CHN_DATA = CHN_DATA.withColumn("Diff" , diff)
​
CHN_DATA = CHN_DATA.withColumn('LayerName', CHN_DATA['_c10'].substr(CHN_DATA.ssn, CHN_DATA.Diff))

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+------------------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|SourcePort|DestinationAddress|DestinationPort|Protocol|Filter_Run_TimeID|         LayerName|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+------------------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|348|377|359|  18|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |   10.117.192.90  |          137  |    17  |         520340  |  receive/accept  |
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|349|378|360|  18|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |  10.117.192.158  |          137  |    17  |         520340  |  receive/accept  |
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+------------------+
only showing top 2 rows

cef = substring_index(CHN_DATA._c10, 'layer run-time id:', -1)
CHN_DATA = CHN_DATA.withColumn("Layer_Run_TimeID", cef)
cef = substring_index(CHN_DATA._c10, 'layer run-time id:', -1)
CHN_DATA = CHN_DATA.withColumn("Layer_Run_TimeID", cef)

CHN_DATA.show(2)
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+------------------+----------------+
| _c3|                 _c4|          _c6|                 _c8|                _c10|Day|Year|    Time|Month|Date| ss| ee|ssn|Diff|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|  Direction|     SourceAddress|SourcePort|DestinationAddress|DestinationPort|Protocol|Filter_Run_TimeID|         LayerName|Layer_Run_TimeID|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+------------------+----------------+
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|348|377|359|  18|   879050036 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |   10.117.192.90  |          137  |    17  |         520340  |  receive/accept  |              44|
|5156|Microsoft-Windows...|Audit Success|Filtering Platfor...|the windows filte...|Tue|2018|00:00:33|  Mar|  06|349|378|360|  18|   879050037 |     76 |                 4|      4  |       system  |  inbound  |  10.117.192.255  |     137  |  10.117.192.158  |          137  |    17  |         520340  |  receive/accept  |              44|
+----+--------------------+-------------+--------------------+--------------------+---+----+--------+-----+----+---+---+---+----+-------------+--------+------------------+---------+---------------+-----------+------------------+----------+------------------+---------------+--------+-----------------+------------------+----------------+
only showing top 2 rows
                                                  16. Dropping Unnecessary Columns

CHN_DATA
CHN_DATA = CHN_DATA.drop("_c10","ss","ee","ssn","Diff")
                                                  17. Renaming Features

CHN_DATA = CHN_DATA.withColumnRenamed("_c3", "EventID")
CHN_DATA = CHN_DATA.withColumnRenamed("_c3", "EventID")
CHN_DATA = CHN_DATA.withColumnRenamed("_c4", "ProviderName")
CHN_DATA = CHN_DATA.withColumnRenamed("_c6", "EventType")
CHN_DATA = CHN_DATA.withColumnRenamed("_c8", "Category")

CHN_DATA.printSchema()
root
 |-- EventID: string (nullable = true)
 |-- ProviderName: string (nullable = true)
 |-- EventType: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Day: string (nullable = true)
 |-- Year: string (nullable = true)
 |-- Time: string (nullable = true)
 |-- Month: string (nullable = true)
 |-- Date: string (nullable = true)
 |-- EventRecordID: string (nullable = true)
 |-- ThreadID: string (nullable = true)
 |-- ExecutionProcessID: string (nullable = true)
 |-- ProcessID: string (nullable = true)
 |-- ApplicationName: string (nullable = true)
 |-- Direction: string (nullable = true)
 |-- SourceAddress: string (nullable = true)
 |-- SourcePort: string (nullable = true)
 |-- DestinationAddress: string (nullable = true)
 |-- DestinationPort: string (nullable = true)
 |-- Protocol: string (nullable = true)
 |-- Filter_Run_TimeID: string (nullable = true)
 |-- LayerName: string (nullable = true)
 |-- Layer_Run_TimeID: string (nullable = true)
                                                  18. Writing Processed Data as CSV file in Blob Storage

CHN_DATA.coalesce(1).write.csv("wasb:///CHN_DATA.csv" , header = True , mode="append")
CHN_DATA.coalesce(1).write.csv("wasb:///CHN_DATA.csv" , header = True , mode="append")

CHN_DATA1 = CHN_DATA.where(col("EventID").isin({"5145"}))
CHN_DATA1 = CHN_DATA.where(col("EventID").isin({"5145"}))
CHN_DATA1.show(5)
+-------+--------------------+-------------+-------------------+---+----+--------+-----+----+-------------+--------+------------------+---------+--------------------+--------------------+-----------------+----------+------------------+---------------+--------+-----------------+---------+--------------------+
|EventID|        ProviderName|    EventType|           Category|Day|Year|    Time|Month|Date|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|     ApplicationName|           Direction|    SourceAddress|SourcePort|DestinationAddress|DestinationPort|Protocol|Filter_Run_TimeID|LayerName|    Layer_Run_TimeID|
+-------+--------------------+-------------+-------------------+---+----+--------+-----+----+-------------+--------+------------------+---------+--------------------+--------------------+-----------------+----------+------------------+---------------+--------+-----------------+---------+--------------------+
|   5145|Microsoft-Windows...|Audit Success|Detailed File Share|Tue|2018|00:00:47|  Mar|  06|   879050390 |    572 |               548|         |object was checke...| share object was...|  10.117.133.28  |          |                  |               |        |                 |         |a network share o...|
|   5145|Microsoft-Windows...|Audit Success|Detailed File Share|Tue|2018|00:00:47|  Mar|  06|   879050391 |    572 |               548|         |object was checke...| share object was...|  10.117.133.28  |          |                  |               |        |                 |         |a network share o...|
|   5145|Microsoft-Windows...|Audit Success|Detailed File Share|Tue|2018|00:00:47|  Mar|  06|   879050392 |    572 |               548|         |object was checke...| share object was...|  10.117.133.28  |          |                  |               |        |                 |         |a network share o...|
|   5145|Microsoft-Windows...|Audit Success|Detailed File Share|Tue|2018|00:00:47|  Mar|  06|   879050393 |    572 |               548|         |object was checke...| share object was...|  10.117.133.28  |          |                  |               |        |                 |         |a network share o...|
|   5145|Microsoft-Windows...|Audit Success|Detailed File Share|Tue|2018|00:00:47|  Mar|  06|   879050394 |    572 |               548|         |object was checke...| share object was...|  10.117.133.28  |          |                  |               |        |                 |         |a network share o...|
+-------+--------------------+-------------+-------------------+---+----+--------+-----+----+-------------+--------+------------------+---------+--------------------+--------------------+-----------------+----------+------------------+---------------+--------+-----------------+---------+--------------------+
only showing top 5 rows
                                                  Filtering on eventID

CHN_DATA.where(col("EventID").isin({"5156", "4624", "4674", "4634", "4672", "4662", "4656", "4776", "4768", "5145", "4648" , "7036", "7045", "4769", "4689", "4688", "5152", "4625", "5447", "5061", "4793", "4771", "5158", "5140", "4670", "4673", "4933", "4932", "5159", "4658", "5157"}))
df_otherEventIDs = CHN_DATA.where(col("EventID").isin({"5156", "4624", "4674", "4634", "4672", "4662", "4656", "4776", "4768", "5145", "4648" , "7036", "7045", "4769", "4689", "4688", "5152", "4625", "5447", "5061", "4793", "4771", "5158", "5140", "4670", "4673", "4933", "4932", "5159", "4658", "5157"}))

USE_
df_otherEventIDs.coalesce(1).write.csv("wasb:///USE_CHN_DATA.csv" , header = True , mode="append")

df_TRAFFICEventID
df_TRAFFICEventID = spark.read.csv("wasb:///CHN_ARC_LOG1.csv").where(col("_c3").isin({"TRAFFIC"}))

4697
SX = CHN_DATA.where(col("EventID").isin({"4698", "4697"}))

SX.count()
0

SX.show(5)
+-------+------------+---------+--------+---+----+----+-----+----+-------------+--------+------------------+---------+---------------+---------+-------------+----------+------------------+---------------+--------+-----------------+---------+----------------+
|EventID|ProviderName|EventType|Category|Day|Year|Time|Month|Date|EventRecordID|ThreadID|ExecutionProcessID|ProcessID|ApplicationName|Direction|SourceAddress|SourcePort|DestinationAddress|DestinationPort|Protocol|Filter_Run_TimeID|LayerName|Layer_Run_TimeID|
+-------+------------+---------+--------+---+----+----+-----+----+-------------+--------+------------------+---------+---------------+---------+-------------+----------+------------------+---------------+--------+-----------------+---------+----------------+
+-------+------------+---------+--------+---+----+----+-----+----+-------------+--------+------------------+---------+---------------+---------+-------------+----------+------------------+---------------+--------+-----------------+---------+----------------+
                                                  start traffic data work

df_TRAFFICEventID
df_TRAFFICEventID.createOrReplaceTempView("pp1")

select * from pp1
%%sql
select * from pp1
Type:
Table
Pie
Scatter
Line
Area
Bar
_c0	_c1	_c2	_c3	_c4	_c5	_c6	_c7	_c8	_c9	_c10
Mar 6 05:27:01 SIC-CHENNAI-FW1 1	2018-03-06 05:27:00	9401035224	TRAFFIC	end	1	2018-03-06 05:27:00	10.144.139.64	192.168.122.34	0.0.0.0	0.0.0.0
Mar 6 05:27:01 SIC-CHENNAI-FW1 1	2018-03-06 05:27:00	9401035224	TRAFFIC	end	1	2018-03-06 05:27:00	10.144.139.64	192.168.122.34	0.0.0.0	0.0.0.0
Mar 6 05:27:04 SIC-CHENNAI-FW1 1	2018-03-06 05:27:03	9401035224	TRAFFIC	drop	1	2018-03-06 05:27:03	10.144.139.80	10.108.26.222	0.0.0.0	0.0.0.0
Mar 6 05:27:04 SIC-CHENNAI-FW1 1	2018-03-06 05:27:03	9401035224	TRAFFIC	start	1	2018-03-06 05:27:03	10.144.139.87	52.113.67.75	192.8.169.84	52.113.67.75
Mar 6 05:27:04 SIC-CHENNAI-FW1 1	2018-03-06 05:27:03	9401035224	TRAFFIC	drop	1	2018-03-06 05:27:03	10.144.139.100	10.33.141.70	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.147	10.150.5.9	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.147	10.150.5.9	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.111	10.98.140.81	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.124	10.150.5.9	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.111	10.98.132.13	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.124	10.150.5.9	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.68	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.52.76	10.98.140.81	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.116	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.123	10.150.5.29	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.124	10.150.5.9	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.116	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.52.143	10.150.5.9	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.116	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.217	10.150.5.15	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.52.30	10.150.5.183	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.52.119	10.150.5.9	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.68	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.190	10.150.5.220	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.68	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.52.164	10.150.5.183	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.68	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.101	10.150.5.30	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.146.76.116	10.150.5.12	0.0.0.0	0.0.0.0
Mar 6 05:29:58 Chn_APRIA_FW1 1	2018-03-06 05:29:58	1801051091	TRAFFIC	end	1	2018-03-06 05:29:58	10.161.53.68	10.150.5.12	0.0.0.0	0.0.0.0
...	...	...	...	...	...	...	...	...	...	...
Mar 6 05:30:12 AP-PNP-FW01 1	2018-03-06 05:30:11	2501002602	TRAFFIC	end	0	2018-03-06 05:30:11	10.115.84.205	10.115.82.10	0.0.0.0	0.0.0.0
Mar 6 05:30:12 AP-PNP-FW01 1	2018-03-06 05:30:11	2501002602	TRAFFIC	end	0	2018-03-06 05:30:11	10.115.147.138	95.13.235.193	192.8.202.121	95.13.235.193
Mar 6 05:30:12 AP-PNP-FW01 1	2018-03-06 05:30:11	2501002602	TRAFFIC	end	0	2018-03-06 05:30:11	10.115.87.67	10.134.49.13	0.0.0.0	0.0.0.0
Mar 6 05:30:12 AP-PNP-FW01 1	2018-03-06 05:30:11	2501002602	TRAFFIC	end	0	2018-03-06 05:30:11	10.115.82.13	10.134.49.13	0.0.0.0	0.0.0.0
Mar 6 05:30:17 AP-PNP-FW01 1	2018-03-06 05:30:16	2501002602	TRAFFIC	end	0	2018-03-06 05:30:16	10.14.33.18	10.115.82.31	0.0.0.0	0.0.0.0
Mar 6 05:30:17 AP-PNP-FW01 1	2018-03-06 05:30:16	2501002602	TRAFFIC	end	0	2018-03-06 05:30:16	10.123.13.184	10.115.82.13	0.0.0.0	0.0.0.0
Mar 6 05:30:17 AP-PNP-FW01 1	2018-03-06 05:30:16	2501002602	TRAFFIC	end	0	2018-03-06 05:30:16	10.115.82.11	4.2.2.2	192.8.202.121	4.2.2.2
Mar 6 05:30:20 AP-PNP-FW01 1	2018-03-06 05:30:19	2501002602	TRAFFIC	end	0	2018-03-06 05:30:19	10.134.49.30	10.115.82.31	0.0.0.0	0.0.0.0
Mar 6 05:30:20 AP-PNP-FW01 1	2018-03-06 05:30:19	2501002602	TRAFFIC	end	0	2018-03-06 05:30:19	10.115.89.186	9.126.86.46	192.8.202.121	9.126.86.46
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.82.20	10.14.44.151	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.111	36.225.119.24	192.8.202.121	36.225.119.24
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.146.181	10.115.82.17	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.138	182.89.219.34	192.8.202.121	182.89.219.34
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.85.52	205.185.208.154	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.89.85	23.99.80.186	192.8.202.121	23.99.80.186
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.83.54	10.134.59.140	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.82.20	10.14.44.160	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.138	95.92.2.60	192.8.202.121	95.92.2.60
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.85.30	172.217.17.99	192.8.202.121	172.217.17.99
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.111	77.177.40.231	192.8.202.121	77.177.40.231
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.138	122.174.163.49	192.8.202.121	122.174.163.49
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.146.57	9.113.59.37	192.8.202.121	9.113.59.37
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.82.20	10.14.44.151	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.172	144.76.80.52	192.8.202.121	144.76.80.52
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.85.52	205.185.208.154	192.8.202.121	205.185.208.154
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.111	13.65.251.197	192.8.202.121	13.65.251.197
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.89.85	23.99.80.186	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.83.54	10.134.59.140	0.0.0.0	0.0.0.0
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.147.138	5.235.240.209	192.8.202.121	5.235.240.209
Mar 6 05:29:54 AP-PNP-FW01 1	2018-03-06 05:29:53	2501002602	TRAFFIC	end	0	2018-03-06 05:29:53	10.115.82.20	10.14.44.158	0.0.0.0	0.0.0.0
2500 rows × 11 columns


/
cef = substring_index(df_TRAFFICEventID._c1, '/', 1)
df_TRAFFICEventID = df_TRAFFICEventID.withColumn("Year", cef)

cef = substring_index(df_TRAFFICEventID._c0, ' ', 1)
df_TRAFFICEventID = df_TRAFFICEventID.withColumn("Month", cef)
cef = substring_index(df_TRAFFICEventID._c0, ' ', 1)
df_TRAFFICEventID = df_TRAFFICEventID.withColumn("Month", cef)

3
cef = substring_index(df_TRAFFICEventID._c0, ' ', 3)
df_TRAFFICEventID = df_TRAFFICEventID.withColumn("Date", cef)

cef = substring_index(df_TRAFFICEventID.Date, ' ', -1)
df_TRAFFICEventID = df_TRAFFICEventID.withColumn("Date", cef)

df_TRAFFICEventID
cef = substring_index(df_TRAFFICEventID._c1, ' ', -1)
df_TRAFFICEventID = df_TRAFFICEventID.withColumn("Time", cef)

df_TRAFFICEventID = df_TRAFFICEventID.withColumnRenamed("_c3", "EventID")

df_TRAFFICEventID
df_TRAFFICEventID = df_TRAFFICEventID.drop("_c0", "_c1", "_c6")

df_TRAFFICEventID.show()
+------------+-------+-----+---+--------------+--------------+------------+------------+----+-----+----+--------+
|         _c2|EventID|  _c4|_c5|           _c7|           _c8|         _c9|        _c10|Year|Month|Date|    Time|
+------------+-------+-----+---+--------------+--------------+------------+------------+----+-----+----+--------+
|009401035224|TRAFFIC|  end|  1| 10.144.139.64|192.168.122.34|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:27:00|
|009401035224|TRAFFIC|  end|  1| 10.144.139.64|192.168.122.34|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:27:00|
|009401035224|TRAFFIC| drop|  1| 10.144.139.80| 10.108.26.222|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:27:03|
|009401035224|TRAFFIC|start|  1| 10.144.139.87|  52.113.67.75|192.8.169.84|52.113.67.75|2018|  Mar|   6|05:27:03|
|009401035224|TRAFFIC| drop|  1|10.144.139.100|  10.33.141.70|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:27:03|
|001801051091|TRAFFIC|  end|  1| 10.161.53.147|    10.150.5.9|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.161.53.147|    10.150.5.9|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.146.76.111|  10.98.140.81|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.161.53.124|    10.150.5.9|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.146.76.111|  10.98.132.13|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.161.53.124|    10.150.5.9|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1|  10.161.53.68|   10.150.5.12|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1|  10.161.52.76|  10.98.140.81|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.146.76.116|   10.150.5.12|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.146.76.123|   10.150.5.29|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.161.53.124|    10.150.5.9|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.146.76.116|   10.150.5.12|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.161.52.143|    10.150.5.9|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.146.76.116|   10.150.5.12|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
|001801051091|TRAFFIC|  end|  1| 10.146.76.217|   10.150.5.15|     0.0.0.0|     0.0.0.0|2018|  Mar|   6|05:29:58|
+------------+-------+-----+---+--------------+--------------+------------+------------+----+-----+----+--------+
only showing top 20 rows

​
