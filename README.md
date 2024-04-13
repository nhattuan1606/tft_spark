# tft_spark

- Big data system in Docker environment with Hadoop, Spark and Hive
- Pyspark code is stored in ***spark_job*** directory

## Start

To run all cluster use command:
```
docker-compose up -d
```

## Extract and load data

Exec to namenode and create Datalake directory in hdfs:
```
docker exec -it namenode bash

hdfs dfs -mkdir -p /datalake
```

Copy pyspark job to spark master:
```
docker cp -L spark_jobs/data_ingestion.py spark-master:/data_ingestion.py
```

Exec to spark master and run job:
```
docker exec -it spark-master bash

spark/bin/spark-submit --conf "spark.mongodb.read.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --conf "spark.mongodb.write.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2 --master spark://spark-master:7077 data_ingestion.py --tableName player
```
This command will extract and load data from player table in mongoDB Atlas to datalake. Use this command to extract and load data from tables ***augment***, ***champion***, ***item***, ***match***, ***trait*** by changing the argunment ***tableName*** with the corresponding table name.

## Transform data
Exec to hive server and start hiveserver2:
```
docker exec -it hive-server bash

hiveserver2
```

Create ***tft*** database:
```
beeline -u jdbc:hive2://localhost:10000 -n root
  
!connect jdbc:hive2://127.0.0.1:10000 scott tiger

create database tft
```

Copy pyspark job to spark master:
```
docker cp -L spark_jobs/player_transform.py spark-master:/player_transform.py
```

Exec to spark master and run transform job:
```
docker exec -it spark-master bash

spark/bin/spark-submit --conf "hive.metastore.uris=thrift://hive-metastore:9083" --master spark://spark-master:7077 player_transform.py
```
This command will transform data about players from datalake and save it to hive database.

Do the same with unit_transform.py to transform data about units.
