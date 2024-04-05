# build and run docker container
docker build -t cluster-apache-spark:3.0.2 .

docker-compose up -d

# Run job about point
docker cp -L pyspark_job/point.py tft_spark_spark-master_1:/opt/spark/jobs/point_job.py

docker-compose exec spark-master spark-submit --conf "spark.mongodb.read.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --conf "spark.mongodb.write.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2 --master spark://spark-master:7077 jobs/point_job.py

docker cp -L tft_spark_spark-master_1:/opt/spark/results/point_output.txt results/point_output.txt

# Run job about unit
docker cp -L pyspark_job/unit.py tft_spark_spark-master_1:/opt/spark/jobs/unit_job.py

docker-compose exec spark-master spark-submit --conf "spark.mongodb.read.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --conf "spark.mongodb.write.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2 --master spark://spark-master:7077 jobs/unit_job.py

docker cp -L tft_spark_spark-master_1:/opt/spark/results/unit_output.txt results/unit_output.txt

#  Run job about player
docker cp -L pyspark_job/player.py tft_spark_spark-master_1:/opt/spark/jobs/player_job.py

docker-compose exec spark-master spark-submit --conf "spark.mongodb.read.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --conf "spark.mongodb.write.connection.uri=mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/" --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2 --master spark://spark-master:7077 jobs/player_job.py

docker cp -L tft_spark_spark-master_1:/opt/spark/results/player_output.txt results/player_output.txt

# shutdown all docker container
docker-compose down
