from pyspark.sql import SparkSession
from argparse import ArgumentParser

def main():
  spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .config("spark.mongodb.read.connection.uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/tft_data.player") \
    .config("spark.mongodb.write.connection.uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/tft_data.player") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.2') \
    .getOrCreate()
  
  # Get argument
  parser = ArgumentParser()
  parser.add_argument("--tableName", help="Please enter table name!")
  args = parser.parse_args()

  if not args.tableName:
    print("tableName argument is missing")
    spark.stop()

    return

  # Read data from mongodb
  table_df = spark.read \
    .format('mongodb') \
    .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
    .option("database", "tft_data") \
    .option("collection", args.tableName) \
    .load()
  
  if table_df.count() == 0:
    print("Table is empty. Please check table name!")
    spark.stop()

    return
  
  # Handle NullType
  table_schema = list(table_df.schema)
  null_columns = []
  for struct_field in table_schema:
    if str(struct_field.dataType) == 'NullType':
      null_columns.append(struct_field)

  for col in null_columns:
    col_name = str(col.name)
    table_df = table_df.withColumn(col_name, table_df[col_name].cast('string'))
  
  # Save data to datalake
  datalake_location = 'hdfs://namenode:9000/datalake/' + args.tableName
  table_df.write.parquet(datalake_location)

  spark.stop()


main()
