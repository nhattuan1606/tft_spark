# Import the necessary modules
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
  .appName("Player app") \
  .config("spark.mongodb.read.connection.uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/tft_data.player") \
  .config("spark.mongodb.write.connection.uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/tft_data.player") \
  .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.2') \
  .getOrCreate()

# Read data from mongodb
match_df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "match") \
  .load()

# Read data from mongodb
player_df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "player") \
  .load()

# Read data from mongodb
unit_df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "champion") \
  .load()

# Read data from mongodb
augment_df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "augment") \
  .load()

# Read data from mongodb
trait_df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "trait") \
  .load()

# Open file output
file = open('results/player_output.txt', 'w')


