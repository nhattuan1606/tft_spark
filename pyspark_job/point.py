# Import the necessary modules
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
  .appName("Point app") \
  .config("spark.mongodb.read.connection.uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/tft_data.player") \
  .config("spark.mongodb.write.connection.uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net/tft_data.player") \
  .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.2') \
  .getOrCreate()

# Read data from mongodb
df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "player") \
  .load()

# Open file output
file = open('results/point_output.txt', 'w')

# Find the lowest point
lowest_point = df.agg({"point": "min"}).collect()[0]['min(point)']
number_of_lowest_point = df.filter(df.point == lowest_point).count()
file.write(f'Lowest point: {lowest_point}\n')
file.write(f'Number of lowest point: {number_of_lowest_point}\n\n')

# Find the highest point
highest_point = df.agg({"point": "max"}).collect()[0]['max(point)']
number_of_highest_point = df.filter(df.point == highest_point).count()
file.write(f'Highest point: {highest_point}\n')
file.write(f'Number of highest point: {number_of_highest_point}\n\n')

# Find the number of players in the point range
# point >= 600
number_of_player = df.filter(df.point >= 600).count()
file.write(f'Number of players has point > 600: {number_of_player}\n')

# point in [500, 600)
number_of_player = df.filter((df.point >= 500) & (df.point < 600)).count()
file.write(f'Number of players has point in [500, 600): {number_of_player}\n')

# point in [400, 500)
number_of_player = df.filter((df.point >= 400) & (df.point < 500)).count()
file.write(f'Number of players has point in [400, 500): {number_of_player}\n')

# point in [300, 400)
number_of_player = df.filter((df.point >= 300) & (df.point < 400)).count()
file.write(f'Number of players has point in [300, 400): {number_of_player}\n')

# point in [200, 300)
number_of_player = df.filter((df.point >= 200) & (df.point < 300)).count()
file.write(f'Number of players has point in [200, 300): {number_of_player}\n')

# point in [100, 200)
number_of_player = df.filter((df.point >= 100) & (df.point < 200)).count()
file.write(f'Number of players has point in [100, 200): {number_of_player}\n')

# point in [0, 100)
number_of_player = df.filter((df.point >= 0) & (df.point < 100)).count()
file.write(f'Number of players has point in [0, 100): {number_of_player}')

file.close()

spark.stop()
