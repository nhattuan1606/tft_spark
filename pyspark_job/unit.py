# Import the necessary modules
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
  .appName("Unit app") \
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
unit_df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "champion") \
  .load()

# Open file output
file = open('results/unit_output.txt', 'w')

match_data = match_df.collect()
unit_data = unit_df.collect()
unit_dict = {'undefined': {'occurrence': 0, 'placement': 0}}

for unit in unit_data:
  if unit.ingame_key:
    unit_dict[unit.ingame_key] = {'occurrence': 0, 'placement': 0}

for match in match_data:
  for player_match_info in match.participants:
    for unit in player_match_info.units:
      if unit.character_id in unit_dict:
        unit_dict[unit.character_id]['occurrence'] += 1
        unit_dict[unit.character_id]['placement'] += player_match_info.placement
      else:
        unit_dict["undefined"]['occurrence'] += 1
    # break
  # break

unit_sorted_by_occurrence = reversed(sorted(unit_dict.items(), key=lambda item: item[1]['occurrence']))

file.write('Sort by occurrence:\n\n')
file.write(f'{"Name": <20}{"Cost": <12}Occurrence\n')
for unit in unit_sorted_by_occurrence:
  if unit[0] == 'undefined': continue
  unit_info = unit_df.filter(unit_df.ingame_key == unit[0]).collect()[0]
  file.write(f'{unit_info.name: <20} {unit_info.cost[0]: <11}{unit[1]["occurrence"]}\n')

def placement(item):
  if item[1]['occurrence']:
    return item[1]['placement']/item[1]['occurrence']
  #
  return 10

unit_sorted_by_placement = sorted(unit_dict.items(), key=placement)

file.write('\n\nSort by placement:\n\n')
file.write(f'{"Name": <20}{"Cost": <12}{"Placement": <17}Occurrence\n')
for unit in unit_sorted_by_placement:
  print(unit)
  if unit[0] == 'undefined': continue
  unit_info = unit_df.filter(unit_df.ingame_key == unit[0]).collect()[0]
  file.write(f'{unit_info.name: <20} {unit_info.cost[0]: <11}  {round(unit[1]["placement"]/unit[1]["occurrence"], 2): <19}{unit[1]["occurrence"]}\n')

file.close()

spark.stop()
