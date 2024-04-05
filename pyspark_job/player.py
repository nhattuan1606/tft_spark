# Import the necessary modules
from pyspark.sql import SparkSession
from copy import deepcopy

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
trait_df = spark.read \
  .format('mongodb') \
  .option("uri", "mongodb+srv://root:1234@cluster0.dsjq879.mongodb.net") \
  .option("database", "tft_data") \
  .option("collection", "trait") \
  .load()


unit_data = unit_df.collect()
default_unit_dict = {'undefined': {'occurrence': 0, 'placement': 0}}
for unit in unit_data:
  default_unit_dict[unit.ingame_key] = {'occurrence': 0, 'placement': 0}

trait_data = trait_df.collect()
default_trait_dict = {'undefined': {'occurrence': 0, 'placement': 0}}
for trait in trait_data:
  default_trait_dict[trait.ingame_key] = {'occurrence': 0, 'placement': 0}

# Open file output
file = open('results/player_output.txt', 'w')


# function to get player information in a match 
def Get_player_info_in_match(player_info_in_match, player_info):
  player_info['placement'] += player_info_in_match.placement
  player_info['level'] += player_info_in_match.level
  if player_info_in_match.placement < 5:
    player_info['in_top'] += 1
    if player_info_in_match.placement == 1:
      player_info['top_1'] += 1

  for unit in player_info_in_match.units:
    if unit.character_id in player_info['unit_dict']:
      player_info['unit_dict'][unit.character_id]['occurrence'] += 1
      player_info['unit_dict'][unit.character_id]['placement'] += player_info_in_match.placement
    else:
      player_info['unit_dict']["undefined"]['occurrence'] += 1

  for trait in player_info_in_match.traits:
    if trait.name in player_info['trait_dict']:
      player_info['trait_dict'][trait.name]['occurrence'] += 1
      player_info['trait_dict'][trait.name]['placement'] += player_info_in_match.placement
    else:
      player_info['trait_dict']["undefined"]['occurrence'] += 1


# get top 10 player with highest point
top_player_df = player_df.sort(player_df.point.desc()).take(10)

for player in top_player_df:
  file.write(f'Player: {player.name}#{player.tag} - Point: {player.point}\n')

  number_of_match = len(player.match_list)
  player_info = {
    "level": 0,
    "placement": 0,
    "in_top": 0,
    "top_1": 0,
    "unit_dict": deepcopy(default_unit_dict),
    "trait_dict": deepcopy(default_trait_dict)
  }

  player_match_list = match_df.filter(match_df.matchId.isin(player.match_list)).collect()
  for match in player_match_list:
    for player_info_in_match in match.participants:
      if player_info_in_match.puuid != player.puuid:
        continue
   
      Get_player_info_in_match(
        player_info_in_match=player_info_in_match,
        player_info=player_info
      )

      break

  if not number_of_match: continue

  file.write(f'Number of match: {number_of_match} - Placment: {player_info["placement"]/number_of_match}\n')
  file.write(f'Number of match in top: {player_info["in_top"]} - In top rate: {round(player_info["in_top"]/number_of_match, 4)*100}%\n')
  file.write(f'Number of match top 1: {player_info["top_1"]} - Top 1 rate: {round(player_info["top_1"]/number_of_match, 4)*100}%\n')

  i = 0
  file.write('Top Units:\n')
  for unit in reversed(sorted(player_info["unit_dict"].items(), key=lambda item: item[1]['occurrence'])):
    i += 1
    unit_info = unit_df.filter(unit_df.ingame_key == unit[0]).collect()[0]
    file.write(f'Name: {unit_info.name} - Occurrence: {unit[1]["occurrence"]} - Placement: {round(unit[1]["placement"]/unit[1]["occurrence"], 2)}\n')
    if i == 3: break

  i = 0
  file.write('Top Traits:\n')
  for trait in reversed(sorted(player_info["trait_dict"].items(), key=lambda item: item[1]['occurrence'])):
    i += 1
    trait_info = trait_df.filter(trait_df.ingame_key == trait[0]).collect()[0]
    file.write(f'Name: {trait_info.name} - Occurrence: {trait[1]["occurrence"]} - Placement: {round(trait[1]["placement"]/trait[1]["occurrence"], 2)}\n')
    if i == 3: break
  file.write('\n\n')


file.close()

spark.stop()
