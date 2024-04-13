from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, size, col
from copy import deepcopy


def Get_player_info_in_match(player_info, player_info_in_match):
  player_info['placement'] += player_info_in_match.placement

  if player_info_in_match.placement < 5:
    player_info['number_of_in_top'] += 1
    if player_info_in_match.placement == 1:
      player_info['number_of_top_1'] += 1

  for unit in player_info_in_match.units:
    if unit.character_id in player_info['unit_dict']:
      player_info['unit_dict'][unit.character_id]['occurrence'] += 1
      player_info['unit_dict'][unit.character_id]['placement'] += player_info_in_match.placement
    else:
      player_info['unit_dict']["undefined"]['occurrence'] += 1


def main():
  spark = SparkSession.builder \
    .appName("Transform data") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

  # Get data from datalake
  player_df = spark.read.parquet('hdfs://namenode:9000/datalake/player')
  match_df = spark.read.parquet('hdfs://namenode:9000/datalake/match')
  unit_df = spark.read.parquet('hdfs://namenode:9000/datalake/champion')

  # Add some column to player data frame
  player_df = player_df.withColumn('number_of_match', size(col("match_list")))

  # Get player collection
  player_data = player_df.collect()
  
  # Create a default unit dictionary
  unit_data = unit_df.collect()
  default_unit_dict = {'undefined': {'occurrence': 0, 'placement': 0}}
  for unit in unit_data:
    default_unit_dict[unit.ingame_key] = {'occurrence': 0, 'placement': 0}

  # Initial new player data list
  new_player_data = []

  # Loop through each player
  for player in player_data:
    # if player doesn't have any match in match list
    if player.number_of_match < 1:
      continue

    # Initial default player information
    player_info = {
      "placement": 0,
      "number_of_in_top": 0,
      "number_of_top_1": 0,
      "unit_dict": deepcopy(default_unit_dict)
    }

    # Get all player's match and loop through each match
    player_match_list = match_df.filter(match_df.matchId.isin(player.match_list)).collect()
    for match in player_match_list:
      # Loop through each participant of this match
      for player_info_in_match in match.participants:
        # check participants is this player
        if player_info_in_match.puuid != player.puuid:
          continue
        # Get player's infomation in this match
        Get_player_info_in_match(
          player_info=player_info,
          player_info_in_match=player_info_in_match
        )
        break

    # Get top unit of player by occurrence  
    top_unit = sorted(player_info["unit_dict"].items(), key=lambda item: item[1]['occurrence'])[-1]
    top_unit_info = unit_df.filter(unit_df.ingame_key == top_unit[0]).collect()[0]
    new_player_data.append([
      player.name,
      player.tag,
      player.point,
      player.puuid,
      player.number_of_match,
      player_info['number_of_top_1'],
      player_info['number_of_in_top'],
      round(float(player_info["placement"])/player.number_of_match, 2),
      top_unit_info.name,
      top_unit[1]["occurrence"],
      round(float(top_unit[1]["placement"])/top_unit[1]["occurrence"], 2)
    ])

  # Create new player data frame  
  column_name = [
    'name',
    'tag',
    'point',
    'puuid',
    'number_of_match',
    'number_of_top_1',
    'number_of_in_top',
    'placement',
    'top_unit_name',
    'top_unit_occurrence',
    'top_unit_placement'
  ]
  new_player_df = spark.createDataFrame(new_player_data, column_name)
  new_player_df.write.format('hive').saveAsTable('tft.player')


  spark.stop()


main()
