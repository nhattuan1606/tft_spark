from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, size, col


def main():
  spark = SparkSession.builder \
    .appName("Transform data") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()
  
  # Get data from datalake
  match_df = spark.read.parquet('hdfs://namenode:9000/datalake/match')
  unit_df = spark.read.parquet('hdfs://namenode:9000/datalake/champion')

  # Create a unit dictionary to store data about unit
  unit_data = unit_df.collect()
  unit_dict = {'undefined': {'occurrence': 0, 'placement': 0}}
  for unit in unit_data:
    if unit.ingame_key:
      unit_dict[unit.ingame_key] = {'occurrence': 0, 'placement': 0}

  # Create match data collection
  match_data = match_df.collect()
  # Loop through each match
  for match in match_data:
    for player_match_info in match.participants:
      for unit in player_match_info.units:
        if unit.character_id in unit_dict:
          unit_dict[unit.character_id]['occurrence'] += 1
          unit_dict[unit.character_id]['placement'] += player_match_info.placement
        else:
          unit_dict["undefined"]['occurrence'] += 1
  
  new_unit_data = []
  # Loop each unit in unit dictionary
  for unit in unit_dict:
    if unit == 'undefined': continue

    unit_info = unit_df.filter(unit_df.ingame_key == unit).collect()[0]
    new_unit_data.append([
      unit_info.ingame_key,
      unit_info.key,
      unit_info.name,
      unit_dict[unit]['occurrence'],
      round(float(unit_dict[unit]['placement'])/unit_dict[unit]['occurrence'], 2) if unit_dict[unit]['occurrence'] else 8.0
    ])

  # Create new unit data frame
  column_name = [
    'ingame_key',
    'key',
    'name',
    'occurrence',
    'placement'
  ]
  new_unit_df = spark.createDataFrame(new_unit_data, column_name)
  new_unit_df.write.format('hive').saveAsTable('tft.unit')


  spark.stop()


main()
