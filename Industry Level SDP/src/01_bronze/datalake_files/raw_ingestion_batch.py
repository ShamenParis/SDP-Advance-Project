from pyspark import pipelines as dp
from pyspark.sql import functions as F
import json

def ingestion_pipeline(table_name:str, table_properties:list, schema:str, file_format:str, source_path:str, header:str, delimiter:str):
    @dp.table(
        name=table_name,
        table_properties=table_properties,
        schema=schema,
        cluster_by_auto = True
    )
    def load_raw_data():
        df = (
            spark
            .readStream
            .format('cloudFiles')
            .option('cloudFiles.format', file_format)
            .option('cloudFiles.allowOverwrites', 'true')
            .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
            .option('header', header)
            .option('delimiter', delimiter)
            .load(source_path)
            .withColumn('insert_date', F.current_timestamp())
            .drop('_rescued_data')
            )
        return df
    

with open('config.json', mode='r') as f:
    data = json.load(f)
    for tables in data.get('tables'):
        print(tables)
        ingestion_pipeline(table_name=tables.get('name'), table_properties=tables.get('table_properties'), schema=tables.get('schema'), file_format=tables.get('file_format'), source_path=tables.get('source_path'), header=tables.get('header'), delimiter=tables.get('delimiter'))