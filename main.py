from database.dbContext import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import collect_set, first
from hdfs import InsecureClient
import json

# Pyspark
def pySpark():
    try :
        # Configura la sesión de Spark
        spark = (
            SparkSession.builder
            .master('local[*]')
            .appName('leerDatos')
            .getOrCreate()
        )
        
        # Crear esquema
        schema = StructType([
            StructField("lang", StringType(), True),
            StructField("user", StructType([
                StructField("id_str", StringType(), True),
                StructField("name", StringType(), True),
                StructField("screen_name", StringType(), True),
                StructField("location", StringType(), True),
                StructField("url", StringType(), True),
                StructField("description", StringType(), True),
                StructField("protected", BooleanType(), True),
                StructField("followers_count", IntegerType(), True),
                StructField("friends_count", IntegerType(), True),
                StructField("created_at", StringType(), True),
                StructField("verified", BooleanType(), True),
                StructField("statuses_count", IntegerType(), True),
                StructField("lang", StringType(), True),
                StructField("default_profile", BooleanType(), True),
                StructField("default_profile_image", BooleanType(), True)
            ]), True),
            StructField("entities", StructType([
                StructField("user_mentions", ArrayType(
                    StructType([
                        StructField("screen_name", StringType(), True)
                    ]), True), 
                True)
            ]), True)
        ])
        
        # Crear un DataFrame leyendo los datos en streaming con el esquema y directorio definido
        sparkDF = (
            spark.read
            .format('json')
            .schema(schema)
            .load('database/dataset/input')
        )
        
        # # Crear un DataFrame leyendo los datos desde socket
        # sparkDF = (
        # spark.readStream.format("socket")
        #     .option("host", "localhost")
        #     .option("port", 123456)
        #     .load())
        # sparkDF = sparkDF.select(functions.from_json(sparkDF.value, schema).alias("data")).select("data.*")
        
        groupedDF = (sparkDF.groupBy("user.id_str")
                        .agg(collect_set("lang")
                        .alias("tweets_lang"), first("user")
                        .alias("user"), collect_set("entities.user_mentions")
                        .alias("mentioned_users")).select("tweets_lang","user.*","mentioned_users"))
        
        # Devolver el DataFrame
        pandasDf = groupedDF.toPandas()
        json_data = pandasDf.to_dict('records')
        return json.dumps(json_data)
    except Exception as error:
        logging.error(error)
    
def subirHDFS(DF):
    try:
        # Instancia de HDFS, ruta http y usuario
        hdfsClient = InsecureClient('http://localhost:50075', user='raj_ops')
        
        # Ruta de archivo hdfs
        path_archivo_hdfs = '/user/raj_ops/prueba.txt'
        
        # hdfsClient.upload(path_archivo_hdfs, path_archivo_local)
        # Escritura de archivo en HDFS
        DF.writeStream \
            .outputMode('complete') \
            .option('checkpointLocation', path_archivo_hdfs) \
            .format('memory') \
            .start()
        
        # with open(path_archivo_local, 'rb') as archivo_local:
        #     hdfsClient.writeStream(path_archivo_hdfs, archivo_local)
    except Exception as error:
        logging.error(error)
    
    
def main():
    # Leer datos y crear DataFrame
    DF = pySpark()
    
    # Importar DataFrame
    context = dbContext()
    context.importFile(DF)
    
    # Subir a HDFS
    # subirHDFS(DF)
    
if __name__ == '__main__':
    main()