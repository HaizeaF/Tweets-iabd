from database.dbContext import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from hdfs import InsecureClient
import pandas as pd
import json

# Pyspark
def pySpark():
    try :
        # Configura la sesión de Spark
        spark = (
            SparkSession.builder
            .master('local[*]')
            .appName('leerDatos')
            # .config("spark.mongodb.input.uri", "mongodb://192.168.22.134:27017/tweetsRetoDb.tweets2")
            # .config("spark.mongodb.output.uri", "mongodb://192.168.22.134:27017/tweetsRetoDb.tweets2")
            # .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
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
                StructField("protected", StringType(), True),
                StructField("followers_count", StringType(), True),
                StructField("friends_count", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("verified", StringType(), True),
                StructField("statuses_count", StringType(), True),
                StructField("lang", StringType(), True),
                StructField("default_profile", StringType(), True),
                StructField("default_profile_image", StringType(), True)
            ]), True),
            StructField("entities", StructType([
                    StructField("user_mentions", StructType([
                            StructField("screen_name", StringType(), True)
                        ]),True)
                ]), True)
            ])
        
        # Crear un DataFrame leyendolos datos en streaming con el esquema y directorio definido
        DF = (
            spark.readStream
            .format('json')
            .schema(schema)
            .load('database/dataset/input')
        )
        
        # # Crear un DataFrame que desde la conexión a localhost:9999
        # DF = (
        #     spark.readStream
        #     .format('socket')
        #     .option('host', 'localhost')
        #     .option('port', 9999)
        #     .load()
        # )
        
        # Mostrar datos por consola
        query = DF.writeStream.format('console').start()
        query.awaitTermination(5)
        
        query = DF.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_row_to_mongo(batch_df)).start() # spark # descomentar si voy a recivir de la base de datos MongoDB
        query.awaitTermination()
        
        # Devolver el DataFrame
        return DF
    except Exception as error:
        logging.error(error)

def write_row_to_mongo(batch_df):
    try:
        print("\n\n\n\n\n", batch_df.toJSON().collect())
        print("\n\n\n\n\n", json.dumps(batch_df.toJSON().collect()))
        # print("\n\n\n\n\n", batch_df.toJSON().map(lambda j: json.loads(j)).collect())
        
        
        context = dbContext()
        context.importFile(json.dumps(batch_df.toJSON().collect()), 'tweetsRetoDb', 'tweets2')
        
        # df_tweets = batch_df
        # df_tweets = df_tweets.filter(df_tweets.lang.isNotNull()) # Limpiar tweets vacios
        # df_tweets.write.format('mongo').mode('append').save()
    except Exception as error:
        logging.error(error)

def exportarDatos(DF):
    try:
        # DF.toJSON().first()
        # Ruta de escritura
        #DF.coalesce(1).writeStream.format("json").option("header", "false").save("database/dataset/output/dataFrame.json")

        # Bueno
        outputJson = (DF.writeStream
            .outputMode("append")
            .format("json")
            .option("path", "database\dataset\output")
            .option("checkpointLocation", "database\dataset\output\\basura")
            .start())
            
        # df_spark.writeStream \
        #     .option('path', 'C:/Users/iabd/Desktop/Grado/Tweets-iabd/database/dataset/output') \
        #     .option('checkpointLocation', 'database/dataset/temp') \
        #     .start()
        
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
    DF = pySpark()
    # exportarDatos(DF)
    # subirHDFS(DF)
    
if __name__ == '__main__':
    main()