from database.dbContext import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import collect_set, first, explode
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
                StructField("location", StringType(), True),
                StructField("url", StringType(), True),
                StructField("description", StringType(), True),
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
                StructField("hashtags", ArrayType(
                    StructType([
                        StructField("text", StringType(), True)
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
        
        hashtagsDF = (sparkDF.groupBy("user.id_str")
                        .agg(explode(first("entities.hashtags")).alias("hashtags"))
                        .select("id_str", "hashtags.text"))

        hashtagsDF = (hashtagsDF.groupBy("id_str")
                        .agg(collect_set("text").alias("hashtags"))
                        .select("id_str","hashtags"))

        userDF = (sparkDF.groupBy("user.id_str")
                        .agg(collect_set("lang").alias("tweets_lang"), first("user").alias("user"))
                        .select("tweets_lang", "user.*"))

        groupedDF = hashtagsDF.join(userDF, hashtagsDF["id_str"] == userDF["id_str"], "inner")
        groupedDF = groupedDF.drop(userDF["id_str"])

        # Devolver el DataFrame
        pandasDf = groupedDF.where((groupedDF.statuses_count > -1) & (groupedDF.followers_count > -1) & (groupedDF.friends_count > -1)).toPandas()
        pandasDf.to_csv("database\dataset\output\\tweets.csv", index=False)
        json_data = pandasDf.to_dict('records')
        return json.dumps(json_data)
    except Exception as error:
        logging.error(error)
    
def subirHDFS():
    try:
        # Instancia de HDFS, ruta http y usuario
        hdfsClient = InsecureClient('http://localhost:50070', user='raj_ops')
        
        # Ruta de archivo hdfs
        path_archivo_hdfs = '/user/raj_ops/tweets.csv'
        path_archivo_local = 'database\dataset\output\\tweets.csv'
        
        # Subir archivo a HDFS
        with open(path_archivo_local, 'rb') as archivo_local:
            hdfsClient.write(path_archivo_hdfs, archivo_local)
    except Exception as error:
        logging.error(error)
    
    
def main():
    # Leer datos y crear DataFrame
    json = pySpark()
    
    # Importar DataFrame
    context = dbContext()
    context.importFile(json, ":P", ":P")
    
    # Subir a HDFS
    subirHDFS()
    
if __name__ == '__main__':
    main()