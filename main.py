from database.dbContext import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from hdfs import InsecureClient

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
        
        newDF = (DF
                 .limit(100))
        
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
        query.awaitTermination(20)
        
        # Devolver el DataFrame
        return DF
    except Exception as error:
        logging.error(error)

def subirMongo(DF):
    context = dbContext()
    dbContext.importFile(DF.toJSON().first())
    
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
    subirMongo(DF)
    # subirHDFS(DF)
    
if __name__ == '__main__':
    main()