from database.dbContext import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Pyspark
def pyspark():
    try :
        # Configura la sesión de Spark
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("leerDatos")
            .getOrCreate())
        
        # Crear esquema
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("nombre", StringType(), True)
        ])
        
        # schema2 = StructType([
        #     StructField("id", IntegerType(), True),
        #     StructField("nombre", StringType(), True),
        #     StructField("valor", FloatType(), True)
        # ])

        # Leer datos en streaming con el esquema y directorio definido
        DF = (
            spark.readStream
            .format("json")
            .schema(schema)
            .load("database\dataset\input"))
        
        # Mostrar datos por consola
        query = DF.writeStream.format("console").start()
        query.awaitTermination()
        
    except Exception as error:
        logging.error(error)

def main():
    pyspark()
    
if __name__ == '__main__':
    main()