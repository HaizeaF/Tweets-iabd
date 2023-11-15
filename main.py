from database.dbContext import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import pandas as pd
import numpy as np

def main():
    # Importar base de datos json a MongoDB
    # db = dbContext()
    # db.dbReach()
    # db.importFile('database/tweets.json','tweetsRetoDb','tweets')
    
    # Leer fichero json de una ruta
    # Configura la sesión de Spark
    try:
        # Configura la sesión de Spark
        spark = SparkSession.builder.master("local[2]").appName("tweetsCadaSegundo").getOrCreate()

        # Configura el contexto de streaming con intervalos de 1 segundo
        ssc = StreamingContext(spark.sparkContext, 1)

        # Lee el directorio como un flujo de texto
        lines = ssc.textFileStream("database/dataset/test2.json")

        # Define una función para imprimir cada RDD
        def print_full_data(rdd):
            # Convierte cada línea JSON a un objeto Python
            json_objects = rdd.map(json.loads)
            
            print("hola")
            # Imprime cada objeto JSON
            for obj in json_objects.collect():
                print(obj)

        # Aplica la función a cada RDD del DStream
        lines.foreachRDD(print_full_data)

        # Inicia el proceso de streaming
        ssc.start()
        ssc.awaitTermination(5)
    except Exception as error:
        logging.error(error)

if __name__ == '__main__':
    main()