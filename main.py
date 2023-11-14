from database.dbContext import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def main():
    # Importar base de datos json a MongoDB
    # db = dbContext()
    # db.dbReach()
    # db.importFile('database/tweets.json','tweetsRetoDb','tweets')
    
    # Leer fichero json de una ruta
    # Configura la sesión de Spark
    try: 
        spark = SparkSession.builder.appName("Intervalo1Segundo").getOrCreate()

        # Lee el archivo completo como un conjunto de datos estático
        lines = spark.read.text("database/dataset/test.json")

        print(lines[0])
        # Muestra las líneas
        lines.show()
    except Exception as error:
        logging.error(error)

if __name__ == '__main__':
    main()