from pyspark.sql import SparkSession 
from pyspark.sql.types import *
import logging

class dataCleaner:
    def cleanData(file='./database/test.json'):
        try:
            sparkSession = SparkSession.builder.appName('tweets').getOrCreate() 
            
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

            df = sparkSession.read.json(file, schema=schema)
            
            return df.toJSON().first()

        except Exception as error:
            logging.error(error)