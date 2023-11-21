from sshtunnel import SSHTunnelForwarder
from configparser import ConfigParser
import pymongo
import json
import logging

class dbContext:
    def __init__(self):
        self.server = None
        self.client = None
        
    def config(self, configFile='database\dbConfig.ini', section='mongoDb'):
        try:
            parser = ConfigParser()
            print(configFile)
            parser.read(configFile)
            dbConfig = {}
            if parser.has_section(section):
                params = parser.items(section)
                for param in params:
                    dbConfig[param[0]] = param[1]
                return dbConfig
            else:
                raise Exception(f'Section {section} not found in the {configFile} file.')
        except Exception as error:
            logging.error(error)
        
    def openConnection(self):
        try:
            params = self.config()
            self.server = SSHTunnelForwarder(**params, remote_bind_address=('127.0.0.1', 27017))
            self.server.start()
            self.client = pymongo.MongoClient('127.0.0.1', self.server.local_bind_port)
        except Exception as error:
            logging.error(error)
            self.server.stop()

    def importFile(self, file=open('database/dataset/input/test.json'), dbname='tweetsRetoDb', collection='tweets'):
        try:
            print("llego")
            self.openConnection()
            db = self.client[dbname]
            collection = db[collection]
            self.client.admin.command('ping')
            logging.info("Pinged your deployment. You successfully connected to MongoDB!")
            print("llego2")
            
            file_tweets = (json.loads(file) if isinstance(file,str) else json.load(file))
            
            if isinstance(file_tweets, list):
                collection.insert_many(file_tweets)
            else:
                collection.insert_one(file_tweets)
                
            self.server.stop()
        except Exception as error:
            logging.error(error)
            self.server.stop()
            
    def dbReach(self):
        try:
            self.openConnection()
            self.client.admin.command('ping')
            logging.info("Ping reached the Virtual Machine")
        except Exception as error:
            logging.info("Ping did not reach the Virtual Machine")
            logging.error(error)
            self.server.stop()