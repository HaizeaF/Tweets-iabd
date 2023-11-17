from database.dbContext import *
from dataCleaner import *
import logging

def main():
    try:
        context = dbContext()
        dc = dataCleaner()
        
        cleanedJson = dc.cleanData()
        context.importFile(file=cleanedJson,collection='cleanedTweetsTest')
    except Exception as error:
        logging.error(error)
        
if __name__ == '__main__':
    main()