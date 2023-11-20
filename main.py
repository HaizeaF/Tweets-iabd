from database.dbContext import *
from dataCleaner import *
import logging

def main():
    try:
        context = dbContext()
        context.importFile()
    except Exception as error:
        logging.error(error)
        
if __name__ == '__main__':
    main()