from database.dbContext import *

def main():
    dbContext.importFile('test.json','tweetsRetoDb','test')

if __name__ == '__main__':
    main()