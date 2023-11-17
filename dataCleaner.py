import pandas as pd
import numpy as np
import logging

class dataCleaner:
    def __init__(self):
        self.df = None

    def countMentions(self,id_str):
        try:
            return sum(1 for mentions in self.df['entities'].apply(lambda row: row.get('user_mentions') if isinstance(row, dict) else np.nan).tolist()
                    for mention in mentions if isinstance(mention,dict) and mention.get('id_str') == id_str)
        except Exception as error:
            logging.error(error)
        
    def cleanData(self, file=open('./database/test.json')):
        try:
            self.df = pd.read_json(file)
            
            self.df['user_id'] = self.df['user'].apply(lambda row: row.get('id_str') if isinstance(row, dict) else np.nan)
            self.df['mentions_count'] = self.df.apply(lambda row: self.countMentions(row['user_id']), axis=1)

            return self.df.groupby('user_id').agg({
                'user': 'first',
                'lang': lambda x: list(set(x)),
                'mentions_count': 'sum'
            }).reset_index().to_json(orient='records')
        except Exception as error:
            logging.error(error)