import pandas as pd
import numpy as np
import logging

class dataCleaner:
    def __init__(self):
        self.df = None

    def countMentions(self,id_str):
        try:
            return sum(1 for mentions in self.df['entities'].apply(lambda row: row.get('user_mentions') if isinstance(row, dict) else {}).tolist()
                    for mention in mentions if isinstance(mention,dict) and mention.get('id_str') == id_str)
        except Exception as error:
            logging.error(f'{error}, id: {id_str}')
        
    def cleanData(self, file=open('./database/test.json')):
        try:
            self.df = pd.read_json(file)
            
            self.df['user_id'] = self.df['user'].apply(lambda row: row.get('id_str') if isinstance(row, dict) else {})
            self.df['mentions_count'] = self.df.apply(lambda row: self.countMentions(row['user_id']), axis=1)
            
            columns_to_remove = ['id','id_str','listed_count','utc_offset','geo_enabled','is_translator','profile_background_color','profile_background_image_url','profile_background_image_url_https','profile_background_tile','profile_image_url','profile_image_url_https','profile_banner_url','profile_link_color','profile_sidebar_border_color','profile_sidebar_fill_color','profile_text_color','profile_use_background_image','following','follow_request_sent','notifications']

            for column in columns_to_remove:
                if column in self.df['user'].iloc[0]: 
                    self.df['user'] = self.df['user'].apply(lambda user: {data: val for data, val in user.items() if data != column})

            return self.df.groupby('user_id').agg({
                'user': 'first',
                'lang': lambda lang: list(set(lang)),
                'mentions_count': 'sum'
            }).reset_index().to_json(orient='records')
        except Exception as error:
            logging.error(error)