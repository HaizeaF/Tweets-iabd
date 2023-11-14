import pandas as pd

dropColumns = ['created_at', 'id', 'id_str', 'text', 'source', 'truncated', 'in_reply_to_status_id', 'in_reply_to_status_id_str',
    'in_reply_to_user_id', 'in_reply_to_screen_name', 'geo', 'coordinates', 'place', 'contributors', 'retweet_count', 'favorited',
    'retweeted', 'possibly_sensitive', 'lang']

# Sustituir la lectura del json por archivo obtenido del Streaming
df = pd.read_json('./database/tweets.json')

df.drop(columns=dropColumns, inplace=True)