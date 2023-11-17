import pandas as pd
import numpy as np

# Sustituir la lectura del json por archivo obtenido del Streaming
df = pd.read_json('./database/tweets.json')

df['user_id'] = df['user'].apply(lambda x: x.get('id_str') if isinstance(x, dict) else np.nan)
df['mentions_count'] = df['entities'].apply(lambda x: len(x.get('user_mentions')) if isinstance(x, dict) and x.get('user_mentions') else 0)

usersDf = df.groupby('user_id').agg({
    'user': 'first',
    'lang': lambda x: list(set(x)),
    'mentions_count': lambda x: sum(x > 0)
}).reset_index()
usersDf['mentions_count'] = df.groupby('user_id')['mentions_count'].sum().reset_index()['mentions_count']

print(usersDf.tail)