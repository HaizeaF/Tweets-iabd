import pandas as pd
import numpy as np

# Sustituir la lectura del json por archivo obtenido del Streaming
df = pd.read_json('./database/tweets.json')

df['user_id'] = df['user'].apply(lambda x: x.get('id_str') if isinstance(x, dict) else np.nan)
usersDf = df.groupby('user_id').agg({
    'lang': lambda x: list(set(x)),
    'user': 'first',
}).reset_index()

print(usersDf)