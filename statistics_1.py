import json
import numpy as np
import pandas as pd
from datetime import datetime

# Carga los tweets desde el archivo JSON
tweets = json.loads(open("tweets.json", "r").read())
df = pd.DataFrame(tweets)
numF = df.shape[0]
grouped = df.groupby(df.lang)
idiomas = []

for lang, group_df in grouped:
    idiomas.append((lang, group_df))  # Guarda tanto el idioma como el DataFrame correspondiente

estadisticas = []
hora_mas_frecuente_por_idioma = []
user_lang_counts = []

with open("estadisticas.json", "w") as f:
    f.write("[")  # Agregar corchete de apertura para iniciar una lista de diccionarios

    for i, (lang, df) in enumerate(idiomas):
        longitudes = df['text'].apply(len)
        med = np.mean(longitudes)
        mediana = np.median(longitudes)
        des = np.std(longitudes)

        # Extrae horas y encuentra la más frecuente
        hours = np.array([datetime.strptime(getattr(tweet, "created_at"), "%a %b %d %H:%M:%S +0000 %Y").hour for tweet in df.itertuples()])
        frecuencia_horas = np.bincount(hours)
        hora_mas_frecuente = np.argmax(frecuencia_horas)

        # Calcula el porcentaje de tweets para el idioma actual
        porcentaje_tweets = len(df) / numF * 100

        # Cuenta las ocurrencias de cada "user.lang"
        user_lang_count = df["user"].apply(lambda x: x["lang"]).value_counts().head(5).index.tolist()

        # Escribir el diccionario actual y agregar una coma si no es el último
        f.write("{\n")
        f.write(f'"idioma": "{lang}",\n')
        f.write(f'"porcentaje_tweets": "{porcentaje_tweets:.2f}%",\n')
        f.write(f'"media": {med},\n')
        f.write(f'"mediana": {mediana},\n')
        f.write(f'"desviacion": {des},\n')
        f.write(f'"hora_mas_frecuente": {hora_mas_frecuente},\n')
        f.write(f'"lenguaje_usuario": {json.dumps(user_lang_count)}\n')
        f.write("}")

        # Si no es el último elemento, agregar una coma
        if i < len(idiomas) - 1:
            f.write(",")

    f.write("]")  # Agregar corchete de cierre para finalizar la lista de diccionarios

print("terminado")
print(numF)


