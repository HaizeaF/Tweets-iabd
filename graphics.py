import json
import matplotlib.pyplot as plt

# Cargar los datos desde el archivo JSON
with open("estadisticas.json", "r") as f:
    data = json.load(f)

def usoIdiomas(data):
    # Extraer datos para el gráfico de barras horizontal
    idiomas = [dato["idioma"] for dato in data]
    porcentajes_tweets = [float(dato["porcentaje_tweets"].replace("%", "")) for dato in data]

    # Emparejar los idiomas y porcentajes, luego ordenar por porcentaje
    datos_ordenados = sorted(zip(idiomas, porcentajes_tweets), key=lambda x: x[1], reverse=True)
    idiomas_ordenados, porcentajes_ordenados = zip(*datos_ordenados)

    # Ajustar el tamaño de la figura
    plt.figure(figsize=(12, 8))

    # Colores personalizados
    colores = ['salmon', 'lightblue', 'lightgreen', 'gold', 'lightcoral', 'lightskyblue', 'palegreen', 'khaki', 'lightpink', 'lightsteelblue']

    # Crear el gráfico de barras horizontal con colores personalizados y etiquetas
    plt.barh(idiomas_ordenados, porcentajes_ordenados, color=colores)
    
    # Etiquetas en las barras
    for i, porcentaje in enumerate(porcentajes_ordenados):
        plt.text(porcentaje + 0.5, i, f'{porcentaje:.2f}%', ha='left', va='center', fontsize=10)

    # Títulos y etiquetas
    plt.xlabel('Porcentaje de Tweets')
    plt.title('Distribución de Tweets por Idioma (Ordenado por Porcentaje)')

    # Añadir una rejilla horizontal
    plt.grid(axis='x', linestyle='--', alpha=0.6)

    plt.show()

def distribucion_en(data):
    ingles = next(dato for dato in data if dato["idioma"] == "en")
    lenguaje_usuario_ingles = ingles["lenguaje_usuario"]

    etiquetas = list(lenguaje_usuario_ingles.keys())
    porcentajes_usuario = [float(valor[:-1]) for valor in lenguaje_usuario_ingles.values()]

    # Ajustar el tamaño de la figura
    plt.figure(figsize=(12, 8))

    # Colores personalizados
    colores = plt.cm.Set3(range(len(etiquetas)))

    # Crear el gráfico de pastel con colores personalizados y mejoras visuales
    explode = [0.1 if i == porcentajes_usuario.index(max(porcentajes_usuario)) else 0 for i in range(len(etiquetas))]
    plt.pie(porcentajes_usuario, labels=None, autopct=None, startangle=90, counterclock=False, colors=colores, explode=explode)

    # Añadir una leyenda a la derecha
    plt.legend([f'{etiqueta} ({porcentaje}%)' for etiqueta, porcentaje in zip(etiquetas, porcentajes_usuario)],
               bbox_to_anchor=(1, 0.5), loc="center left", title="Idioma", fancybox=True, shadow=True)

    # Título del gráfico
    plt.title('Distribución del Lenguaje del Usuario para Tweets en Inglés')

    # Mostrar el gráfico
    plt.show()



def distribucion_es(data):
    ingles = next(dato for dato in data if dato["idioma"] == "es")
    lenguaje_usuario_ingles = ingles["lenguaje_usuario"]

    etiquetas = list(lenguaje_usuario_ingles.keys())
    porcentajes_usuario = [float(valor[:-1]) for valor in lenguaje_usuario_ingles.values()]

    # Ajustar el tamaño de la figura
    plt.figure(figsize=(12, 8))

    # Colores más vistosos
    colores = plt.cm.Set1(range(len(etiquetas)))

    # Crear el gráfico de pastel con colores más vistosos y mejoras visuales
    explode = [0.1 if i == porcentajes_usuario.index(max(porcentajes_usuario)) else 0 for i in range(len(etiquetas))]
    plt.pie(porcentajes_usuario, labels=None, autopct=None, startangle=90, counterclock=False, colors=colores, explode=explode)

    # Añadir una leyenda a la derecha
    plt.legend([f'{etiqueta} ({porcentaje}%)' for etiqueta, porcentaje in zip(etiquetas, porcentajes_usuario)],
               bbox_to_anchor=(1, 0.5), loc="center left", title="Idioma", fancybox=True, shadow=True)

    # Título del gráfico
    plt.title('Distribución del Lenguaje del Usuario para Tweets en Inglés')

    # Mostrar el gráfico
    plt.show()

def longIdioma(data):
    # Crear un diccionario para almacenar las longitudes medias de los tweets por idioma
    datos_por_idioma = {}

    # Agrupar datos por idioma
    for dato in data:
        idioma = dato["idioma"]
        longitud_tweet_media = dato["media"]
        
        datos_por_idioma[idioma] = longitud_tweet_media

    # Ordenar los datos por longitud del tweet
    datos_ordenados = dict(sorted(datos_por_idioma.items(), key=lambda item: item[1], reverse=True))

    # Ajustar el tamaño de la figura
    plt.figure(figsize=(12, 8))

    # Crear el gráfico de barras con colores atractivos y rejilla
    colores = plt.cm.Paired(range(len(datos_ordenados)))
    plt.bar(datos_ordenados.keys(), datos_ordenados.values(), color=colores)
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Títulos y etiquetas
    plt.title('Longitud Media del Tweet por Idioma')
    plt.xlabel('Idioma')
    plt.ylabel('Longitud Media del Tweet')

    # Mostrar el gráfico
    plt.show()


usoIdiomas(data)
longIdioma(data)
distribucion_en(data)
distribucion_es(data)



