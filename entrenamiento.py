import tensorflow as tf # Librería de ML, permite construir y entrenar modelos de manera eficiente.
from tensorflow.keras.preprocessing.text import Tokenizer # Proporciona herramientas para preprocesar texto en modelos de aprendizaje automático
from tensorflow.keras.preprocessing.sequence import pad_sequences # Módulo que se utiliza para el preprocesamiento de secuencias
import numpy as np # Librería para computación numérica y manipulación de arrays multidimensionales
import sys # Permite interactuar con el entorno del sistema proporcionando acceso a variables y funciones específicas
import time # proporciona funciones relacionadas con la manipulación y medición del tiempo

inicio_ejecucion = time.time()


def entrenar_modelo(mensajes, puntuaciones, epochs=1000, guardar_modelo=True, nombre_modelo="modelo_guardado"):
    # Tokenización de mensajes
    tokenizer = Tokenizer()
    tokenizer.fit_on_texts(mensajes)
    vocab_size = len(tokenizer.word_index) + 1
    print(vocab_size)

    # Convertir mensajes a secuencias de números
    secuencias = tokenizer.texts_to_sequences(mensajes)
    print("---------------\n", secuencias)

    # Padding para que todas las secuencias tengan la misma longitud
    padded_secuencias = pad_sequences(secuencias)
    print("---------------\n", padded_secuencias)

    # Crear el modelo de red neuronal
    model = tf.keras.Sequential([
        tf.keras.layers.Embedding(vocab_size, 16, input_length=padded_secuencias.shape[1]),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(64, activation='relu'),
        tf.keras.layers.Dense(1)  # Una neurona de salida para la puntuación
    ])

    # Compilar el modelo
    model.compile(optimizer='adam', loss='mean_squared_error')

    # Entrenar el modelo sin mostrar mensajes de época
    model.fit(padded_secuencias, np.array(puntuaciones), epochs=epochs, verbose=0)

    # Guardar el modelo si se especifica
    if guardar_modelo:
        model.save(nombre_modelo + ".keras")
        print(f"Modelo guardado como {nombre_modelo}.keras")

    return model, tokenizer

def cargar_modelo(nombre_modelo="modelo_guardado"):
    # Cargar el modelo previamente guardado
    model = tf.keras.models.load_model(nombre_modelo + ".keras")
    print(f"Modelo cargado desde {nombre_modelo}.h5")
    return model

def predecir_puntuacion(modelo, tokenizer, nuevo_mensaje):
    nueva_secuencia = tokenizer.texts_to_sequences(nuevo_mensaje)
    nueva_secuencia_padded = pad_sequences(nueva_secuencia, maxlen=modelo.layers[0].input_shape[1])

    # Suprimir la salida de mensajes durante la predicción
    with tf.device('/cpu:0'):
        prediccion = modelo.predict(nueva_secuencia_padded, verbose=0)

    return prediccion[0][0]

# Datos de ejemplo
mensajes = [
    "Este es un buen producto.",
    "No estoy satisfecho con la calidad.",
    "Increíble servicio al cliente.",
    "No recomendaría este producto a nadie.",
    "No nadie, basura",
    "Buenisimo juego, maravilloso"
]

puntuaciones = [4.5, 2.0, 5.0, 1.5, 0.5, 5.0]

# Obtener el nombre del modelo desde la línea de comandos
nombre_modelo_argumento = None
if len(sys.argv) > 1:
    print(sys.argv[1])
    nombre_modelo_argumento = sys.argv[1]

# Determinar si se debe cargar un modelo existente o entrenar uno nuevo
if nombre_modelo_argumento:
    # Cargar el modelo existente
    modelo_entrenado, tokenizer = cargar_modelo(nombre_modelo_argumento)
else:
    # Entrenar un nuevo modelo
    modelo_entrenado, tokenizer = entrenar_modelo(mensajes, puntuaciones, epochs=1000)

# Realizar pruebas con los mismos parámetros de entrada
for mensaje, puntuacion_esperada in zip(mensajes, puntuaciones):
    prediccion = predecir_puntuacion(modelo_entrenado, tokenizer, [mensaje])
    print(f"Puntuación predicha para el nuevo mensaje: {prediccion}, esperada: {puntuacion_esperada}")

fin_ejecucion = time.time()
tiempo_ejecucion = fin_ejecucion - inicio_ejecucion
print(f"Tiempo de ejecucion: {tiempo_ejecucion} segundos")