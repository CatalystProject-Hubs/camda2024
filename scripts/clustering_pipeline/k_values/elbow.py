# Librerias
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm


from sklearn.cluster import KMeans
from kneed import KneeLocator


def printElbow(K, sse, knee):
     # Crear un gráfico para visualizar la prueba del codo
    plt.plot(K, sse, 'bx-')
    plt.xlabel('Número de clusters (K)')
    plt.ylabel('Suma de los cuadrados de los errores (SSE)')
    plt.title('Prueba del codo')
    
    plt.axvline(knee.knee, color='r', linestyle='--')
    plt.text(knee.knee, sse[knee.knee-1], f"K = {knee.knee}", ha='center', va='bottom', fontsize=12)
    plt.show()

def elbow(clustering_method, data):
    K = range(1, 20)
    sse = []
    
    for k in K:
        model = clustering_method(n_clusters=k)
        model.fit(data)
        sse.append(model.inertia_)

    knee = KneeLocator(K, sse, curve='convex', direction='decreasing')
    
    #printElbow(K, sse, knee)

    return knee.knee

