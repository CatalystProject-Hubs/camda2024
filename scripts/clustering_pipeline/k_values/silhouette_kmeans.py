
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm


from sklearn.metrics import silhouette_samples, silhouette_score
def plot_silhouette(data, silhouette_values, best_k):
    plt.plot(range(3, 10), silhouette_values)
    plt.xlabel('Número de clusters (k)')
    plt.ylabel('Puntuación de silueta')
    plt.title('Gráfica de silueta')

    # Agregar una marca en el mejor valor
    plt.plot(best_k, max(silhouette_values), 'ro')  # 'ro' significa "red circle"
    plt.annotate(f'Mejor k: {best_k}', (best_k, max(silhouette_values)), textcoords="offset points", xytext=(0,10), ha='center')

    plt.show()

def silhouette(data):
    data = data.to_numpy()
    K = range(3, 10)
    silhouette_values = []
    
    for k in K:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto")
        cluster_labels = kmeans.fit_predict(data)
    
        silhouette_avg = silhouette_score(data, cluster_labels)
        silhouette_values.append(silhouette_avg)
        #print(f"For n_clusters = {k}, The average silhouette_score is : {silhouette_avg}")

        sample_silhouette_values = silhouette_samples(data, cluster_labels)
    
    best_k = K[silhouette_values.index(max(silhouette_values))]
    #plot_silhouette(data, silhouette_values, best_k)
    return best_k
