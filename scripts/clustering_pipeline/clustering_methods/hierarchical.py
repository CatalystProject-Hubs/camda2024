import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.cluster import AgglomerativeClustering

# Toma Dataset preprocesado 

def clustering(data, numClusters):
  
  klabels = []

  for n in numClusters:

    Z1 = AgglomerativeClustering(n_clusters=n, linkage='ward')

    Z1.fit_predict(data)

    klabels.append(Z1.labels_)

  return (klabels)