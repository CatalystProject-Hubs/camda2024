import pandas as pd
from gap_statistic import OptimalK
from sklearn.datasets import make_classification
import warnings
warnings.filterwarnings('ignore')

# Recibe base de datos en formato dataframe preprocesade y regresa el número óptimo de clusters calculados

def gap_statistic(data):

    gs_obj = OptimalK(n_jobs=1, n_iter= 50)
    n_clusters = gs_obj(data, n_refs=10, cluster_array=np.arange(1, 20))


    return n_clusters

