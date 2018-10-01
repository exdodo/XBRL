# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 14:56:03 2018
https://www.kaggle.com/sgunjan05/document-clustering-using-doc2vec-word2vec
https://qiita.com/Ikeda_yu/items/94247d819e6a0808d0b7
@author: Yusuke
"""

import warnings
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
import gensim
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from collections import defaultdict

if __name__ == "__main__":
    dir_model=r'C:\\Users\\Yusuke.SERVICE\\Documents\\DATABASE\\word2vecmodel\\'
    model_name='xbrl_doc2vecmodel'
    df1 = pd.read_csv('EdinetcodeDlInfo.csv',encoding='cp932',header=1,index_col=0)
    cluster_num=100 #分類数
    edinetdic=df1['提出者名'].to_dict()    
    # モデルのロード
    d2v_model = gensim.models.Doc2Vec.load(dir_model+model_name)
    #m2= gensim.models.Doc2Vec.load(dir_model+'xbrl_doc2vecmodeloverview')
    #kMeans
    kmeans_model = KMeans(n_clusters=cluster_num, init='k-means++', max_iter=100)  
    X = kmeans_model.fit(d2v_model.docvecs.vectors_docs)
    labels=kmeans_model.labels_.tolist()
    #ドキュメントタグのリスト
    edinet_tags=d2v_model.docvecs.doctags
    #edinet code To company name
    #edinetdic_swap={v:k for k,v in edinetdic.items()}
    doc_tags=[edinetdic.get(x,'no_name') for x in edinet_tags]
    #ラベルとドキュメント番号の辞書づくり    
    cluster_to_docs = defaultdict(list)
    for cluster_id, doc_tag in zip(labels, doc_tags):
        cluster_to_docs[cluster_id].append(doc_tag)
    #クラスター出力
    for docs in cluster_to_docs.values():
        print(docs)
    #クラスターの名前をwikiのword2vecを使い類推    
    l = kmeans_model.fit_predict(d2v_model.docvecs.vectors_docs)
    pca = PCA(n_components=2).fit(d2v_model.docvecs.vectors_docs)
    datapoint = pca.transform(d2v_model.docvecs.vectors_docs)
    plt.figure
    colorlist = ["r", "g", "b", "c", "m", "y", "k", "w"]
    color = [colorlist[7//(i+1)] for i in labels]   
    plt.scatter(datapoint[:, 0], datapoint[:, 1], c=color)
    centroids = kmeans_model.cluster_centers_
    centroidpoint = pca.transform(centroids)
    plt.scatter(centroidpoint[:, 0], centroidpoint[:, 1], marker='^', s=150, c='#000000')
    plt.show()
    