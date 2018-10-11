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

def word2vec_most_sim(model, posls):
    # similarityチェック
    return model.wv.most_similar(positive=posls, topn=1)

def wv_list(doc,model):
    docls=[]
    for doc_item in doc :
        try :
            vector = model.wv[doc_item]
            docls.append(doc_item)
        except KeyError :
            continue
    return docls

if __name__ == "__main__":
    dir_model=r'C:\\Users\\Yusuke.SERVICE\\Documents\\DATABASE\\word2vecmodel\\'
    model_name='xbrl_doc2vecmodel'
    df1 = pd.read_csv('EdinetcodeDlInfo.csv',encoding='cp932',header=1,index_col=0)
    cluster_num=10 #分類数
    print('分類数',cluster_num)
    print('分類するモデル',model_name)
    edinetdic=df1['提出者名'].to_dict()
    #\u3000削除
    edinetdic={x:v.replace('\u3000','') for x,v in edinetdic.items()}
    # モデルのロード
    d2v_model = gensim.models.Doc2Vec.load(dir_model+model_name)
    #wiki_model = gensim.models.word2vec.Word2Vec.load(dir_model + 'wiki_model')
    category_model=d2v_model #wiki_model
    print('分類の名前付けに使うモデル',category_model)
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
        #各クラスターのタグ名をwikiより指定
        #doc要素から’株式会社’削除
        doc=[i.replace('株式会社','') for i in docs ]
        docls=wv_list(doc,category_model) #wiki_model or d2v_model
        if(docls==[]):
            print('分類名','該当なし')
        else :
            print('分類名',word2vec_most_sim(category_model,docls)) #wiki_model or d2v_mode
        #print(docs) #分類の個別企業を見る場合
    #分布をグラフ化    
    l = kmeans_model.fit_predict(d2v_model.docvecs.vectors_docs)
    pca = PCA(n_components=2).fit(d2v_model.docvecs.vectors_docs)
    datapoint = pca.transform(d2v_model.docvecs.vectors_docs)
    plt.figure
    colorlist = ["r", "g", "b", "c", "m", "y", "k", "w"]
    color = [colorlist[i%7] for i in labels]   
    plt.scatter(datapoint[:, 0], datapoint[:, 1], c=color)
    centroids = kmeans_model.cluster_centers_
    centroidpoint = pca.transform(centroids)
    plt.scatter(centroidpoint[:, 0], centroidpoint[:, 1], marker='^', s=150, c='#000000')
    plt.show()
    
