# -*- coding: utf-8 -*-
"""
Created on Wed Jan  9 14:37:37 2019

@author: Yusuke
"""
import pandas as pd
import difflib
import unicodedata
import warnings
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
import gensim
#import matplotlib.pyplot as plt
#import seaborn as sns

#from collections import defaultdict

def getNearestValue(edinetnamels, corpname='ソニー株式会社'):
    """
    概要: リストedinetnamelsからある文字corpnameに最も近いものを返す
    """
    max_num=0
    max_name='ソニー株式会社'#defalut値
    for edinetname in edinetnamels :
        s = difflib.SequenceMatcher(None, 
                unicodedata.normalize('NFKC',edinetname), corpname).ratio()
        if max_num<s :
            max_num=s
            max_name=edinetname
    return max_name

def scdv(company,df_scdv) :
    print('scdv model--fasttext')    
    df_scdv['comp_x']=df_scdv[df_scdv['提出者名']==company]['x'].tolist()[0]
    df_scdv['comp_y']=df_scdv[df_scdv['提出者名']==company]['y'].tolist()[0]
    df_scdv['r2']=(df_scdv['x']-df_scdv['comp_x'])**2+(df_scdv['y']-df_scdv['comp_y'])**2
    df_scdv = df_scdv.sort_values('r2', ascending=True)
    #df_scdv['com_x']=df_scdv['x']-comp_x
    print(df_scdv[['提出者名','r2']].head(11))
    df_plot=df_scdv[['x','y','提出者名','r2']].head(11)
    #print(df_scdv.tail(10))
    # 散布図を描画    
    ax = df_plot.plot.scatter(x='x',y='y',title=company)    
    # 各要素にラベルを表示
    for k, v in df_plot.iterrows():        
        if k==df_plot.index[0]:
            ax.annotate(v[2], xy=(v[0],v[1]), size=15,fontsize=18,color='red')
        else :
            ax.annotate(v[2], xy=(v[0],v[1]), size=15,fontsize=12,color='blue')
def doc2vec(company) :
    df1 = pd.read_csv('EdinetcodeDlInfo.csv',
                      encoding='cp932',header=1,index_col=0)
    edinetdt=df1['提出者名'].to_dict()     
    edinetkeyls = [k for k, v in edinetdt.items() if v == company] 
    dir_model=u'd:\\data\\word2vecmodel\\'
    model_name='xbrl_doc2vec_2018model'
    print('doc2vec model')
    # モデルのロード
    d2v_model = gensim.models.Doc2Vec.load(dir_model+model_name)
    simls= (d2v_model.docvecs.most_similar(edinetkeyls[0]))
    for sim in simls :
        print(edinetdt[sim[0]],sim[1])    

def fasttext(company) :
    #df1 = pd.read_csv('EdinetcodeDlInfo.csv',encoding='cp932',header=1,index_col=0)
    #edinetdt=df1['提出者名'].to_dict()     
    #edinetkeyls = [k for k, v in edinetdt.items() if v == company] 
    dir_model=u'd:\\data\\word2vecmodel\\'
    model_name='xbrl_ft_model_2018'
    print("FastText model")
    # モデルのロード
    ft_model = gensim.models.FastText.load(dir_model+model_name)    
    print('対象ワード：　',company)
    simls= (ft_model.wv.most_similar(company))    
    for sim in simls :
        print(sim[0],sim[1])
    
if __name__ == '__main__':
    company='トヨタ自動車'
    df_scdv = pd.read_csv('xbrl2018_scdv.csv',encoding='cp932',index_col=0)
    edinetnamels=df_scdv['提出者名'].values.tolist()
    comp_name=getNearestValue(edinetnamels, company)
    #SCDV model
    scdv(comp_name,df_scdv)
    doc2vec(comp_name)
    fasttext(comp_name)
    
        