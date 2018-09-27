# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 09:05:29 2018
https://qiita.com/icchi_h/items/fc0df3abb02b51f81657
#EDINETコード 会社名の辞書を作る
#https://disclosure.edinet-fsa.go.jp/からEDINETコードリスト（EdinetcodeDlInfo.csv）をDL
#EdinetcodeDlInfo.csvを同じフォルダーに置いておく
@author: Yusuke
"""
import warnings
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
import gensim
import pandas as pd
import difflib
import unicodedata

def getNearestValue(edinetnamels, corpname):
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

def print_ediname(simls):
    for sim in simls :
        print(sim[0],edinetdt[sim[0]],sim[1])

def word2vec_simil(model,sample) : #類義語発見
    results = model.wv.most_similar(positive=[sample])
    print(sample)
    for result in results:
        print(result)

if __name__ == "__main__":
    dir_model=r'C:\\Users\\Yusuke.SERVICE\\Documents\\DATABASE\\word2vecmodel\\'
    corpname1=u'トヨタ自動車'
    corpname1=n = unicodedata.normalize('NFKC', corpname1)
    
    df1 = pd.read_csv('EdinetcodeDlInfo.csv',encoding='cp932',header=1,index_col=0)
    edinetdt=df1['提出者名'].to_dict()    
    corpname=getNearestValue(edinetdt.values(), corpname1)
    edinetkeyls = [k for k, v in edinetdt.items() if v == corpname]    
    
    # モデルのロード
    m = gensim.models.Doc2Vec.load(dir_model+'xbrl_doc2vecmodel')
    m2= gensim.models.Doc2Vec.load(dir_model+'xbrl_doc2vecmodeloverview')
    #1.類似度の高い文書 
    print('1.',edinetkeyls[0],corpname) #edinetkeylsに[0]を削除するか？
    simls= (m.docvecs.most_similar(edinetkeyls[0]))
    print('有価証券報告書doc2vec model')
    print_ediname(simls) 
    print('1.',edinetkeyls[0],corpname) #edinetkeylsに[0]を削除するか？
    simls2= (m2.docvecs.most_similar(edinetkeyls[0]))
    print('有価証券報告書doc2vec overview model')
    print_ediname(simls2) 
    '''
    #wikipedia word2doc
    model = gensim.models.word2vec.Word2Vec.load(dir_model+'wiki_model')
    #類義語発見
    print('WIKIPEDIA word2vec model')
    word2vec_simil(model,corpname1)
    '''    