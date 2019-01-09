# -*- coding: utf-8 -*-
"""
Created on Thu Jan  3 08:33:15 2019
DOC2VECとfastetextのモデルを作成する
FastTextで解析しSparse Document Vecotrsを加え図に表す
処理に時間がかかるのでパソコンを新調してしまった
win10+Corei7-8700k+32GBメモリー+M.2 SSD
fastetextのモデルでworkers=10に設定

参考URL
word2vec fasttext
https://srbrnote.work/archives/1315
https://qiita.com/akira_/items/f9bb46cad6834da32367
http://db-event.jpn.org/deim2017/papers/100.pdf
https://deepage.net/machine_learning/2017/01/08/doc2vec.html
https://srbrnote.work/archives/1315
https://www.fsa.go.jp/search/20180228.html
http://naron-a.hatenablog.com/entry/2018/01/04/141647

svcm
http://touch-sp.hatenablog.com/entry/2018/02/14/005029
https://qiita.com/fufufukakaka/items/a7316273908a7c400868
https://www.pytry3g.com/entry/TSNE-example
@author: Yusuke
"""

from lxml import objectify as ET
import os
import re
import gensim
from gensim.models import FastText
import subprocess
from pathlib import Path
import shutil
from tqdm import tqdm
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.mixture import GaussianMixture
import numpy as np
import pickle
#from MulticoreTSNE import MulticoreTSNE as TSNE
from sklearn.manifold import TSNE
import pandas as pd

def parse_file(file_path):
    if  os.path.isfile(file_path) :
        with open(file_path,'rt', encoding='utf-8') as fxbrl:
            parse_xbrl(fxbrl,file_path)
            
def parse_xbrl(fxbrl,file_path):   
    """
    XBRLからtextを取り出す
    """    
    ET_xbrl=ET.parse(fxbrl)
    root=ET_xbrl.getroot()
    #nslist=['jpcrp_cor:*','jpdei_cor:*','jppfs_cor:*']
    nslist=[ i+':*' for i in root.nsmap ]
    xbrldoc=''
    for ele in nslist :
        anchors=root.findall(ele, root.nsmap)
        for anchor in anchors:
            #ele=ele.replace(':*','_')
            #element_id=ele.replace('//','')+anchor.tag.split('}')[1]
            if anchor.attrib.get('contextRef')=='FilingDateInstant' :
            #if 'Overview' in element_id : #概要分だけ
                anc=delete_tag(anchor.text) #tagを正規表現で外す
                if anc is not None : 
                    xbrldoc=xbrldoc+anc
    #空行を削除
    xbrldoc='\n'.join(filter(lambda x: x.strip(), xbrldoc.split('\n')))
    #数字記号削除
    xbrldoc=re.sub(r'[0-9０-９,，.．]+','',xbrldoc)
    #空白が2個以上－＞1個の空白
    xbrldoc=re.sub(r's/\s+/',' ',xbrldoc)
    #記号削除
    xbrldoc=re.sub(r'[!-/:-@[-`{-~]', '',xbrldoc)
    xbrldoc=re.sub(u'[︰-＠]', '',xbrldoc)

    #save \doc 
    base_name=os.path.basename(file_path)
    base_name=base_name.replace('.xbrl','.txt')
    os.makedirs('doc', exist_ok=True)
    f = open(os.path.join('doc', base_name), 'w',encoding='utf-8') # 書き込みモードで開く
    f.write(xbrldoc) # 引数の文字列をファイルに書き込む
    f.close() # ファイルを閉じる
           
    #分かち書き処理 save /wakati
    os.makedirs('wakati', exist_ok=True)
    #print(base_name)
    wakati_in=os.path.join('doc', base_name)
    wakati_out=os.path.join('wakati', base_name)
    #wakatiを一つのコーパスにまとめる
    subwakati = 'mecab -O wakati '+wakati_in+' -o '+wakati_out+' -b 10000000'
    subprocess.check_call(subwakati,shell=True)
            
def delete_tag(anchor_text) : #tagを正規表現で外す
    text=str(anchor_text)
    if '<' in text :    
        p = re.compile(r"<[^>]*?>")
        return p.sub("", text)
    return anchor_text

def wakati(dbpath) :
    print ('Creating 分かち書き処理 ')
    file_paths=Path(dbpath).glob('**/*.xbrl')
    for file_path in tqdm(file_paths):
        dirpath, file_name = os.path.split(file_path)
        s=os.path.basename(file_path)
        edinetcode=s[20:26] #EDINET Code
        edinetcheck=s[42:44] #報告書提出回数
        edinetasr=s[12:15] #年次有価証券報告書
        #print(edinetcheck)
        if 'AuditDoc' not in dirpath and \
            'E' in edinetcode and \
            '1' in edinetcheck and \
            'asr' in edinetasr  :  
            parse_file(file_path)

def doc2vec_model(model_name,dir_model) :
    print('creating DOC2VEC model' )
    trainings = []   
    cwd=os.getcwd()
    for file_path in tqdm(Path(cwd+'\\wakati').glob('**/*.txt')):        
        with open(file_path, 'r',encoding='utf-8') as wafile:
            text = (wafile.read()).replace('\n','')
            text = text.replace('\r','')
            text = text.split(' ')
            tag_name = os.path.basename(file_path)
            tag_name= tag_name[20:26]
            #print(tag_name)
            tag=[tag_name]
            sentence=gensim.models.doc2vec.TaggedDocument(words=text, tags=tag)
            #sentence = [gensim.models.doc2vec.TaggedDocument(words = data,tags = [i]) for i,data in enumerate(textls)] 
            #sentence=[gensim.models.doc2vec.TaggedDocument(doc, [i]) for i, doc in enumerate(wafile)]
            trainings.append(sentence)            
            
    modelD2V = gensim.models.Doc2Vec(documents=trainings, vector_size=100,
                            alpha=0.0025,
                            dm=1, #dm=0, dbow_words=1,
                            min_alpha=0.000001,
                            window=5, min_count=1)
    modelD2V.save(dir_model+model_name)
    
def fasttext_model(model_name,dir_model) :
    print('Creating fasttext model') 
    trainings = []   
    cwd=os.getcwd()
    file_pathes=Path(cwd+'\\wakati').glob('**/*.txt')
    for file_path in file_pathes:        
        with open(file_path, 'r',encoding='utf-8') as wafile:
            #wakati text整形
            text = (wafile.read()).replace('\n','')
            text = re.sub(r"\text+", " ", text)
            #label付与
            tag_name = os.path.basename(file_path)
            tag_name= '__label__'+tag_name[20:26]+'　,　'
            text=tag_name+text
            text=text.replace('\u3000','')
            text=text.replace('\xa0','')
            textls=text.split(' ')
            textls2 = [x for x in textls if x ] #空item削除            
            trainings.append(textls2)            
    #print(list(trainings[0]))           
    print('fasttext modeling')
    model_ft = FastText( trainings,  size=300, window=15, 
                        min_count=5, iter=10, workers=10, sg=1)
    model_ft.save(dir_model + model_name)

def tf_idf():
    #TF-IDFを計算（必要なのはIDFのみ）
    print('Creating IDF vector')
    traindata=[]
    cwd=os.getcwd()
    file_pathes=Path(cwd+'\\wakati').glob('**/*.txt')
    for file_path in tqdm(file_pathes):        
        with open(file_path, 'r',encoding='utf-8') as wafile:
            #wakati text整形
            text = (wafile.read()).replace('\n','')
            text = re.sub(r"\text+", " ", text)
            #label付与
            tag_name = os.path.basename(file_path)
            tag_name= '__label__'+tag_name[20:26]+'　,　'
            text=tag_name+text
            text=text.replace('\u3000','')
            text=text.replace('\xa0','')
            text=text+','
            #textls=text.split(' ')
            #textls2 = [x for x in textls if x ] #空item削除            
            traindata.append(text) 
    
    np.set_printoptions(precision=2) 
    docs = np.array(traindata)
    tfv = TfidfVectorizer(dtype=np.float32, lowercase=False)
    #https://qiita.com/m__k/items/709a9cae184769e2243f
    tfidfmatrix_traindata = tfv.fit_transform(docs)
    feature_names = tfv.get_feature_names()
    idf = tfv._tfidf.idf_
    word_idf_dict = dict(zip(feature_names, idf))
    pickle.dump(word_idf_dict, open("word_idf_dict.pkl", "wb"))
    
def word_clustering(model_name,dir_model):
    #単語ベクトルクラスタリング
    '''
    「idx」はそれぞれの単語がどのクラスタに属するかを表した配列
    「idx_proba」はそれぞれの単語が各クラスタに属する確率
    '''
    print ("Creating Cluster ---")
    model = FastText.load(dir_model+model_name)
    word_vectors = model.wv.vectors
    clf =  GaussianMixture(n_components=60,
                       covariance_type="tied", init_params='kmeans', max_iter=50)
    clf.fit(word_vectors)
    idx_proba = clf.predict_proba(word_vectors)
    idx_proba_dict = dict(zip( model.wv.index2word, idx_proba ))
    pickle.dump(idx_proba_dict, open("idx_proba_dict.pkl", "wb"))
    
def prob_vector(model_name,dir_model):
    print('Creating fasttext+idf+cluster vector model')
    model = FastText.load(dir_model+model_name)
    idx_proba_dict = pickle.load(open("idx_proba_dict.pkl","rb"))
    word_idf_dict = pickle.load(open("word_idf_dict.pkl","rb"))
    prob_wordvecs = {}
    for word in idx_proba_dict:
        prob_wordvecs[word] = np.zeros( 60 * 300, dtype="float32" )
        for index in range(0, 60): #60=cluster number
            try:
                prob_wordvecs[word][index*300:(index+1)*300] = \
                model[word] * idx_proba_dict[word][index] * word_idf_dict[word]
            except:
                continue            
    pickle.dump(prob_wordvecs,open("prob_wordvecs.pkl","wb"))

def document_vector():
    print('Sparse Document Vecotrs')
    #文章ベクトルを求める documentに出てくる単語vecを足し合わせる
    cwd=os.getcwd()
    file_pathes=Path(cwd+'\\wakati').glob('**/*.txt')
    #file count
    f_count=0
    files=os.listdir(cwd+'\\wakati')
    for file in files :
        index=re.search('.txt',file)
        if index :
            f_count += 1
    #        
    gwbowv = np.zeros((f_count, 300*60)).astype(np.float32) #ゼロ行列作成（単語×次元）        
    cnt=0
    corp_dic={}
    word_centroid_map = pickle.load(open("word_idf_dict.pkl","rb"))
    prob_wordvecs = pickle.load(open("idx_proba_dict.pkl","rb"))    
    for file_path in file_pathes :        
        with open(file_path, 'r',encoding='utf-8') as wafile:
            #wakati text整形
            text = (wafile.read()).replace('\n','')
            text = re.sub(r"\text+", " ", text)
            #label付与
            tag_name = os.path.basename(file_path)
            corp_name=tag_name[20:26]
            tag_name= '__label__'+tag_name[20:26]+'　,　'            
            text=tag_name+text
            text=text.replace('\u3000','')
            text=text.replace('\xa0','')
            text=text+','
            gwbowv[cnt]=create_cluster_vector_and_gwbowv(text,
                  word_centroid_map,prob_wordvecs)
            corp_dic[cnt]=corp_name
            cnt +=1
    return gwbowv,corp_dic
    
def create_cluster_vector_and_gwbowv(tokens,word_centroid_map,prob_wordvecs):
    
    # This function computes SDV feature vectors.
    bag_of_centroids = np.zeros(300*60, dtype="float32")
    for token in tokens:#文章の単語=token
        try:
            temp = word_centroid_map[token]
        except:
            continue
        bag_of_centroids += prob_wordvecs[token]
    norm = np.sqrt(np.einsum('...i,...i', bag_of_centroids, bag_of_centroids))
    if norm != 0:
        bag_of_centroids /= norm    
    return bag_of_centroids

def tsne_plot(gwbow,corp_dic) :
    corp_sr=pd.Series(corp_dic)    
    #300*60次元あるベクトルをt-sneで2次元へ
    tsne_model = TSNE( n_components=2, random_state=0, verbose=2)
    np.set_printoptions(suppress=True) #指数表記を禁止にして常に小数で表示
    tsne_model.fit(gwbow)
    # 散布図の表示    
    skip=0
    limit=4100 
    plain_tsne = pd.DataFrame(tsne_model.embedding_[skip:limit, 0],columns = ["x"])
    plain_tsne["y"] = pd.DataFrame(tsne_model.embedding_[skip:limit, 1])    
    plain_tsne['corp_name']=corp_sr
    #df_edinetcode = pd.read_csv('EdinetcodeDlInfo.csv',encoding='cp932',header=1,index_col=0)    
    #df_merge=pd.merge(plain_tsne,df_edinetcode,left_on='corp_name',right_on='ＥＤＩＮＥＴコード')
    #df_tsne=df_merge[['x','y','提出者名']].copy()    
    df_tsne=plain_tsne
    ax=df_tsne.plot.scatter(x="x",y="y",figsize=(10, 10),s=30)
    #各要素にラベルを表示
    for k,v in df_tsne.iterrows() :
        ax.annotate(v[2],xy=(v[0],v[1]),size=15) 
        
if __name__=='__main__':
    dbpath=r'd:\data\xbrl\download\EDINET\2018'
    model_name_w2v='xbrl_w2v_model_2018'
    model_name_ft='xbrl_ft_model_2018'
    dir_model=r'd:\\data\\word2vecmodel\\'    
    #xbrl 前処理
    wakati(dbpath)
    #DOC2VEC FASTTEXT idfモデル化
    doc2vec_model(model_name_w2v,dir_model)
    fasttext_model(model_name_ft,dir_model) #Word Vector(fasttext) Clustering 
    word_clustering(model_name_ft,dir_model) #全単語の分類分けをし所属する確率を求める
    tf_idf() #単語のIDF値を計算
    #SDV(Sparse Document Vecotrs) 作成
    prob_vector(model_name_ft,dir_model) #Document Topic-Vector Formation
    gwbow,corp_dic=document_vector() #Sparse Document Vecotrs
    tsne_plot(gwbow,corp_dic) #t-sne plot    
    #後処理　フォルダー(\wakati \doc)削除    
    shutil.rmtree('wakati')
    shutil.rmtree('doc')
