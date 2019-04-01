 -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 15:34:50 2019
q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ
edinet apiを使い過去の書類一覧のjson化
・どのくらいの頻度のrequestsを送って良いか不明
・3日で1M　5年だと600M位のサイズになるのかな
と言うわけで実行はしていない
書類一覧が日付指定でしか取れないのは改善して欲しいな
参考URL
http://d.hatena.ne.jp/xef/20121027/p2
https://note.nkmk.me/python-pandas-json-normalize/
https://note.mu/tom_programming/n/n45131a205717
http://73spica.tech/blog/requests-insecurerequestwarning-disable/
@author: Yusuke
"""

import pandas as pd
from datetime import date,timedelta
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
from itertools import chain
from time import sleep
import os
import pickle
from tqdm import tqdm

def jsonToList(sdt):    
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents.json'
    params = {'date': sdt, 'type': 2}
    headers = {'User-Agent': 'hoge'}
    res = requests.get(url, params=params, verify=False,timeout=3.5, headers=headers)
    sleep(1) #1秒間をあける
    rjson=res.json()
    return rjson
def datelog():
    #読込日を覚えておく
    datelogs=[] 
    if os.path.exists('datelog.pkl') :
        with open('datelog.pkl','rb') as log:
            datelogs=pickle.load(log)            
            datelogs.sort()        
    if(len(datelogs)>(1826)) : #過去5年を超える日付は削除
        del datelogs[0:len(datelogs)-1826]                
    return datelogs

def json_docs(dt_5y,dt_today,df_doc1) :    
    doc_list=[]
    datelogs=datelog()    
    pbar = tqdm(total=(dt_today-dt_5y).days)
    while(dt_5y < dt_today):
        sdt=dt_5y.strftime('%Y-%m-%d')        
        if sdt not in datelogs : #過去に読み込んだ日付は読み込まない
            datelogs.append(sdt)
            rjson=jsonToList(sdt)
            if rjson['metadata']['status']=='200':
                if  rjson['results'] !=[] : 
                    doc_list.append(rjson['results'])                
        pbar.update(1)
        dt_5y=dt_5y+timedelta(days=1)
    pbar.close()    
    with open('datelog.pkl','wb') as log_file:
        datelogs.sort()
        pickle.dump(datelogs, log_file)    
    docs=list(chain.from_iterable(doc_list))  #flatten    
    df_docs=pd.io.json.json_normalize(docs) #To Dataframe
    df_concat1=pd.concat([df_doc1, df_docs])
    df_concat1.to_json('xbrldocs.json')
    
    
def get_xbrl(docID) :
    #書類取得
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docID
    params = { 'type': 2}
    headers = {'User-Agent': 'hoge'}
    res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)
    sleep(1) #1秒間をあける
    contentType = res.headers['Content-Type']
    contentDisposition = res.headers['Content-Disposition']
    ATTRIBUTE = 'filename='
    fileName = contentDisposition[contentDisposition.find(ATTRIBUTE) + len(ATTRIBUTE):]
    print(contentType,fileName)
    
if __name__=='__main__':
    last_day=date.today()
    start_day=last_day-timedelta(days=365) #約5年前なら5*365　正確さを求めるならrelativedelta
    if last_day < start_day : start_day=last_day    
    if os.path.exists('xbrlmetas.json') :  df_doc1=pd.read_json('xbrldocs.json')
    else : df_doc1 = pd.DataFrame(index=[])
    #過去ｎ日文の書類リストをまとめる
    json_docs(start_day,last_day,df_doc1)    
'''    
    #docIDを取得する
    df_read = pd.read_json('xbrldocs.json')
    docIDs=df_read['docID'].to_list()
    
    #書類取得
    get_xbrl(docIDs[0])
'''    
    
