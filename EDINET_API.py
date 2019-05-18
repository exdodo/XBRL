# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 15:34:50 2019
q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ
edinet apiを使い過去の書類一覧のjson化
・どのくらいの頻度のrequestsを送って良いか不明
・5年分試してみたら約173M 40分conda
前日までの書類一覧を'xbrldocs.json'で保存
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
import json
def request_json(sdt,datelogs):    
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents.json'
    params = {'date': sdt, 'type': 2}
    headers = {'User-Agent': 'mail-address'}
    res = requests.get(url, params=params, verify=False,timeout=3.5, headers=headers)
    sleep(1) #1秒間をあける
    try :
        rjson=res.json()        
        datelogs.append(sdt) #エラーなければ日付追加    
        return rjson,datelogs
    except json.JSONDecodeError as e:
        print(e)
        print('通信回線混雑のためupdateされません。少し立って実行してください')
        rjson={"metadata":{"status": 404}} #仮の(-：：-)P
        return rjson,datelogs
        
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

def json_docs(dt_5y,dt_today) :    
    doc_list=[]
    datelogs=datelog()  #過去の読込日を呼び出す  
    pbar = tqdm(total=(dt_today-dt_5y).days)
    while(dt_5y < dt_today):
        sdt=dt_5y.strftime('%Y-%m-%d')        
        if sdt not in datelogs : #過去に読み込んだ日付は読み込まない            
            rjson,datelogs=request_json(sdt,datelogs)            
            if rjson['metadata']['status']=='200' :
                rjson_list=rjson['results']
                if  rjson_list !=[] :
                    doc_list.append(rjson_list)                
        pbar.update(1)
        dt_5y=dt_5y+timedelta(days=1)    
    pbar.close()
    print('UPDATE最終日 :'+datelogs[-1])          
    docs=list(chain.from_iterable(doc_list))  #flatten    
    df_doc2=pd.io.json.json_normalize(docs) #To Dataframe
    df_doc2=df_doc2.reset_index(drop=True)
    with open('datelog.pkl','wb') as log_file:
        datelogs.sort()
        pickle.dump(datelogs, log_file)    
    return df_doc2
    
def get_xbrl(docID,path='d:\\data') :
    #書類取得
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docID
    params = { 'type': 2}
    headers = {'User-Agent': 'exdodo@gmail.com'}
    res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)
    sleep(1) #1秒間をあける
    contentType = res.headers['Content-Type']
    contentDisposition = res.headers['Content-Disposition']
    ATTRIBUTE = 'filename='
    fileName = contentDisposition[contentDisposition.find(ATTRIBUTE) + len(ATTRIBUTE):]
    print(contentType,fileName)

def concat_df(df_doc2):
    #concat
    if os.path.exists('xbrldocs.json') :  
        df_doc1=pd.read_json('xbrldocs.json')
        df_concat=pd.concat([df_doc1,df_doc2],sort=True)
        df_concat=df_concat.reset_index(drop=True)        
        df_shape=df_shaping(df_concat) #dataframe整形処理
        df_shape.to_json('xbrldocs.json')        
    else :
        df_shape=df_shaping(df_doc2) #dataframe整形処理
        df_shape.to_json('xbrldocs.json')

def df_shaping(df):    
    df=df.drop_duplicates(subset='docID')#重複削除    
    df=df.dropna(subset=['submitDateTime'])#docIDだけあり他がｎｕｌｌ（諸般の事情で削除された）書類）が2000近くあるから削除
    df=df.sort_values('submitDateTime')#sort
    df.reset_index(drop=True, inplace=True) #index振り直し 
    #5年目より古いsubmitDateTime削除
    return df
def main_jsons(last_day=date.today(),start_day=date.today()-timedelta(days=365*5)):
    if last_day < start_day : start_day=last_day    
    df_doc2=json_docs(start_day,last_day) #過去ｎ日文の書類リストをまとめる
    if df_doc2.empty==False:
        concat_df(df_doc2)         

def convert_hdf():
    df = pd.read_json('xbrldocs.json')
    df.to_hdf('xbrldocs.h5','df',mode='w',format='table',data_columns=True)
    
if __name__=='__main__':
    main_jsons() #json形式で保存
          
    '''    
    #docIDを取得する
    df_read = pd.read_json('xbrldocs.json')
    docIDs=df_read['docID'].to_list()
    
    #書類取得
    get_xbrl(docIDs[0])
    '''    
    