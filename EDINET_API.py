"""
Created on Thu Mar 28 15:34:50 2019
q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ
edinet apiを使い過去の書類一覧のHDF化(json fileも作成)
前日までの書類一覧をh5XBRL先へHDF5とjosn形式で保存
書類一覧が日付指定でしか取れないのは改善して欲しいな
参考URL
http://d.hatena.ne.jp/xef/20121027/p2
https://note.nkmk.me/python-pandas-json-normalize/
https://note.mu/tom_programming/n/n45131a205717
http://73spica.tech/blog/requests-insecurerequestwarning-disable/
https://stackoverflow.com/questions/49121365/implementing-retry-for-requests-in-python
HDF関連

@author: Yusuke
"""
import pandas as pd
import numpy as np
import h5py
from datetime import date,timedelta
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
from itertools import chain
from time import sleep
from pathlib import Path
import pickle
from tqdm import tqdm
import json
import pickle

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
def load_datelog(h5XBRL):
    #読込日を覚えておく
    datelogs=[]
    with h5py.File(h5XBRL, 'a') as h5File:    
        if 'index' in h5File.keys() :
            np_datelogs=h5File['index/datelogs'] #.value
            datelogs=(np_datelogs.value).tolist()  #np to list
            #print(datelogs[-5:0])
            datelogs=[ i.decode('utf-8') for i in datelogs]    #バイナリー文字列　to string
            datelogs.sort()
            if(len(datelogs)>(1826)) : #過去5年を超える日付は削除
                del datelogs[0:len(datelogs)-1826]        #5年前以上の日付は削除 
    return datelogs
def json_shaping(df):
    df['JCN']=df['JCN'].astype(np.float64)
    df['attachDocFlag']=df['attachDocFlag'].astype(np.int64)
    df['disclosureStatus']=df['disclosureStatus'].astype(np.int64)
    df['docInfoEditStatus']=df['docInfoEditStatus'].astype(np.int64)
    df['docTypeCode']=df['docTypeCode'].astype(np.float64)
    df['englishDocFlag']=df['englishDocFlag'].astype(np.int64)
    df['ordinanceCode']=df['ordinanceCode'].astype(np.float64)
    df['pdfFlag']=df['pdfFlag'].astype(np.int64)
    df['xbrlFlag']=df['xbrlFlag'].astype(np.int64)
    df['secCode']=df['secCode'].astype(np.float64)
    df['withdrawalStatus']=df['withdrawalStatus'].astype(np.int64)    
    df=df.drop_duplicates(subset='docID')#重複削除    
    df=df.dropna(subset=['submitDateTime'])#docIDだけあり他がｎｕｌｌ（諸般の事情で削除された）書類）が2000近くあるから削除
    df=df.sort_values('submitDateTime')#sort
    df.reset_index(drop=True, inplace=True) #index振り直し 
    #5年目より古いsubmitDateTime削除
    return df
def main_jsons(h5XBRL,last_day=date.today(),start_day=date.today()-timedelta(days=365*5)):
    if last_day < start_day : start_day=last_day
    #過去ｎ日文の書類リストをまとめる
    doc_list=[]    
    datelogs=load_datelog(h5XBRL)  #過去の読込日を呼び出す list形式 
    pbar = tqdm(total=(start_day-last_day).days)
    while( last_day > start_day ):
        sdt=start_day.strftime('%Y-%m-%d')             
        if  not sdt in datelogs : #過去に読み込んだ日付は読み込まない
            rjson,datelogs=request_json(sdt,datelogs)            
            if rjson['metadata']['status']=='200' :
                rjson_list=rjson['results']
                if  rjson_list !=[] :
                    doc_list.append(rjson_list)                                  
        pbar.update(1)
        start_day=start_day+timedelta(days=1)    
    pbar.close()
    datelogs.sort()
    print('UPDATE最終日 :'+datelogs[-1])
    docs_json=list(chain.from_iterable(doc_list))  #flatten      
    #save datelogs & docs_json to HDF
    if len(datelogs)>0 :       
        h5File=h5py.File(h5XBRL,'a')
        if 'index' in h5File.keys() :   #上書き処理のため元dataset削除         
                del h5File['index/datelogs']               
        h5File.create_dataset('index/datelogs', data=np.array(datelogs, dtype='S'))
        h5File.flush() 
        h5File.close()
        #念のためpickle形式でも保存
        p=Path(h5XBRL)
        json_path=p.parent.resolve()           
        json_file=str(json_path)+'\\datelogs.pkl'
        with open(json_file,'wb') as log_file:
            datelogs.sort()
            if(len(datelogs)>(1826)) : #過去5年を超える日付は削除
                del datelogs[0:len(datelogs)-1826]        
            pickle.dump(datelogs, log_file) 
    if len(docs_json)>0:        
        df_doc2=pd.io.json.json_normalize(docs_json) #To Dataframe From Json
        df_doc2=df_doc2.reset_index(drop=True)
        if Path(h5XBRL).exists():
            h5File=h5py.File(h5XBRL,'a')
            if 'edinetdocs' in h5File['index'].keys() : 
                df_doc1=pd.read_hdf(h5XBRL,'index/edinetdocs')
                df_docs=pd.concat([df_doc1,df_doc2])
            else :
                df_docs=df_doc2
            h5File.close() 
        else :
            df_docs=df_doc2
        df_docs=json_shaping(df_docs)
        #print(df_docs['JCN']) 
        df_docs.to_hdf(h5XBRL,'index/edinetdocs', format='table', mode='a',
                   data_columns=True, index=True, encoding='utf-8')
        #念のためjson,形式でも保存
        p=Path(h5XBRL)
        json_path=p.parent.resolve()           
        json_file=str(json_path)+'\\xbrlDocs.json'
        df_docs.to_json(json_file)
    return         
def restoreHDFfromJSON(h5XBRL):
    p=Path(h5XBRL)
    json_path=p.parent.resolve()           
    json_file=str(json_path)+'\\xbrlDocs.json'
    print(h5XBRL)
    print(json_file)
    df=pd.read_json(json_file)
    df.to_hdf(h5XBRL,'index/edinetdocs',mode='w',format='table',data_columns=True)    

def restoreHDFfromDatelog(h5XBRL):    
    p=Path(h5XBRL)
    json_path=p.parent.resolve()    
    datelog_file=str(json_path)+'\\datelog.pkl'
    print(datelog_file)
    if Path(datelog_file).exists() :
        with open(datelog_file,'rb') as log:
            datelogs=pickle.load(log)            
            datelogs.sort() 
            print(datelogs[-5:])        
        with h5py.File(h5XBRL, 'a') as h5File:            
            if 'index' in h5File.keys() :   #上書き処理のため元dataset削除
                if 'datelogs' in h5File['index'].keys() :         
                    del h5File['index/datelogs']               
            
            print(h5File.keys())
            print(h5File['index'].keys())
            h5File.create_dataset('index/datelogs', data=np.array(datelogs, dtype='S10'))
            print(h5File['index'].keys())
            h5File.flush() 
            h5File.close()       
    return
if __name__=='__main__':
    '''
    edinetxbrl.h5 HDF file
    index/datelogs 過去に読み込んだ日付リスト
    index/edinetdocs EDINETから取得した書類一覧前日まで
    '''
    h5XBRL='d:\\data\\hdf\\xbrl.h5'
    main_jsons(h5XBRL) #過去5年分の書類一覧HDF形式で保存
    #restoreHDFfromJSON(h5XBRL)
    #restoreHDFfromDatelog(h5XBRL)
    
    