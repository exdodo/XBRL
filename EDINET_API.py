# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 15:34:50 2019
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

def jsonToList(sdt):    
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents.json'
    params = {'date': sdt, 'type': 2}
    headers = {'User-Agent': 'hoge'}
    res = requests.get(url, params=params, verify=False,timeout=3.5, headers=headers)
    rjson=res.json()    
    result_list=rjson['results']    
    return result_list

def json_docs() :    
    dt_today = date.today()
    dt_5y = dt_today-timedelta(days=3) #約5年前なら5*365　正確さを求めるならrelativedelta
    doc_list=[]
    while(dt_5y <= dt_today):
        sdt=dt_5y.strftime('%Y-%m-%d')
        if  jsonToList(sdt)!=[] :
            doc_list.append(jsonToList(sdt))
        dt_5y=dt_5y+timedelta(days=1)
    docs=list(chain.from_iterable(doc_list))  #flatten  
    df_docs=pd.io.json.json_normalize(docs) #To Dataframe
    #df_docs.to_excel('xbrldocs.xls',encoding='cp932')
    #df_docs.to_hdf('xbrldocs.h5',df_docs) 未完成
    df_docs.to_json('xbrldocs.json')
    
if __name__=='__main__':
    #過去ｎ日文の書類リストをまとめる
    json_docs()    
    
    #docIDを取得する
    df_read = pd.read_json('xbrldocs.json')
    docIDs=df_read['docID'].to_list()
    
    #書類取得
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docIDs[0]
    params = { 'type': 1}
    headers = {'User-Agent': 'hoge'}
    res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)
    contentType = res.headers['Content-Type']
    contentDisposition = res.headers['Content-Disposition']
    ATTRIBUTE = 'filename='
    fileName = contentDisposition[contentDisposition.find(ATTRIBUTE) + len(ATTRIBUTE):]
    print(fileName)
