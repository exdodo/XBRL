# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 08:06:22 2019
参考url
https://note.mu/tom_programming/n/n45131a205717
http://73spica.tech/blog/requests-insecurerequestwarning-disable/
@author: Yusuke
"""
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
import json

def save_json():    
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents.json'
    params = {'date': '2018-07-30', 'type': 2}
    res = requests.get(url, params=params, verify=False) 
    rjson=res.json()
    file_name = 'sample.json'
    if res.status_code == 200:
        with open(file_name, 'w',encoding='utf-8') as sfile:            
            json.dump(rjson,sfile)
def load_json():
    docIDs=[]
    with open('sample.json','rb') as json_file:        
        jsonObj = json.load(json_file)        
        for jsn_val in jsonObj['results']:
            docIDs.append(jsn_val['docID'])
    return docIDs


if __name__=='__main__':    
    #save_json()
    #docIDs=load_json()
    #--------------------------------------------------------------
    #書類一覧から書類番号リスト取得
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents.json'
    params = {'date': '2018-07-30', 'type': 2}
    res = requests.get(url, params=params, verify=False) 
    jsonObj=res.json()
    docIDs=[]       
    for jsn_val in jsonObj['results']:
            docIDs.append(jsn_val['docID'])
    print(docIDs)    
    
    #書類取得
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docIDs[0]
    params = { 'type': 1}
    res = requests.get(url, params=params,verify=False)
    contentType = res.headers['Content-Type']
    contentDisposition = res.headers['Content-Disposition']
    ATTRIBUTE = 'filename='
    fileName = contentDisposition[contentDisposition.find(ATTRIBUTE) + len(ATTRIBUTE):]
    print(fileName)
    