import pickle
import zipfile
from datetime import datetime
from io import BytesIO
from itertools import chain
from pathlib import Path
from time import sleep

import h5py
import numpy as np
import pandas as pd
import requests
import urllib3
from lxml import etree as ET
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
def docIDsFromDirectory(save_path,dir_string='**/PublicDoc/*.xbrl'):
    '''
    save_path:xbrl保存基幹directory
    dir_string :書類種別指定するとき　ex年次有報)dir_string='**/PublicDoc/*asr*.xbrl'
    '''
    p_dir = Path(save_path)
    #xbrlファイルのあるディレクトリーのみを抽出 年次有価証券報告書('asr')
    p_winpath=list(p_dir.glob(dir_string)) 
    dir_docIDs=[docID.parents[2].name for docID in p_winpath] #一個上parents[0]
    return dir_docIDs
def column_shape(df_json,nYears=[]) :
    if nYears==[] :
        nYears=[2014,int(datetime.now().year)]
    
    #submitDateTime 日付型へ
    df_json['dtDateTime']=pd.to_datetime(df_json['submitDateTime']) #obj to datetime
    df_json['dtDate']=df_json['dtDateTime'].dt.date #時刻を丸める　normalize round resample date_range
    df_json['secCode'] = df_json['secCode'].fillna(0)
    df_json['secCode'] = df_json['secCode'].astype(int)
    df_json['secCode'] = df_json['secCode']/10
    df_json['secCode'] = df_json['secCode'].map('{:.0f}'.format)
    cols=['docTypeCode','ordinanceCode','xbrlFlag']
    for col_name in cols :
        df_json[col_name]=df_json[col_name].fillna(0)
        df_json[col_name]=df_json[col_name].astype(int)
        df_json[col_name]=df_json[col_name].astype(str)   
    #期間指定
    df_json=df_json[(df_json['dtDateTime'].dt.year >= min(nYears)) 
            & (df_json['dtDateTime'].dt.year <= max(nYears))]
    #docIDだけあり他がｎｕｌｌ（諸般の事情で削除された）が2000近くあるから削除
    df_json=df_json.dropna(subset=['submitDateTime'])
    df_json=df_json.sort_values('submitDateTime')
    df_docs=df_json[df_json['xbrlFlag']=='1'] #xbrl fileだけ扱う         
    return df_docs
def download_xbrl(df_docs,save_path,docIDs):
    print('xbrl downloading...')
    #error_docID={}
    error_docIDs=[]
    #docIDから既にdowloadしたもんか判断
    for docID in tqdm(docIDs) :                 
        #docIDsからdataframe 抽出
        if docID in df_docs['docID'].to_list()  : #削除ドキュメント対策
            sDate=df_docs[df_docs['docID']==docID].submitDateTime.to_list()[0]
            flag=df_docs[df_docs['docID']==docID].xbrlFlag.to_list()[0]
            file_dir=save_path+'\\'+str(int(sDate[0:4]))+'\\'+\
                str(int(sDate[5:7]))+'\\'+str(int(sDate[8:10]))+'\\'\
                +docID+'\\'+docID
            if not Path(file_dir).exists() and flag=='1':  #xdrl fileなければ取得            
                #書類取得
                url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docID
                params = { 'type': 1} #1:zip 2 pdf
                headers = {'User-Agent': 'add mail address'}            
                res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)            
                sleep(1)
                if 'stream' in res.headers['Content-Type'] :
                    with zipfile.ZipFile(BytesIO(res.content)) as existing_zip:        
                        existing_zip.extractall(file_dir)                    
                else :
                    error_docIDs.append(docID)
                    #error_docID[docID]=res.headers
    
    if len(error_docIDs)>0 :
        print(str(len(error_docIDs))+'件xbrl保存せず。　log.txt')
        with open('log.txt', mode='w') as f:
            f.writelines(error_docIDs)
            #print(error_docID)

def del_datelogs(h5XBRL) :
#書類一覧 再読み込みのため過去のHDFのindex データ削除
    with h5py.File(h5XBRL, 'a') as h5File:
            #h5File=h5py.File(h5XBRL,'a')
            if 'index' in h5File.keys() :   #上書き処理のため元dataset削除         
                    #del h5File['index/datelogs']               
                    #del h5File['index/edinetdocs']
                    del h5File['index']
            h5File.flush()
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
def restoreHDFfromJSON(h5XBRL):
    p=Path(h5XBRL)
    json_path=p.parent.resolve()           
    json_file=str(json_path)+'\\xbrlDocs.json'
    print(h5XBRL)
    print(json_file)
    df=pd.read_json(json_file)
    df.to_hdf(h5XBRL,'index/edinetdocs',mode='w',format='table',data_columns=True)    
def findHDF(docID,df_docs,h5xbrl):
    sr_docs=df_docs.set_index('docID')['edinetCode']
    edinet_code=sr_docs[docID]
    print(edinet_code+'/'+docID)
    with h5py.File(h5xbrl,'r') as h5File:
        if 'E02224' in h5File.keys() :
            docID_keys=list(h5File['E02224'].keys())
            print(docID_keys)
            if 'S100G85N_000' in docID_keys:
                print('bingo') 

if __name__=='__main__':
    #save_path='d:\\data\\xbrl\\download\\edinet' #自分用
    h5xbrl='d:\\data\\hdf\\xbrl.h5' #xbrl 書類一覧Hdf　保存先
    df_json=pd.read_hdf(h5xbrl,key='/index/edinetdocs')
    df_docs = column_shape(df_json) #dataframeを推敲
     
    #  
    docIDs=['S100G85N','S100G21U',]    
    print(docIDs)
    #----
    findHDF(docIDs[0],df_docs,h5xbrl)