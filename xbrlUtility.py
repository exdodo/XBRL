import pickle
import unicodedata
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
def docIDsFromFreeword(df_docs,seek_words=['トヨタ自動車'],
        seek_columns=['currentReportReason',  'docDescription', 'docID',
         'docInfoEditStatus', 'docTypeCode', 'edinetCode', 'filerName', 'formCode', 'fundCode',
       'issuerEdinetCode',  'ordinanceCode', 'parentDocID',
        'secCode', 'seqNumber','subjectEdinetCode']) :
    '''
    df_docs:検索対象データーフレーム
    seek_words:検索用語
    seek_columns:検索列
    '''
    seek_words=[str(n) for n in seek_words ] #文字列に変換
    seek_words=[ unicodedata.normalize("NFKC", n) 
        if n.isdigit() else n for n in seek_words ] #数字は半角文字列に統一 
    if len(seek_columns)==0 : 
        seek_columns=df_docs.columns
    docIDs=[]    
    for col_name in seek_columns :
        if col_name=='dtDateTime' : continue #object type以外は検索しない
        if col_name=='dtDate' :  continue 
        if df_docs[col_name].dtype!=object :continue   
        for seek_word in seek_words :            
            df_contains = df_docs[df_docs[col_name].str.contains(seek_word,na=False)]
            df_contains = df_contains.sort_values('submitDateTime')
            if len(df_contains['docID'].to_list())>0 : 
                docIDs.append(df_contains['docID'].to_list())
    flat_docs = [item for sublist in docIDs for item in sublist]#flatten
    unique_docs=list(set(flat_docs))    
    return unique_docs
def docIDsFromDirectory(save_path,dir_string='**/XBRL/PublicDoc/*.xbrl'):
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
    cols=['docTypeCode','ordinanceCode','xbrlFlag'] #文字列として設定
    for col_name in cols :
        df_json[col_name]=df_json[col_name].fillna(0)
        df_json[col_name]=df_json[col_name].astype(int)
        df_json[col_name]=df_json[col_name].astype(str)   
    #期間指定
    df_json=df_json[(df_json['dtDateTime'].dt.year >= min(nYears)) 
            & (df_json['dtDateTime'].dt.year <= max(nYears))]
    #docIDだけあり他がｎｕｌｌ（諸般の事情で削除された）が2000近くあるから削除
    docIDs=df_json['docID'][df_json['submitDateTime'].isnull()].to_list()
    df_json=df_json[~df_json['docID'].isin(docIDs)]
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
        sDate=df_docs[df_docs['docID']==docID].submitDateTime.to_list()[0]
        flag=df_docs[df_docs['docID']==docID].xbrlFlag.to_list()[0]
        file_dir=save_path+'\\'+str(int(sDate[0:4]))+'\\'+\
            str(int(sDate[5:7]))+'\\'+str(int(sDate[8:10]))+'\\'\
            +docID+'\\'+docID
        if not Path(file_dir).exists() and flag=='1':  #xdrl fileなければ取得            
            #書類取得
            url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docID
            params = { 'type': 1} #1:zip 2 pdf
            headers = {'User-Agent': 'exdodo@gmail.com'} 
            try :
                res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)
                sleep(1) #1秒間をあける
            except requests.exceptions.Timeout :
                continue
            if 'stream' in res.headers['Content-Type'] :
                with zipfile.ZipFile(BytesIO(res.content)) as existing_zip:
                    #print(file_dir)        
                    existing_zip.extractall(file_dir)                    
            else :
                error_docIDs.append(docID)
                #error_docID[docID]=res.headers
    
    if len(error_docIDs)>0 :
        #print(str(len(error_docIDs))+'件xbrl保存せず。　log.txt')
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
            #print(h5File.keys())
            #print(h5File['index'].keys())
            h5File.create_dataset('index/datelogs', data=np.array(datelogs, dtype='S10'))
            print(h5File['index'].keys())
            h5File.flush() 
            h5File.close()       
    return
def restoreHDFfromJSON(h5XBRL):
    p=Path(h5XBRL)
    json_path=p.parent.resolve()           
    json_file=str(json_path)+'\\xbrlDocs.json'
    #print(h5XBRL)
    #print(json_file)
    df=pd.read_json(json_file)
    df.to_hdf(h5XBRL,'index/edinetdocs',mode='a',format='table',data_columns=True)    

def test_h5xbrl(h5xbrl) :
    
    with h5py.File(h5xbrl, 'a') as h5File:
        if 'index' in h5File.keys():
            print(list(h5File['index'].keys()))
        if 'E12460' in h5File.keys():
            
            print(len(list(h5File['E12460'].keys())))
def docIDsFromHDF2(h5xbrl):
    #/year/edinet_code/docID
    hdf_docIDs=[]    
    if Path(h5xbrl).exists() :
        with h5py.File(h5xbrl, 'r') as h5File:
            key_list1=list(h5File.keys())
            key_list2=[ list(h5File[key].keys()) for key in key_list1 if key!='index']
            key_list2=list(chain.from_iterable(key_list2)) #flatten
            key_list2=list(set(key_list2))
            #print(key_list2)
            key_groups=[]
            for key1 in key_list1 :
                for key2 in key_list2:
                    key_groups.append(key1+'/'+key2)            
            key_groups=[key_group for key_group in key_groups if key_group in h5File ] #group名あるのだけ残す
            key_list3=[ list(h5File[key].keys()) for key in key_groups ]
            key_list3=list(chain.from_iterable(key_list3)) #flatten
            key_list3=list(set(key_list3)) #unique
            hdf_docIDs=[ key[0:8] for key in key_list3] #追番削除
            hdf_docIDs=list(set(hdf_docIDs)) #unique
            return hdf_docIDs
    return hdf_docIDs
if __name__=='__main__':
    #edinet downloadをupdateする
    #週末にに一度ほど実行する
    save_path='e:\\data\\xbrl\\download\\edinet' #edinetからxbrlデータ保存先
    h5xbrl='d:\\data\\hdf\\xbrl.h5' #xbrl 書類一覧Hdf　保存先
    dirDocIDs=docIDsFromDirectory(save_path,'**/XBRL/PublicDoc/*.xbrl')
    df_json=pd.read_hdf(h5xbrl,key='/index/edinetdocs')
    df_docs = column_shape(df_json) #dataframeを推敲
    jsonDocIDs=df_docs['docID']
    #jsonDocIDs=docIDsFromHDF2(h5xbrl)
    dlDocIDs=list(set(jsonDocIDs)-set(dirDocIDs))
    download_xbrl(df_docs,save_path,dlDocIDs)
    print(len(dlDocIDs))
    
    #save_path='d:\\data\\xbrl\\download\\edinet' #自分用
    #h5xbrl='d:\\data\\hdf\\xbrl.h5' #xbrl 書類一覧Hdf　保存先
    #restoreHDFfromDatelog(h5xbrl)
    #restoreHDFfromJSON(h5xbrl)
    #test_h5xbrl(h5xbrl)
    
    #HDF　index 初期化
    #del_datelogs(h5xbrl)
    #del_groupName(h5xbrl)
    #restore datelog
    #restoreHDFfromDatelog(h5xbrl)
    #restoreHDFfromJSON(h5xbrl)
    
    #docIDs 検索
    #df_json=pd.read_hdf(h5xbrl,key='/index/edinetdocs')
    #df_docs = column_shape(df_json) #dataframeを推敲
    
    #seek_words=['ソニー']
    #seek_columns=['filerName']
    #docIDs=docIDsFromFreeword(df_docs,seek_words=['トヨタ自動車'],seek_columns=[])
    #'S100AX75' 
