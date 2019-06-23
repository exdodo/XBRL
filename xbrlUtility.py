import requests
import zipfile
import pandas as pd
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from time import sleep
from io import BytesIO
from itertools import chain

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
    df_json=df_json[df_json['xbrlFlag']=='1'] #xbrl fileだけ扱う         
    return df_json
def download_xbrl(df_json,save_path,docIDs):
    print('xbrl downloading...')
    #error_docID={}
    error_docIDs=[]
    #docIDから既にdowloadしたもんか判断
    for docID in tqdm(docIDs) :                 
        #docIDsからdataframe 抽出
        if docID in df_json['docID'].to_list()  : #削除ドキュメント対策
            sDate=df_json[df_json['docID']==docID].submitDateTime.to_list()[0]
            flag=df_json[df_json['docID']==docID].xbrlFlag.to_list()[0]
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
if __name__=='__main__':
    save_path='d:\\data\\xbrl\\download\\edinet' #自分用
    h5xbrl='d:\\data\\hdf\\xbrl.h5' #xbrl 書類一覧Hdf　保存先
    df_json=pd.read_hdf(h5xbrl,key='/index/edinetdocs')
    df_json = column_shape(df_json) #dataframeを推敲