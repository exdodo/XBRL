import datetime as dt
import zipfile
from io import BytesIO
from itertools import chain
from pathlib import Path
from time import sleep

import h5py
import numpy as np
import pandas as pd
import requests
import urllib3
from tqdm import tqdm

from EDINET_API import main_jsons
from EdinetXbrlParser import zipParser
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
def shapingJson(h5xbrl,nYears=[]) :
    df_json=pd.read_hdf(h5xbrl,key='/index/edinetdocs')
    if nYears==[] :
        nYears=[2014,int((dt.date.today()).year)]
    
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
    #重複削除
    df_json=df_json.drop_duplicates()         
    return df_json
def docIDsFromHDF(h5xbrl):
    '''#旧バージョン　/edinet_code/docID 用途
    if Path(h5xbrl).exists() :
        with h5py.File(h5xbrl, 'r') as h5File:
            key_list1=list(h5File.keys())
            key_list2=[ list(h5File[key].keys()) for key in key_list1 if key!='index']
            key_list2=list(chain.from_iterable(key_list2)) #flatten
            hdf_docIDs=[ key[0:8] for key in key_list2] #追番削除
            hdf_docIDs=list(set(hdf_docIDs)) #unique
            return hdf_docIDs
    ''' 
    '''#旧バージョン /year/month/date/docID
    hdf_docIDs=[]    
    with h5py.File(h5xbrl, 'r') as h5File:
        h5File.visit(hdf_docIDs.append) #『Python and HDF5:Unlocking Scientific Data』この本がなければ分からなかった><
        #hdf_docIDs=[s for s in hdf_docIDs if s[0:4].isdigit()]
        hdf_docIDs=[s for s in hdf_docIDs if s.count('/')>3] #3階層以上あるもの        
        hdf_docIDs=[s.split('/')[3] for s in hdf_docIDs ] #docID+oiban 抜き出し
        hdf_docIDs=list(set(hdf_docIDs)) #unique
        hdf_docIDs=[s for s in hdf_docIDs if len(s)==12]
        hdf_docIDs=[ key[0:8] for key in hdf_docIDs] #追番削除
        hdf_docIDs=list(set(hdf_docIDs)) #unique       
    return hdf_docIDs
    '''
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
def docIDsFromHDF2(h5xbrl):
    hdf_docIDs=[]

    return hdf_docIDs      
def createGroupName(sDate,docID,edinet_code): #HDF File group name決定
    #group_name='y'+str(int(sDate[0:4]))+'/'+'m'+str(int(sDate[5:7]))+'/'+'d'+str(int(sDate[8:10]))
    group_name=edinet_code+'/y'+str(int(sDate[0:4]))
    return group_name

def directHdfFromZIP(df_docs,docIDs,h5xbrl):
    print('zip to HDF...')
    hdf_docIDs=docIDsFromHDF(h5xbrl)
    docIDs=list(set(docIDs)-set(hdf_docIDs))
    sr_docs=df_docs.set_index('docID')['edinetCode'] #dataframe to Series print(sr_docs['S100FSTI'])    
    if len(docIDs)==0 :
        return
    for docID in tqdm(docIDs) :                 
        #docIDsからdataframe 抽出
        edinet_code=sr_docs[docID]
        sDate=df_docs[df_docs['docID']==docID].submitDateTime.to_list()[0] #hdf group名決めるため提出日活用        
        group_name=createGroupName(sDate,docID,edinet_code)  
        #flag=df_docs[df_docs['docID']==docID].xbrlFlag.to_list()[0]                       
        #書類取得
        url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docID
        params = { 'type': 1} #1:zip 2 pdf
        headers = {'User-Agent': 'mail_address'}            
        try :
            res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)
        except requests.exceptions.Timeout :
            #print(res.headers)
            continue                        
        sleep(1)
        if 'stream' in res.headers['Content-Type'] :
            with zipfile.ZipFile(BytesIO(res.content)) as existing_zip:
                #files=existing_zip.infolist()
                files=existing_zip.namelist()        
                files=[i for i in files if 'XBRL/PublicDoc/' in i ]                    
                count_files=[ i for i in files if '.xbrl' in i ]
                #print(count_files)                   
                for i in range(len(count_files)) :
                    if i<10:
                        oiban='00'+str(i)
                    elif i>=10 and i<100:
                        oiban='0'+str(i)
                    elif i>=100 :
                        oiban=str(i)
                    xbrlfile=[i for i in files if i.split('/')[-1][27:30]==oiban and 'xbrl' in i][0]
                    bXbrl=BytesIO(existing_zip.read(xbrlfile))
                    xsdfile=[i for i in files if '.xsd' in i and i.split('/')[-1][27:30]==oiban][0]
                    bXsd=BytesIO(existing_zip.read(xsdfile))                        
                    typefile=[i for i in files if '_pre.xml' in i and i.split('/')[-1][27:30]==oiban][0]
                    bType=BytesIO(existing_zip.read(typefile))
                    comp_files=[i for i in files if '_lab.xml' in i ]
                    if len(comp_files)>0 : 
                        companyfile=[i for i in comp_files if  i.split('/')[-1][27:30]==oiban]
                        if len(companyfile)>0 :
                            bCompany=BytesIO(existing_zip.read(companyfile[0]))
                            company_file_name=companyfile[0].split('/')[-1]
                        else :
                            #print(comp_files,oiban,docID)
                            bCompany=None
                            company_file_name=''
                    else :
                        bCompany=None
                        company_file_name=''                        
                    df=zipParser(bXbrl,bXsd,bType,bCompany,company_file_name)                    
                    print(df)
                    # saveToHDF                   
                    df['amount']=df['amount'].str.replace(' ','') #空白文字削除
                    df['amount']=df['amount'].str[:220] #pytable制限                        
                    df.to_hdf(h5xbrl,group_name+ '/' + docID+'_'+oiban , format='table',
                        mode='a', data_columns=True, index=True, encoding='utf-8')
                                                  
    return
def toHDFFromEdinet(h5xbrl,start_date=dt.date.today() - dt.timedelta(weeks=1),end_datee=dt.date.today()):
    print('calucalateing docID...')
    hdf_docIDs=docIDsFromHDF(h5xbrl) #HDF group名から求める　docIDs
    print('HDF docIDS:'+str(len(hdf_docIDs)))
    df_docs=shapingJson(h5xbrl)
    df_docs=df_docs[df_docs['dtDate']>=start_date]
    df_docs=df_docs[df_docs['dtDate']<end_date]
    json_docIDs=df_docs['docID'].to_list()
    print('json docIDs:'+str(len(json_docIDs)))
    docIDs=list(set(json_docIDs)-set(hdf_docIDs))
    #docIDs=['S100GETV']
    print('HDF化するdocIDS:'+str(len(docIDs)))
    directHdfFromZIP(df_docs,docIDs,h5xbrl)
    
if __name__=='__main__':
    h5xbrl='d:\\data\\hdf\\xbrl.h5'  #HDF file保存先    
    start_date=dt.date.today() - dt.timedelta(weeks=1) #期間設定
    end_date=dt.date.today()
    toHDFFromEdinet(h5xbrl,start_date,end_date)
