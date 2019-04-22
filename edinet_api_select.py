# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 07:39:42 2019
参考URL
https://qiita.com/simonritchie/items/dd737a52cf32b662675c
https://kunai-lab.hatenablog.jp/entry/2018/04/08/134924
https://blog.statsbeginner.net/entry/2017/05/08/072357
@author: Yusuke
"""
import pandas as pd
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
from time import sleep
import zipfile
import io
def get_xbrl(docID,df,save_path) :
    '''
    過去５年分のEDINETファイル情報は３０万以上あり有価証券報告書だけで1tbに迫ります
    指定したpathへsubDateTimeから'\年\月\日\文章コード'のディレクトリーを作成し保存
    docIDが分かれば保存先も判明    
    '''
    #path
    sDate=df[df['docID']==docID].submitDateTime.to_list()[0]
    save_path=save_path+'\\'+sDate[0:4]+'\\'+sDate[5:7]+'\\'+sDate[8:10]+'\\'+docID
    if os.path.isdir(save_path) == True : #過去に読み込んだ事あるか(dirあるか)
        return
    #書類取得
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docID
    params = { 'type': 1} #1:zip 2 pdf
    headers = {'User-Agent': 'exdodo@gmail.com'}
    res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)
    sleep(1) #1秒間をあける    
    if 'stream' in res.headers['Content-Type'] :
        with zipfile.ZipFile(io.BytesIO(res.content)) as existing_zip:        
            existing_zip.extractall(save_path)
    else :
        print('error : '+docID)

def largeShare_people(df,nYear=2019):
    #人名検索　大量保有報告書 提出ランキング
    #mode_value = df['filerName'].mode()    
    df=df[df['ordinanceCode']==60.0]  #大量保有報告書  
    #人名＝8文字未満　会社　abc除外
    df=df[~df['filerName'].str.contains('会社',na=False)]
    df['s_len'] = df['filerName'].apply(lambda x: len(str(x).replace(' ', '')))    
    df=df[df.s_len<8]  #8文字未満のみ選択  
    #df=df[~df['filerName'].str.contains(u'[ァ-ン]',na=False)]
    df=df[~df['filerName'].str.contains(u'[Ａ-Ｚ]+',na=False)] #アルファベット削除
    mode_value= df['filerName'].value_counts()    
    print(str(nYear)+'年 人名検索　大量保有報告書 提出回数ランキング')
    print(mode_value)

def select_docIDs(df,seek_words,df_columns) :    
    docIDs=[]
    for df_column in df_columns :
        for seek_word in seek_words :
            df_contains = df[df[df_column].str.contains(seek_word,na=False)]    
            df_contains=df_contains.sort_values('submitDateTime')
            if len(df_contains['docID'].to_list())>0 : 
                docIDs.append(df_contains['docID'].to_list())
    flat_docs = [item for sublist in docIDs for item in sublist] #flatten
    unique_docs=list(set(flat_docs)) #重複削除
    return unique_docs  

def select_dict_docIDs(df,seek_word):
    #辞書にして抽出
    df=df.set_index('docID')
    df=df.sort_index()
    filerName_dict = df.filerName.to_dict()
    #docDescription_dict=df.docDescription.to_dict()    
    #辞書から検索    
    docIDs = [k for k, v in filerName_dict.items() if v == 'アネスト岩田株式会社']
    return docIDs

def colunm_shape(df) :
    df['filerName'] = df['filerName'].astype(str)
    df['docDescription'] = df['docDescription'].astype(str)
    df['secCode']= df['secCode'].fillna(0.0)
    df['secCode'] = df['secCode'].astype(int)
    df['secCode'] = df['secCode']/10
    df['secCode'] = df['secCode'].astype(str)
    return df

def df_From_docIDS(docIDs,df) :
    #docIDsからdataframe 抽出
    df=df.set_index('docID')
    df=df.sort_values('dtDate')
    df=df.loc[docIDs]
    df=df.sort_values('submitDateTime')
    print(df[['submitDateTime','filerName']])
    
if __name__=='__main__':
    seek_words=['6501','野村'] #証券コードのとき文字列にする　例'6501'
    nYears=[2019,2019] #期間指定　年　以上以内
    path='' #保存先指定
    df = pd.read_json('xbrldocs.json') #約30万行
    df = colunm_shape(df)
    #検索対象列の決定 提出者名,提出者証券コード,提出書類概要
    df_columns=['filerName','secCode','docDescription']           
    df['dtDate']=pd.to_datetime(df['submitDateTime']) #obj to datetime    
    df=df[(df['dtDate'].dt.year >= min(nYears)) 
            & (df['dtDate'].dt.year <= max(nYears))]        
    #提出者名のdocID取得
    docIDs=select_docIDs(df,seek_words,df_columns)    
    df_From_docIDS(docIDs,df)
    #largeShare_people(df,nYear) #大量保有報告書人名ランキング
    #for docID in docIDs :        
        #get_xbrl(docID,df,path)    
    '''
    21:連番	seqNumber,5:書類管理番号	docID,8:提出者EDINETコード	edinetCode
    20:提出者証券コード secCode,0:提出者法人番号	JCN,5:提出者名 filerName
    ファンドコード fundCode,15:府令コード ordinanceCode,11:様式コード	formCode
    7:書類種別コード docTypeCode,17:期間（自） periodStart,18:期間（至）	periodEnd
    23:提出日時	submitDateTime,4:提出書類概要	docDescription
    13:発行会社EDINETコード	issuerEdinetCode
    22:対象EDINETコード	subjectEdinetCode,24:子会社EDINETコード	subsidiaryEdinetCode
    2:臨報提出事由	currentReportReason,16:親書類管理番号	parentDocID
    14:操作日時	opeDateTime,25:取下区分	withdrawalStatus
    6:書類情報修正区分	docInfoEditStatus,開示不開示区分	disclosureStatus
    26:XBRL有無フラグ	xbrlFlag,PDF有無フラグ	pdfFlag,代替書面・添付文書有無フラグ	attachDocFlag
    英文ファイル有無フラグ	englishDocFlag
    '''
