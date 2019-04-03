# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 07:39:42 2019

@author: Yusuke
"""
import pandas as pd
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
from time import sleep
'''
連番	seqNumber,書類管理番号	docID,提出者EDINETコード	edinetCode
提出者証券コード	secCode,4:提出者法人番号	JCN,5:提出者名	filerName
ファンドコード	fundCode,府令コード	ordinanceCode,様式コード	formCode
書類種別コード	docTypeCode,期間（自）	periodStart,期間（至）	periodEnd
提出日時	submitDateTime,提出書類概要	docDescription
発行会社EDINETコード	issuerEdinetCode
対象EDINETコード	subjectEdinetCode,子会社EDINETコード	subsidiaryEdinetCode
臨報提出事由	currentReportReason,親書類管理番号	parentDocID
操作日時	opeDateTime,取下区分	withdrawalStatus
書類情報修正区分	docInfoEditStatus,開示不開示区分	disclosureStatus
XBRL有無フラグ	xbrlFlag,PDF有無フラグ	pdfFlag,代替書面・添付文書有無フラグ	attachDocFlag
英文ファイル有無フラグ	englishDocFlag

'''
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

def docID_filerName(df,seek_word,df_column='filerName') :    
    df_contains = df[df[df_column].astype(str).str.contains(seek_word,na=False)]
    df_contains=df_contains.sort_values('submitDateTime')
    print(df_contains[[df_column,'filerName','docDescription']]) 
    docIDs=df_contains['docID'].to_list()
    return docIDs    


if __name__=='__main__':
    df = pd.read_json('xbrldocs.json')
    df_columns=df.columns      
    nYear=2018
    seek_word='65010' #証券コードのとき10倍して文字列にする　例'65010'
    df_column=df_columns[20] #提出者証券コード：20 提出者名：10　提出書類概要：4
    '''
    '0:JCN', '1:attachDocFlag', '2:currentReportReason', '3:disclosureStatus',
    '4:docDescription', '5:docID', '6:docInfoEditStatus', '7:docTypeCode',
    '8:edinetCode', '9:englishDocFlag', '10:filerName', '11:formCode', '12:fundCode',
    '13:issuerEdinetCode', '14:opeDateTime', '15:ordinanceCode', '16:parentDocID',
    '17:pdfFlag', '18:periodEnd', '19:periodStart', '20:secCode', '21:seqNumber',
    '22:subjectEdinetCode', '23:submitDateTime', '24:subsidiaryEdinetCode',
    '25:withdrawalStatus', '26:xbrlFlag'
    '''
    #docIDだけあり他がｎｕｌｌ（諸般の事情で削除された）が2000近くあるから削除
    df=df.dropna(subset=['submitDateTime'])
    df=df.sort_values('submitDateTime')    
    df['dtDate']=pd.to_datetime(df['submitDateTime']) #obj to datetime
    #期間指定
    df=df[df['dtDate'].dt.year==nYear]
    #print(df['dtDate'].dt.year)    
    #largeShare_people(df,nYear)    
    #提出者名のdocID取得
    docIDs=docID_filerName(df,seek_word,df_column)
    print(docIDs)
    