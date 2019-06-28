# -*- coding: utf-8 -*-
"""
Created on Thu May 16 10:23:34 2019
注意：過去５年分のEDINETファイル情報は３０万以上あり有価証券報告書だけで1TBに迫ります
書類一覧項目から大量にXBRLファイルをダウンロードするために作成
githubからは削除
@author: Yusuke
"""
import pandas as pd
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
import unicodedata
from EDINET_API import main_jsons
from select_docIDs_freeword import display_From_docIDS
from xbrlUtility import column_shape,download_xbrl
def select_docIDs_docType(df,dict_cond) :
    docIDs=[]
    df_focus=df              
    for type_key,cond_value in dict_cond.items() :
        if cond_value.isdigit() : #数値は半角文字列にする
            cond_value= unicodedata.normalize("NFKC", cond_value)        
        df_focus=df_focus[df_focus[type_key]==cond_value]
    if len(df_focus['docID'].to_list())>0 : 
        docIDs.append(df_focus['docID'].to_list())
    flat_docs = [item for sublist in docIDs for item in sublist]#flatten
    unique_docs=list(set(flat_docs))
    return unique_docs
def settei(dict_cond,nYears,save_path,hdf_path) :
    
    #main_jsons(hdf_path) #前日まで提出書類一覧を取得  
    df=pd.read_hdf(hdf_path,key='/index/edinetdocs')
    df = column_shape(df) #dataframeを推敲
    df=df[(df['dtDateTime'].dt.year >= min(nYears)) 
            & (df['dtDateTime'].dt.year <= max(nYears))]    
    docIDs=select_docIDs_docType(df,dict_cond)    
    display_From_docIDS(df,docIDs)#取得docIDs情報表示
    print('docIDsが '+str(len(docIDs))+' 件見つかりました。')
    download_xbrl(df,save_path,docIDs)
    return     
if __name__=='__main__':
    #---過去５年分のEDINETファイル情報は３０万以上あり有価証券報告書だけで1TBに迫ります----
    #--DISK容量が十分にあるかダウンロード対象のdocIDsを絞らないとシステムに深刻な影響を与えます--
    #dict_cond={'formCode':'030000', 'ordinanceCode':'10'}　#年次有価証券報告書 取得用
    dict_cond={'secCode':'6758'}       
    nYears=[2019,2019] #期間指定　年　以上以内
    save_path='d:\\data\\xbrl\\download\\edinet' #自分用
    hdf_path='d:\\data\\hdf\\xbrl.h5' #xbrl 書類一覧HDF　保存先         
    settei(dict_cond,nYears,save_path,hdf_path)
    '''
    書類一覧項目{'JCN':'提出者法人番号', 'attachDocFlag':'代替書面・添付文書有無フラグ', 
     'currentReportReason':'臨報提出事由', 'disclosureStatus':'開示不開示区分',
       'docDescription':'提出書類概要', 'docID':'書類管理番号', 
       'docInfoEditStatus':'書類情報修正区分', 'docTypeCode':'書類種別コード',
       'edinetCode':'提出者EDINETコード', 'englishDocFlag':'英文ファイル有無フラグ',
       'filerName':'提出者名', 'formCode':'様式コード', 'fundCode':'ファンドコード',
       'issuerEdinetCode':'発行会社EDINETコード', 'opeDateTime':'操作日時',
       'ordinanceCode':'府令コード', 'parentDocID':'親書類管理番号','pdfFlag':'PDF有無フラグ', 
       'periodEnd':'期間（至）', 'periodStart':'期間（自）', 
       'secCode':'提出者証券コード', 'seqNumber':'連番','subjectEdinetCode':'対象EDINETコード', 
       'submitDateTime':'提出日時', 'subsidiaryEdinetCode':'子会社EDINETコード',
       'withdrawalStatus':'取下区分', 'xbrlFlag':'XBRL有無フラグ'}
    書類種別コード{10:'有価証券通知書',20:'変更通知書（有価証券通知書）',30:'有価証券届出書', 
     40:'訂正有価証券届出書',50:'届出の取下げ願い',60:'発行登録通知書', 
     70:'変更通知書（発行登録通知書）',80:'発行登録書',90:'訂正発行登録書', 
     100:'発行登録追補書類',110:'発行登録取下届出書',120:'有価証券報告書', 
     130:'訂正有価証券報告書',135:'確認書',136:'訂正確認書',140:'四半期報告書', 
     150:'訂正四半期報告書',160:'半期報告書',170:'訂正半期報告書',180:'臨時報告書', 
     190:'訂正臨時報告書',200:'親会社等状況報告書',210:'訂正親会社等状況報告書', 
     220:'自己株券買付状況報告書',230:'訂正自己株券買付状況報告書', 
     235:'内部統制報告書',236:'訂正内部統制報告書',240:'公開買付届出書', 
     250:'訂正公開買付届出書',260:'公開買付撤回届出書',270:'公開買付報告書', 
     280:'訂正公開買付報告書',290:'意見表明報告書',300:'訂正意見表明報告書', 
     310:'対質問回答報告書',320:'訂正対質問回答報告書',330:'別途買付け禁止の特例を受けるための申出書', 
     340:'訂正別途買付け禁止の特例を受けるための申出書', 
     350:'大量保有報告書',360:'訂正大量保有報告書',370:'基準日の届出書',380:'変更の届出書'}
    府令コード{} 
    '''
    