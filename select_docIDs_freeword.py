# -*- coding: utf-8 -*-
"""
Created on Tue May 14 07:33:55 2019
https://cortyuming.hateblo.jp/entry/2015/12/26/085736
@author: Yusuke
"""
import pandas as pd
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False対策
from time import sleep
from EDINET_API import main_jsons
import zipfile
import io
from tqdm import tqdm
import os
import unicodedata

def select_dict_docIDs(df,seek_word):
    #辞書にして抽出
    df=df.set_index('docID')
    df=df.sort_index()
    filerName_dict = df.filerName.to_dict()
    #docDescription_dict=df.docDescription.to_dict()    
    #辞書から検索    
    docIDs = [k for k, v in filerName_dict.items() if v == seek_word]
    return docIDs

def get_xbrl_from_docIDs(df_json,save_path,docIDs):
    '''
    docIDからXBRLファイルを探す
    なければ取得する
    '''     
    df_json['dtDateTime']=pd.to_datetime(df_json['submitDateTime']) #obj to datetime
    df_json['dtDate']=df_json['dtDateTime'].dt.date #時刻を丸める　normalize round resample date_range
    #dirls=[]
    for docID in tqdm(docIDs):                 
        #docIDsからdataframe 抽出
        sDate=df_json[df_json['docID']==docID].submitDateTime.to_list()[0]
        flag=df_json[df_json['docID']==docID].xbrlFlag.to_list()[0]
        file_dir=save_path+'\\'+str(int(sDate[0:4]))+'\\'+\
            str(int(sDate[5:7]))+'\\'+str(int(sDate[8:10]))+'\\'\
            +docID+'\\'+docID+'\\XBRL\\PublicDoc'
        if not os.path.isdir(file_dir): #xdrl fileなければ取得
            if flag=='1' : #xbrl有無flag確認
                get_xbrl(docID,df_json,save_path)            
        #dirls.append(file_dir)
    #return dirls

def select_docIDs_freeword(df,seek_words,seek_columns=[]) :
    '''
    df:検索対象データーフレーム
    seek_words:検索用語
    seek_columns:検索列
    '''
    if len(seek_columns)==0 : seek_columns=df.columns
    docIDs=[]    
    for col_name in seek_columns :
        if col_name=='dtDateTime' : continue #object type以外は検索しない
        if col_name=='dtDate' :  continue    
        for seek_word in seek_words :            
            df_contains = df[df[col_name].str.contains(seek_word,na=False)]
            df_contains = df_contains.sort_values('submitDateTime')
            if len(df_contains['docID'].to_list())>0 : 
                docIDs.append(df_contains['docID'].to_list())
    flat_docs = [item for sublist in docIDs for item in sublist]#flatten
    unique_docs=list(set(flat_docs))    
    return unique_docs
def colunm_shape(df) :
    #submitDateTime 日付型へ
    df['dtDateTime']=pd.to_datetime(df['submitDateTime']) #obj to datetime
    df['dtDate']=df['dtDateTime'].dt.date #時刻を丸める　normalize round resample date_range
    df['secCode'] = df['secCode'].fillna(0)
    df['secCode'] = df['secCode'].astype(int)
    df['secCode'] = df['secCode']/10
    df['secCode'] = df['secCode'].map('{:.0f}'.format)
    cols=['docTypeCode','ordinanceCode']
    for col_name in cols :
        df[col_name]=df[col_name].fillna(0)
        df[col_name]=df[col_name].astype(int)
        df[col_name]=df[col_name].astype(str)
    
    #docIDだけあり他がｎｕｌｌ（諸般の事情で削除された）が2000近くあるから削除
    df=df.dropna(subset=['submitDateTime'])
    df=df.sort_values('submitDateTime')         
    return df   
def get_xbrl(docID,df,save_path) :
    '''
    
    指定したpathへsubDateTimeから'\年\月\日\文章コード\文章コード'のディレクトリーを作成し保存
    docIDが分かれば保存先も判明    
    '''    
    #path
    sDate=df[df['docID']==docID].submitDateTime.to_list()[0]
    save_path=save_path+'\\'+str(int(sDate[0:4]))+'\\'+str(int(sDate[5:7]))+\
                '\\'+str(int(sDate[8:10]))+'\\'+docID+'\\'+docID
    if os.path.isdir(save_path) == True : #過去に読み込んだ事あるか(dirあるか)
        return
    #書類取得
    url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'+docID
    params = { 'type': 1} #1:zip 2 pdf
    headers = {'User-Agent': 'メールアドレス'}    
    res = requests.get(url, params=params,verify=False,timeout=3.5, headers=headers)
    sleep(1) #1秒間をあける    
    if 'stream' in res.headers['Content-Type'] :
        with zipfile.ZipFile(io.BytesIO(res.content)) as existing_zip:        
            existing_zip.extractall(save_path)
    elif 'application/json' in res.headers['Content-Type'] :
        print('error@ : '+docID)
        print(res.json())        
    else :
        print('error : '+docID)
        print(res.headers)
def display_From_docIDS(docIDs,df) :
    #edinetコード　提出者名辞書作成
    df_edinetCode=pd.read_csv('EdinetcodeDLInfo.csv',header=1,encoding='cp932')
    df_edinetCode=df_edinetCode.set_index('ＥＤＩＮＥＴコード')
    sr_edinetCode=df_edinetCode['提出者名']
    dic_edinet_code=sr_edinetCode.to_dict()
    #docIDsからdataframe 抽出
    df=df.set_index('docID')
    df=df.sort_values('dtDate')
    df=df.loc[docIDs]
    df=df.sort_values('submitDateTime')
    df['edinetName']=df['issuerEdinetCode'].map(dic_edinet_code)
    pd.set_option('display.max_columns', None)
    df['docDescription']=df['docDescription'].str[:10]
    df['filerName']=df['filerName'].str[:10]
    print(df[['dtDate','secCode','filerName','docDescription']])

def select_docIDs_docType(df,docType) :
    docIDs=[] 
    return docIDs        

if __name__=='__main__':
    #-------------------------------------------------------------------------
    save_path='d:\\data\\xbrl\\temp' #xbrl file保存先の基幹フォルダー
    #save_path='d:\\data\\xbrl\\download\\edinet' #有報キャッチャー自分用      
    seek_words=['6501',6501,'６５０１','日立製作所'] #'030000':年次有価証券報告書
    seek_words=[str(n) for n in seek_words ] #文字列に変換 
    #列指定したいならば書類一覧項目を下記にしるす　なければ[]
    seek_columns=['filerName','secCode','docDescription','subjectEdinetCode']
    nYears=[2019,2019] #期間指定　年　以上以内      
    #-----------------------------------------------------------------------
    main_jsons() #前日まで提出書類一覧を取得  
    df=pd.read_json('xbrldocs.json',dtype='object') #5年分約30万行
    df = colunm_shape(df) #dataframeを推敲
    seek_words=[ unicodedata.normalize("NFKC", n) 
        if n.isdigit() else n for n in seek_words ] #数字は半角文字列に統一     
    df=df[(df['dtDateTime'].dt.year >= min(nYears)) 
            & (df['dtDateTime'].dt.year <= max(nYears))]    
    docIDs=select_docIDs_freeword(df,seek_words,seek_columns)#or検索
    #---過去５年分のEDINETファイル情報は３０万以上あり有価証券報告書だけで1TBに迫ります----
    #取得json情報表示（docID・日付・提出者名・書類内容）
    #display_From_docIDS(docIDs,df)
    #get_xbrl_from_docIDs(df,save_path,docIDs)    
    
    print('docIDsが '+str(len(docIDs))+' 件見つかりました。')
    ans = input("ダウンロードしてよろしいですか(y/n)")
    if ans == "y":
        get_xbrl_from_docIDs(df,save_path,docIDs)
    
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
    '書類種別コード'{10:'有価証券通知書',20:'変更通知書（有価証券通知書）',30:'有価証券届出書', 
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
    #docIDs=['S100DJ2G',]#['S100DAZ4']  
    '''
    
