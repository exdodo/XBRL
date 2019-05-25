# -*- coding: utf-8 -*-
"""
Created on Tue May 21 07:11:04 2019
保存先directoryからdocIDsを求める
参考
https://note.nkmk.me/python-pathlib-iterdir-glob/
http://d.hatena.ne.jp/xef/20121027/p2
https://note.nkmk.me/python-list-common/
@author: Yusuke
"""
#https://srbrnote.work/archives/1315
    # 2018年版EDINETタクソノミの公表について https://www.fsa.go.jp/search/20180228.html
    # 報告書インスタンス作成ガイドライン
    # 4-2-4 XBRL インスタンスファイル
    # jp{府令略号}{様式番号}-{報告書略号}-{報告書連番(3 桁)}_{EDINETコード又はファンドコード}-
    # {追番(3 桁)}_{報告対象期間期末日|報告義務発生日}_{報告書提出回数(2 桁)}_{報告書提出日}.xbrl
    # 0         1         2         3         4         5         6
    # 0123456789012345678901234567890123456789012345678901234567890
    # jpcrp030000-asr-001_E00000-000_2017-03-31_01_2017-06-29.xbrl

from pathlib import Path
from  EdinetXbrlParser import xbrl_to_dataframe
import pandas as pd
import h5py
from itertools import chain
import os
def docIDs_from_directory(save_path,dir_string):
    p_dir = Path(save_path)
    #xbrlファイルのあるディレクトリーのみを抽出 年次有価証券報告書('asr')
    p_winpath=list(p_dir.glob(dir_string)) 
    dl_docIDs=[docID.parents[2].name for docID in p_winpath] #一個上parents[0]
    xbrl_file_names=[p.name for p in p_winpath if p.is_file()] #ファイル名（basename）のみを抽出    
    dic_docIDs = dict(zip( dl_docIDs,xbrl_file_names))
    return dic_docIDs
    
def docIDs_to_HDF(save_path,dict_docIDs,df_json,data_path='d:\\data\\hdf\\xbrl.h5'):
    for docID,xbrl_file_name in dict_docIDs.items() :
        edinet_code=xbrl_file_name.split('_')[1]
        edinet_code=edinet_code.split('-')[0]
        print(docID,xbrl_file_name)                     
        sDate=df_json[df_json['docID']==docID].submitDateTime.to_list()[0]             
        xbrlfile=save_path+'\\'+str(int(sDate[0:4]))+'\\'+\
            str(int(sDate[5:7]))+'\\'+str(int(sDate[8:10]))+'\\'\
            +docID+'\\'+docID+'\\XBRL\\PublicDoc\\'+xbrl_file_name
        df_xbrl=xbrl_to_dataframe(xbrlfile)
        df_xbrl['amount']=df_xbrl['amount'].str.replace(' ','') #空白文字削除
        df_xbrl['amount']=df_xbrl['amount'].str[:220] #pytable制限
        # saveToHDF
        df_xbrl.to_hdf(data_path,edinet_code + '/' + docID , format='table',
                      mode='a', data_columns=True, index=True, encoding='utf-8')                
    return
def docIDs_from_HDF(data_path='d:\\data\\hdf\\xbrl.h5'):
    docIDs=[]
    if os.path.exists(data_path) :
        with h5py.File(data_path,'r') as f:
            docIDs=[]
            for edinetcode in f.keys() :
                second_keys=list(f[edinetcode].keys())            
                docIDs.append(second_keys)        
            docIDs=list(chain.from_iterable(docIDs))        
    return docIDs

if __name__=='__main__':
    '''
    ・ダウンロードしたXBRLファイルをHDFかするためのプログラム
    ・テキストは225文字以上だとpytableの警告が出るので空白を削除して先頭から220文字
    hdf_docIDs:HDFファイルの保存しているdocID
    json_docIDs:EdinetからダウンロードしたdocID　基準になる
    dir_docIDs:'save_path'から求めたxbrlフィルをダウンロードしたdocID
    dict_docIDs:dir_docIDsと保存先ディレクトリーの辞書
    HDFに保存すべきdocIDをもとめる
    1．json_docIDsとdir_docIDs 共通のリスト作成common_docIDs
    2．common_docIDsからhdf_docIDs重ねれば削除
    3.残ったものがダウンロードすべきdocID
    '''
    #main_jsons() #前日まで提出書類一覧を取得
    #save_path='d:\\data\\xbrl\\temp' #xbrl fileの基幹フォルダー
    save_path='d:\\data\\xbrl\\download\\edinet' #有報キャッチャー自分用
    save_path_1=save_path #年指定　2014年4月以降なら'\\2014\\4'
    dir_string='**/PublicDoc/*asr*.xbrl' #'**/PublicDoc/*asr*E*.xbrl'
    data_path='d:\\data\\hdf\\xbrl.h5'#HDF保存先とファイル名
    
    #docIDの整合性を整える 過去にHDFかしたかjsonに記載のないものはHDF化しない
    hdf_docIDs=docIDs_from_HDF(data_path) #HDF保存済み　docIDs        
    df_json = pd.read_json('xbrldocs.json',dtype='object') #5年分約30万行
    json_docIDs=df_json['docID'].to_list()    
    dict_docIDs=docIDs_from_directory(save_path_1,dir_string)
    dir_docIDs=dict_docIDs.keys()
    #json_docIDsとdir_docIDs 共通のリスト作成
    common_docIDs=list(set(json_docIDs) & set(dir_docIDs))
    #common_docIDsからhdf_docIDs重ねれば削除
    docIDs=list(set(common_docIDs) ^ set(hdf_docIDs))
    dict_doc={}
    for docID in docIDs:
        if docID in dict_docIDs:         
            dict_doc[docID]=dict_docIDs[docID]    
    docIDs_to_HDF(save_path,dict_doc,df_json)
    