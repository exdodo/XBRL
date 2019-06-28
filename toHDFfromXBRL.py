# -*- coding: utf-8 -*-
"""
Created on Tue May 21 07:11:04 2019
保存先directoryからdocIDsを求める
参考
https://note.nkmk.me/python-pathlib-iterdir-glob/
http://d.hatena.ne.jp/xef/20121027/p2
https://note.nkmk.me/python-list-common/
https://www.hdfgroup.org/
https://www.fsa.go.jp/search/20180228/2b_InstanceGuide.pdf
o'reilly『Python and HDF5』
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

import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from EdinetXbrlParser import xbrl_to_dataframe
#from select_docIDs_freeword import docIDs_from_directory
from EDINET_HDF import docIDsFromHDF
from xbrlUtility import column_shape
from xbrlUtility import download_xbrl
from xbrlUtility import docIDsFromDirectory
                                     
def docIDsToHDF(docIDs,h5xbrl,save_path,df_docs):
    sr_docs=df_docs.set_index('docID')['edinetCode']
    for docID in tqdm(docIDs) :
        edinet_code=sr_docs[docID][0]
        sDate=df_docs[df_docs['docID']==docID].submitDateTime.to_list()[0]
        #追番処理 一つのdocIDで複数の財務諸表を提示
        xbrl_dir=save_path+'\\'+str(int(sDate[0:4]))+'\\'+\
            str(int(sDate[5:7]))+'\\'+str(int(sDate[8:10]))+'\\'\
            +docID+'\\'+docID+'\\XBRL\\PublicDoc\\'        
        p_xbrl=Path(xbrl_dir) #xbrl fileの数を求める
        p_xbrlfiles=list(p_xbrl.glob('*.xbrl'))
        xbrl_file_names=[p.name for p in p_xbrlfiles]
        for xbrl_file_name in xbrl_file_names:
            oiban=xbrl_file_name[27:30]
            xbrlfile=xbrl_dir+xbrl_file_name
            df_xbrl=xbrl_to_dataframe(xbrlfile)
            df_xbrl['amount']=df_xbrl['amount'].str.replace(' ','') #空白文字削除
            df_xbrl['amount']=df_xbrl['amount'].str[:220] #pytable制限
            # saveToHDF
            df_xbrl.to_hdf(h5xbrl,edinet_code + '/' + docID+'_'+oiban , format='table',
                          mode='a', data_columns=True, index=True, encoding='utf-8')     
if __name__=='__main__':
    '''
    ・ダウンロードしたXBRLファイルを一括してHDF化するためのプログラム
    ・テキストは225文字以上だとpytableの警告が出るので空白を削除して先頭から220文字
    hdf_docIDs:HDFファイルに保存しているdocID(HDF fileのseccode以下のdocID)
    1.json_docIDs:EdinetからダウンロードしたdocID　基準になる(HDF fileのindex/edinetdocsのdocID)
    2.dir_docIDs:'save_path'から求めたxbrlファイルをダウンロードしたdocID
    dict_docIDs:dir_docIDsと保存先ディレクトリーの辞書
    dl_docIDs:json_docIDsにあるがdir_docIDsにないもの
    HDFに保存すべきdocIDをもとめる
    1．dir_docIDsとhdf_docIDsから HDF化すべきtoHDF_docIDs抽出
    2．josn_docIDsとtoHDF_docIDsの共通するdocIDをHDF化
    
    '''
    save_path='d:\\data\\xbrl\\download\\edinet' #ダウンロードし解凍したxbrl file保存先基幹ファイル
    h5xbrl='d:\\data\\hdf\\xbrl.h5'  #HDF file保存先    
    start_date=datetime.date(2019, 6, 1)
    end_date=datetime.date.today()
    #フォルダー指定のアルゴリズ考えるのめんどいので簡易版
    if(start_date.year!=end_date.year) :
        limited_save_path=save_path
    else :    
        limited_save_path=save_path+'\\'+str(start_date.year)\
            +'\\'+str(start_date.month) 
    
    #dir_string='**/PublicDoc/*.xbrl' #'**/PublicDoc/*asr*E*.xbrl'
    print('calucalateing docID...')
    #docIDの整合性を整える 過去にHDF保存したものや書類一覧に記載のないものはHDF化しない
    hdf_docIDs=docIDsFromHDF(h5xbrl) #HDF group名から求める　docIDs        
    print('HDF docIDS:'+str(len(hdf_docIDs)))
    df_json=pd.read_hdf(h5xbrl,key='/index/edinetdocs') #edinetからｄｌした書類一覧のdocIDs
    df_json=column_shape(df_json)
    df_json=df_json[df_json['dtDate']>=start_date]
    #df_json=df_json[df_json['dtDate']<end_date]
    json_docIDs=df_json['docID'].to_list()
    print('json docIDs:'+str(len(json_docIDs)))    
    #dict_docIDs=docIDs_from_directory(limited_save_path,dir_string) #xbrl file
    #dir_docIDs=list(dict_docIDs.keys())
    dir_docIDs=docIDsFromDirectory(limited_save_path)
    print('directory docIDs:'+str(len(dir_docIDs)))
    #json_docIDsのうちdir_docIDsに含まれていないものを抽出
    dl_docIDs=list(set(json_docIDs) - set(dir_docIDs))
    print('downloading docIDs='+str(len(dl_docIDs)))
    if len(dl_docIDs)>0 : 
        download_xbrl(df_json,save_path,dl_docIDs) #なければダウンロード
        dir_docIDs.extend(dl_docIDs)
    toHDF_docIDs=list(set(dir_docIDs)-set(hdf_docIDs)) #フォルダーにあるがHDF化していないも
    toHDF_docIDs=list(set(json_docIDs)&set(toHDF_docIDs)) #json_docIDsとフォルダーに共通に含まれている
    if len(toHDF_docIDs)>0 : 
        download_xbrl(df_json,save_path,toHDF_docIDs) #なければダウンロード
        print('To HDF docIDs='+str(len(toHDF_docIDs)))    
        docIDsToHDF(toHDF_docIDs,h5xbrl,save_path,df_json)# xbrlToHDF  
    '''
    #E01737 S100G21U
    #print(docIDs)
    if 'S100FYT6' in hdf_docIDs :
        print('hdf')
    if 'S100FYT6' in json_docIDs :
        print('json')
    if 'S100FYT6' in dir_docIDs :
        print('directory')
    df_json=df_json[df_json['docID']=='S100FYT6']
    print(df_json['docTypeCode'])
    '''
