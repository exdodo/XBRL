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

from pathlib import Path
from  EdinetXbrlParser import xbrl_to_dataframe
import pandas as pd
import h5py
from itertools import chain
from tqdm import tqdm
def docIDs_from_directory(save_path,dir_string):
    p_dir = Path(save_path)
    #xbrlファイルのあるディレクトリーのみを抽出 年次有価証券報告書('asr')
    p_winpath=list(p_dir.glob(dir_string)) 
    dl_docIDs=[docID.parents[2].name for docID in p_winpath] #一個上parents[0]
    xbrl_file_names=[p.name for p in p_winpath if p.is_file()] #ファイル名（basename）のみを抽出    
    dic_docIDs = dict(zip( dl_docIDs,xbrl_file_names))
    return dic_docIDs
    
def docIDs_to_HDF(save_path,h5xbrl,dict_docIDs,df_json):
    '''
    docIDsからxbrl fileのディレクトリーをもとめ、財務諸表が複数あれば（追番）全部求める
    '''    
    print('xbrl To HDF writing...')
    for docID,xbrl_file_name in tqdm(dict_docIDs.items()) :        
        edinet_code=xbrl_file_name.split('_')[1]
        edinet_code=edinet_code.split('-')[0]
        #print(docID,xbrl_file_name)                     
        sDate=df_json[df_json['docID']==docID].submitDateTime.to_list()[0]             
        #追番処理 一つのdocIDで複数の財務諸表を提示
        xbrl_dir=save_path+'\\'+str(int(sDate[0:4]))+'\\'+\
            str(int(sDate[5:7]))+'\\'+str(int(sDate[8:10]))+'\\'\
            +docID+'\\'+docID+'\\XBRL\\PublicDoc\\'        
        p_xbrl=Path(xbrl_dir) #xbrl fileの数を求める
        p_xbrlfiles=list(p_xbrl.glob('*.xbrl'))
        xbrl_file_names=[p.name for p in p_xbrlfiles]
        for xbrl_file_name in xbrl_file_names:
            #xbrlfile=xbrl_dir+xbrl_file_name
            oiban=xbrl_file_name[27:30]
            xbrlfile=xbrl_dir+xbrl_file_name
            df_xbrl=xbrl_to_dataframe(xbrlfile)
            df_xbrl['amount']=df_xbrl['amount'].str.replace(' ','') #空白文字削除
            df_xbrl['amount']=df_xbrl['amount'].str[:220] #pytable制限
            # saveToHDF
            df_xbrl.to_hdf(h5xbrl,edinet_code + '/' + docID+'_'+oiban , format='table',
                          mode='a', data_columns=True, index=True, encoding='utf-8')                
    return
def docIDs_from_HDF(h5xbrl):
    hdf_docIDs=[]
    if Path(h5xbrl).exists() :
        with h5py.File(h5xbrl, 'r') as h5File:
            key_list1=h5File.keys()
            key_list2=[ list(h5File[key].keys()) for key in key_list1 if key!='index']
            key_list2=list(chain.from_iterable(key_list2)) #flatten
            hdf_docIDs=[ key[0:8] for key in key_list2] #追番削除
            hdf_docIDs=list(set(hdf_docIDs)) #unique
    return hdf_docIDs

if __name__=='__main__':
    '''
    ・ダウンロードしたXBRLファイルを一括してHDF化するためのプログラム
    ・テキストは225文字以上だとpytableの警告が出るので空白を削除して先頭から220文字
    hdf_docIDs:HDFファイルに保存しているdocID(HDF fileのseccode以下のdocID)
    json_docIDs:EdinetからダウンロードしたdocID　基準になる(HDF fileのindex/edinetdocsのdocID)
    dir_docIDs:'save_path'から求めたxbrlファイルをダウンロードしたdocID
    dict_docIDs:dir_docIDsと保存先ディレクトリーの辞書
    HDFに保存すべきdocIDをもとめる
    1．json_docIDsとdir_docIDs 共通のリスト作成common_docIDs
    2．common_docIDsからhdf_docIDs重ねれば削除
    3.残ったものがダウンロードすべきdocID
    '''
    save_path='d:\\data\\xbrl\\download\\edinet' #ダウンロードし解凍したxbrl file保存先基幹ファイル
    h5xbrl='d:\\data\\hdf\\xbrl.h5'  #HDF file保存先
    #test
    #h5xbrl='d:\\data\\test\\testxbrl.h5'
    #save_path='d:\\data\\xbrl\\temp' #xbrl file保存先の基幹フォルダー 

    limited_path_word='' #年指定　2014年4月なら'\\2014\\4' 2014年'\\2014'
    limited_save_path=save_path+limited_path_word
    dir_string='**/PublicDoc/*.xbrl' #'**/PublicDoc/*asr*E*.xbrl'
    print('calucalateing docID...')
    #docIDの整合性を整える 過去にHDF保存したかjsonに記載のないものはHDF化しない
    hdf_docIDs=docIDs_from_HDF(h5xbrl) #HDF group名から求める　docIDs        
    print('HDF docIDS:'+str(len(hdf_docIDs)))
    df_json=pd.read_hdf(h5xbrl,key='/index/edinetdocs') #edinetからｄｌした書類一覧
    json_docIDs=df_json['docID'].to_list()
    print('json docIDs:'+str(len(json_docIDs)))    
    dict_docIDs=docIDs_from_directory(limited_save_path,dir_string) #xbrl file
    dir_docIDs=dict_docIDs.keys()
    print('directory docIDs:'+str(len(dir_docIDs)))
    #json_docIDsとdir_docIDs 共通のリスト作成
    common_docIDs=list(set(json_docIDs) & set(dir_docIDs))
    #common_docIDsからhdf_docIDs重ねれば削除
    docIDs=list(set(common_docIDs) ^ set(hdf_docIDs))
    docIDs.sort()
    print('To HDF docIDs='+str(len(docIDs)))
    
    #リスト内包表記で書きたい＞＜
    dict_doc={}
    for docID in docIDs:
        if docID in dict_docIDs:         
            dict_doc[docID]=dict_docIDs[docID]    
    docIDs_to_HDF(save_path,h5xbrl,dict_doc,df_json)
    