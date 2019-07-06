# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 13:38:26 2019
参考図書
『金融データ解析の基礎』　金明哲
『簡単にできるXBRL　基礎編』　池田智之
『会計人のためのXBRL入門』坂上学
https://qiita.com/shima_x/items/58634a838ab37c3607b5
https://www.meganii.com/blog/2017/06/18/what-is-edinet-xbrl/
http://orangain.hatenablog.com/entry/namespaces-in-xpath
http://www.fsa.go.jp/search/20170228.html
https://srbrnote.work/archives/1315
https://srbrnote.work/archives/1768
https://note.nkmk.me pandas関係
https://www.fsa.go.jp/search/20130301/02_a1.pdf
EDINETタクソノミは© Copyright 2014 Financial Services Agency, The Japanese Government
# 2018年版EDINETタクソノミの公表について https://www.fsa.go.jp/search/20180228.html
# 報告書インスタンス作成ガイドライン
# 4-2-4 XBRL インスタンスファイル
# jp{府令略号}{様式番号}-{報告書略号}-{報告書連番(3 桁)}_{EDINETコード又はファンドコード}-
# {追番(3 桁)}_{報告対象期間期末日|報告義務発生日}_{報告書提出回数(2 桁)}_{報告書提出日}.xbrl
# 0         1         2         3         4         5         6
# 0123456789012345678901234567890123456789012345678901234567890
# jpcrp030000-asr-001_E00000-000_2017-03-31_01_2017-06-29.xbrl
#20180331以降 2018
@author: Yusuke
"""
import glob
from pathlib import Path

import h5py
import pandas as pd

from EDINET_HDF import createGroupName
from xbrlUtility import column_shape


def ToExcel_finace_sheets(df_docs,docIDs,h5xbrl) :
    sr_docs=df_docs.set_index('docID')['edinetCode'] #dataframe to Series print(sr_docs['S100FSTI'])   
    for docID in docIDs :
        edinet_code=sr_docs[docID]
        sDate=df_docs[df_docs['docID']==docID].submitDateTime.to_list()[0] #hdf group名決めるため提出日活用    
        group_name=createGroupName(sDate,docID,edinet_code) 
        group_name=group_name+'/'+docID+'_000'       
        with h5py.File(h5xbrl,'r') as h5File :
                if edinet_code in h5File.keys() :
                        print(edinet_code)
                        #print(list(h5File[edinet_code].keys()))
                        df_fs=pd.read_hdf(h5xbrl,group_name)    
                        df_fs['amount']=df_fs['amount'].str[:3000]   
                        df_fs=df_fs[~df_fs['context_ref'].str.contains('Prior')]
                        df_fs=df_fs.dropna(subset=['role_id'],how='any')
                        #有価証券報告書　list作成
                        rols=pd.unique(df_fs['role_id'])
                        parent_dir=str(Path(h5xbrl).parents[0])+'\\'
                        with pd.ExcelWriter(parent_dir+edinet_code+'_'+docID + '.xlsx') as writer:        
                                for rol in rols:  # assuming they're already DataFrames
                                        df_name = df_fs[df_fs['role_id'] == rol]
                                        df_name=df_name.sort_values(['from_element_id','order'])
                                        df_name=df_name[['from_string', 'label_string', 'amount','context_ref']] 
                                        df_name.to_excel(writer, sheet_name=rol[4:34], index=False)
                                writer.save()  # we only need to save to disk at the very end!
if __name__=='__main__':    
    h5xbrl='d:\\Data\\hdf\\xbrl.h5' #xbrlをHDF化したファイルの保存先
    df_docs=pd.read_hdf(h5xbrl,'index/edinetdocs')
    df_docs=column_shape(df_docs) #dataframeを推敲
    docIDs=['S100GETV'] #2019/7/3三重交通E04233
    ToExcel_finace_sheets(df_docs,docIDs,h5xbrl)
