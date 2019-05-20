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
#'jpcrp_cor'財務　'jplvh_cor'大量保有報告書
#from lxml import objectify as ET
import pandas as pd
import glob
from pathlib import Path
from EdinetXbrlParser import xbrl_to_dataframe,seek_from_docID

def ToExcel_finace_sheets(df_fs,filename) :
    df_fs=df_fs[~df_fs['context_ref'].str.contains('Prior')]
    df_fs=df_fs.dropna(subset=['role_id'],how='any')
    #有価証券報告書　list作成
    rols=pd.unique(df_fs['role_id'])
    print(rols)    
    with pd.ExcelWriter(mandatory_year(filename) + '_' + edinet_code(filename) + '.xlsx') as writer:        
        for rol in rols:  # assuming they're already DataFrames
                df_name = df_fs[df_fs['role_id'] == rol]
                df_name=df_name.sort_values(['from_element_id','order'])
                df_name=df_name[['from_string', 'label_string', 'amount','context_ref']] 
                df_name.to_excel(writer, sheet_name=rol[4:34], index=False)
        writer.save()  # we only need to save to disk at the very end!
def mandatory_year(filename):
    
    #https://srbrnote.work/archives/1315
    # 2018年版EDINETタクソノミの公表について https://www.fsa.go.jp/search/20180228.html
    # 報告書インスタンス作成ガイドライン
    # 4-2-4 XBRL インスタンスファイル
    # jp{府令略号}{様式番号}-{報告書略号}-{報告書連番(3 桁)}_{EDINETコード又はファンドコード}-
    # {追番(3 桁)}_{報告対象期間期末日|報告義務発生日}_{報告書提出回数(2 桁)}_{報告書提出日}.xbrl
    # 0         1         2         3         4         5         6
    # 0123456789012345678901234567890123456789012345678901234567890
    # jpcrp030000-asr-001_E00000-000_2017-03-31_01_2017-06-29.xbrl
    #20180331以降 2018
    #20170331
    #20160331
    #20150331
    #20140331
    #20130331    
    p=Path(filename)
    return p.name[-29:-25] # 報告書義務発生年

def edinet_code(filename):
    p=Path(filename)
    return p.name[20:26]
if __name__=='__main__':    
    save_path='d:\\data\\xbrl\\temp'
    docIDs=['S100DJ2G',]
    filenames=[]
    dirls=seek_from_docID(save_path,docIDs)
    for dirt in dirls: 
        for file_name in glob.glob(dirt+'\\*.xbrl') :
            filenames.append(file_name)#ここにXBRL File 指定
    xbrlfile=filenames[0]    
    df_fs=xbrl_to_dataframe(xbrlfile)
    df_fs['amount']=df_fs['amount'].str[:3000]   
    ToExcel_finace_sheets(df_fs,xbrlfile)