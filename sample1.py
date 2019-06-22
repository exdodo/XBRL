#旧村上ファンドグループをリスト化する
#'株式会社レノ'をキーワードとして書類一覧をもとめ、書類一覧から各書類を調べ、共同保有者を抜き出す。
#次に共同保有者から書類一覧をもとめ共同保有者のリストを充実させていく
import collections
from itertools import chain
from pathlib import Path

import h5py
import pandas as pd

from EdinetXbrlParser import xbrl_to_dataframe
from select_docIDs_freeword import (column_shape, download_xbrl,
                                    select_docIDs_freeword)
from toHDFfromXBRL import docIDs_from_HDF, docIDsToHDF


def collect_holders(docIDs,df_docs,xbrl_path) :
    sr_docs=df_docs.set_index('docID')['edinetCode'] #dataframe to Series print(sr_docs['S100FSTI'])
    holders=[]
    with h5py.File(xbrl_path, 'r') as h5File:       
        for doc in docIDs :        
            hdf_group=sr_docs[doc]+'/'+doc+'_000'
            if sr_docs[doc] in h5File.keys() : 
                if doc+'_000' in h5File[sr_docs[doc]].keys() :
                    df=pd.read_hdf(xbrl_path,key=hdf_group)
                    holders.append((df['amount'][df['element_id']=='jplvh_cor_FilerNameInJapaneseDEI']).tolist())
    holders=list(chain.from_iterable(holders))  #flatten
    #holders=list(set(holders)) #unique
    return holders


if __name__=='__main__':
    h5xbrl='d:\\Data\\hdf\\xbrl.h5' #xbrlをHDF化したファイルの保存先
    save_path='d:\\data\\xbrl\\download\\edinet' #xbrl file保存先(自分用)
    #test
    #h5xbrl='d:\\Data\\test\\testxbrl.h5' #xbrlをHDF化したファイルの保存先
    #save_path='d:\\data\\xbrl\\temp' #xbrl file保存先の基幹フォルダー
    df_docs=pd.read_hdf(h5xbrl,'index/edinetdocs')
    df_docs=column_shape(df_docs) #dataframeを推敲
    holders=['株式会社レノ']#['Ｅｖｏ　Ｆｕｎｄ']#    
    count=0    
    for i in range(10): #10回以上繰り返して増えていくのは無限増殖の可能性あり
        gross_holders=[]
        docIDs=select_docIDs_freeword(df_docs,holders,['filerName'])
        hdf_docIDs=docIDs_from_HDF(h5xbrl) #HDF保存済み　docIDs
        dl_docIDs=list(set(docIDs)-set(hdf_docIDs)) #集合 docIDのみに含まれる
        download_xbrl(df_docs,save_path,dl_docIDs) #なければダウンロード       
        docIDsToHDF(dl_docIDs,h5xbrl,save_path,df_docs)# xbrlToHDF
        holders=collect_holders(docIDs,df_docs,h5xbrl)
        gross_holders.append(holders)
        holders=list(set(holders)) #unique
        if len(holders)==count :
            break
        else:
            count=len(holders)
    gross_holders=list(chain.from_iterable(gross_holders)) #flatten
    c=collections.Counter(gross_holders) #報告書　出現回数
    print(c)
    holders=list(set(holders)) #unique
    holders.sort()
    print(holders)
'''
['三浦恵美', '中島章智', '村上世彰', '株式会社ATRA', '株式会社ATRAホールディングス', '株式会社C&IHoldings', '株式会社エス
グラントコーポレーション', '株式会社オフィスサポート', '株式会社シティインデックスサード', '株式会社シティインデックスホ
スピタリティ', '株式会社シティインデックスホールディングス', '株式会社シティインデックス舞子', '株式会社フォルティス', '
株式会社リビルド', '株式会社レノ', '株式会社南青山不動産', '株式会社ＡＴＲＡ', '福島啓修', '野村幸弘', '野村絢', '鈴木俊
英']
'''