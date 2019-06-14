#旧村上ファンドグループをリスト化する
#'株式会社レノ'をキーワードとして書類一覧をもとめ、書類一覧から各書類を調べ、共同保有者を抜き出す。
#次に共同保有者から書類一覧をもとめ共同保有者のリストを充実させていく
import pandas as pd
from itertools import chain
from select_docIDs_freeword import select_docIDs_freeword
import h5py
def collect_holders(docIDs,df_docs,xbrl_path) :
    sr_docs=df_docs.set_index('docID')['edinetCode'] #dataframe to Series print(sr_docs['S100FSTI'])
    holders=[]
    h5File=h5py.File(xbrl_path,'r')
    for doc in docIDs :        
        hdf_group=sr_docs[doc]+'/'+doc+'_000'
        if sr_docs[doc] in h5File.keys() : 
            if doc+'_000' in h5File[sr_docs[doc]].keys() :
                df=pd.read_hdf(xbrl_path,key=hdf_group)
                holders.append((df['amount'][df['element_id']=='jplvh_cor_FilerNameInJapaneseDEI']).tolist())    
    h5File.close()
    holders=list(chain.from_iterable(holders))  #flatten
    holders=list(set(holders)) #unique
    return holders
if __name__=='__main__':
    xbrl_path='d:\\Data\\hdf\\xbrl.h5' #xbrlをHDF化したファイルの保存先
    edinetDocs='D:\\data\\xbrl\\edinetxbrl.h5' #書類一覧の保存先
    df_docs=pd.read_hdf(edinetDocs,'index/edinetdocs')
    #docIDs=['S100FXF3','S100FDM4'] #2019-05-17  株式会社レノ   変更報告書
    holders=['株式会社レノ']
    count=0
    for i in range(100):
        docIDs=select_docIDs_freeword(df_docs,holders,['filerName'])
        holders=collect_holders(docIDs,df_docs,xbrl_path)
        if len(holders)==count :
            break
        else:
            count=len(holders)
    holders.sort()
    print(holders)
'''
['株式会社シティインデックス舞子', '株式会社オフィスサポート', '野村絢', '株式会社レノ', '株式会社エスグラントコーポレー
ション', '中島章智', '株式会社ＡＴＲＡ', '株式会社リビルド', '株式会社南青山不動産', '鈴木俊英', '株式会社シティインデッ
クスホスピタリティ', '株式会社フォルティス', '三浦恵美', '村上世彰', '株式会社C&IHoldings', '株式会社シティインデックスホ
ールディングス']
'''