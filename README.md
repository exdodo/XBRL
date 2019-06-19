# XBRL


---注意：過去５年分のEDINETファイル情報は30万以上あり2013年からの年次有価証券報告書だけで1TBを超えます。Disk空き容量注意---
EDINETのXBRLをPYTHONの勉強がてらいじってみる。
2019年3月からEDINETがAPIに対応しXBRLが使いやすくなるためその準備をしていく。

EDINET_API.py:q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ

edinetからAPIで前日までの提出書類一覧情報をHDF FILEへ書き込む.
'xbrldocs.json'もバックアップ用に作成

select_docIDs_freeword.py:∪・ω・∪ドッグ

HDF FILEから検索用語を部分一致の検索でdocIDsを抽出しxbrlをdownloadし保存

select_docIDs_docType.py:👨‍⚕️ドク

HDF FILEから年次有価証券報告書を一括してxbrlを取得するため作成。

XbrlParser.py:

downloadしたXBRLをデーターフレームへ。
IFRS形式で提出している企業の連結情報がとれないので調べてみたら、仕様が糞だった（参考URL：https://blog.boost-up.net/）
https://www.fsa.go.jp/search/20190228.html
を読むと2019年からはタクソノミが用意されるので開示が義務化されるならいいが過去に渡って適用してくれないと財務分析には使えんな。

toHDFfromXBRL.py:👼怪僧　CM:NASAが選んだデーター形式

・ダウンロードしたXBRLファイルをHDF化するためのプログラム
・テキストは文字数が一定値を超えるとpytableの警告が出るので空白を削除して先頭から220文字
'S100CUSF'ではエラーが出ることもある（原因不明）
・現状、事業別に対応知っていないので将来仕様変更するつもり
過去5年分26万程のXBRLファイルをHDF化したら242GB。ここまでしてやっと遊べることができる。

sample1.py：🌞🏊‍

提出者名から共同保有者を検索し、その共同保有者から提出者名を検索。
買収ファンドの中には色々な名義で大量保有報告書を出してくるので一覧が欲しくて作成


