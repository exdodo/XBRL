# XBRL
2019年3月からEDINETがAPIに対応しXBRLが使いやすくなる。その活用を考える。  
バグがあちこちにある糞プログラムです。

EDINET_API.py:q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ  
edinetからAPIで前日までの提出書類一覧情報(json形式)をHDF FILEへ書き込む.'xbrldocs.json'もバックアップ用に作成.  
個人的にはpowershellでスクリプトを書き、タスクスケジューラーで自動実行している

EDINET_HDF.py：🌠🚀　HDF　NASAが選んだデーター形式  
EDINET_APIで取得した提出書類一覧をもとにZIP形式のXBRLファイルをpandas datframeに落とし込みHDF化。  
EDINETコード/year/文書コード＋追番　の形式でグループ名設定  
直近一週間をHDF化するようにしてあります。

EdinetXbrlParser.py:xbrl形式をデータフレームへ

・問題点
1．事業別(セグメント)の対応ができていない。2．文字数制限で220文字まで

xbrlUtility.py:色々な関数

sample1.py：🌞🏊‍ sun+pool  
提出者名から共同保有者を検索し、その共同保有者から提出者名を検索。
買収ファンドの中には色々な名義で大量保有報告書を出してくるので一覧が欲しくて作成

XbrlToXls.py:XLS形式へ  
将来的にはxbrlUtility.pyの関数へ
