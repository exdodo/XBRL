# XBRL


---注意：過去５年分のEDINETファイル情報は30万以上あり2013年からの年次有価証券報告書だけで1TBを超えます。Disk空き容量注意---
EDINETのXBRLをPYTHONの勉強がてらいじってみる。
2019年3月からEDINETがAPIに対応しXBRLが使いやすくなる。その活用を考える。

EDINET_API.py:q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ

edinetからAPIで前日までの提出書類一覧情報(json形式)をHDF FILEへ書き込む.'xbrldocs.json'もバックアップ用に作成.
個人的にはpowershellでスクリプトを書き、タスクスケジューラーで自動実行している

EDINET_HDF.py：✨　HDF　NASAが選んだデーター形式

EDINET_APIで取得した提出書類一覧をもとにZIP形式のXBRLファイルを直接HDF化。
EDINETコード/文書コード＋追番　の形式でグループ名設定

EdinetXbrlParser.py:

zipファイルをデータフレームへ
downloadしたXBRLをデーターフレームへ。
IFRS形式で提出している企業の連結情報がとれないので調べてみたら、仕様が糞だった（参考URL：https://blog.boost-up.net/）
https://www.fsa.go.jp/search/20190228.html
を読むと2019年からはタクソノミが用意されるようです。
・現状、事業別の対応ができていない。将来仕様変更するつもり。

xbrlUtility.py:

その他の関数

sample1.py：🌞🏊‍ sun+pool

提出者名から共同保有者を検索し、その共同保有者から提出者名を検索。
買収ファンドの中には色々な名義で大量保有報告書を出してくるので一覧が欲しくて作成


