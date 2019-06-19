# XBRL


---注意：過去５年分のEDINETファイル情報は30万以上あり2013年からの年次有価証券報告書だけで1TBを超えます。Disk空き容量注意---
EDINETのXBRLをPYTHONの勉強がてらいじってみる。
2019年3月からEDINETがAPIに対応しXBRLが使いやすくなる。その活用を考える。

EDINET_API.py:q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ

edinetからAPIで前日までの提出書類一覧情報(json形式)をHDF FILEへ書き込む.'xbrldocs.json'もバックアップ用に作成.
個人的にはpowershellでスクリプトを書き、タスクスケジューラーで自動実行している

select_docIDs_freeword.py:∪・ω・∪ドッグ

HDF FILEから検索用語を部分一致の検索でdocIDsを抽出しxbrlをdownloadし保存
他にも様々な関数を追加し将来は名前を変えてクラス化したい。

select_docIDs_docType.py:👨‍⚕️ドク

年次有価証券報告書を一括してdownloadしxbrlを取得するため作成。
ダウンロードには何日もかかるはず（私は手元にxbrl　fileを持っていたので全実行はしていない）。
実行には要注意（ネットにも負担、ＤＩＳＫ容量も必要）。

EdinetXbrlParser.py:

downloadしたXBRLをデーターフレームへ。
IFRS形式で提出している企業の連結情報がとれないので調べてみたら、仕様が糞だった（参考URL：https://blog.boost-up.net/）
https://www.fsa.go.jp/search/20190228.html
を読むと2019年からはタクソノミが用意されるようです。
・現状、事業別の対応ができていない。将来仕様変更するつもり。

toHDFfromXBRL.py:👼怪僧　CM:NASAが選んだデーター形式

・ダウンロードしたXBRLファイルをHDF化するためのプログラム
・テキストは文字数が一定値を超えるとpytableの警告が出るので空白を削除して先頭から220文字
・1つのXBRLから複数の財務諸表を作成する追番に対応
'S100CUSF'ではエラーが出ることもある（原因不明）
過去5年分26万程のXBRLファイルをHDF化したら242GB。ここまでしてやっとデータで遊ぶことができる。

sample1.py：🌞🏊‍ sun+pool

提出者名から共同保有者を検索し、その共同保有者から提出者名を検索。
買収ファンドの中には色々な名義で大量保有報告書を出してくるので一覧が欲しくて作成


