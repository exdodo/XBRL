# XBRL

EDINETのXBRLをPYTHONの勉強がてらいじってみる。
---注意：過去５年分のEDINETファイル情報は30万以上あり2013年からの年次有価証券報告書だけで1TBを超えます。
ダウンロードするときはディスク容量に注意してください---
2019年3月からEDINETがAPIに対応しXBRLが使いやすくなるためその準備をしていく。

EDINET_API.py:q(-::-) 〆(-：：-) (-：：-)P ｼﾞｪｲｿﾝ
edinetからAPIで前日までの提出書類一覧情報をJSON形式で'xbrldocs.json'を取得する。


select_docIDs_freeword.py:∪・ω・∪doc
EDINET_APIで作成した'xbrldocs.json'から検索用語を部分一致の検索でdocIDsを抽出しxbrlを取得。

select_docIDs_docType.py:👨‍⚕️doc
EDINET_APIで作成した'xbrldocs.json'から年次有価証券報告書を一括してxbrlを取得するため作成。

XbrlParser.py:
XBRLをデーターフレームへ。
IFRS形式で提出している企業の連結情報がとれないので調べてみたら、仕様が糞だった。
参考URL：https://blog.boost-up.net/

XbrlToXls:
エクセル形式にに報告書を変換。連結単独とかぐちゃぐちゃでcontext_refを上手に分ける必要あり

https://www.fsa.go.jp/search/20190228.html
を読むと2019年からはタクソノミが用意されるので開示が義務化されるならいいな
でも過去に渡って適用してくれないと財務分析には使えんな。

toHDFfromXBRL.py:👼怪僧　CM:NASAが選んだデーター形式

・ダウンロードしたXBRLファイルをHDF化するためのプログラム
・テキストは225文字以上だとpytableの警告が出るので空白を削除して先頭から220文字
2014年からの年次有報ではエラーが出なかったが(S1007QLZ,S1007QMD除く)、きちんと写せているか他の書類タイプでできるかは不明

S1007QLZ,S1007QMDのように追番があるものの処理をXBRLParserに必要

