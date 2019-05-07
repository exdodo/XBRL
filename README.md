# XBRL

EDINETのXBRLをPYTHONの勉強がてらいじってみる

2019年3月からEDINETがAPIに対応しXBRLが使いやすくなるためその準備をしていく

EDINET_API.py: 

edinetからAPIで前日までの提出書類一覧情報をJSON形式で'xbrldocs.json'を取得する

edinet_api_select.py: 

EDINET_APIで作成した'xbrldocs.json'から部分一致の検索でdocIDsを抽出しxbrlをフォルダー分けして取得

XbrlToXls.py:

XBRLをエクセル型式へ
docIDsからXBRLを探してexcel形式に変換

xbrl_d2v_ft__sdv.py

2018年の全上場企業の年次有価証券報告書をローカルフォルダーに持っていたので
各企業のxbrlファイルを抜き出し名詞だけのコーパスを作成、で分かち書きし、
gensimでdoc2vecとfasttextモデルを作成しました。
さらに、fasttextはIDF値をもとめ、SCDV(Sparse Composite Document Vectors)で
図にして表示してみました。別の表現ではfasttextで各有価証券報告書をベクター化し、
それをIDFを使い単語の出現確率を加味すると、各有価証券報告書は300×60次元で
分類されるので、t-SNEを用いて次元圧縮し2次元にして表示する。

xbrl2018_scdv.csv：

t-SNEで2次元圧縮した一覧のCSV

tsne_cluster.py：

企業数が4000もあると図で表示しても見難い為、近い企業10社を図で表示

benford_law.py：

全企業の有価証券報告書からベンフォードの法則で怪しい企業を見つけるために作成


