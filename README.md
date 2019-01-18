# XBRL

EDINETのXBRLをPYTHONの勉強がてらいじってみる

2019年3月からEDINETがAPIに対応しXBRLが使いやすくなるためその準備をしていく

XBRLToXLS.py:財務諸表をエクセル型式へ

XBRLは多分4つのファイルより成り立っており

1.拡張子xbrl　報告書インスタンス　財務諸表の値が格納

2.拡張子xsd 語彙層　勘定科目の名称や財務諸表等規則

3._pre　presentation 位置関係を決める

4._lab.xml　company 企業の拡張定義

それぞれのファイルをlxmlでパースして要素を抜き出し一つの表にまとめました。
一部のラベル情報の取得はまだ出来ておらずelement_idで表示されています。

xbrl_d2v_ft__sdv.py

2018年の全上場企業の年次有価証券報告書をローカルフォルダーに持っていたので
各企業のxbrlファイルを抜き出し名詞だけのコーパスを作成、で分かち書きし、
gensimでdoc2vecとfasttextモデルを作成しました。
さらに、fasttextはIDF値をもとめ、SCDV(Sparse Composite Document Vectors)で
図にして表示してみました。別の表現ではfasttextで各有価証券報告書をベクター化し、
それをIDFを使い単語の出現確率を加味すると、各有価証券報告書は300×60次元で
分類されるので、t-SNEを用いて次元圧縮し2次元にして表示する。

xbrl2018_scdv.csv：t-SNEで2次元圧縮した一覧のCSV

tsne_cluster.py：企業数が4000もあると図で表示しても見難い為、近い企業10社を図で表示

benford_law.py：全企業の有価証券報告書からベンフォードの法則で怪しい企業を見つけるために作成
