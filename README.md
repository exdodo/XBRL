# XBRL

EDINETのXBRLをPYTHONの勉強がてらいじってみる

2019年3月からEDINETがAPIに対応しXBRLが使いやすくなるためその準備をしていく

XBRLToXLS.py:財務諸表をエクセル型式へ

XBRLは多分4つのファイルより成り立っており

1.拡張子xbrl　報告書インスタンス　財務諸表の値が格納

2.拡張子xsd 語彙層　勘定科目の名称や財務諸表等規則

3._pre　presentation 位置関係を決める

4._lab.xml　company 企業の拡張定義

それぞれのファイルをlxmlでパースして要素を抜き出し一つの表にまとめました。一部のラベル情報の取得はまだ出来ておらずelement_idで表示されています

doc2vec_xbrl.py 有価証券報告書をdoc2vec

2017年の全上場企業の年次有価証券報告書をローカルフォルダーに持っていたので
各企業のxbrlファイルを抜き出し文字情報だけのコーパスを作成、
mecabで分かち書きし、gensimでword2vecモデルを作成しました

doc2vec_test.py

作成したword2vecモデルを使用したテスト

doc2vec_cluster

k Means法（どういった方法か知らない）で分類分け

タグ名をつけたがあまり面白くないな。ウィキペディアのword2vecでやれば小売とか運輸とかでないかと予想したが企業名が出てきてしまう。
日本企業を2分類すると三越伊勢丹か旭硝子というのは少し面白い。

benford_law.py

全企業の有価証券報告書からベンフォードの法則で怪しい企業を見つけるために作成

今後の目標

・会社四季報CD-ROMを四半期毎に購入しているので買わなくて済むようにしたいなー

・有価証券報告書の文字情報は自然言語処理の発達でもっと活用できないかな

・優先順位は低いが年次有価証券報告書以外のXBRLやTDNETへの対応
