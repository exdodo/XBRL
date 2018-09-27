# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 14:55:51 2018

@author: Yusuke
有価証券報告書でdoc2vec
参考URL
https://srbrnote.work/archives/1315
https://qiita.com/akira_/items/f9bb46cad6834da32367
http://db-event.jpn.org/deim2017/papers/100.pdf
https://deepage.net/machine_learning/2017/01/08/doc2vec.html
https://srbrnote.work/archives/1315
https://www.fsa.go.jp/search/20180228.html
http://naron-a.hatenablog.com/entry/2018/01/04/141647
"""

from lxml import objectify as ET
import os
import re
import gensim
import subprocess
from pathlib import Path
import shutil

def parse_file(file_path):
    if  os.path.isfile(file_path) :
        with open(file_path,'rt', encoding='utf-8') as fxbrl:
            parse_xbrl(fxbrl,file_path)
            
def parse_xbrl(fxbrl,file_path):   
    """
    XBRLからtextを取り出す
    """    
    ET_xbrl=ET.parse(fxbrl)
    root=ET_xbrl.getroot()
    #nslist=['jpcrp_cor:*','jpdei_cor:*','jppfs_cor:*']
    nslist=[ i+':*' for i in root.nsmap ]
    xbrldoc=''
    for ele in nslist :
        anchors=root.findall(ele, root.nsmap)
        for anchor in anchors:
            #ele=ele.replace(':*','_')
            #element_id=ele.replace('//','')+anchor.tag.split('}')[1]
            if anchor.attrib.get('contextRef')=='FilingDateInstant' :
            #if 'Overview' in element_id : #概要分だけ
                anc=delete_tag(anchor.text) #tagを正規表現で外す
                if anc is not None : 
                    xbrldoc=xbrldoc+anc
    #空行を削除
    xbrldoc='\n'.join(filter(lambda x: x.strip(), xbrldoc.split('\n')))
    #数字記号削除
    xbrldoc=re.sub(r'[0-9０-９,，.．]+','',xbrldoc)
    #空白が2個以上－＞1個の空白
    xbrldoc=re.sub(r's/\s+/',' ',xbrldoc)
    #記号削除
    xbrldoc=re.sub(r'[!-/:-@[-`{-~]', '',xbrldoc)
    xbrldoc=re.sub(u'[︰-＠]', '',xbrldoc)

    #save \doc 
    base_name=os.path.basename(file_path)
    base_name=base_name.replace('.xbrl','.txt')
    os.makedirs('doc', exist_ok=True)
    f = open(os.path.join('doc', base_name), 'w',encoding='utf-8') # 書き込みモードで開く
    f.write(xbrldoc) # 引数の文字列をファイルに書き込む
    f.close() # ファイルを閉じる
           
    #分かち書き処理 save /wakati
    os.makedirs('wakati', exist_ok=True)
    print(base_name)
    wakati_in=os.path.join('doc', base_name)
    wakati_out=os.path.join('wakati', base_name)
    subwakati = 'mecab -O wakati '+wakati_in+' -o '+wakati_out+' -b 10000000'
    subprocess.check_call(subwakati,shell=True)
            
def delete_tag(anchor_text) : #tagを正規表現で外す
    text=str(anchor_text)
    if '<' in text :    
        p = re.compile(r"<[^>]*?>")
        return p.sub("", text)
    return anchor_text


def word2vec_test1(model,sample) : #類義語発見
    results = model.wv.most_similar(positive=[sample])
    for result in results:
        print(result)    
    
if __name__=='__main__':
    dbpath=r'C:\Users\Yusuke.SERVICE\Documents\DATABASE\xbrl\download\EDINET\2017'
    dir_model=r'C:\\Users\\Yusuke.SERVICE\\Documents\\DATABASE\\word2vecmodel\\'
    """ファイル名を解析"""
        #https://srbrnote.work/archives/1315
        # 2018年版EDINETタクソノミの公表について https://www.fsa.go.jp/search/20180228.html
        # 報告書インスタンス作成ガイドライン
        # 4-2-4 XBRL インスタンスファイル
        # jp{府令略号}{様式番号}-{報告書略号}-{報告書連番(3 桁)}_{EDINETコード又はファンドコード}-
        # {追番(3 桁)}_{報告対象期間期末日|報告義務発生日}_{報告書提出回数(2 桁)}_{報告書提出日}.xbrl
        # 0         1         2         3         4         5         6
        # 0123456789012345678901234567890123456789012345678901234567890
        # jpcrp030000-asr-001_E00000-000_2017-03-31_01_2017-06-29.xbrl
    print ('Creating 分かち書き処理 ')
    for file_path in Path(dbpath).glob('**/*.xbrl'):
        dirpath, file_name = os.path.split(file_path)
        s=os.path.basename(file_path)
        edinetcode=s[20:26] #EDINET Code
        edinetcheck=s[42:44] #報告書提出回数
        edinetasr=s[12:15] #年次有価証券報告書
        #print(edinetcheck)
        if 'AuditDoc' not in dirpath and \
            'E' in edinetcode and \
            '1' in edinetcheck and \
            'asr' in edinetasr  :  
            parse_file(file_path)
    
    #doc2vec
    print('Creating doc2vecmodel') 
    trainings = []
    cwd=os.getcwd()
    for file_path in Path(cwd+'\\wakati').glob('**/*.txt'):
        
        with open(file_path, 'r',encoding='utf-8') as wafile:
            text = (wafile.read()).replace('\n','')
            text = text.replace('\r','')
            text = text.split(' ')
            tag_name = os.path.basename(file_path)
            tag_name= tag_name[20:26]
            print(tag_name)
            tag=[tag_name]
            sentence=gensim.models.doc2vec.TaggedDocument(words=text, tags=tag)
            #sentence = [gensim.models.doc2vec.TaggedDocument(words = data,tags = [i]) for i,data in enumerate(textls)] 
            #sentence=[gensim.models.doc2vec.TaggedDocument(doc, [i]) for i, doc in enumerate(wafile)]
            trainings.append(sentence)            
       
    model = gensim.models.Doc2Vec(documents=trainings, vector_size=100,
                            alpha=0.0025,dm=1,
                            min_alpha=0.000001,
                            window=5, min_count=1)
    model.save(dir_model+'xbrl_doc2vecmodel')
    #後処理　フォルダー(\wakati \doc)削除
    shutil.rmtree('wakati')
    shutil.rmtree('doc')