# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 13:38:26 2019
参考図書
『金融データ解析の基礎』　金明哲
『簡単にできるXBRL　基礎編』　池田智之
『会計人のためのXBRL入門』坂上学
https://qiita.com/shima_x/items/58634a838ab37c3607b5
https://www.meganii.com/blog/2017/06/18/what-is-edinet-xbrl/
http://orangain.hatenablog.com/entry/namespaces-in-xpath
http://www.fsa.go.jp/search/20170228.html
https://srbrnote.work/archives/1315
https://srbrnote.work/archives/1768
https://note.nkmk.me pandas関係
https://www.fsa.go.jp/search/20130301/02_a1.pdf
https://lxml.de/4.2/tutorial.html
https://shotanuki.com/beautifulsoup%E3%81%A7%E8%A4%87%E6%95%B0%E3%82%BF%E3%82%B0%E3%82%92%E5%90%8C%E6%99%82%E3%81%AB%E6%8C%87%E5%AE%9A%E3%81%99%E3%82%8B/
https://blog.boost-up.net/
EDINETタクソノミは© Copyright 2014 Financial Services Agency, The Japanese Government
# 2018年版EDINETタクソノミの公表について https://www.fsa.go.jp/search/20180228.html
# 報告書インスタンス作成ガイドライン
# 4-2-4 XBRL インスタンスファイル
# jp{府令略号}{様式番号}-{報告書略号}-{報告書連番(3 桁)}_{EDINETコード又はファンドコード}-
# {追番(3 桁)}_{報告対象期間期末日|報告義務発生日}_{報告書提出回数(2 桁)}_{報告書提出日}.xbrl
# 0         1         2         3         4         5         6
# 0123456789012345678901234567890123456789012345678901234567890
# jpcrp030000-asr-001_E00000-000_2017-03-31_01_2017-06-29.xbrl
#20180331以降 2018
@author: Yusuke
"""
#from lxml import objectify as ET
from lxml import etree as ET
#import os 
import pandas as pd
from collections import defaultdict
import re
import glob
import pickle
from tqdm import tqdm
from select_docIDs_freeword import download_xbrl,column_shape
from pathlib import Path
def get_label_links(xsd_file):
    #xsd_file = xbrlfile.replace('.xbrl','.xsd')
    labels=ET.parse(xsd_file)
    root= labels.getroot()
    ns= root.nsmap    
    linkbase = labels.findall('.//link:linkbaseRef',ns)
    xsd_links = []    
    for link_node in linkbase:
        link_href = link_node.attrib['{'+ns['xlink']+'}href']
        if '_lab.xml' in link_href and 'http://' in link_href:
            xsd_links.append(link_href)    
    return xsd_links

def get_label1(link_item):
    # get common taxonomy from link_base
    link_dict = defaultdict(list)
    #link_base file名から頭文字を抽出 element_id接頭語決定
    #index=os.path.basename(link_item).find('_')
    #prefix=os.path.basename(link_item)[0:index]+'_cor_'
    index=str(Path(link_item).name).find('_')   
    prefix=str(Path(link_item).name)[0:index]+'_cor_'
    #print(prefix)
    #https://stackoverflow.com/questions/10457564/error-failed-to-load-external-entity-when-using-python-lxml
    parser = ET.XMLParser(recover=True)
    labels=ET.parse(link_item,parser)
    root= labels.getroot()
    ns= root.nsmap    
    ns['xml']='http://www.w3.org/XML/1998/namespace'    
    for link_label in labels.findall('.//link:label',ns) :        
        if 'id' in link_label.attrib:            
            link_dict['lab_type'].append(link_label.attrib['{'+ns['xlink']+'}type'])
            link_dict['label'].append(link_label.attrib['{'+ns['xlink']+'}label'])
            link_dict['role'].append(link_label.attrib['{'+ns['xlink']+'}role'])
            link_dict['lang'].append(link_label.attrib['{'+ns['xml']+'}lang'])
            element_id=link_label.attrib['id'].replace('label_',prefix)
            link_dict['element_id'].append(element_id)
            link_dict['lab_name']=link_label.attrib['id']
            link_dict['label_string'].append(link_label.text)        
    return pd.DataFrame(link_dict)

def parse_companyxml(company_file) :   
    company_label = defaultdict(list)
    company_loc=defaultdict(list)
    company_arc=defaultdict(list)
    #loc->label->labelArc
    with open(company_file,'rt', encoding='utf-8') as comp_file :
        labels = ET.parse(comp_file)    
        root=labels.getroot()
        ns=root.nsmap
        ns['xml']='http://www.w3.org/XML/1998/namespace'
        #jpcrp030000-asr-001_E01737-000_2018-03-31_01_2018-06-29_lab.xml
        #prefix=os.path.basename(company_file)[0:15]+'_'+\
        #        os.path.basename(company_file)[20:30]+'_'
        prefix=str(Path(company_file).name)[0:15]+'_'+str(Path(company_file).name)[20:30]+'_'
        for child in labels.findall('.//link:labelLink',ns) :
            for grand_child in child.iter():                
                if grand_child.tag=='{http://www.xbrl.org/2003/linkbase}loc':
                    #company_loc['type'].append(grand_child.attrib['{'+ns['xlink']+'}type'])
                    company_loc['href'].append(grand_child.attrib['{'+ns['xlink']+'}href'])
                    href=grand_child.attrib['{'+ns['xlink']+'}href']
                    element_id=href.split('#')[1]
                    #company_loc['element_id'].append(element_id)
                    #company_loc['label'].append(grand_child.attrib['{'+ns['xlink']+'}label'])
                    #company_loc['title'].append(grand_child.attrib['{'+ns['xlink']+'}title'])
                    #company_loc['serial_num'].append(serial_num)                
                if grand_child.tag=='{http://www.xbrl.org/2003/linkbase}label':
                    element_id=grand_child.attrib['{'+ns['xlink']+'}label']
                    element_id=element_id.replace('label_',prefix)
                    company_label['lab_type'].append(grand_child.attrib['{'+ns['xlink']+'}type'])
                    company_label['label'].append(grand_child.attrib['{'+ns['xlink']+'}label'])
                    #company_label['role'].append(grand_child.attrib['{'+ns['xlink']+'}role'])
                    company_label['lang'].append(grand_child.attrib['{'+ns['xml']+'}lang'])
                    company_label['element_id'].append( element_id )
                    #company_label['lab_name']=grand_child.attrib['{'+ns['xlink']+'}label']
                    #label_dict['lab_name']=label_node.attrib['id']
                    company_label['label_string'].append( grand_child.text)                    
                    #company_label['serial_num'].append(serial_num)
                if grand_child.tag=='{http://www.xbrl.org/2003/linkbase}labelArc':
                    company_arc['from_element_id'].append(grand_child.attrib['{'+ns['xlink']+'}from'])
                    company_arc['to_element_id'].append(grand_child.attrib['{'+ns['xlink']+'}to'])
                    company_arc['elemnt_id'].append( element_id ) #label から流用            
        return pd.DataFrame(company_label),pd.DataFrame(company_arc) 

def parse_facts(fxbrl):   
    """
    return(element_id, amount, context_ref, unit_ref, decimals)
    element_id:識別子
    amount:値　金額など
    contextRef:コンテキスト(連結・単体、相対年度、期間・時点)の指定
    unit_ref:通貨の種類
    decimals:数値の精度 
    classif:用途　区分
    scenario:ディメンション　事業所別報告などに使う        
    """
    facts_dict = defaultdict(list)
    parser = ET.XMLParser(recover=True)
    ET_xbrl=ET.parse(fxbrl,parser)   
    root=ET_xbrl.getroot()
    nslist=[ i+':*' for i in root.nsmap ]
    #print(nslist)
    for ele in nslist :
        anchors=root.findall(ele, root.nsmap)
        for anchor in anchors:
            #if len(anchor.tag.split('}')[1])<1 :
            #    continue
            ele=ele.replace(':*','_')
            element_id=ele.replace('//','')+anchor.tag.split('}')[1]
            
            facts_dict['element_id'].append( element_id )
            #tagを正規表現で外す
            text=str(anchor.text)
            if '<' in text :    
                p = re.compile(r"<[^>]*?>")
                anc= p.sub("", text)
            else: 
                anc=anchor.text
            #
            facts_dict['amount'].append( anc )
            facts_dict['context_ref'].append( anchor.attrib.get('contextRef') )
            facts_dict['unit_ref'].append( anchor.attrib.get('unitRef') )
            facts_dict['decimals'].append( anchor.attrib.get('decimals'))                               
    return pd.DataFrame(facts_dict)

def parse_type(xmlfile):
    """
    return(element_id, label_string, lang, label_role, href)
    element_id:
    role_id
    order:要素の順番
    fromElementalID:親要素名
    toElementalID:子要素名
    """
    type_dict_loc = defaultdict(list)
    type_dict_arc = defaultdict(list)
    types = ET.parse(xmlfile)
    root=types.getroot()
    ns=root.nsmap
    serial_num=0
    for child in types.findall('.//link:presentationLink',ns):
        role_id=child.attrib['{'+ns['xlink']+'}role']                      
        for grand_child in child.iter() :
            #if isinstance(grand_child.tag, str):
            if grand_child.tag== '{http://www.xbrl.org/2003/linkbase}loc':
                type_dict_loc['role_id'].append(role_id.split('/')[-1])
                #type_dict_loc['tag_name'].append(['{'+ns['xlink']+'}type'])
                type_dict_loc['type_label'].append(grand_child.attrib['{'+ns['xlink']+'}label'])                    
                href_str=grand_child.attrib['{'+ns['xlink']+'}href']
                href_str=href_str.split('#')[1]
                #if href_str[-2]=='_': #末尾の数字一桁を外す
                #    href_str=href_str[:-2]
                #type_dict_loc['element_id'].append(href_str)
                prefix=href_str.rsplit('_',1)[0]+'_'                      
                type_dict_loc['prefix'].append(prefix)
                type_dict_loc['serial_num'].append(serial_num)
            if grand_child.tag== '{http://www.xbrl.org/2003/linkbase}presentationArc':
                #type_dict_arc['tag_name'].append(['{'+ns['xlink']+'}arc'])
                type_dict_arc['from_element_id'].append(grand_child.attrib['{'+ns['xlink']+'}from'])
                type_dict_arc['to_element_id'].append(grand_child.attrib['{'+ns['xlink']+'}to'])
                type_dict_arc['order'].append(grand_child.attrib['order'])
                type_dict_arc['serial_num'].append(serial_num)
                serial_num+=1
    #複数のlink:locへ presentationArcまとめる
    df_loc=pd.DataFrame(type_dict_loc)
    df_arc=pd.DataFrame(type_dict_arc)
    df_type=pd.merge(df_loc,df_arc,on=['serial_num'],how='inner')
    #prefix追加 element_id from_e to_e
    df_type['element_id']=df_type['prefix']+df_type['type_label']    
    df_type_prefix=df_type[['prefix','type_label']]
    df_type_prefix=df_type_prefix.set_index('type_label')  
    dict_type_prefix=df_type_prefix.to_dict()
    df_type['from_prefix']=df_type['from_element_id'].map(dict_type_prefix['prefix'])
    df_type['to_prefix']=df_type['to_element_id'].map(dict_type_prefix['prefix'])
    df_type['from_element_id']=df_type['from_prefix']+df_type['from_element_id']
    df_type['to_element_id']=df_type['to_prefix']+df_type['to_element_id']
    #不要列削除
    df_type=df_type.drop(columns=['serial_num','prefix','type_label'])    
    return df_type    

def get_xml_attrib_value( node, attrib):
    if attrib in node.attrib.keys():
        return node.attrib[attrib]
    else:
        return None

def seek_from_docIDs(save_path,docIDs):
    '''
    docIDからXBRLファイルのディレクトリーリストを取得
    
    '''    
    df_json=pd.read_json('xbrldocs.json',dtype='object') #5年分約30万行
    df_json = column_shape(df_json) #dataframeを推敲    
    download_xbrl(df_json,save_path,docIDs) #XBRLファイルをなければ取得
    dirls=[]
    for docID in tqdm(docIDs):                 
        #docIDsからdataframe 抽出
        if docID in df_json['docID'].to_list()  : #削除ドキュメント対策
            sDate=df_json[df_json['docID']==docID].submitDateTime.to_list()[0]
            file_dir=save_path+'\\'+str(int(sDate[0:4]))+'\\'+\
                str(int(sDate[5:7]))+'\\'+str(int(sDate[8:10]))+'\\'\
                +docID+'\\'+docID+'\\XBRL\\PublicDoc'
            dirls.append(file_dir)
    return dirls

def xbrl_to_dataframe(xbrlfile) :
    '''
    taxxomyをその都度読むとネットワークに負荷を掛けるので過去に読んだ事のあるものは'label'
    フォルダーに保存してそこから読み出す
    '''
    #.xsd,_lab.xml,_pre.xml,_cal.xmlを探す
    
    #if not os.path.isdir('label') :
    #    os.mkdir('label')
    if not Path('label').exists():
        Path('label').mkdir()  
    df_label = pd.DataFrame(index=[], columns=[])
    linklogs=linklog()  #過去の読込日を呼び出す     
    xsd_file=search_filename(xbrlfile,'.xsd')
    for link_item in get_label_links(xsd_file) :
        #label_name, xml = os.path.splitext(os.path.basename(link_item))
        label_name=Path(link_item).stem
        label_name='./label/'+label_name+'.json'
        #if os.path.exists(label_name) :
        if Path(label_name).exists():
            df_label=df_label.append(pd.read_json(label_name))       
        elif link_item not in linklogs :
            temp_df=get_label1(link_item)            
            df_label=df_label.append(temp_df)            
            linklogs.append(link_item)
            temp_df.reset_index(drop=True, inplace=True) #index振り直し 
            temp_df.to_json(label_name)
            #df_label=df_label.drop_duplicates(subset=['element_id', 'label_string']) #重複削除
    with open('linklog.pkl','wb') as log_file:
        pickle.dump(linklogs, log_file)
    company_file=search_filename(xbrlfile,'_lab.xml')
    if company_file!=None :        
        df_comp_label,df_comp_type=parse_companyxml(company_file)        
        df_all_label=pd.concat([df_comp_label,df_label],sort=False)        
    else :
        df_all_label=df_label
    df_facts=parse_facts(xbrlfile)
    df_type=parse_type(search_filename(xbrlfile,'_pre.xml'))
    df_xbrl=merge_df(df_all_label,df_facts,df_type)
    if 'from_element_id' in df_xbrl.columns : #日本語ラベル追加
        df_all_label.reset_index(drop=True, inplace=True)
        add_label_string(df_xbrl,df_all_label)
    #df_all_label.to_excel('all_label.xls',encoding='cp938')
    df_xbrl=df_xbrl.dropna(subset=['amount']) #amount空　削除
    return df_xbrl
def search_filename(xbrlfile,sType) : #sType='.xsd,','_pre.xml','_lab.xml'
    glob_string='*'+sType
    if Path(xbrlfile.replace('.xbrl',sType)).exists() :        
        return xbrlfile.replace('.xbrl',sType)
    else :
       p=Path(xbrlfile)
       file_names=[s for s in p.parent.glob(glob_string) if re.search(p.name[0:30], str(s))]
       if file_names==[] :
           return None 
       else:
           return str(file_names[0])
def merge_df(df_all_label,df_facts,df_type) :
    #マージ 
    df_facts['amount']=df_facts['amount'].str[:3000]
    df_merge=pd.merge(df_all_label,df_facts,on=['element_id'],how='inner') #型作成    
    #df_merge.to_excel('labelfacts.xls',encoding='cp938')    
    df_xbrl=pd.merge(df_type,df_merge,on=['element_id'],how='outer') #数値埋め込み
    return df_xbrl 
def linklog():
    #読込linkを覚えておく
    linklogs=[] 
    if Path('linklog.pkl').exists() :
        with open('linklog.pkl','rb') as log:
            linklogs=pickle.load(log)
    return linklogs

def add_label_string(df_xbrl,df_label) :
    #add_label_string(df_merge,df_all_label) #from_element_idなどへlabel_string追加
    df_label=df_label[['element_id','label_string']]
    df_label=df_label.set_index('element_id')
    dic_label=df_label.to_dict()
    #df_xbrl['label_string']=df_xbrl['element_id'].map(dic_label['label_string'])
    df_xbrl['from_string']=df_xbrl['from_element_id'].map(dic_label['label_string'])
    df_xbrl['to_string']=df_xbrl['to_element_id'].map(dic_label['label_string'])
    return df_xbrl

if __name__=='__main__':      
    #初期化したいときは'linklog.pkl','labelフォルダー'削除
    save_path='d:\\data\\xbrl\\download\\edinet' #有報キャッチャー自分用
    hdf_path='d:\\data\\xbrl\\xbrl.h5' #xbrl 書類一覧HDF　保存先
    #test
    #save_path='d:\\data\\xbrl\\temp' #xbrl fileの基幹フォルダー
    #hdf_path='d:\\data\\xbrl\\edinetxbrl.h5' #xbrl 書類一覧HDF　保存先
    
    
    docIDs=['S100EKNV','S100EUTL','S100D6OS','S100DKFI','S100DJ2G',]
    #docIDs=['S100DAZ4']#['S100DJ2G',]#['S100DAZ4']
    #確認書以外はOK 確認書はxbrlがない
    filenames=[]
    dirls=seek_from_docIDs(save_path,docIDs)    
    for dir_text in dirls: 
        for file_name in glob.glob(dir_text+'\\*.xbrl') :
            filenames.append(file_name)#ここにXBRL File 指定
    if filenames :
        for xbrlfile in filenames :
            df_xbrl=xbrl_to_dataframe(xbrlfile)
            #print(df_xbrl)        
            df_xbrl['amount']=df_xbrl['amount'].str[:3000] #excel cell 文字数制限   
            #xlsname, ext = os.path.splitext(os.path.basename(xbrlfile))
            xlsname=Path(xbrlfile).stem
            print(xlsname)
            df_xbrl.to_excel(xlsname+'.xls',encoding='cp938')            
    else : print('xbrl file 見つかりません')
    
