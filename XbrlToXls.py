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
#'jpcrp_cor'財務　'jplvh_cor'大量保有報告書
#from lxml import objectify as ET
from lxml import etree as ET
import os
import pandas as pd
from collections import defaultdict
import re
import glob
from pathlib import Path
def get_label_links(xrdlfile):
    xsd_file = xrdlfile.replace('.xbrl','.xsd')
    #xml = requests.get(xsd_file)
    #labels = ET.fromstring(xml.content)
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
    prefix=os.path.basename(link_item)[0:6]+'cor_'
    #print(prefix)
    #https://stackoverflow.com/questions/10457564/error-failed-to-load-external-entity-when-using-python-lxml
    parser = ET.XMLParser(recover=True)
    labels=ET.parse(link_item,parser)
    root= labels.getroot()
    ns= root.nsmap    
    ns['xml']='http://www.w3.org/XML/1998/namespace'    
    for link_label in labels.findall('.//link:label',ns) :        
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
    label_dict = defaultdict(list)
    with open(company_file,'rt', encoding='utf-8') as comp_file :
        labels = ET.parse(comp_file)    
        root=labels.getroot()
        ns=root.nsmap
        ns['xml']='http://www.w3.org/XML/1998/namespace'
        #jpcrp030000-asr-001_E01737-000_2018-03-31_01_2018-06-29_lab.xml
        prefix=os.path.basename(company_file)[0:15]+'_'+\
                os.path.basename(company_file)[20:30]+'_'
        #print(prefix) #jpcrp030000-asr_E01737-000_
        for label_node in labels.findall('.//link:label',ns):            
            element_id=label_node.attrib['id'].replace('label_',prefix)
            label_dict['lab_type'].append(label_node.attrib['{'+ns['xlink']+'}type'])
            label_dict['label'].append(label_node.attrib['{'+ns['xlink']+'}label'])
            label_dict['role'].append(label_node.attrib['{'+ns['xlink']+'}role'])
            label_dict['lang'].append(label_node.attrib['{'+ns['xml']+'}lang'])
            label_dict['element_id'].append( element_id )
            label_dict['lab_name']=label_node.attrib['id']
            label_dict['label_string'].append( label_node.text)            
    return pd.DataFrame(label_dict)        

def parse_facts(fxbrl):   
    """
    return(element_id, amount, context_ref, unit_ref, decimals)
    element_id:識別子
    amount:値　金額など
    contextRef:コンテキスト(連結・単体、相対年度、期間・時点)の指定
    unit_ref:通貨の種類
    decimals:数値の精度 
    classif:用途　区分       
    """
    facts_dict = defaultdict(list)    
    ET_xbrl=ET.parse(fxbrl)
    root=ET_xbrl.getroot()
    nslist=[ i+':*' for i in root.nsmap ]
    #print(nslist)
    for ele in nslist :
        anchors=root.findall(ele, root.nsmap)
        for anchor in anchors:
            if len(anchor.tag.split('}')[1])<1 :
                continue
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
    return pd.DataFrame(facts_dict)#result['facts']

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
    df_type['element_id']=df_type['prefix']+df_type['type_label']
    df_type=df_type.drop(columns=['serial_num','prefix','type_label'] )      
    return df_type

def get_xml_attrib_value( node, attrib):
    if attrib in node.attrib.keys():
        return node.attrib[attrib]
    else:
        return None

def seek_from_docID(save_path,docIDs):
    df_json=pd.read_json('xbrldocs.json') #5年分約30万行 
    df_json['dtDateTime']=pd.to_datetime(df_json['submitDateTime']) #obj to datetime
    df_json['dtDate']=df_json['dtDateTime'].dt.date #時刻を丸める　normalize round resample date_range
    dirls=[]
    for docID in docIDs:        
        #docIDsからdataframe 抽出
        sDate=df_json[df_json['docID']==docID].submitDateTime.to_list()[0]
        file_dir=save_path+'\\'+str(int(sDate[0:4]))+'\\'+str(int(sDate[5:7]))+\
                '\\'+str(int(sDate[8:10]))+'\\'+docID+'\\'+docID
        file_dir=file_dir+'\\XBRL\\PublicDoc'
        dirls.append(file_dir)
    return dirls
def ToExcel_finace_sheets(df_fs,filename) :
    df_fs=df_fs[df_fs['context_ref'].str.contains('Current')]
    #財務諸表 list作成
    ls=df_fs['role_id'].values.tolist()
    rols=list(set(ls))
    #print(rols)    
    with pd.ExcelWriter(mandatory_year(filename) + '_' + edinet_code(filename) + '.xlsx') as writer:
        for rol in rols:
            df_name='df_'+rol
            df_name = df_fs[df_fs['role_id'] == rol]
            df_name=df_name.sort_values(['from_element_id','order'])
            df_name=df_name[['from_string', 'label_string', 'amount','context_ref']]            
            df_name.to_excel(writer, sheet_name=rol[4:34]) #Title 31文字以内
    
def add_role_label(df,df_label) : #from_elemetid to role_label
    df_label=df_label[['element_id','label_string']]
    df_label=df_label.set_index('element_id')
    dic_label=df_label.to_dict()
    df['from_string']=df['from_element_id'].map(dic_label['label_string'])
    df['to_string']=df['to_element_id'].map(dic_label['label_string'])
    return df
def unique_element(df,ele):
    df=df.drop_duplicates(subset=ele)
    uq_ls=df[ele].tolist()
    return uq_ls

def elementToLabel(element_item,df_label) :
    if element_item in df_label.index :
        element_label=df_label['label_string'].loc[element_item]
    else :
        element_label=element_item    
    return element_label
    
def mandatory_year(filename):
    
    #https://srbrnote.work/archives/1315
    # 2018年版EDINETタクソノミの公表について https://www.fsa.go.jp/search/20180228.html
    # 報告書インスタンス作成ガイドライン
    # 4-2-4 XBRL インスタンスファイル
    # jp{府令略号}{様式番号}-{報告書略号}-{報告書連番(3 桁)}_{EDINETコード又はファンドコード}-
    # {追番(3 桁)}_{報告対象期間期末日|報告義務発生日}_{報告書提出回数(2 桁)}_{報告書提出日}.xbrl
    # 0         1         2         3         4         5         6
    # 0123456789012345678901234567890123456789012345678901234567890
    # jpcrp030000-asr-001_E00000-000_2017-03-31_01_2017-06-29.xbrl
    #20180331以降 2018
    #20170331
    #20160331
    #20150331
    #20140331
    #20130331
    
    p=Path(filename)
    return p.name[-29:-25] # 報告書義務発生年

def edinet_code(filename):
    p=Path(filename)
    return p.name[20:26]
if __name__=='__main__':    
    save_path='d:\\data\\xbrl\\temp'
    docIDs=['S100DJ2G',]
    filenames=[]
    dirls=seek_from_docID(save_path,docIDs)
    for dirt in dirls: 
        for file_name in glob.glob(dirt+'\\*.xbrl') :
            filenames.append(file_name)
    xrdlfile=filenames[0]#ここにXBRL File 指定
    df_label = pd.DataFrame(index=[], columns=[])
    for link_item in get_label_links(xrdlfile) :
        df_label=df_label.append(get_label1(link_item))
    df_comp=parse_companyxml(xrdlfile.replace('.xbrl','_lab.xml'))
    df_all_label=pd.concat([df_comp,df_label],sort=False) 
    #df_all_label.to_excel('label.xls',encoding='cp938')
    df_facts=parse_facts(xrdlfile)
    df_type=parse_type(xrdlfile.replace('.xbrl','_pre.xml'))
    df_merge=pd.merge(df_all_label,df_facts,how='inner')
    df_merge=pd.merge(df_merge,df_type,how='inner')
    #df_merge.to_csv('dataframe.csv')
    df_fs=add_role_label(df_merge,df_all_label)
    df_fs['amount']=df_fs['amount'].str[:3000] 
    #df_fs.to_excel('df_fs.xls',encoding='cp938')    
    ToExcel_finace_sheets(df_fs,xrdlfile)
    '''
    df_merge['amount']=df_merge['amount'].str[:3000]    
    xlsname, ext = os.path.splitext(os.path.basename(xrdlfile))
    print(xlsname)
    df_merge.to_excel(xlsname+'.xls',encoding='cp938')
    '''    
