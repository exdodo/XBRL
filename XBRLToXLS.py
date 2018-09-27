# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 16:08:30 2018

@author: Yusuke
参考図書　URL　ありがとうございます
『金融データ解析の基礎』　金明哲
『簡単にできるXBRL　基礎編』　池田智之
『会計人のためのXBRL入門』坂上学
https://qiita.com/shima_x/items/58634a838ab37c3607b5
http://orangain.hatenablog.com/entry/namespaces-in-xpath
http://www.fsa.go.jp/search/20170228.html
https://srbrnote.work/archives/1315
https://note.nkmk.me 
EDINETタクソノミは© Copyright 2014 Financial Services Agency, The Japanese Government
"""
from lxml import etree as ET
import os
import pandas as pd
from collections import defaultdict
import re
from pathlib import Path

def get_link_base(xrdfile):
    label_file_name = xrdfile.replace('.xbrl','.xsd')
    labels= ET.parse(label_file_name)
    root= labels.getroot()
    ns= root.nsmap
    linkbase = labels.findall('.//link:linkbaseRef',ns)
    link_base = []
    for link_node in linkbase:
        link_href = link_node.attrib['{'+ns['xlink']+'}href']
        if '_lab.xml' in link_href and 'http://' in link_href:
            link_base.append(link_href)
    return link_base

def get_label(link_base):
    link_dict = defaultdict(list)
    #https://stackoverflow.com/questions/10457564/error-failed-to-load-external-entity-when-using-python-lxml
    labels = ET.parse(link_base)
    root= labels.getroot()
    ns= root.nsmap    
    # get common taxonomy
    link_labels=labels.findall('.//link:label',ns)    
    for link_label in link_labels :
        link_dict['lab_type'].append(link_label.attrib['{'+ns['xlink']+'}type'])
        link_dict['label'].append(link_label.attrib['{'+ns['xlink']+'}label'])
        link_dict['roll'].append(link_label.attrib['{'+ns['xlink']+'}role'])
        link_dict['lang'].append(link_label.                     
                     attrib['{http://www.w3.org/XML/1998/namespace}lang'])
                    #attrib['{'+ns['xlink']+'}lang'])
        element_id=link_label.attrib['id'].replace('label_','jppfs_cor_')
        link_dict['element_id'].append(element_id)
        link_dict['lab_name']=link_label.attrib['id']
        link_dict['label_string'].append(link_label.text)
    
    return pd.DataFrame(link_dict)

def parse_xbrl(fxbrl):   
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
    #nslist=['jpcrp_cor:*','jpdei_cor:*','jppfs_cor:*']
    nslist=[ i+':*' for i in root.nsmap ]
    if 'jpcrp_cor' in root.nsmap :
        for ele in nslist :
            anchors=root.findall(ele, root.nsmap)
            for anchor in anchors:
                if len(anchor.tag.split('}')[1])<1 :
                    continue
                ele=ele.replace(':*','_')
                element_id=ele.replace('//','')+anchor.tag.split('}')[1]
                facts_dict['element_id'].append( element_id )
                anc=delete_tag(anchor.text) #tagを正規表現で外す
                facts_dict['amount'].append( anc )
                facts_dict['context_ref'].append( anchor.attrib.get('contextRef') )
                facts_dict['unit_ref'].append( anchor.attrib.get('unitRef') )
                facts_dict['decimals'].append( anchor.attrib.get('decimals'))
        result['facts']=pd.DataFrame(facts_dict)                       
    return result

def delete_tag(anchor_text) :
    #tagを正規表現で外す
    text=str(anchor_text)
    if '<' in text :    
        p = re.compile(r"<[^>]*?>")
        return p.sub("", text)
    return anchor_text     

def parse_presentation(xmlfile):
    """
    return(element_id, label_string, lang, label_role, href)
    role_id:財務諸表の種類（貸借対照表、損益計算書、キャッシュフローなど)
    arcrole:
    order:要素の順番
    closed:
    usable:
    context_element:
    preferred_label:
    fromElementalID:親要素名
    toElementalID:子要素名
    """
    type_dict = defaultdict(list)
    types = ET.parse(xmlfile)
    root=types.getroot()
    ns=root.nsmap
    for type_link_node in types.findall('.//link:presentationLink',namespaces=ns):
        for type_arc_node in type_link_node.findall('.//link:presentationArc',namespaces=ns):
            type_arc_from = type_arc_node.attrib['{'+ns['xlink']+'}from']
            type_arc_to = type_arc_node.attrib['{'+ns['xlink']+'}to']

            matches = 0
            for loc_node in type_link_node.findall('.//link:loc',namespaces=ns):
                loc_label = loc_node.attrib['{'+ns['xlink']+'}label']

                if loc_label == type_arc_from:
                    if '{'+ns['xlink']+'}href' in loc_node.attrib.keys():
                        href_str = loc_node.attrib['{'+ns['xlink']+'}href']
                        type_dict['from_href'].append( href_str )
                        type_dict['from_element_id'].append( href_str.split('#')[1] )
                        matches += 1
                elif loc_label == type_arc_to:
                    if '{'+ns['xlink']+'}href' in loc_node.attrib.keys():
                        href_str = loc_node.attrib['{'+ns['xlink']+'}href']
                        type_dict['to_href'].append( href_str )
                        type_dict['to_element_id'].append( href_str.split('#')[1] )
                        type_dict['element_id'].append( href_str.split('#')[1] )
                        matches += 1                    
                if matches==2: break

            role_id = type_link_node.attrib['{'+ns['xlink']+'}role']
            arcrole = type_arc_node.attrib['{'+ns['xlink']+'}arcrole']
            order = get_xml_attrib_value(type_arc_node, 'order')
            closed = get_xml_attrib_value(type_arc_node, 'closed')
            usable = get_xml_attrib_value(type_arc_node, 'usable')
            context_element = get_xml_attrib_value(type_arc_node, 'contextElement')
            preferred_label = get_xml_attrib_value(type_arc_node, 'preferredLabel')

            type_dict['role_id'].append( role_id.split('/')[-1] )
            type_dict['arcrole'].append( arcrole )
            type_dict['order'].append( order )
            type_dict['closed'].append( closed )
            type_dict['usable'].append( usable )                
            type_dict['context_element'].append( context_element )
            type_dict['preferred_label'].append( preferred_label )
    result['types']=pd.DataFrame( type_dict )
    return result

def get_xml_attrib_value( node, attrib):
    if attrib in node.attrib.keys():
        return node.attrib[attrib]
    else:
        return None
def parse_companyxml(lab_file) :   
    comp_labels = None
    label_dict = defaultdict(list)
    labels = ET.parse(lab_file)
    root=labels.getroot()
    ns=root.nsmap
    for label_node in labels.findall('.//link:label',namespaces=ns):
        label_label = label_node.attrib['{'+ns['xlink']+'}label'] 
        
        for labelArc_node in labels.findall('.//link:labelArc',namespaces=ns):
            if label_label != labelArc_node.attrib['{'+ns['xlink']+'}to']:
                continue
            for loc_node in labels.findall('.//link:loc',namespaces=ns):
                loc_label = loc_node.attrib['{'+ns['xlink']+'}label']
                if loc_label != labelArc_node.attrib['{'+ns['xlink']+'}from']:
                    continue
                lang =label_node.attrib['{http://www.w3.org/XML/1998/namespace}lang']
                label_role = label_node.attrib['{'+ns['xlink']+'}role']
                href = loc_node.attrib['{'+ns['xlink']+'}href']

                label_dict['element_id'].append( href.split('_')[-1] )
                label_dict['label_string'].append( label_node.text)
                label_dict['lang'].append( lang )
                label_dict['label_role'].append( label_role )
                label_dict['href'].append( href )               
    comp_labels=pd.DataFrame( label_dict)
    result['comp_labels'] = comp_labels
    #company prezentation
    type_dict = defaultdict(list)
    for arc_node in root.findall('.//link:labelArc',namespaces=ns):            
        arcrole = arc_node.attrib['{'+ns['xlink']+'}arcrole']
        from_element_id=arc_node.attrib['{'+ns['xlink']+'}from']
        to_element_id=arc_node.attrib['{'+ns['xlink']+'}to']
        element_id=arc_node.attrib['{'+ns['xlink']+'}to']        
        order = get_xml_attrib_value(arc_node, 'order')
        closed = get_xml_attrib_value(arc_node, 'closed')
        usable = get_xml_attrib_value(arc_node, 'usable')
        context_element = get_xml_attrib_value(arc_node, 'contextElement')
        preferred_label = get_xml_attrib_value(arc_node, 'preferredLabel')

        type_dict['element_id'].append( element_id.replace('label_','') )
        type_dict['from_element_id'].append( from_element_id )
        type_dict['to_element_id'].append( to_element_id )
        type_dict['arcrole'].append( arcrole )
        type_dict['order'].append( order )
        type_dict['closed'].append( closed )
        type_dict['usable'].append( usable )          
        type_dict['context_element'].append( context_element )
        type_dict['preferred_label'].append( preferred_label )
    comp_types=pd.DataFrame( type_dict )
    result['comp_types']=comp_types
    return result

def merge_df(result) :
    df_fact=result['facts']
    df_type=result['types']
    df_label=result['labels']
    df_comp_label=result['comp_labels']
    df_comp_type=result['comp_types']
    comp_flag=0
    if 'element_id' in df_comp_type :
        df_comp=pd.merge(df_comp_label,df_comp_type,
                         on=['element_id'],how='outer') #型作成
    else :
        comp_flag=1
    #sort
    df_type.sort_values(by=['from_element_id','order'], ascending=True)
    #マージ    
    df_fs_type=pd.merge(df_label,df_type,on=['element_id'],how='outer') #型作成    
    if comp_flag==0 : 
        df_fs_type=pd.concat([df_fs_type,df_comp],sort=True) #結合    
    df_fs=pd.merge(df_fact,df_fs_type,on=['element_id'],how='outer') #数値埋め込み
    return df_fs

def unique_element(df,ele):
    df=df.drop_duplicates(subset=ele)
    uq_ls=df[ele].tolist()
    return uq_ls

def add_from_label(df,result) :
    df_label=result['labels']
    df_label=df_label.set_index('element_id')
    df['order'].astype(float)#order 文字列To数値
    df=df.dropna(axis=0, subset=['role_id'])  #role_id空白行削除
    role_ls=unique_element(df,'role_id') #各種財務諸表のリスト
    df_fs= pd.DataFrame(index=[], columns=[])
    for role in role_ls :
        role_label=elementToLabel(role,df_label)
        dfdum=df            
        dfdum=dfdum[dfdum['role_id'] .str.contains( role.replace('rol_',''))] 
        dfdum=dfdum.drop_duplicates()
        from_ls=unique_element(dfdum,'from_element_id')
        for from_item in from_ls :            
            from_label=elementToLabel(from_item,df_label)            
            df_1=dfdum[dfdum['from_element_id']==from_item]
            df_1=df_1.assign (rol_label=role_label)
            df_1=df_1.assign(f_label=from_label)
            df_1.sort_values('order')
            df_fs=df_fs.append(df_1)
    return df_fs

def elementToLabel(element_item,df_label) :
    if element_item in df_label.index :
        element_label=df_label['label_string'].loc[element_item]
    else :
        element_label=element_item    
    return element_label    

def saveToHDF(HDFPath,df_fs,year_path,edinet_code) :
    df_fs.to_hdf(HDFPath,year_path+'/'+edinet_code+'/financesheet',format='table'
               ,mode='a',data_columns=True,index=True,encoding='utf-8')        

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
    result = defaultdict(dict)
    #filename='C:/Users/EDINET/2018/6/25/S100D6OS/S100D6OS/XBRL/PublicDoc/jpcrp030000-asr-001_E03563-000_2018-03-31_01_2018-06-25.xbrl'
    filename='__file__'#ここにXBRL File 指定
    #xsd
    link_base=get_link_base(filename)        
    df_label=pd.DataFrame(index=[], columns=[])
    for link_item in link_base :
        df=get_label(link_item)
        df_label=df_label.append(df)
    result['labels']=df_label
    #xbrl
    if  os.path.isfile(filename) :
        with open(filename,'rt', encoding='utf-8') as fxbrl:
            parse_xbrl(fxbrl)
    prefilename=(filename).replace('.xbrl','_pre.xml')
    #presentation
    if  os.path.isfile(prefilename) :
        with open(prefilename,'rt', encoding='utf-8') as fxbrl:                        
            parse_presentation(fxbrl)
    labfilename=(filename).replace('.xbrl','_lab.xml')
    #copmany
    if  os.path.isfile(labfilename) :
        with open(labfilename,'rt', encoding='utf-8') as fxbrl:                        
            parse_companyxml(fxbrl)
    #統合
    df_merge=merge_df(result)
    #from_elementのラベル作成
    df_fs=add_from_label(df_merge,result)
    #財務情報以外削除
    df_fs=df_fs.dropna(axis=0, subset=['element_id'])  #element_id空白削除
    df_fs=df_fs.dropna(axis=0, subset=['amount'])  #amount空白削除
    df_fs=df_fs.dropna(axis=0, subset=['label_string'])  #label_string空白削除    
    df_fs.to_excel(mandatory_year(filename)+'_'+edinet_code(filename)+'.xlsx',
                   sheet_name='new_sheet_name')
    
    #sampleで連結貸借対照表　表示
    df_fs=df_fs[~df_fs['context_ref'].str.contains('NonConsolidatedMember',na=False)] #連結抜き出す
    df_fs=df_fs[~df_fs['context_ref'].str.contains('Prior',na=False)] #今期だけを抜き出す
    df_fs=df_fs[~df_fs['context_ref'].str.contains('CurrentYearInstant_',na=False)] #純資産対策 
    df_cbs=df_fs[df_fs['role_id']=='rol_ConsolidatedBalanceSheet']
    print(df_cbs[['f_label','label_string','amount']])
    
