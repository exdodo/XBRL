# -*- coding: utf-8 -*-
"""
Created on Thu May 16 10:23:34 2019
���ӁF�ߋ��T�N����EDINET�t�@�C�����͂R�O���ȏ゠��L���،��񍐏�������1TB�ɔ���܂�
@author: Yusuke
"""

# -*- coding: utf-8 -*-
"""
Created on Tue May 14 07:33:55 2019

@author: Yusuke
"""
import pandas as pd
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning) #verify=False�΍�
import unicodedata
from edinet_jsons import main_jsons
from select_docIDs_freeword import colunm_shape,display_From_docIDS,get_xbrl_from_docIDs

def select_docIDs_docType(df,dict_cond) :
    docIDs=[]
    df_focus=df              
    for type_key,cond_value in dict_cond.items() :
        if cond_value.isdigit() : #���l�͔��p������ɂ���
            cond_value= unicodedata.normalize("NFKC", cond_value)        
        df_focus=df_focus[df_focus[type_key]==cond_value]
    if len(df_focus['docID'].to_list())>0 : 
        docIDs.append(df_focus['docID'].to_list())
    flat_docs = [item for sublist in docIDs for item in sublist]#flatten
    unique_docs=list(set(flat_docs))
    return unique_docs
if __name__=='__main__':
    #---�ߋ��T�N����EDINET�t�@�C�����͂R�O���ȏ゠��L���،��񍐏�������1TB�ɔ���܂�----
    #--DISK�e�ʂ��\���ɂ��邩�_�E�����[�h�Ώۂ�docIDs���i��Ȃ��ƃV�X�e���ɐ[���ȉe����^���܂�--
    save_path='d:\\data\\xbrl\\temp' #xbrl file�ۑ���̊�t�H���_�[
    #dict_cond={'formCode':'030000', 'ordinanceCode':'10'}�@#'030000':�N���L���،��񍐏�
    dict_cond={'secCode':'6501'}       
    nYears=[2018,2018] #���Ԏw��@�N�@�ȏ�ȓ�      
    main_jsons() #�O���܂Œ�o���ވꗗ���擾  
    df=pd.read_json('xbrldocs.json',dtype='object') #5�N����30���s
    df = colunm_shape(df) #dataframe�𐄝�
    df=df[(df['dtDateTime'].dt.year >= min(nYears)) 
            & (df['dtDateTime'].dt.year <= max(nYears))]    
    docIDs=select_docIDs_docType(df,dict_cond)    
    get_xbrl_from_docIDs(df,save_path,docIDs)
    display_From_docIDS(docIDs,df)#�擾docIDs���\��
    '''
    ���ވꗗ����{'JCN':'��o�Җ@�l�ԍ�', 'attachDocFlag':'��֏��ʁE�Y�t�����L���t���O', 
     'currentReportReason':'�Օ��o���R', 'disclosureStatus':'�J���s�J���敪',
       'docDescription':'��o���ފT�v', 'docID':'���ފǗ��ԍ�', 
       'docInfoEditStatus':'���ޏ��C���敪', 'docTypeCode':'���ގ�ʃR�[�h',
       'edinetCode':'��o��EDINET�R�[�h', 'englishDocFlag':'�p���t�@�C���L���t���O',
       'filerName':'��o�Җ�', 'formCode':'�l���R�[�h', 'fundCode':'�t�@���h�R�[�h',
       'issuerEdinetCode':'���s���EDINET�R�[�h', 'opeDateTime':'�������',
       'ordinanceCode':'�{�߃R�[�h', 'parentDocID':'�e���ފǗ��ԍ�','pdfFlag':'PDF�L���t���O', 
       'periodEnd':'���ԁi���j', 'periodStart':'���ԁi���j', 
       'secCode':'��o�ҏ،��R�[�h', 'seqNumber':'�A��','subjectEdinetCode':'�Ώ�EDINET�R�[�h', 
       'submitDateTime':'��o����', 'subsidiaryEdinetCode':'�q���EDINET�R�[�h',
       'withdrawalStatus':'�扺�敪', 'xbrlFlag':'XBRL�L���t���O'}
    '���ގ�ʃR�[�h'{10:'�L���،��ʒm��',20:'�ύX�ʒm���i�L���،��ʒm���j',30:'�L���،��͏o��', 
     40:'�����L���،��͏o��',50:'�͏o�̎扺���肢',60:'���s�o�^�ʒm��', 
     70:'�ύX�ʒm���i���s�o�^�ʒm���j',80:'���s�o�^��',90:'�������s�o�^��', 
     100:'���s�o�^�Ǖ⏑��',110:'���s�o�^�扺�͏o��',120:'�L���،��񍐏�', 
     130:'�����L���،��񍐏�',135:'�m�F��',136:'�����m�F��',140:'�l�����񍐏�', 
     150:'�����l�����񍐏�',160:'�����񍐏�',170:'���������񍐏�',180:'�Վ��񍐏�', 
     190:'�����Վ��񍐏�',200:'�e��Г��󋵕񍐏�',210:'�����e��Г��󋵕񍐏�', 
     220:'���Ȋ������t�󋵕񍐏�',230:'�������Ȋ������t�󋵕񍐏�', 
     235:'���������񍐏�',236:'�������������񍐏�',240:'���J���t�͏o��', 
     250:'�������J���t�͏o��',260:'���J���t�P��͏o��',270:'���J���t�񍐏�', 
     280:'�������J���t�񍐏�',290:'�ӌ��\���񍐏�',300:'�����ӌ��\���񍐏�', 
     310:'�Ύ���񓚕񍐏�',320:'�����Ύ���񓚕񍐏�',330:'�ʓr���t���֎~�̓�����󂯂邽�߂̐\�o��', 
     340:'�����ʓr���t���֎~�̓�����󂯂邽�߂̐\�o��', 
     350:'��ʕۗL�񍐏�',360:'������ʕۗL�񍐏�',370:'����̓͏o��',380:'�ύX�̓͏o��'}
    #docIDs=['S100DJ2G',]#['S100DAZ4']  
    '''
    