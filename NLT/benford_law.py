# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 07:59:30 2017

@author: Yusuke

https://rosettacode.org/wiki/Benford%27s_law#Python
EDINETタクソノミは© Copyright 2014 Financial Services Agency, The Japanese Government
"""

from __future__ import division
from itertools import islice
from collections import Counter #出現頻度を数える
from math import log10
import pandas as pd
import os
import sqlite3
 
expected = [log10(1+1/d) for d in range(1,10)]
  
def heads(s):
    for a in s: 
        yield int(str(a)[0])
 
def show_dist(title, s):
    c = Counter(s)
    size = sum(c.values())
    res = [c[d]/size for d in range(1,10)] #Benford's law
 
    print("\n%s Benfords deviation" % title)
    for r, e in zip(res, expected):
        print("%5.1f%% %5.1f%%  %5.1f%%" % (r*100., e*100., abs(r - e)*100.))
    
def amount_list(file):
    df_fact=pd.read_csv(file,encoding="utf-8",index_col=0)
    df=pd.to_numeric(df_fact['amount'],errors='coerce',downcast='unsigned')
    df=df.dropna()
    if df.duplicated().any():
        df=df.drop_duplicates()
    return list(map(abs,df.values.tolist()))

def fibonacci(n):
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, b+a

def edinet_name(file):
    dbpath=r'C:\Users\Yusuke.SERVICE\Documents\Embarcadero\Studio\Projects\TeCAPro\TeCAPro.db'
    filename=file.replace('.csv','')
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()
    # SELECT
    select='SELECT NAME,DATE FROM edinetdata where '
    sel_cond1='DOCID="'+filename+'"'
    select=select+sel_cond1
    cursor.execute(select) 
    # 全件取得は cursor.fetchall()
    comp_names = cursor.fetchall()
    conn.commit()
    conn.close()    
    return   comp_names
     
if __name__ == '__main__':
    ospath=os.getcwd()
    dirpath=ospath+'\\data\\'
    files = os.listdir(dirpath)
    for file in files :
        filepass=dirpath+file
        show_dist("amount",islice(heads(amount_list(filepass)),1000))
        comp_names=edinet_name(file)
        print(comp_names)
    