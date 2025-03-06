# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 16:21:34 2022

@author: SVC-GASQuant2-Prod
"""

import pandas as pd

class CEid(object):
    
    df=pd.DataFrame()
    
    def __init__(self,ini_df):
        self.df = ini_df
        
    def replace(self):    
        df_idtoname=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/ECcode.xlsx',header=(0),sheet_name=0)
        df_idtoname = pd.DataFrame(df_idtoname)
        df_idtoname.set_index('seriesId', inplace=True)
        #print(df_idtoname)
        for col in self.df.columns:
            self.df.rename(columns={str(col):str(df_idtoname.loc[int(col),'seriesName'])},inplace=True)
            
        return self.df
    