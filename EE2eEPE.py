# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 12:22:40 2021
Efectivizacion de exposcion esperada: a partir de eEPE se calcula EPE y EPE siguiendo la metodologia implementada en MAG
"""
import os
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import numpy as np
import pickle



def f_EE2eEE(ps_EE):
    
    ps_eEE = ps_EE.copy()
    ps_eEE[:] = 0
    EEmax = 0
    
    for i in ps_eEE.index:
        
        EEmax = max(ps_EE[i], EEmax)
        ps_eEE[i] = EEmax
        
    return ps_eEE

def f_EE2EPE(ps_EE, numDays = 365):
    
    l_dates = ps_EE.index.tolist()
    l_daysAcum = pd.Series( [  (x-l_dates[0]).days for x in l_dates  ])
    
    l_days = l_daysAcum.diff().fillna(0)
    l_days[l_daysAcum>numDays] = 0
    
    l_days[len(l_days) - len(l_days[l_daysAcum>numDays])] = max( 0 ,  numDays - l_daysAcum[l_daysAcum<=numDays].iloc[-1] )
    
    
    l_days.index = ps_EE.index
  
    return (ps_EE * l_days).sum() / l_days.sum()
    
    
def f_EE2eEPE(ps_EE,numDays = 365): 
    
    return f_EE2EPE(f_EE2eEE(ps_EE), numDays = numDays)



def f_readsEEfile(path):
    df_EE = pd.DataFrame( columns = l_referenceDates  )
    
    n_line = 0
    with open( path , 'r' ) as file:
        for line in file.readlines():
            n_line += 1
            
            line = line[:-1]
            
            # if n_line == 5:
            #     break
            
            if line == '':
                continue
            
            if line[:12] == ownEntity:
                
                cpty = line.split(",")[1]
                v_EE = line.split(",")[5:]
                v_EE[0], v_EE[-1] = v_EE[0][1:], v_EE[-1][:-1]
                
                v_EE = list(map(float, v_EE ) )
                
                df_EE.loc[cpty,:] = v_EE
    return df_EE

    