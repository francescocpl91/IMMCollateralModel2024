# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 17:25:31 2021

@author: CIzquierdo - SS&C Algorithmics

Funcion que agrega al diccionario de flujos los flujos a fecha Break Clause
"""
import numpy as np
import pandas as pd


# i_trade = df_BCDateMtM.columns[0]


# truncates CF cube after BC date and adds BC flow
def f_addBCFlow(dic_CF, dic_ccy, df_MtMtradesInfo, df_BCDateMtM, log_addBCFlow):  
    
    # dic_CF , df_MtMtradesInfo, df_BCDateMtM
    
    # i_trade = 'MX_35313498_SYN'
    
    n_trade = 0
    n_trades = len(df_BCDateMtM.columns)
    
    for i_trade in df_BCDateMtM:
        n_trade += 1
        
        BCDate = df_MtMtradesInfo.loc[i_trade, 'BCDate']
        
        # add BC flow
        if log_addBCFlow:
            print('\rCalculando flujo BC '+format(n_trade)+' de '+format(n_trades), end='')
            
            mtmCcy = df_MtMtradesInfo.loc[i_trade, 'Ccy']
            ClosestRefDate = df_MtMtradesInfo.loc[i_trade, 'ClosestRefDate']
            
            MtMBeforeBC = df_MtMtradesInfo.loc[i_trade, 'MtMBeforeBC']
            ps_TFAdjust = pd.Series(index = range(1,2001), dtype='float64').fillna(0)
        
            if i_trade not in dic_CF: # create new key
                dic_CF[i_trade] = {}
            
            else: # calculate CF adjustement to estimate BC flow    
                
                if MtMBeforeBC:
                    startDate, endDate = ClosestRefDate , BCDate
                else:
                    startDate, endDate = BCDate , ClosestRefDate
                    
                for i_tipoflujo in dic_CF[i_trade]:
                    dic = dic_CF[i_trade][i_tipoflujo]
                    outType = dic["outType"]
                    ccy = dic["ccy"]
                    # flowType = dic["flowType"]
                    cube = dic["cube"].astype(float)
                            
                    if outType != "Fixed":
                        ps_TFAdjust += cube.loc[:,  np.logical_and(startDate <= cube.columns, cube.columns <  endDate) ].sum(axis=1) * \
                                    dic_ccy[ccy][ClosestRefDate] / dic_ccy[mtmCcy][ClosestRefDate]
                    else:
                        for i_index in cube.index:
                            if startDate <= i_index < endDate:
                                ps_TFAdjust += cube[i_index] * dic_ccy[ccy][ClosestRefDate] / dic_ccy[mtmCcy][ClosestRefDate]
                                
            signo = -1 if MtMBeforeBC else 1
            BCFlow = df_BCDateMtM[i_trade] + signo * ps_TFAdjust
            
            dic_CF[i_trade]["BCFlow"] = {'cube':BCFlow.to_frame(name= BCDate ),
                                                  'ccy': mtmCcy ,
                                                  'outType' : 'BCFlow',
                                                  'flowType' : 'cash'+mtmCcy }
        
        if i_trade in dic_CF:
            # trucates the cubes after BC date
            for i_tipoflujo in dic_CF[i_trade]:
                if i_tipoflujo != "BCFlow": #truncate
                    outType = dic_CF[i_trade][i_tipoflujo]["outType"]
                    if outType != "Fixed":
                        # truncates after BC date
                        dic_CF[i_trade][i_tipoflujo]['cube'].loc[:, dic_CF[i_trade][i_tipoflujo]['cube'].columns >= BCDate] = 0
                    else:
                        dic_CF[i_trade][i_tipoflujo]['cube'].loc[dic_CF[i_trade][i_tipoflujo]['cube'].index >= BCDate] = 0
                        
    
    
    if log_addBCFlow:  print('CF en fecha Break Clause añadido a cubo de CF...........')
    
    print('Cubos de CF truncados en fecha Break Clause...........')
    
    return dic_CF
    
    
    