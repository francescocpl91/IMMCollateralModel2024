# -*- coding: utf-8 -*-
"""
Ejecutable codigo de validacion modelo colateral simulado IMM, este codigo depende de funciones adjuntas.
Se recomienda guardar este codigo y el resto de dependencias en el mismo directorio (codePath)
"""
import os
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import numpy as np
import pickle

from Aggregation import f_readMtMRWFile, f_readCFRWFile, f_AggregatesPerCcyCLS, f_ColModelCFMetrics, f_readCcyRWFile,f_cashmitigants
from BCCFAdd import f_addBCFlow
from BrownianBridge import f_readsRandomDraws, f_BrownianBridge, f_BrownianBridge_PhysColl
from VariationMargin import f_VariationMargin, f_VariationMargin_unitsCMP
from auxFunctions import f_loadInitialProfiles, f_extractCSAList, f_readInputs, f_readDIM, f_EEReport, \
    f_readInputs_phyColl

# from EE2eEPE import f_EE2eEPE, f_EE2EPE

WORKPARTH = r"C:\Users\fcapelli\OneDrive - SS&C Technologies, Inc\Documents\Francesco_intro\SPV\IMM Collateral Model"
Physical_Collateral = True
reporCcy = "EUR"
l_MA = ['N_118487']#['N_118452']#['N_118487']

EarlyExit = True
log_addBCFlow = True # breakclause flow

dic_ccy , l_referenceDates = f_readCcyRWFile(WORKPARTH, reporCcy, load=0)  # load =1 to read from RW file

#read cash mitigants file

if Physical_Collateral:
    df_cashMitigant = pd.read_csv(WORKPARTH + "\\Inputs\\CashMitigants\\XKOZ_udsloader_node_mitigants_HC.csv", index_col=0)

# writes refDates and ccy cubes
pd.Series(l_referenceDates).to_csv(WORKPARTH + '\\Outputs\\listRefDates.csv')
with open(WORKPARTH + '\\Outputs\\dic_ccy.pickle', 'wb') as config_dictionary_file:
    pickle.dump(dic_ccy, config_dictionary_file)  


sessionDate = l_referenceDates[0]

# Break Clause trades
df_BCDates = pd.read_csv( WORKPARTH + "\\Inputs\\BreakClauseDates.csv" ).set_index("Name")  
df_BCDates.loc[:,"Break Clause Dates"] = pd.to_datetime(df_BCDates.loc[:,"Break Clause Dates"] , dayfirst = True)

# CLS Trades
l_CLSTrades = pd.read_csv( WORKPARTH + "\\Inputs\\CLSTrades.csv" )["IDENTIFIER"].to_list()

# Initial Profiles
df_InitialProfiles = f_loadInitialProfiles(WORKPARTH)

# baseline trades Pos
df_BaselinePos = pd.read_csv(WORKPARTH + '\\Inputs\\BaselineTrades_Pos.csv', index_col=0)

# ITERATE OVER NETTING AGREEMENT
i_MA = l_MA[0]
for i_MA in l_MA:
    
    outputMAFolder = WORKPARTH + "\\Outputs\\" + i_MA
    inputMAFolder = WORKPARTH + "\\Inputs\\" + i_MA
    
    # create output directory for MA
    if not os.path.isdir(outputMAFolder):
        os.mkdir(outputMAFolder)
    
    l_CSA = f_extractCSAList(inputMAFolder)
    
    df_CVFull_MA = pd.DataFrame(0.0, index=range(1,2001), columns= l_referenceDates  )
    df_CVFull_ISDA_MA = pd.DataFrame(0.0, index=range(1,2001), columns= l_referenceDates  )
    df_CVSmooth_MA = pd.DataFrame(0.0, index=range(1,2001), columns= l_referenceDates  )
    
    i_CSA = l_CSA[0]
    
    # ITERATE OVER CSA
    for i_CSA in l_CSA:
        outputCSAFolder = outputMAFolder + "\\" + i_CSA
        inputCSAFolder = inputMAFolder + "\\" + i_CSA
        
        nodeType = "CSA" if i_CSA[:2] == "C_" else "NC"
       
     
        # read RW info collateral agreement trades
        print('Leyendo informacion exportada de Risk Watch de las operaciones en el acuerdo de colateral: '+i_CSA)
        
        # create output directory for CSA
        if not os.path.isdir(outputCSAFolder):
            os.mkdir(outputCSAFolder)
                
        ## Read RW export
        # read MtM cube, truncates with BC and aggregates it, returns MtM at closest to BC date to calculate BC flow
        df_MtM_BC, df_MtM_NoBC, df_MtMtradesInfo, df_BCDateMtM = f_readMtMRWFile(WORKPARTH, i_MA , i_CSA, nodeType,  dic_ccy , l_referenceDates, df_BCDates, df_BaselinePos['PositionUnitsVAL'], l_trades ='all', load = 1)
         
        df_MtM_BC.to_csv(outputCSAFolder + "\\MtMNodeAgBC.csv") ; df_MtM_NoBC.to_csv(outputCSAFolder + "\\MtMNodeAgNoBC.csv") ; df_MtMtradesInfo.to_csv(outputCSAFolder + "\\MtMTradeInfo.csv") ; df_BCDateMtM.to_csv(outputCSAFolder + "\\BCDateMtM.csv")
        
        df_MtM = df_MtM_BC if EarlyExit else df_MtM_NoBC
        
        
        # IF VM COLLATERAL NODE
        if nodeType == "CSA":

            if Physical_Collateral:
                df_CMPuser, df_CMPcpty, df_CMPuserHaD, df_CMPcptyHaD = f_cashmitigants(i_MA, i_CSA, df_cashMitigant, dic_ccy,l_referenceDates)
                ## Read CSA parameters and SNS Allocation in case of Physical Collateral
                dic_CSAParams, ps_IPs = f_readInputs_phyColl(dic_ccy, WORKPARTH, i_CSA, df_InitialProfiles,df_cashMitigant)

            ## Read CSA parameters and SNS Allocation
            else: dic_CSAParams, ps_IPs = f_readInputs(WORKPARTH, i_CSA, df_InitialProfiles)
            
            # read CF for each trade
            dic_CF = f_readCFRWFile(WORKPARTH, i_MA , i_CSA, df_BaselinePos['PositionUnitsVAL'], load = 1)
            
            with open(outputCSAFolder + "\\dic_CF.pickle", 'wb') as config_dictionary_file:
                pickle.dump(dic_CF, config_dictionary_file)

            df_MtMtradesInfo['CFExported'] = [(xtrade in list(dic_CF.keys())) for xtrade in df_MtMtradesInfo.index ]   # CF exported
            df_MtMtradesInfo.to_csv(outputCSAFolder + "\\MtMTradeInfo.csv")
            
            # truncates cube, if log_addBCFlow=True adds BC flow
            if EarlyExit: dic_CF = f_addBCFlow(dic_CF, dic_ccy, df_MtMtradesInfo, df_BCDateMtM, log_addBCFlow)
        
            # aggregates by ccy and flow type, export cube after aggregating all trade flows and another excluding CLS
            dic_CFAg, dic_CFAgCLS = f_AggregatesPerCcyCLS(dic_CF, dic_CSAParams['SettlementNettingCcys'], l_CLSTrades, load = 1, filepath =  outputCSAFolder )
            with open(outputCSAFolder + "\\dic_CFAg.pickle", 'wb') as config_dictionary_file:
                pickle.dump(dic_CFAg, config_dictionary_file)
                
            # Extrae ultima fecha de vto
            maxVto = sessionDate
            for i_ccy in dic_CFAg:
                maxVto = max( maxVto ,max(dic_CFAg[i_ccy].columns.tolist()))
            
            # generates CF metrics used in the colateral model
            df_MPORTF, df_MPORSP ,df_MPORSP_ISDA, df_ModelPay = f_ColModelCFMetrics(dic_CFAg, dic_CFAgCLS, dic_ccy, l_referenceDates, reporCcy, dic_CSAParams['CSACcy'],
                                                                  int(dic_CSAParams['SettleLag']) , int(dic_CSAParams['MPOR']), sessionDate, WORKPARTH, i_MA, i_CSA, load = 1, readFromMAG = False)
            
            # save some of the read files 
            df_MPORTF.to_csv(outputCSAFolder + "\\MPORTradeFlows.csv") ; df_MPORSP.to_csv(outputCSAFolder + "\\MPORSettleFlows.csv") ; df_MPORSP_ISDA.to_csv(outputCSAFolder + "\\MPORSettleFlowsISDA.csv") ; df_ModelPay.to_csv(outputCSAFolder + "\\ModelledPayMtM.csv")
            
            ## 3 - Brownian Bridge
            # try to read the cube, otherwise generate it    
            df_RD = f_readsRandomDraws(inputCSAFolder +  '\\BBRandomDraws.csv', dic_CSAParams, l_referenceDates, sessionDate, maxVto)

            if Physical_Collateral:
                df_RD_CMPuser = f_readsRandomDraws(inputCSAFolder + '\\BBRandomDrawsCMPuser.csv', dic_CSAParams, l_referenceDates, sessionDate, maxVto)
                df_RD_CMPcpty = f_readsRandomDraws(inputCSAFolder + '\\BBRandomDrawsCMPcpty.csv', dic_CSAParams, l_referenceDates, sessionDate, maxVto)
             
            # execute Brownian Bridge
            if Physical_Collateral:
                df_BBMtM, df_BB_CMPuser, df_BB_CMPcpty = f_BrownianBridge_PhysColl(
                    inputCSAFolder, sessionDate, l_referenceDates, df_RD, df_RD_CMPuser, df_RD_CMPcpty,
                    (df_MtM / dic_ccy[dic_CSAParams['CSACcy']]), df_ModelPay , (df_CMPuser / dic_ccy[dic_CSAParams['CSACcy']]),
                    (df_CMPcpty / dic_ccy[dic_CSAParams['CSACcy']]), dic_CSAParams, load=1, transf_matrix_load = 1, filepath=outputCSAFolder)

                df_BBMtM_dates = [x for x in df_BBMtM.columns if x > maxVto]
                df_BBMtM_report = df_BBMtM.drop(df_BBMtM_dates, axis=1)
                df_BB_CMPuser_report = df_BB_CMPuser.drop(df_BBMtM_dates, axis=1)
                df_BB_CMPcpty_report = df_BB_CMPcpty.drop(df_BBMtM_dates, axis=1)
                # save some of the generated files
                df_BBMtM.to_csv(outputCSAFolder + "\\BBMtM.csv")
                df_BB_CMPuser.to_csv(outputCSAFolder + "\\BBCMPuser.csv")
                df_BB_CMPcpty.to_csv(outputCSAFolder + "\\BBCMPcpty.csv")
                df_BBMtM_report.to_csv(outputCSAFolder + "\\BBMtM_reduced.csv")
                df_BB_CMPuser_report.to_csv(outputCSAFolder + "\\BBCMPuser_reduced.csv")
                df_BB_CMPcpty_report.to_csv(outputCSAFolder + "\\BBCMPcpty_reduced.csv")

            else:
                df_BBMtM = f_BrownianBridge(sessionDate, l_referenceDates, df_RD, (df_MtM / dic_ccy[dic_CSAParams['CSACcy']]), df_ModelPay, dic_CSAParams, load=1, filepath=outputCSAFolder)
                # save some of the generated files
                df_BBMtM.to_csv(outputCSAFolder + "\\BBMtM.csv")

            ## 4 - Calculate Variation Margin in CSA currency for each time step and scenario
            if Physical_Collateral:
                df_VMFull, df_VMSmooth = f_VariationMargin_unitsCMP(sessionDate, l_referenceDates, df_BBMtM, df_BB_CMPuser, df_BB_CMPcpty, dic_CSAParams, df_CMPuserHaD, df_CMPcptyHaD)

            else:
                df_VMFull, df_VMSmooth = f_VariationMargin(sessionDate,l_referenceDates, df_BBMtM, dic_CSAParams)

            ##adjusting the variation margin as units of CMP plus HC at Default.

            (df_VMFull*dic_ccy[dic_CSAParams['CSACcy']]).to_csv(outputCSAFolder + "\\VMBalanceFull.csv")
            (df_VMSmooth*dic_ccy[dic_CSAParams['CSACcy']]).to_csv(outputCSAFolder + "\\VMBalanceSmooth.csv")
               
            ## 5 - Lee cubo DIM
            df_DIM = f_readDIM(inputCSAFolder + '\\Dynamic IM.csv')
            
            ## 6 - Calcula Collateralized Value y exporta exposiciones sin IM
            df_CVFull = df_MtM - df_VMFull * dic_ccy[dic_CSAParams['CSACcy']] + df_MPORTF - df_MPORSP 
            df_CVFull_ISDA = df_MtM - df_VMFull * dic_ccy[dic_CSAParams['CSACcy']] + df_MPORTF - df_MPORSP_ISDA 
            df_CVSmooth = df_MtM - df_VMSmooth * dic_ccy[dic_CSAParams['CSACcy']] + df_MPORTF 
            
            df_EE = f_EEReport(df_CVFull, df_CVSmooth, df_CVFull_ISDA, l_referenceDates, ps_IPs, maxVto,  sessionDate, outputCSAFolder + "\\OutputExposuresNoIM.csv")
            
            ## 7 - Calcula Collateralized Value y exporta exposiciones  con IM
            df_CVFull = df_CVFull - df_DIM
            df_CVFull_ISDA = df_CVFull_ISDA - df_DIM
            df_CVSmooth = df_CVSmooth - df_DIM
            
        else:
            df_CVFull , df_CVFull_ISDA , df_CVSmooth = df_MtM , df_MtM , df_MtM

              
        df_CVFull.to_csv(outputCSAFolder + "\\CVFull.csv")
        df_CVSmooth.to_csv(outputCSAFolder + "\\CVSmooth.csv")
        df_CVFull_ISDA.to_csv(outputCSAFolder + "\\CVFullISDA.csv")
    
        ## 7 - Calcula eEPE Full y Smooth y EPE CF Spikes, exporta csv en output folder
        df_EE = f_EEReport(df_CVFull, df_CVSmooth, df_CVFull_ISDA, l_referenceDates, ps_IPs, maxVto,  sessionDate, outputCSAFolder + "\\OutputExposuresIM.csv")
    
        # aggrega CV a nivel MA
        df_CVFull_MA += df_CVFull
        df_CVFull_ISDA_MA += df_CVSmooth
        df_CVSmooth_MA += df_CVFull_ISDA
        
    ## Calcula eEPE Full y Smooth y EPE CF Spikes a nivel MA, exporta csv en output folder
    df_EE = f_EEReport(df_CVFull_MA, df_CVFull_ISDA_MA, df_CVSmooth_MA, l_referenceDates, ps_IPs, maxVto,  sessionDate, outputMAFolder + "\\OutputExposuresIM.csv")

        
        
        
        
