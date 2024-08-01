# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 12:37:43 2021

@author: CIzquierdo - SS&C Algorithmics

Funciones auxiliares usadas en Exec.py
    Lectura de inputs
    Calculo de metricas exposicion a partir de valor colateralizado (EE, EPE, eEPE y EPESpikeAddon). Dependencia con spript EE2eEPE.py
"""
import os
import numpy as np
import pandas as pd

from EE2eEPE import f_EE2eEPE, f_EE2EPE


def f_readInputs_phyColl(dic_ccy, WORKPARTH, CSAName, df_InitialProfiles, df_mitigant):
    dic_CSAParams = {'numAddCallDates': 1, 'userIA': 0.0,
                     'cptyIA': 0.0}  # IA posted by the user and the cpty respectively

    try:
        df_CSAParam = pd.read_csv(WORKPARTH + "\\Inputs" + '\\CollateralAgreementVM.csv', sep=",").set_index("Id")
        dic_CSAParams.update(
            {'CurePeriod': df_CSAParam["CurePeriod"][CSAName], 'MarginLag': df_CSAParam["MarginLag"][CSAName],
             'SettleLag': df_CSAParam["SettlementLag"][CSAName],
             'CallPeriod': df_CSAParam["CallPeriod"][CSAName], 'CSACcy': df_CSAParam["AgreementCurrency"][CSAName],
             'Round': df_CSAParam["RoundoffAmount"][CSAName],
             'UserVMMinTransfer': df_CSAParam["UserVMMinTransfer"][CSAName],
             'CPVMMinTransfer': df_CSAParam["CPVMMinTransfer"][CSAName],
             'UserVMThreshold': df_CSAParam["UserVMThreshold"][CSAName],
             'CPVMThreshold': df_CSAParam["CPVMThreshold"][CSAName],
             'SettlementNettingCcys': df_CSAParam["SettlementNettingCcys"][CSAName].split(", ")})

    except:
        df_CSAParamLeg = pd.read_csv(WORKPARTH + "\\Inputs" + '\\CollateralAgreementLegacy.csv', sep=",").set_index(
            "Id")
        dic_CSAParams.update(
            {'CurePeriod': df_CSAParamLeg["CurePeriod"][CSAName], 'MarginLag': df_CSAParamLeg["MarginLag"][CSAName],
             'SettleLag': df_CSAParamLeg["SettlementLag"][CSAName],
             'CallPeriod': df_CSAParamLeg["CallPeriod"][CSAName],
             'CSACcy': df_CSAParamLeg["AgreementCurrency"][CSAName], 'Round': df_CSAParamLeg["RoundoffAmount"][CSAName],
             'UserVMMinTransfer': df_CSAParamLeg["UserVMMinTransfer"][CSAName],
             'CPVMMinTransfer': df_CSAParamLeg["CPVMMinTransfer"][CSAName],
             'UserVMThreshold': df_CSAParamLeg["UserVMThreshold"][CSAName],
             'CPVMThreshold': df_CSAParamLeg["CPVMThreshold"][CSAName],
             'SettlementNettingCcys': df_CSAParamLeg["SettlementNettingCcys"][CSAName].split(", ")})

    dic_CSAParams['MPOR'] = dic_CSAParams['CurePeriod'] + max(dic_CSAParams['MarginLag'], dic_CSAParams['SettleLag'])

    df_Mitigants = pd.read_csv(WORKPARTH + "\\Inputs" + '\\Mitigants.csv', sep=",")
    VMToCpty_Mitigants = df_Mitigants[np.logical_and(df_Mitigants['CollateralAgreementId'] == CSAName,
                                           df_Mitigants['PostDirection'] == 'To Counterparty')]
    VMToUser_Mitigants = df_Mitigants[
        np.logical_and(df_Mitigants['CollateralAgreementId'] == CSAName, df_Mitigants['PostDirection'] == 'To User')]

    # current VM collateral
    VMToCpty = df_mitigant.loc[(df_mitigant['CollateralAgreement'] == CSAName) &
                               (df_mitigant['POSTDIRECTION'] == 'To Counterparty') & (
                                       df_mitigant["APPLICABILITY"] == "INITIAL_BALANCE")]['NumberOfUnits']
    VMToUser = df_mitigant.loc[(df_mitigant['CollateralAgreement'] == CSAName) &
                               (df_mitigant['POSTDIRECTION'] == 'To User') & (
                                       df_mitigant["APPLICABILITY"] == "INITIAL_BALANCE")]['NumberOfUnits']
    VMToCpty_MP = df_mitigant.loc[(df_mitigant['CollateralAgreement'] == CSAName) &
                               (df_mitigant['POSTDIRECTION'] == 'To Counterparty') & (
                                           df_mitigant["APPLICABILITY"] == "MITIGANT_POOL")]
    VMToUser_MP = df_mitigant.loc[(df_mitigant['CollateralAgreement'] == CSAName) &
                               (df_mitigant['POSTDIRECTION'] == 'To User') & (
                                           df_mitigant["APPLICABILITY"] == "MITIGANT_POOL")]
    if len(VMToCpty) == 0:
        dic_CSAParams['VMToCpty'] = 0

    if not len(VMToCpty) == 0:
        for index, row in VMToCpty_MP.iterrows():
            if row["Currency"] != VMToCpty_Mitigants['Currency'].item():
                VMToCpty_MP['HCcalc'] = (1 - VMToCpty_MP['HAIRCUTFACTOR'] / 100) * VMToCpty_MP['NumberOfUnits'] * \
                                        dic_ccy[row["Currency"]][dic_ccy[row["Currency"]].columns[0]][1]
            else:
                VMToCpty_MP['HCcalc'] = (1 - VMToCpty_MP['HAIRCUTFACTOR'] / 100) * VMToCpty_MP['NumberOfUnits']

        if VMToCpty_Mitigants['Currency'].item() != "EUR":
            dic_CSAParams['VMToCpty'] = VMToCpty_MP['HCcalc'].sum() * 1/dic_ccy[VMToCpty_Mitigants['Currency']][dic_ccy[VMToCpty_Mitigants['Currency']].columns[0]][1]

        else:
            dic_CSAParams['VMToCpty'] = VMToCpty_MP['HCcalc'].sum()


    if len(VMToUser) == 0:
        dic_CSAParams['VMToCpty'] = 0

    if not len(VMToUser) == 0:

        for index, row in VMToUser_MP.iterrows():
            if row["Currency"] != VMToUser_Mitigants['Currency'].item():
                VMToUser_MP['HCcalc'] = (1 - VMToUser_MP['HAIRCUTFACTOR'] / 100) * VMToUser_MP['NumberOfUnits'] * \
                                        dic_ccy[row["Currency"]][dic_ccy[row["Currency"]].columns[0]][1]
            else:
                VMToUser_MP['HCcalc'] = (1 - VMToUser_MP['HAIRCUTFACTOR'] / 100) * VMToUser_MP['NumberOfUnits']

        if VMToUser_Mitigants['Currency'].item() != "EUR":
            dic_CSAParams['VMToUser'] = VMToUser_MP['HCcalc'].sum() * 1 / dic_ccy[VMToUser_Mitigants['Currency']][dic_ccy[VMToUser_Mitigants['Currency']].columns[0]][1]

        else:
            dic_CSAParams['VMToUser'] = VMToUser_MP['HCcalc'].sum()

    # VMToCpty_MP['HCcalc'] = (1 - VMToCpty_MP['HAIRCUTFACTOR'] / 100) * VMToCpty_MP['NumberOfUnits']
    # VMToUser_MP['HCcalc'] = (1 - VMToUser_MP['HAIRCUTFACTOR'] / 100) * VMToUser_MP['NumberOfUnits']
    # dic_CSAParams['VMToCpty'] = 0 if len(VMToCpty) == 0 else VMToCpty_MP['HCcalc'].sum()
    # dic_CSAParams['VMToUser'] = 0 if len(VMToUser) == 0 else VMToUser_MP['HCcalc'].sum()

    try:
        ps_IPs = pd.Series(
            list(map(float, df_InitialProfiles.loc[df_InitialProfiles['CSA'] == CSAName, 'Factors'].iloc[0])))
        ps_IPs = ps_IPs.rename(dict(zip(ps_IPs.index, l_referenceDates)))
    except:
        ps_IPs = 0

    try:
        df_SNSC = pd.read_csv(WORKPARTH + "\\Inputs" + '\\CollateralAllocationSNS.csv', sep=",").set_index('CSA')
        CSA_AllocFactor = df_SNSC.loc[CSAName, 'AllocFactor']
        CSA_AllocVM = df_SNSC.loc[CSAName, 'VM']

        dic_CSAParams['VMToCpty'] = - min(CSA_AllocVM, 0)
        dic_CSAParams['VMToUser'] = max(CSA_AllocVM, 0)

        # SNS CSA Allocation
        for k in ['userIA', 'cptyIA', 'UserVMMinTransfer', 'CPVMMinTransfer', 'UserVMThreshold', 'CPVMThreshold']:
            dic_CSAParams[k] = dic_CSAParams[k] * CSA_AllocFactor

    except:
        pass

    print('Paramétros acuerdo de colateral e Intial Profiles leidos: ' + CSAName)

    return dic_CSAParams, ps_IPs

def f_readInputs(WORKPARTH, CSAName, df_InitialProfiles):
    
    dic_CSAParams = { 'numAddCallDates' : 1, 'userIA':0.0, 'cptyIA':0.0 } # IA posted by the user and the cpty respectively
    
    try:
        df_CSAParam = pd.read_csv(  WORKPARTH + "\\Inputs" +  '\\CollateralAgreementVM.csv', sep="," ).set_index("Id")  
        dic_CSAParams.update({'CurePeriod':df_CSAParam["CurePeriod"][CSAName] ,  'MarginLag':df_CSAParam["MarginLag"][CSAName]  ,  'SettleLag':df_CSAParam["SettlementLag"][CSAName] , 
                              'CallPeriod':df_CSAParam["CallPeriod"][CSAName] ,  'CSACcy':df_CSAParam["AgreementCurrency"][CSAName] , 'Round':df_CSAParam["RoundoffAmount"][CSAName] ,
                              'UserVMMinTransfer':df_CSAParam["UserVMMinTransfer"][CSAName] ,  'CPVMMinTransfer':df_CSAParam["CPVMMinTransfer"][CSAName] , 
                              'UserVMThreshold':df_CSAParam["UserVMThreshold"][CSAName] ,  'CPVMThreshold':df_CSAParam["CPVMThreshold"][CSAName] , 
                              'SettlementNettingCcys':df_CSAParam["SettlementNettingCcys"][CSAName].split(", ") }    )
        
    except:
        df_CSAParamLeg = pd.read_csv(  WORKPARTH + "\\Inputs" +  '\\CollateralAgreementLegacy.csv', sep="," ).set_index("Id")     
        dic_CSAParams.update({'CurePeriod':df_CSAParamLeg["CurePeriod"][CSAName] ,  'MarginLag':df_CSAParamLeg["MarginLag"][CSAName]  ,  'SettleLag':df_CSAParamLeg["SettlementLag"][CSAName] , 
                      'CallPeriod':df_CSAParamLeg["CallPeriod"][CSAName] ,  'CSACcy':df_CSAParamLeg["AgreementCurrency"][CSAName] , 'Round':df_CSAParamLeg["RoundoffAmount"][CSAName] ,
                      'UserVMMinTransfer':df_CSAParamLeg["UserVMMinTransfer"][CSAName] ,  'CPVMMinTransfer':df_CSAParamLeg["CPVMMinTransfer"][CSAName] , 
                      'UserVMThreshold':df_CSAParamLeg["UserVMThreshold"][CSAName] ,  'CPVMThreshold':df_CSAParamLeg["CPVMThreshold"][CSAName] , 
                      'SettlementNettingCcys':df_CSAParamLeg["SettlementNettingCcys"][CSAName].split(", ") }    )
  
    
    dic_CSAParams['MPOR'] = dic_CSAParams['CurePeriod'] + max(dic_CSAParams['MarginLag'], dic_CSAParams['SettleLag'])
    
    # current VM collateral
    df_Mitigants = pd.read_csv(  WORKPARTH + "\\Inputs" +  '\\Mitigants.csv', sep="," )
    VMToCpty = df_Mitigants[ np.logical_and( df_Mitigants[ 'CollateralAgreementId'  ] == CSAName, df_Mitigants[ 'PostDirection'  ] == 'To Counterparty') ]['NumberOfUnits']
    VMToUser = df_Mitigants[ np.logical_and( df_Mitigants[ 'CollateralAgreementId'  ] == CSAName, df_Mitigants[ 'PostDirection'  ] == 'To User') ]['NumberOfUnits']
    dic_CSAParams['VMToCpty'] = 0 if len(VMToCpty) == 0 else VMToCpty.iloc[0]
    dic_CSAParams['VMToUser'] = 0 if len(VMToUser) == 0 else VMToUser.iloc[0]

    # adjust using the allocation factor
    # try:
    #     df_AllocFactor = pd.read_csv( WORKPARTH + "\\Inputs" +  '\\AllocationFactor.csv', sep="," ).set_index("CSA")  
    #     dic_CSAParams.update({'AllocationFactorCSA': df_AllocFactor["AllocFactorCSA"][CSAName] , 'AllocationFactorVM': df_AllocFactor["AllocFactorVM"][CSAName]  ,
    #                           'AllocationFactorIM': df_AllocFactor["AllocFactorIM"][CSAName] })  
    # except:
    #     dic_CSAParams.update({'AllocationFactorCSA': 1.0 , 'AllocationFactorVM': 1.0 , 'AllocationFactorIM': 1.0 })   
        
        
    try:
        ps_IPs = pd.Series( list(map(float, df_InitialProfiles.loc[ df_InitialProfiles['CSA']== CSAName, 'Factors' ].iloc[0] ) ) )
        ps_IPs = ps_IPs.rename( dict(zip(ps_IPs.index, l_referenceDates )  ) )
    except: 
        ps_IPs = 0
    
    
    try:
        df_SNSC = pd.read_csv(  WORKPARTH + "\\Inputs" +  '\\CollateralAllocationSNS.csv', sep="," ).set_index('CSA')
        CSA_AllocFactor = df_SNSC.loc[CSAName ,'AllocFactor']
        CSA_AllocVM = df_SNSC.loc[CSAName ,'VM']
        
        dic_CSAParams['VMToCpty'] = - min(CSA_AllocVM, 0) 
        dic_CSAParams['VMToUser'] = max(CSA_AllocVM, 0) 
        
        # SNS CSA Allocation
        for k in ['userIA','cptyIA','UserVMMinTransfer','CPVMMinTransfer', 'UserVMThreshold', 'CPVMThreshold']:
            dic_CSAParams[k] = dic_CSAParams[k] * CSA_AllocFactor
        
    except:
        pass
    
    
    print('Paramétros acuerdo de colateral e Intial Profiles leidos: '+CSAName)
    
    return dic_CSAParams, ps_IPs


# CSAName = i_CSA
def f_loadInitialProfiles(WORKPARTH):
    try:
        df_loaded =  pd.read_csv(  WORKPARTH + "\\Inputs" + '\\InitialProfiles.csv' )
        ps1 = df_loaded.loc[:,'Factors'].apply(lambda x : x.split(",") )
        df1 = pd.DataFrame( { 'CSA': df_loaded['SubContainerId'], 'Factors': ps1 } )
        
        # df['CounterpartyId'] = df_loaded['CounterpartyId']
        # df['ContainerId'] = df_loaded['ContainerId']
        # df['ExpressionId'] = df_loaded['ExpressionId']
        
        df1 = df1.loc[ df_loaded['ExpressionId']=='TRIM_EE_FullExposure_NoIM_P8', :  ]
        
        return  df1.dropna(subset = ['CSA'])
    
    except:
        return 0

def f_EEReport(df_CVFull, df_CVSmooth, df_CVFull_ISDA, l_referenceDates, ps_IPs, maxVto, sessionDate, outputCSAFile):   
    
    df_ExpFull = df_CVFull[df_CVFull>0].fillna(0)
    df_ExpSmooth = df_CVSmooth[df_CVSmooth>0].fillna(0)
    df_ExpFull_ISDA = df_CVFull_ISDA[df_CVFull_ISDA>0].fillna(0)
        
    df_EE = pd.DataFrame(index = l_referenceDates)
    df_EE['FullExp'], df_EE['SmoothExp'] , df_EE['FullExpISDA'] = df_ExpFull.mean() + ps_IPs , df_ExpSmooth.mean() + ps_IPs , df_ExpFull_ISDA.mean() + ps_IPs
    df_EE['SpikeExp'] =  df_EE['FullExp'] - df_EE['SmoothExp']
    df_EE['SpikeExpISDA'] =  df_EE['FullExpISDA'] - df_EE['SmoothExp']
    
    # truncamos exposicion mas alla de ultimo vencimiento
    df_EE.loc[ df_EE.index > maxVto, : ] = 0.0
    
    
    eEPEHorizon = min( (maxVto - sessionDate).days, 365)
    # eEPEHorizon = 365
    eEPEFull    = f_EE2eEPE(df_EE['FullExp'], numDays = eEPEHorizon)
    eEPESmooth  = f_EE2eEPE( df_EE['SmoothExp'], numDays = eEPEHorizon)
    eEPEFullISDA  = f_EE2eEPE( df_EE['FullExpISDA'], numDays = eEPEHorizon)
    EPECFSpikes = f_EE2EPE(df_EE['SpikeExp'] , numDays = eEPEHorizon)
    EPECFSpikesISDA = f_EE2EPE(df_EE['SpikeExpISDA'] , numDays = eEPEHorizon)
    
    df_EE['eEPEFull'], df_EE['eEPEFullISDA'], df_EE['eEPESmooth'], df_EE['EPECFSpikes'], df_EE['EPECFSpikesISDA'] = np.nan, np.nan, np.nan, np.nan, np.nan
    df_EE['eEPEFull'].iloc[0], df_EE['eEPEFullISDA'].iloc[0] , df_EE['eEPESmooth'].iloc[0],  df_EE['EPECFSpikes'].iloc[0],  df_EE['EPECFSpikesISDA'].iloc[0] = eEPEFull, eEPEFullISDA, eEPESmooth, EPECFSpikes, EPECFSpikesISDA
    
    
    df_EE.to_csv(outputCSAFile)
    
    return df_EE

    

def f_readDIM(pathDIM):
    
    try: 
        df_DIM =  pd.read_csv( pathDIM, sep="," , index_col=0 ).set_axis(  range(1,2001) ) # random draws
        df_DIM = df_DIM.set_axis(pd.to_datetime(df_DIM.columns.tolist(), dayfirst= True), axis=1)
    except:
        df_DIM = 0
    
    return df_DIM

def f_extractCSAList(inputMAFolder):
    directory_list = list()
    for root, dirs, files in os.walk(inputMAFolder, topdown=False):
        for name in dirs:
            directory_list.append(os.path.join(root, name))
    l_CSAs = []
    for dirs in directory_list:
        i_CSA = dirs.split("\\")[-1] 
        if i_CSA[:2] == "C_" or i_CSA == "NC":        
            l_CSAs.append( i_CSA )
    return l_CSAs

    