# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 10:48:57 2021
Funciones de lectura de cubos exportados a texto plano con aplicacion MAG ShowCube 
Funciones de agreagacion de MtM y flujos
Funcion para calculo de CF Spikes y metrica MPORTradeFlows (flujos en MPOR)
"""

import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import numpy as np
import pickle

def f_getMaturityDateFromCF(dic_CF):
    
    ps_matDate = pd.Series(data= pd.to_datetime("1950-01-1"), index = dic_CF.keys())
    for i_trade in dic_CF:
        mat_date = pd.to_datetime("1950-01-1")
        for i_flowType in dic_CF[i_trade]:
            if dic_CF[i_trade][i_flowType]['outType'] != 'Fixed':
                mat_date = max( [mat_date , max(  pd.to_datetime(dic_CF[i_trade][i_flowType]['cube'].columns.tolist() )  )] )
            else:
                mat_date = max( [mat_date , max(  pd.to_datetime(dic_CF[i_trade][i_flowType]['cube'].index.tolist() )  )] )
        
        ps_matDate[i_trade] = mat_date     
    return ps_matDate
        
# CSACcy = 'EUR'
# SettleLag = 3
# MPOR = 14

def f_ColModelCFMetrics( dic_CFAg ,dic_CFAgCLS, dic_ccy, l_referenceDates, reporCcy, CSACcy, SettleLag , MPOR, sessionDate, WORKPARTH, i_MA, i_CSA,
                        load = 1,  # if 0 reads from RW file, 1 reads already generated file , filepath necessary if read = 0
                        readFromMAG = False):  # if True reads from MAG Detail Functions from Input Folder
  
    if readFromMAG:
        filepath = WORKPARTH + "\\Inputs\\" + i_MA + "\\" + i_CSA
        df_MPORTF, df_MPORSP , df_ModelPay = pd.read_csv(filepath + "\\MPORTradeFlows.csv", index_col=0) , pd.read_csv(filepath + "\\MPORSettleFlows.csv", index_col=0) , pd.read_csv(filepath + "\\ModelledPayMtM.csv", index_col=0)  
        return df_MPORTF.set_axis(pd.to_datetime(df_MPORTF.columns.tolist(), yearfirst = True), axis=1), df_MPORSP.set_axis(pd.to_datetime(df_MPORSP.columns.tolist(), yearfirst = True), axis=1), df_ModelPay.set_axis(pd.to_datetime(df_ModelPay.columns.tolist(), yearfirst = True), axis=1)
 
    if load == 0: # do not generate again, read the saved one
        filepath = WORKPARTH + "\\Outputs\\" + i_MA + "\\" + i_CSA
        df_MPORTF, df_MPORSP, df_MPORSP_ISDA , df_ModelPay = pd.read_csv(filepath + "\\MPORTradeFlows.csv", index_col=0) , pd.read_csv(filepath + "\\MPORSettleFlows.csv", index_col=0) , pd.read_csv(filepath + "\\MPORSettleFlowsISDA.csv", index_col=0), pd.read_csv(filepath + "\\ModelledPayMtM.csv", index_col=0)  
        return df_MPORTF.set_axis(pd.to_datetime(df_MPORTF.columns.tolist(), yearfirst = True), axis=1), df_MPORSP.set_axis(pd.to_datetime(df_MPORSP.columns.tolist(), yearfirst = True), axis=1), df_MPORSP_ISDA.set_axis(pd.to_datetime(df_MPORSP_ISDA.columns.tolist(), yearfirst = True), axis=1),  df_ModelPay.set_axis(pd.to_datetime(df_ModelPay.columns.tolist(), yearfirst = True), axis=1)
 
    # RETURNS THE EQUIVALENT TO DETAIL EXPRESSION MPORTradeFlows, MPORSettledPayments_Sett AND ModelledMtMPayments
    
    # inicialize output
    df_MPORTF = pd.DataFrame(index = range(1,2001), columns =  l_referenceDates ).fillna(0.0)
    df_MPORSP = pd.DataFrame(index = range(1,2001), columns =  l_referenceDates ).fillna(0.0)
    df_MPORSP_ISDA = pd.DataFrame(index = range(1,2001), columns =  l_referenceDates ).fillna(0.0)
    df_ModelPay = pd.DataFrame(index = range(1,2001))
    
    # dict containig outflows
    dic_OFAg = {}
    for i_ccy in dic_CFAgCLS:
        dic_OFAg[i_ccy] = dic_CFAgCLS[i_ccy].copy()
        dic_OFAg[i_ccy][dic_OFAg[i_ccy] > 0] = 0
        
    print('\rAgregando CF: MPORTradeFlows y MPORSettledPayments', end='')
    
    for i_date in l_referenceDates:
        
        startMPOR = max( sessionDate, i_date - timedelta(days=MPOR) )
        endSettleLag =  i_date - timedelta(days=MPOR-SettleLag) 
        for i_ccy in dic_CFAg:
            ccy = i_ccy[-3:] 
            # MPORTradeFlows
            MPORTF = dic_CFAg[i_ccy].loc[:, np.logical_and(  startMPOR <= dic_CFAg[i_ccy].columns, dic_CFAg[i_ccy].columns <  i_date )  ].sum(axis=1)
            df_MPORTF.loc[:, i_date] += MPORTF * dic_ccy[ccy][i_date]
        
        for i_ccy in dic_CFAgCLS:
            # MPORSettledPayments_Sett
            ccy = i_ccy[-3:]
            MPORSP = dic_OFAg[i_ccy].loc[:, np.logical_and(  startMPOR <= dic_OFAg[i_ccy].columns, dic_OFAg[i_ccy].columns <  endSettleLag )  ].sum(axis=1)
            MPORSP_ISDA = dic_CFAgCLS[i_ccy].loc[:, np.logical_and(  startMPOR <= dic_CFAgCLS[i_ccy].columns, dic_CFAgCLS[i_ccy].columns <  endSettleLag )  ].sum(axis=1)
            df_MPORSP.loc[:, i_date] += MPORSP * dic_ccy[ccy][i_date]
            df_MPORSP_ISDA.loc[:, i_date] += MPORSP_ISDA * dic_ccy[ccy][i_date]
    
    df_MPORSP_ISDA[df_MPORSP_ISDA > 0] = 0
    
    print('\rCubos MPORTradeFlows y MPORSettledPayments agregados y guardados en divisa de reporting: '+reporCcy, end='\n')
    
    # ModelledMtMPayments
    print('\rAgregando CF: ModelledMtMPayments', end='')
    
    for i_ccy in dic_CFAg:
        
        ccy = i_ccy[-3:]
        
        for i_date in dic_CFAg[i_ccy]:
            
            #i_date = pd.to_datetime("2020-08-27")
            
            # gets index of the closest date to i_date in l_referenceDates, if two dates at the same distance it takes the higher of the dates, 
            # if we wanted to take the lower use commented code below
            ccyDate_index = len(l_referenceDates) - np.argmin( [ abs((x-i_date).days) for x in  l_referenceDates[::-1] ] ) - 1
            # ccyDate_index = np.argmin( [ abs((x-i_date).days) for x in  l_referenceDates ] )
            
            # ccy exchange rate taken from the closest Reference Date to the flow date
            ccyDate = l_referenceDates[ccyDate_index]
            
            if i_date in df_ModelPay:
                df_ModelPay.loc[:,i_date] += dic_CFAg[i_ccy].loc[:, i_date] * dic_ccy[ccy].loc[:, ccyDate] / dic_ccy[CSACcy].loc[:, ccyDate]
            else:            
                newcolum = dic_CFAg[i_ccy].loc[:, i_date] * dic_ccy[ccy].loc[:, ccyDate] / dic_ccy[CSACcy].loc[:, ccyDate]
                df_ModelPay = pd.concat([df_ModelPay, newcolum.rename(i_date)], axis=1)
                
    df_ModelPay = df_ModelPay.reindex(sorted(df_ModelPay.columns), axis=1)
    
    print('\rCubo ModelledMtMPayments agregado y transformado en divisa del acuerdo de colateral: '+CSACcy )
    
    return df_MPORTF.fillna(0) , df_MPORSP.fillna(0) , df_MPORSP_ISDA.fillna(0) , df_ModelPay.fillna(0)
    
      


# filepath = outputCSAFolder
# # i_trade = "MX_31104318_SYN"
# # i_tipoflujo = "USD"
# l_NettingCcys = dic_CSAParams['SettlementNettingCcys']
   # adds BC flow 
   
def f_AggregatesPerCcyCLS(dic_CF ,l_NettingCcys, l_CLSTrades, load = 1, filepath = ''):
    
    # execution with all trades
    dic_CFAg = f_AggregatesPerCcy(dic_CF ,l_NettingCcys, load = load, filepath = filepath )
    
    # execution only with CLS trade flows
    dic_CFCLS = dic_CF.copy()   
    l_CLSinCSA = [i_trade for i_trade in dic_CFCLS.keys() if i_trade in l_CLSTrades]
    for i_trade in l_CLSinCSA: del dic_CFCLS[i_trade]
    dic_CFAgCLS = f_AggregatesPerCcy(dic_CFCLS ,l_NettingCcys, load = load, filepath = filepath )
    
    return dic_CFAg, dic_CFAgCLS
   
   
def f_AggregatesPerCcy(dic_CF ,l_NettingCcys, load = 1, filepath = ''):
    # AGGREGATE CF CUBES PER CCY, EACH CCY CF CUBES FORM A DATA FRAME INSIDE THE DICTIONARY dic_CFAg
    
    if load == 0: # do not generate again, read the saved one
        with open(filepath + "\\dic_CFAg.pickle", 'rb') as config_dictionary_file:
            return pickle.load( config_dictionary_file)
    # with open(filepath + "\\dic_CFAg.pickle", 'rb') as config_dictionary_file:
    #     dic_CFAg = pickle.load( config_dictionary_file)
      
    n_trade = 0
    n_trades = len(dic_CF)
    dic_CFAg = {}
    
    for i_trade in dic_CF:
        n_trade += 1
        
        print('\rAgregando cubo trade '+format(n_trade)+' de '+format(n_trades), end='')
        
        for i_tipoflujo in dic_CF[i_trade]:

            dic = dic_CF[i_trade][i_tipoflujo]
            outType = dic["outType"]
            ccy = dic["ccy"]
            flowType = dic["flowType"]
            cube = dic["cube"].astype(float)
            
            # cash flow
            if flowType[:4] == 'cash' and ccy in l_NettingCcys:
                if flowType in dic_CFAg.keys():
                    if outType != "Fixed":
                        dic_CFAg[flowType] = dic_CFAg[flowType].add(cube, fill_value=0)
                    else:
                        for i_index in cube.index:
                            if i_index in dic_CFAg[flowType].columns:
                                dic_CFAg[flowType][i_index] += cube[i_index]
                            else:
                                dic_CFAg[flowType][i_index] = cube[i_index]
                              
                else:
                    if outType != "Fixed":
                        dic_CFAg[flowType] = cube
                    else:
                        dic_CFAg[flowType] = pd.DataFrame(columns = cube.index , index = range(1,2001))
                        for i_index in cube.index:
                            dic_CFAg[flowType][i_index] = cube[i_index]
            
            # security flow or ccy not aggregated, no agregation within ccys
            else:
                if outType != "Fixed":
                    dic_CFAg[i_trade + flowType] = cube
                else:
                    dic_CFAg[i_trade + flowType] = pd.DataFrame(columns = cube.index , index = range(1,2001))
                    for i_index in cube.index:
                        dic_CFAg[i_trade + flowType][i_index] = cube[i_index]
  
    print('\rCash flow trade cubes agregados por divisa...........')
        
    
    return dic_CFAg


def f_readCcyRWFile(WORKPARTH, reporCcy, load = 1):
    # READ CCY FILE
    # retreive ccy cubes  and load them into dict of data frames dic_ccy

    if load == 0: # do not generate again, read the saved one
        with open(WORKPARTH + "\\Outputs\\dic_ccy.pickle", 'rb') as config_dictionary_file:
            return pickle.load( config_dictionary_file) , pd.to_datetime(pd.read_csv(WORKPARTH + '\\Outputs\\listRefDates.csv', index_col=0).iloc[:,0].tolist(), dayfirst= True).tolist()
    
    dic_ccy = {}
    
    with open( WORKPARTH + '\\Inputs\\' + 'dumpCubes_Currency.txt' , 'r' ) as ccyfile:
        n_ccys = 0
        for line in ccyfile.readlines():
            
            if (line == '\n' or line == ''):
                n_ccys += 1  
                print('\rLeyendo cubo divisa '+format(n_ccys), end='')
                
                if (n_ccys > 1 ):
                    dic_ccy[cur] = df_cur
                    
            elif (line[:10] == "Instrument" ): #extract reference dates
                lineSplit = line.split(",")[2:]
                l_referenceDates = pd.to_datetime([x[1:11] for x in lineSplit], dayfirst= True).tolist()
                
            
            elif (line[3] == ','):
                df_cur = pd.DataFrame( columns =  l_referenceDates )
                cur = line[:3]
                n_scen = 0
                    
            elif (line[:7]==",Credit"):   
                n_scen += 1
                numbers = line.split(",")[2:]
                #numbers[-1] = numbers[-1][:-1]
                
                df_cur.loc[  n_scen ,:] = list(map(float, numbers))
    
    print('\rCubo de divisas leido...........')
    
    # all ccy cubes taking as reference reporCcy 
    if (reporCcy not in dic_ccy):
        dic_ccy[reporCcy] = pd.DataFrame(index = range(1,2001), columns =  l_referenceDates ).fillna(1.0)
    
    else:  
        for i_ccy in dic_ccy:
            dic_ccy[i_ccy] = dic_ccy[i_ccy] / dic_ccy[reporCcy]
    
    print('Cubo de divisas tranformado tomando como referencia la fecha de reporting: '+reporCcy)
    
    return dic_ccy , l_referenceDates

def f_cashmitigants(nameMA, nameCSA, df_mitigant, dic_ccy,l_referenceDates):
    df_filtered = df_mitigant.loc[(df_mitigant["MasterAgreement"] == nameMA) & (df_mitigant["CollateralAgreement"] == nameCSA) & (df_mitigant["APPLICABILITY"] == "MITIGANT_POOL")]
    df_CMPcounterparty = df_filtered.loc[df_filtered["POSTDIRECTION"] == "To User"]
    df_CMPuser = df_filtered.loc[df_filtered["POSTDIRECTION"] == "To Counterparty"]
    df_CMPuser = df_CMPuser.reset_index(drop=True)
    df_CMPcounterparty = df_CMPcounterparty.reset_index(drop=True)

    if len(df_CMPuser) > 0:
        CMPuser = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)
        CMPuserHaD = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)
        for index, row in df_CMPuser.iterrows():
            CMPuser += dic_ccy[row["MTMCcy"]]*row["MTM"]*(1-row["HAIRCUTFACTOR"]/100)
            CMPuserHaD += dic_ccy[row["MTMCcy"]]*row["MTM"]*(1-row["HAIRCUTATDEFAULT"]/100)

    if len(df_CMPuser) == 0:
        CMPuser = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)
        CMPuserHaD = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)

    if len(df_CMPcounterparty) > 0:
        CMPcpty = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)
        CMPcptyHaD = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)
        for index, row in df_CMPcounterparty.iterrows():
            CMPcpty += dic_ccy[row["MTMCcy"]]*row["MTM"]*(1-row["HAIRCUTFACTOR"]/100)
            CMPcptyHaD += dic_ccy[row["MTMCcy"]] * row["MTM"] * (1 - row["HAIRCUTATDEFAULT"] / 100)

    if len(df_CMPcounterparty) == 0:
        CMPcpty = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)
        CMPcptyHaD = pd.DataFrame(index=range(1, 2001), columns=l_referenceDates).fillna(0)

    return CMPuser, CMPcpty, CMPuserHaD, CMPcptyHaD

def f_readMtMRWFile(WORKPARTH, nameMA, nameCSA, nodeType,  dic_ccy , l_referenceDates, df_BCDates, ps_posUnits, l_trades ='all', load = 1):
    # READ TRADE FLOW FILE
    # retreive aggregated MtM cube at node level, list of reference dates and pandas series of trades and its valuation ccy

    if load == 0: # do not generate again, read the saved one
        df_MtM_BC = pd.read_csv(WORKPARTH + "\\Outputs\\" + nameMA + "\\" + nameCSA + "\\MtMNodeAgBC" + ".csv", index_col=0)
        df_MtM_NoBC = pd.read_csv(WORKPARTH + "\\Outputs\\" + nameMA + "\\" + nameCSA + "\\MtMNodeAgNoBC" + ".csv", index_col=0)
        df_trades = pd.read_csv(WORKPARTH  + "\\Outputs\\" + nameMA + "\\" + nameCSA +  "\\MtMTradeInfo" + ".csv", index_col=0)
        df_BCDateMtM = pd.read_csv(WORKPARTH  + "\\Outputs\\" + nameMA + "\\" + nameCSA +  "\\BCDateMtM" + ".csv", index_col=0)
        
        df_trades['BCDate'] = pd.to_datetime(df_trades['BCDate'] , yearfirst = True)
        df_trades['ClosestRefDate'] = pd.to_datetime(df_trades['ClosestRefDate'] , yearfirst = True)
        return df_MtM_BC.set_axis(pd.to_datetime(df_MtM_BC.columns.tolist(), yearfirst = True), axis=1) , df_MtM_NoBC.set_axis(pd.to_datetime(df_MtM_NoBC.columns.tolist(), yearfirst = True), axis=1), df_trades, df_BCDateMtM
    
    n_scens = 2000
    n_scen = 0
    n_line = 0
    n_trades = 0
    findelbucle = False
    
    # data frame with trade info
    df_trades = pd.DataFrame(columns=["TradeID", "Ccy", "MtMSession", "BCDate", "MtMBeforeBC", "ClosestRefDate" ] )
    
    # create data frame output
    df_MtM_BC = pd.DataFrame(index=range(1,2001), columns=l_referenceDates).fillna(0)
    df_MtM_NoBC = pd.DataFrame(index=range(1,2001), columns=l_referenceDates).fillna(0)
    
    df_BCDateMtM = pd.DataFrame(index=range(1,2001))
    
    CFCubeInputFile = WORKPARTH + "\\Inputs\\" + nameMA + "\\" + nameCSA + "\\dumpCubes_Instruments_" + nodeType + ".txt"
        
    while not findelbucle:
    # while n_line < 2100:
        readMtMCube = False
        with open( CFCubeInputFile , 'r' ) as CFfile:
            for line in CFfile.readlines()[n_line:]:
                n_line += 1
                line = line[:-1]
                
                # if n_line == 6:
                #     break
                
                if line == '':
                    continue
                
                if (line[0] == "[" or line == "\n" ):
                    continue
                
                if (line[:14] == ",Base Scenario" or line[:10] == "Instrument" ):
                    continue
                    
                elif (line[:7] == ',Credit') :
                    
                    if l_trades =='all' or tradeID in l_trades:
                        readMtMCube = True
                        n_line -= 1
                        break
                    else:
                        readMtMCube = False
                    
                else: # new trade
                    
                    if (n_trades > 0): # save old trade info
                                            
                        df_MtMTrade = df_MtMTrade.fillna(0)         
                        # scale by baseline position units
                        df_MtMTrade = df_MtMTrade * ps_posUnits[tradeID]
                        
                        df_MtMTradeBC.loc[:,:] = df_MtMTrade.copy()
                        
                        # save closest MtM if BC 
                        if tradeID in df_BCDates.index:        
                            
                            BCDate = df_BCDates.loc[tradeID, "Break Clause Dates"]
                            # gets index of the closest date to i_date in l_referenceDates, if two dates at the same distance it takes the higher of the dates, 
                            # if we wanted to take the lower use commented code below
                            ClosestDate_index = len(l_referenceDates) - np.argmin( [ abs((x-BCDate).days) for x in  l_referenceDates[::-1] ] ) - 1
                            # ClosestDate_index = np.argmin( [ abs((x-BCDate).days) for x in  l_referenceDates ] )
                            
                            # ccy exchange rate taken from the closest Reference Date to the flow date
                            ClosestRefDate = l_referenceDates[ClosestDate_index]
                            
                            # add BC date
                            df_trades.iloc[-1,3] = BCDate
                            df_trades.iloc[-1,4] = ClosestRefDate < BCDate 
                            df_trades.iloc[-1,5] = ClosestRefDate
                                   
                            # add MtM that will be used to calculate BC flow
                            df_BCDateMtM.loc[:, tradeID ] = df_MtMTrade.loc[:, ClosestRefDate]
                            
                            # truncates after BC date                            
                            df_MtMTradeBC.loc[:, df_MtMTradeBC.columns > BCDate] = 0
    
                        # add MtM at session date
                        df_trades.iloc[-1,2] = df_MtMTrade.iloc[0,0]
                        
                        df_MtM_BC += df_MtMTradeBC * dic_ccy[ccy]  # multiplication by ccy cube, nan values not added 
                        df_MtM_NoBC += df_MtMTrade * dic_ccy[ccy]
                        
                        
                    if (line[:7] == "Elapsed"):
                        findelbucle = True
                        break  # end of the file
                    
                    else: # new trade info
                        n_trades += 1
                        n_scen = 0  
                        print('\rLeyendo cubo MtM, trade: '+format(n_trades), end='')
                        ccy = line.split(",")[-1][-3:]
                        tradeID = line.split(",")[0]
                        df_trades = df_trades.append(  {"TradeID": tradeID, "Ccy":ccy, "MtMSession":np.nan } ,ignore_index=True )
                        df_MtMTrade = pd.DataFrame(index=range(1,2001), columns=l_referenceDates).fillna(0)
                        df_MtMTradeBC = pd.DataFrame(index=range(1,2001), columns=l_referenceDates).fillna(0)
                     
        if readMtMCube: # lee el cubo, 2000 escenarios de golpe     
             
            n_scen = 1
            with open( CFCubeInputFile , 'r' ) as CFfile1:
                cubeLines = [line for line in CFfile1.readlines()[(n_line):(n_line+n_scens)  ] ]
            
            df_MtMTrade.loc[:,:] = pd.DataFrame( list(map(lambda line: list(map(float, line.split(",")[2:]  ) ), cubeLines ) ), columns= df_MtMTrade.columns, index= df_MtMTrade.index )
             
            # print(df_MtMTrade)
            n_line += n_scens
                   
    print('\rCubo de MtM leido '+ format(n_trades) +' operaciones....')
    
    df_trades = df_trades.set_index("TradeID") 
    
    return df_MtM_BC, df_MtM_NoBC, df_trades, df_BCDateMtM


# nameCSA= i_CSA
# nameMA= i_MA
# ps_posUnits = df_BaselinePos['PositionUnitsVAL']

def f_readCFRWFile(WORKPARTH, nameMA , nameCSA, ps_posUnits, load = 1):
    # READ TRADE FLOW FILE
    # retreive CF cubes  and load them into dict of data frames dic_CF
    
    oldMerge = False # read old merged CF cube w/o SFTType column
    # numeros de columna
    if oldMerge:
        col_scen = 5
        col_data = 6
    else:
        col_scen = 6
        col_data = 7      
        
    
    if load == 0: # do not generate again, read the saved one
        with open(WORKPARTH + "\\Outputs\\" + nameMA + "\\" + nameCSA + "\\dic_CF.pickle", 'rb') as config_dictionary_file:
            return pickle.load( config_dictionary_file) 
    
    dic_CF = {}   
    
    l_flowLine = []
    n_flowTypes = 0
    n_scen = 0
    n_line = 0
    n_trades = 0
    flow_date_line = 'd'
    numScens = 2000
    
    with open( WORKPARTH + '\\Inputs\\' + nameMA + "\\" + nameCSA +  '\\dumpCubes_SettlementFlow.txt' , 'r' ) as CFfile:
        

        
        for line in CFfile.readlines():
            

            
            n_line += 1
            line = line[:-1]
            
            # if n_line==8014:
            #     break
            
            if line == '':
                continue
            
            if (line[0] == "[" or line == "\n" or line[:10] == "Instrument" or line[:2] == "20"):
                continue
            
            elif (line[0] == ','):
                          
                lineSplit = line.split(",")
                
                if lineSplit[-1] == '':
                    if n_scen == 0 :
                        df_CFCube = pd.DataFrame( )
                    continue
                
                if (l_flowLine != lineSplit[:5] and n_scen > 0 ): # start a new cube and last cube is saved
                    # last cube save
                    
                    if l_flowLine[1] == 'Variable Dates': # fill empty scenarios
                        df_CFCube = (pd.DataFrame(columns = df_CFCube.columns, index = range(1,numScens+1)).fillna(0) + df_CFCube).fillna(0)  
                    
                    dic_Trade[ l_flowLine[2] + l_flowLine[4] ] = { 'cube':df_CFCube.fillna(0)*ps_posUnits[trade], 'ccy': l_flowLine[4], 'outType' : l_flowLine[1], 'flowType': l_flowLine[3]+l_flowLine[4] }
                    
                    # new flow line and reset scenario
                    l_flowLine = lineSplit[:5]
                    n_scen = 0
                    
                    
                l_flowLine = lineSplit[:5]
                
                if lineSplit[1] == 'Fixed Dates' : 
                    
                    if lineSplit[col_scen] == '' : # dates row    
                        l_dates = pd.to_datetime(lineSplit[col_data:], yearfirst = True )
                        df_CFCube = pd.DataFrame( columns =  l_dates )
                        n_scen = 0
                                     
                    elif lineSplit[col_scen][:6] == 'Credit' : # flow row
                        n_scen = int(lineSplit[col_scen].split("_")[-1])
                        df_CFCube.loc[  n_scen ,:] = list(map(float, lineSplit[col_data:] ))
                        df_CFCube.loc[  n_scen ,:] = df_CFCube.loc[  n_scen ,:] 
                        
                elif lineSplit[1] == 'Fixed' : 
                    
                    if (flow_date_line=="d"): # date line
                        flow_date_line = "f"
                        l_dates = pd.to_datetime(lineSplit[col_data:], yearfirst = True )
                        
                    else: # flow line
                        flow_date_line="d"
                        df_CFCube = pd.Series(index =  l_dates, data= list(map(float, lineSplit[col_data:] )) )
                        n_scen = 1
                    
                elif lineSplit[1] == 'Variable Dates' : 
                    if lineSplit[col_scen] == 'Base Scenario':
                        continue
                    
                    if (flow_date_line=="d"): # date line
                        flow_date_line = "f"
                        l_dates = pd.to_datetime(lineSplit[col_data:], yearfirst = True )
                        if n_scen == 0 :
                            df_CFCube = pd.DataFrame( )
                        
                        
                    else: # flow line
                        flow_date_line="d"
                        n_scen = int(lineSplit[col_scen].split("_")[-1])
                        for i_date in l_dates:
                            if i_date not in df_CFCube.columns:
                                df_CFCube[i_date] = 0
                                df_CFCube = df_CFCube.reindex(sorted(df_CFCube.columns), axis=1)
                                
                        
                        df_CFCube.loc[  n_scen , [x in l_dates for x in df_CFCube.columns  ] ] = list(map(float, lineSplit[col_data:] ))
                        df_CFCube.loc[  n_scen , [x in l_dates for x in df_CFCube.columns  ] ] = df_CFCube.loc[  n_scen , [x in l_dates for x in df_CFCube.columns  ] ]
                        
                
            else : #new trade and save the last one (it will save the last of the trades in txt file as will enter this code after the Elapsed Time line)
                n_trades += 1
                n_scen = 0
                
                if n_trades > 1:
                    print('\rLeyendo cubo Cash Flows, trade: '+format(n_trades), end='')
                    
                    if l_flowLine[1] == 'Variable Dates': # fill empty scenarios
                        df_CFCube = ( pd.DataFrame(columns = df_CFCube.columns, index = range(1,numScens+1)).fillna(0) + df_CFCube ).fillna(0)             
                    
                    dic_Trade[ l_flowLine[2] + l_flowLine[4] ] = { 'cube':df_CFCube.fillna(0)*ps_posUnits[trade], 'ccy': l_flowLine[4], 'outType' : l_flowLine[1], 'flowType': l_flowLine[3]+l_flowLine[4] }
                    dic_CF[trade] = dic_Trade
                trade = line.split(",")[0]
                
                
                dic_Trade = {}
    
    print('\rCubo de cash flows leido '+ format(n_trades-1) +' operaciones....')
    return dic_CF
               


    


