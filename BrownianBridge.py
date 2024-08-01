# -*- coding: utf-8 -*-
"""
Created on Fri Sep  3 10:44:26 2021

@author: CIzquierdo - SS&C Algorithmics

Funcion de replica de calculo de interpolacion Bronwnian Bridge usada en estimacion de Variation Margin Full y Smooth
"""

import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import numpy as np
from numpy.linalg import linalg

def f_BrownianBridge_PhysColl(inputCSAFolder, sessionDate, l_referenceDates, df_RD, df_RD_CMPuser, df_RD_CMPcpty, df_NodeMtM , df_MtMPayments, df_CMPuser, df_CMPcpty, dic_CSAParams, load=1, transf_matrix_load = 1, filepath=''):
    if load == 0:  # do not generate again, read the saved one
        df_Output_BBMtM = pd.read_csv(filepath + "\\BBMtM.csv", index_col=0)
        df_Output_BBuser = pd.read_csv(filepath + "\\BBCMPuser.csv", index_col=0)
        df_Output_BBcpty = pd.read_csv(filepath + "\\BBCMPcpty.csv", index_col=0)
        return df_Output_BBMtM.set_axis(pd.to_datetime(df_Output_BBMtM.columns.tolist(), yearfirst = True), axis=1), df_Output_BBuser.set_axis(pd.to_datetime(df_Output_BBuser.columns.tolist(), yearfirst=True), axis=1), df_Output_BBcpty.set_axis(pd.to_datetime(df_Output_BBcpty.columns.tolist(), yearfirst=True), axis=1)

    MPOR, MarginLag, CallPeriod, numAddCallDates = int(dic_CSAParams['MPOR']), int(dic_CSAParams['MarginLag']), \
        int(dic_CSAParams['CallPeriod']), int(dic_CSAParams['numAddCallDates'])
    print('\rInterpolando Brownian Bridge', end='')
    l_scen = df_NodeMtM.index.tolist()

    l_BBDates = f_datesToBBInterpol(l_referenceDates, dic_CSAParams, MarginLag, numAddCallDates, sessionDate)

    l_allMtMDates = sorted(list(set(l_BBDates + l_referenceDates)))

    # output BB MtM cube
    df_BBMtM = pd.DataFrame(index=l_scen, columns=l_allMtMDates)
    df_BBAux = pd.DataFrame(index=l_scen, columns=l_allMtMDates)
    df_BBCMP_user = pd.DataFrame(index=l_scen, columns=l_allMtMDates)
    # df_BBAuxCMP_user = pd.DataFrame(index=l_scen, columns=l_allMtMDates)
    df_BBCMP_cpty = pd.DataFrame(index=l_scen, columns=l_allMtMDates)
    # df_BBAuxCMP_cpty = pd.DataFrame(index=l_scen, columns=l_allMtMDates)

    # data frame of BB in each scenario
    df_MtMScen = pd.DataFrame({'startDate': pd.Series(np.nan, index=l_allMtMDates),
                               'endDate': pd.Series(np.nan, index=l_allMtMDates),
                               'lastRefDate': pd.Series(np.nan, index=l_allMtMDates),
                               'weight': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[0,0]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[0,1]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[0,2]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[1,0]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[1,1]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[1,2]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[2,0]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[2,1]': pd.Series(np.nan, index=l_allMtMDates),
                               'Inv_Transf_Matrix[2,2]': pd.Series(np.nan, index=l_allMtMDates), })

    # 'TFAdj': pd.Series(0 , index = l_allMtMDates ) ,

    # weights vector for BB dates not in reference date list
    l_BBNoNRefDates = [x for x in l_BBDates if (x not in l_referenceDates)]
    for i_date in l_BBNoNRefDates:
        lowerDate = [x for x in l_allMtMDates if x < i_date][-1]
        higherDate = [x for x in l_referenceDates if x > i_date][0]
        weight = (higherDate - i_date).days / (higherDate - lowerDate).days

        df_MtMScen.loc[i_date, 'weight'] = weight
        df_MtMScen.loc[i_date, 'startDate'] = lowerDate
        df_MtMScen.loc[i_date, 'endDate'] = higherDate
        df_MtMScen.loc[i_date, 'lastRefDate'] = [x for x in l_referenceDates if x < i_date][-1]

    #create a list of numpy 3x3 matrices
    if transf_matrix_load ==1 :
        l_BB_TM = [f_BB_Random_Normal_vector(x_ED, l_referenceDates, df_NodeMtM, df_MtMPayments, df_CMPuser, df_CMPcpty) for x_ED in
                  l_referenceDates[1:]]
        ps_BBSD = pd.Series(l_BB_TM, index=l_referenceDates[1:])

        for i_row in df_MtMScen.index:
            if (not pd.isna(df_MtMScen.loc[i_row, 'endDate'])):
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[0,0]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][0][0]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[0,1]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][0][1]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[0,2]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][0][2]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[1,0]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][1][0]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[1,1]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][1][1]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[1,2]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][1][2]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[2,0]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][2][0]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[2,1]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][2][1]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[2,2]'] = ps_BBSD[df_MtMScen.loc[i_row, 'endDate']][2][2]

        df_MtMScen.to_csv(filepath + "\\Inv_Transformation_Matrix.csv")

    else:
        Transformation_Matrix = pd.read_csv(inputCSAFolder + "\\Transformation_Matrix.csv")
        l_BB_TM = []
        dates = []
        for i_date in Transformation_Matrix.columns:
                mat = Transformation_Matrix[i_date]
                trn_mat = np.array([[float(mat[0]), float(mat[1]), float(mat[2])], [float(mat[3]), float(mat[4]), float(mat[5])], [float(mat[6]), float(mat[7]), float(mat[8])]])
                if trn_mat.sum()>0:
                    inv_mat = np.transpose(trn_mat)
                    l_BB_TM.append(inv_mat)
                    dates.append(pd.to_datetime(i_date))
                else:
                    continue
        ps_BBSD = pd.Series(l_BB_TM, index=dates)
        for i_row in df_MtMScen.index:
            if (i_row in ps_BBSD.index):
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[0,0]'] = ps_BBSD[i_row][0][0]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[0,1]'] = ps_BBSD[i_row][0][1]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[0,2]'] = ps_BBSD[i_row][0][2]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[1,0]'] = ps_BBSD[i_row][1][0]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[1,1]'] = ps_BBSD[i_row][1][1]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[1,2]'] = ps_BBSD[i_row][1][2]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[2,0]'] = ps_BBSD[i_row][2][0]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[2,1]'] = ps_BBSD[i_row][2][1]
                df_MtMScen.loc[i_row, 'Inv_Transf_Matrix[2,2]'] = ps_BBSD[i_row][2][2]

        df_MtMScen.to_csv(filepath + "\\Inv_Transformation_Matrix.csv")

    df_MtMScen['BB'] = np.nan
    df_MtMScen['MtM'] = 0

    i_date = l_referenceDates[0]
    for i_date in l_referenceDates[:-1]:

        #MtM
        df_BBMtM.loc[:, i_date] = df_NodeMtM[i_date][:]

        df_BBAux.loc[:, i_date] = df_NodeMtM[i_date] - df_MtMPayments.loc[:,
                                                       np.logical_and(i_date <= df_MtMPayments.columns,
                                                                      df_MtMPayments.columns < l_referenceDates[
                                                                          l_referenceDates.index(i_date) + 1])].sum(
            axis=1)

        #CMP user
        df_BBCMP_user.loc[:, i_date] = df_CMPuser[i_date][:]

        # df_BBAuxCMP_user.loc[:, i_date] = df_CMPuser[i_date]

        #CMP counterparty
        df_BBCMP_cpty.loc[:, i_date] = df_CMPcpty[i_date][:]

        # df_BBAuxCMP_cpty.loc[:, i_date] = df_CMPcpty[i_date]

    df_BBMtM.loc[:, l_referenceDates[-1]] = df_NodeMtM.loc[:, l_referenceDates[-1]]
    df_BBCMP_user.loc[:, l_referenceDates[-1]] = df_CMPuser.loc[:, l_referenceDates[-1]]
    df_BBCMP_cpty.loc[:, l_referenceDates[-1]] = df_CMPcpty.loc[:, l_referenceDates[-1]]


    i_date = l_BBNoNRefDates[0]
    for i_date in l_BBNoNRefDates:

        if (i_date not in df_RD.columns):
            break

        vector_random_numbers = pd.DataFrame({'df_RD':df_RD.loc[:, i_date], 'df_RD_CMPcpty':df_RD_CMPcpty.loc[:, i_date], 'df_RD_CMPuser':df_RD_CMPuser.loc[:, i_date]})
        vector_random_numbers = vector_random_numbers.to_numpy()

        Inv_Transf_Matrix = np.array([
            [df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[0,0]'],df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[0,1]'],df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[0,2]']],
            [df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[1,0]'],df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[1,1]'],df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[1,2]']],
            [df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[2,0]'],df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[2,1]'],df_MtMScen.loc[i_date, 'Inv_Transf_Matrix[2,2]']]])

        randm_norm_vector = np.dot(vector_random_numbers, Inv_Transf_Matrix)
        randm_norm_vector = pd.DataFrame(randm_norm_vector)

        #MtMBB generation
        v_ranComp = randm_norm_vector[0] * np.sqrt(
            (i_date - df_MtMScen.loc[i_date, 'startDate']).days * (df_MtMScen.loc[i_date, 'endDate'] - i_date).days /
            (df_MtMScen.loc[i_date, 'endDate'] - df_MtMScen.loc[i_date, 'startDate']).days / (
                        df_MtMScen.loc[i_date, 'endDate'] - df_MtMScen.loc[i_date, 'lastRefDate']).days
        )
        v_ranComp = v_ranComp.set_axis(l_scen)

        df_BBAux.loc[:, i_date] = df_MtMScen.loc[i_date, 'weight'] * df_BBAux.loc[:,
                                                                     df_MtMScen.loc[i_date, 'startDate']] + \
                                  (1 - df_MtMScen.loc[i_date, 'weight']) * df_BBMtM.loc[:, df_MtMScen.loc[
                                                                                               i_date, 'endDate']] + v_ranComp

        df_BBMtM.loc[:, i_date] = df_BBAux.loc[:, i_date] + df_MtMPayments.loc[:,
                                                            np.logical_and(i_date <= df_MtMPayments.columns,
                                                                           df_MtMPayments.columns < df_MtMScen.loc[
                                                                               i_date, 'endDate'])].sum(axis=1)
        # CMP_user_BB generation
        v_ranComp_CMP_user = randm_norm_vector[1] * np.sqrt(
                (i_date - df_MtMScen.loc[i_date, 'startDate']).days * (
                            df_MtMScen.loc[i_date, 'endDate'] - i_date).days /
                (df_MtMScen.loc[i_date, 'endDate'] - df_MtMScen.loc[i_date, 'startDate']).days / (
                        df_MtMScen.loc[i_date, 'endDate'] - df_MtMScen.loc[i_date, 'lastRefDate']).days
            )

        v_ranComp_CMP_user = v_ranComp_CMP_user.set_axis(l_scen)

        df_BBCMP_user.loc[:, i_date] = df_MtMScen.loc[i_date, 'weight'] * df_BBCMP_user.loc[:,
                                                                         df_MtMScen.loc[i_date, 'startDate']] + \
                                      (1 - df_MtMScen.loc[i_date, 'weight']) * df_BBCMP_user.loc[:, df_MtMScen.loc[
                                                                                                   i_date, 'endDate']] + v_ranComp_CMP_user

        # CMP_cpty_BB generation
        v_ranComp_CMP_cpty = randm_norm_vector[2] * np.sqrt(
            (i_date - df_MtMScen.loc[i_date, 'startDate']).days * (
                    df_MtMScen.loc[i_date, 'endDate'] - i_date).days /
            (df_MtMScen.loc[i_date, 'endDate'] - df_MtMScen.loc[i_date, 'startDate']).days / (
                    df_MtMScen.loc[i_date, 'endDate'] - df_MtMScen.loc[i_date, 'lastRefDate']).days
        )
        v_ranComp_CMP_cpty = v_ranComp_CMP_cpty.set_axis(l_scen)

        df_BBCMP_cpty.loc[:, i_date] = df_MtMScen.loc[i_date, 'weight'] * df_BBCMP_cpty.loc[:,
                                                                             df_MtMScen.loc[i_date, 'startDate']] + \
                                          (1 - df_MtMScen.loc[i_date, 'weight']) * df_BBCMP_cpty.loc[:, df_MtMScen.loc[
                                                                                                            i_date, 'endDate']] + v_ranComp_CMP_cpty


    return df_BBMtM.fillna(0), df_BBCMP_user.fillna(0), df_BBCMP_cpty.fillna(0)

def f_BrownianBridge(sessionDate, l_referenceDates , df_RD, df_NodeMtM , df_MtMPayments, dic_CSAParams , load=1, filepath=''):
        
    if load == 0: # do not generate again, read the saved one 
        df_Output_BBMtM = pd.read_csv(filepath + "\\BBMtM.csv", index_col=0) 
        return df_Output_BBMtM.set_axis(pd.to_datetime(df_Output_BBMtM.columns.tolist(), yearfirst = True), axis=1)
    
    
    MPOR, MarginLag, CallPeriod, numAddCallDates = int(dic_CSAParams['MPOR']), int(dic_CSAParams['MarginLag']), \
                                                            int(dic_CSAParams['CallPeriod']), int(dic_CSAParams['numAddCallDates'])
    print('\rInterpolando Brownian Bridge', end='')
    l_scen = df_NodeMtM.index.tolist()
    
    # df_NodeMtM = df_NodeMtM.set_axis(l_referenceDates, axis=1)
    # df_RD = df_RD.set_axis(pd.to_datetime(df_RD.columns.tolist(), dayfirst= True), axis=1)
    # df_MtMPayments = df_MtMPayments.set_axis(pd.to_datetime(df_MtMPayments.columns.tolist(), dayfirst= True), axis=1)
    
    
    l_BBDates = f_datesToBBInterpol(l_referenceDates, dic_CSAParams , MarginLag , numAddCallDates, sessionDate)
    
    l_BBSD = [ f_BBStandardDeviation(x_ED, l_referenceDates, df_NodeMtM, df_MtMPayments) for x_ED in l_referenceDates[1:] ]
    ps_BBSD = pd.Series(l_BBSD, index = l_referenceDates[1:] )
    
    l_allMtMDates = sorted(list(set(l_BBDates+l_referenceDates)))
    
    #output BB MtM cube
    df_BBMtM = pd.DataFrame( index=l_scen , columns =l_allMtMDates )
    
    df_BBAux = pd.DataFrame( index=l_scen , columns =l_allMtMDates )
    
    # data frame of BB in each scenario
    
    df_MtMScen = pd.DataFrame( {'startDate': pd.Series(np.nan , index = l_allMtMDates ) , 
                                'endDate': pd.Series(np.nan , index = l_allMtMDates ) , 
                                'lastRefDate': pd.Series(np.nan , index = l_allMtMDates ) , 
                                'weight': pd.Series(np.nan , index = l_allMtMDates ) , 
                                'StdDev': pd.Series(np.nan , index = l_allMtMDates )  } )
                                #'TFAdj': pd.Series(0 , index = l_allMtMDates ) ,
    
    #weights vector for BB dates not in reference date list
    l_BBNoNRefDates = [x for x in l_BBDates if (x not in l_referenceDates)]
    for i_date in l_BBNoNRefDates:
        lowerDate = [x for x in l_allMtMDates if x < i_date][-1]
        higherDate = [x for x in l_referenceDates if x > i_date][0]
        weight = (higherDate - i_date).days / (higherDate - lowerDate).days
        
        df_MtMScen.loc[i_date, 'weight'] = weight
        df_MtMScen.loc[i_date, 'startDate'] = lowerDate
        df_MtMScen.loc[i_date, 'endDate'] = higherDate
        df_MtMScen.loc[i_date, 'lastRefDate'] = [x for x in l_referenceDates if x < i_date][-1]
    
    
    for i_row in df_MtMScen.index:
        if ( not pd.isna( df_MtMScen.loc[i_row,'endDate']  )):
            df_MtMScen.loc[i_row,'StdDev'] = ps_BBSD[ df_MtMScen.loc[i_row,'endDate'] ]
    
    
    df_MtMScen['BB'] = np.nan
    df_MtMScen['MtM'] = 0
    
    i_date = l_referenceDates[0]
    for i_date in l_referenceDates[:-1]:
        df_BBMtM.loc[:,i_date] = df_NodeMtM[i_date][:]
        
        df_BBAux.loc[:,i_date] = df_NodeMtM[i_date] - df_MtMPayments.loc[:,
            np.logical_and(i_date <= df_MtMPayments.columns, df_MtMPayments.columns < l_referenceDates[ l_referenceDates.index(i_date) + 1] )  ].sum(axis=1)
    
    df_BBMtM.loc[:,l_referenceDates[-1]] = df_NodeMtM.loc[:,l_referenceDates[-1]]
    
    
    i_date = l_BBNoNRefDates[0]
    for i_date in l_BBNoNRefDates:
        
        if (i_date not in  df_RD.columns ):
            break
    
        v_ranComp = df_MtMScen.loc[i_date,'StdDev'] * df_RD.loc[:, i_date] * np.sqrt(
              (i_date - df_MtMScen.loc[i_date,'startDate']).days * (df_MtMScen.loc[i_date,'endDate'] - i_date).days / 
              (df_MtMScen.loc[i_date,'endDate'] - df_MtMScen.loc[i_date,'startDate']).days / (df_MtMScen.loc[i_date,'endDate'] - df_MtMScen.loc[i_date,'lastRefDate']).days 
            )
        
        df_BBAux.loc[:,i_date]   = df_MtMScen.loc[i_date,'weight'] * df_BBAux.loc[:,df_MtMScen.loc[i_date,'startDate']] + \
                                        (1-df_MtMScen.loc[i_date,'weight'] ) * df_BBMtM.loc[:,df_MtMScen.loc[i_date,'endDate']] + v_ranComp
        
        
        df_BBMtM.loc[:,i_date] = df_BBAux.loc[:,i_date] + df_MtMPayments.loc[:, 
                np.logical_and(i_date <= df_MtMPayments.columns, df_MtMPayments.columns <  df_MtMScen.loc[i_date,'endDate'] )  ].sum(axis=1)
    
    print('\rInterpolacion Brownian Bridge completada')
    
    return df_BBMtM.fillna(0)

def f_readsRandomDraws(pathRD, dic_CSAParams, l_referenceDates, sessionDate, maxVto):
    
    try: 
        df_RD = pd.read_csv( pathRD, sep="," , index_col=0 ).set_axis(  range(1,2001) ) # random draws
        df_RD = df_RD.set_axis(pd.to_datetime(df_RD.columns.tolist(), dayfirst= True), axis=1)
    except:
        np.random.seed(100)
        l_randomInterDates =  f_datesToBBInterpol(l_referenceDates, dic_CSAParams , dic_CSAParams['MarginLag'] , dic_CSAParams['numAddCallDates'] , sessionDate)
        l_randomInterDates = [x for x in l_randomInterDates if (x < maxVto) and (x not in l_referenceDates)]
        df_RD = pd.DataFrame(np.random.normal(size=(2000, len(l_randomInterDates))) , index=range(1,2001), columns= l_randomInterDates  )
    
    return df_RD

def f_datesToBBInterpol(l_referenceDates, dic_CSAParams , MarginLag , numAddCallDates, sessionDate):

    MPOR = dic_CSAParams['MPOR']

    if dic_CSAParams['CallPeriod']==1:

        l_output = []
        for i_date in l_referenceDates:
            for j in range(MPOR-MarginLag,  MPOR+1+numAddCallDates):
                l_output.append( i_date - timedelta(days=j) )
        l_output = sorted(list(set(l_output)))
        l_output = [i for i in l_output if i > sessionDate]

    else:

        frequency = str(dic_CSAParams['CallPeriod']) + 'D'
        ending_date = max(l_referenceDates)
        calldates = pd.date_range(start=sessionDate, end=ending_date, freq=frequency)

        # (1') Reporting date - MPoR
        RepdateminuMPoR = [max(date - timedelta(days=int(MPOR)), sessionDate) for date in l_referenceDates]

        # (2') Find first call date on or before date caculated in (1')
        FindfirstCallDate = []
        for date in RepdateminuMPoR:

            if date in calldates:

                FindfirstCallDate.append(date)

            else:

                missing_date = calldates[np.searchsorted(calldates, date) - 1]

                FindfirstCallDate.append(missing_date)

        calldates = pd.Series(calldates)
        FindfirstCallDate = pd.Series(FindfirstCallDate)

        # (3') Additional call dates
        AddCalldates = []
        for date in FindfirstCallDate:
            position = max(0, np.where(calldates == date)[0][0] + 1 - 1 - numAddCallDates)

            AddCalldates.append(calldates[position])

        AddCalldates = pd.Series(AddCalldates)

        # merge (2) and (3), drop duplicates, drop dates present in reference dates and smaller than session date and grater than max maturity
        merged_dates = pd.concat([AddCalldates, FindfirstCallDate]).drop_duplicates().reset_index(drop=True)

        l_output = list(merged_dates.sort_values(axis=0, ascending=True).reset_index(drop=True))
        l_output = [i for i in l_output if i > sessionDate]

    return l_output

def f_BBStandardDeviation(endDate, l_referenceDates, df_NodeMtM, df_MtMPayments):
    startDate = l_referenceDates[ l_referenceDates.index(endDate) - 1]
    ps_MtMStart = df_NodeMtM[startDate]
    ps_MtMEnd = df_NodeMtM[endDate]
    l_payDates = [ x for x in df_MtMPayments.columns.tolist() if startDate <= x < endDate ]
    ps_Payments = df_MtMPayments[l_payDates].sum(axis=1)
    return (ps_MtMEnd - ps_MtMStart + ps_Payments).std()

def f_BBStandardDeviationCMP(endDate, l_referenceDates, CMP):
    startDate = l_referenceDates[l_referenceDates.index(endDate) - 1]
    ps_MtMStart = CMP[startDate]
    ps_MtMEnd = CMP[endDate]
    return (ps_MtMEnd - ps_MtMStart).std()

def f_BB_Random_Normal_vector(endDate, l_referenceDates, df_NodeMtM, df_MtMPayments, df_CMPuser, df_CMPcpty):

    #calculate the VCV matrix (3X3) of netted MtMs, CMP user and CMP cpty

    #netted MtMs
    startDate = l_referenceDates[l_referenceDates.index(endDate) - 1]
    ps_MtMStart = df_NodeMtM[startDate]
    ps_MtMEnd = df_NodeMtM[endDate]
    l_payDates = [x for x in df_MtMPayments.columns.tolist() if startDate <= x < endDate]
    ps_Payments = df_MtMPayments[l_payDates].sum(axis=1)
    netted_MtMs = ps_MtMEnd - ps_MtMStart + ps_Payments

    #CMP_user
    ps_CMPuserstart = df_CMPuser[startDate]
    ps_CMPuserEnd = df_CMPuser[endDate]
    CMPuser = ps_CMPuserEnd - ps_CMPuserstart

    #CMP_cpty
    ps_df_CMPcptystart = df_CMPcpty[startDate]
    ps_df_CMPcptyEnd = df_CMPcpty[endDate]
    CMPcpty = ps_df_CMPcptyEnd - ps_df_CMPcptystart

    data_aggregated = np.array([netted_MtMs.fillna(0), CMPcpty.fillna(0), CMPuser.fillna(0)])
    VCV = np.cov(data_aggregated)

    #calculate the transformed matrix
    eigenValues, eigenVectors = linalg.eig(VCV)
    idx = eigenValues.argsort()
    eigenVectors = eigenVectors[:, idx]
    eigenValues.sort()
    Λ = np.diag(eigenValues)
    S = np.sqrt(Λ)
    Transformation_Matrix = np.matmul(eigenVectors, S)
    Transformation_Matrix_T = np.transpose(Transformation_Matrix)

    return Transformation_Matrix_T