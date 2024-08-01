# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 13:46:21 2021
Funcion de replica de calculo de estimacion de Variation Margin Full y Smooth
"""
from datetime import timedelta
import numpy as np
import pandas as pd
from BrownianBridge import f_datesToBBInterpol
from tqdm import tqdm

def f_VariationMargin_unitsCMP(sessionDate, l_referenceDates, df_BBMtM, df_BB_CMPuser, df_BB_CMPcpty, dic_CSAParams, df_CMPuserHaD, df_CMPcptyHaD):
    MPOR, CurePeriod, MarginLag, SettleLag, CallPeriod, numAddCallDates, VMToCpty, VMToUser, Round, UserVMMinTransfer, CPVMMinTransfer, UserVMThreshold, CPVMThreshold, userIA, cptyIA = \
        dic_CSAParams['MPOR'], dic_CSAParams['CurePeriod'], dic_CSAParams['MarginLag'], dic_CSAParams['SettleLag'], \
        dic_CSAParams['CallPeriod'], \
            dic_CSAParams['numAddCallDates'], dic_CSAParams['VMToCpty'], dic_CSAParams['VMToUser'], dic_CSAParams[
            'Round'], dic_CSAParams['UserVMMinTransfer'], \
            dic_CSAParams['CPVMMinTransfer'], dic_CSAParams['UserVMThreshold'], dic_CSAParams['CPVMThreshold'], \
        dic_CSAParams['userIA'], dic_CSAParams['cptyIA']

    ###### AUX FUNCTIONS ####################

    def f_VMRequired(mtm):
        if mtm + userIA + cptyIA > CPVMThreshold:
            return mtm + userIA + cptyIA - CPVMThreshold
        elif mtm + userIA + cptyIA < UserVMThreshold:
            return mtm + userIA + cptyIA - UserVMThreshold
        else:
            return 0

    def f_VMMarginCall(VMBalanceAntes, VMRequired):
        if (UserVMMinTransfer < VMRequired - VMBalanceAntes < CPVMMinTransfer):
            return 0
        else:
            return round((VMRequired - VMBalanceAntes) / Round) * Round

    f_VMMarginCallVec = np.vectorize(f_VMMarginCall)

    print('\rCalculando Margin Calls', end='')

    l_BBDates = f_datesToBBInterpol(l_referenceDates, dic_CSAParams, MarginLag, numAddCallDates, sessionDate)

    df_VMRequired = df_BBMtM.apply(np.vectorize(f_VMRequired))

    # df_VMRequired.to_csv(outputCSAFolder + "\\VMRequired.csv")

    df_VMBalance = pd.DataFrame(index=df_BBMtM.index, columns=[sessionDate] + l_BBDates)
    df_VMMarginCall = pd.DataFrame(index=df_BBMtM.index, columns=[sessionDate] + l_BBDates)

    df_VMBalance.loc[:, sessionDate] = VMToUser - VMToCpty
    df_VMMarginCall.loc[:, sessionDate] = f_VMMarginCallVec(df_VMBalance.loc[:, sessionDate],
                                                            df_VMRequired[sessionDate])
    df_VMBalance.loc[:, sessionDate] = df_VMBalance.loc[:, sessionDate] + df_VMMarginCall.loc[:, sessionDate]

    for i_date in l_BBDates:

        prev_call_date = i_date - timedelta(days=int(dic_CSAParams['CallPeriod']))
        # check if MC was calculated for the previous day

        if prev_call_date in [sessionDate] + l_BBDates:

            df_VMMarginCall.loc[:, i_date] = f_VMMarginCallVec(df_VMBalance.loc[:, prev_call_date],
                                                               df_VMRequired.loc[:, i_date])
            df_VMBalance.loc[:, i_date] = df_VMBalance.loc[:, prev_call_date] + df_VMMarginCall.loc[:, i_date]

        else:
            df_VMBalance.loc[:, i_date] = df_VMRequired.loc[:, i_date]#0
            df_VMMarginCall.loc[:, i_date] = 0 #df_VMRequired.loc[:, i_date]

    print('\rMargin Calls Calculadas para todos los escenarios')
    # df_VMMarginCall.to_csv(outputCSAFolder + "\\VMMarginCall.csv")

    # negative MC payments
    df_VMMarginCallNeg = df_VMMarginCall[df_VMMarginCall < 0].fillna(0)
    # df_VMMarginCallNeg = df_VMBalance[df_VMBalance < 0].fillna(0)

    VMMPORStart = df_VMBalance.loc[:, sessionDate].copy()

    # output of the function, table with the modelled collateral for each ref date and scenario
    df_VMSmooth = pd.DataFrame(index=df_BBMtM.index, columns=l_referenceDates)
    df_VMFull = pd.DataFrame(index=df_BBMtM.index, columns=l_referenceDates)

    print('\rCalculando Variation Margin Full y Smooth', end='')

    for i_date in tqdm(l_referenceDates):

        startMPOR = i_date - timedelta(days=int(MPOR))  # excluido en MC
        endMarginLag = i_date - timedelta(days=int(MPOR - MarginLag))  # incluido en MC

        if startMPOR < sessionDate:
            VMMPORStart.loc[:] = VMToUser - VMToCpty
            CMP_date = sessionDate
        else:

            if startMPOR in df_VMBalance:
                VMMPORStart.loc[:] = df_VMBalance.loc[:, startMPOR] #+ df_VMMarginCall.loc[:, startMPOR]
                CMP_date = startMPOR

            else:
                startMPOR = df_VMBalance.columns[df_VMBalance.columns < startMPOR].sort_values(ascending=False)[0]
                VMMPORStart.loc[:] = df_VMBalance.loc[:, startMPOR]
                CMP_date = startMPOR

        if MarginLag > 0:
            Full = VMMPORStart + df_VMMarginCallNeg.loc[:, np.logical_and(startMPOR < df_VMMarginCallNeg.columns,
                                                                      df_VMMarginCallNeg.columns <= endMarginLag)].sum(
            axis=1)
        else:
            Full = VMMPORStart

        #units of CMP creation and VM calculations
        for j in range(1, len(VMMPORStart) + 1):
            if VMMPORStart[j]<0:
                df_VMSmooth.loc[:, i_date][j] = (VMMPORStart[j]/df_BB_CMPuser.loc[:, CMP_date][j])*df_CMPuserHaD.loc[:, i_date][j]
            if VMMPORStart[j] >= 0:
                df_VMSmooth.loc[:, i_date][j] = (VMMPORStart[j] / df_BB_CMPcpty.loc[:, CMP_date][j])*df_CMPcptyHaD.loc[:, i_date][j]

        for z in range(1, len(Full) + 1):
            if Full[z] < 0:
                df_VMFull.loc[:, i_date][z] = (Full[z]/df_BB_CMPuser.loc[:, CMP_date][z])*df_CMPuserHaD.loc[:, i_date][z]

            if Full[z] >= 0:
                df_VMFull.loc[:, i_date][z] = (Full[z]/df_BB_CMPcpty.loc[:, CMP_date][z])*df_CMPcptyHaD.loc[:, i_date][z]

    print('\rVariation Margin Full y Smooth calculados', end='')

    return df_VMFull, df_VMSmooth

def f_VariationMargin(sessionDate,l_referenceDates, df_BBMtM, dic_CSAParams ):
    
    MPOR, CurePeriod, MarginLag, SettleLag, CallPeriod, numAddCallDates, VMToCpty, VMToUser, Round, UserVMMinTransfer, CPVMMinTransfer, UserVMThreshold, CPVMThreshold, userIA, cptyIA = \
        dic_CSAParams['MPOR'], dic_CSAParams['CurePeriod'], dic_CSAParams['MarginLag'],  dic_CSAParams['SettleLag'], dic_CSAParams['CallPeriod'], \
        dic_CSAParams['numAddCallDates'], dic_CSAParams['VMToCpty'], dic_CSAParams['VMToUser'], dic_CSAParams['Round'], dic_CSAParams['UserVMMinTransfer'], \
        dic_CSAParams['CPVMMinTransfer'], dic_CSAParams['UserVMThreshold'], dic_CSAParams['CPVMThreshold'], dic_CSAParams['userIA'], dic_CSAParams['cptyIA']
    
    
    ###### AUX FUNCTIONS ####################

    def f_VMRequired(mtm):
        if mtm + userIA + cptyIA > CPVMThreshold:
            return mtm + userIA + cptyIA - CPVMThreshold
        elif mtm + userIA + cptyIA < UserVMThreshold:
            return mtm + userIA + cptyIA - UserVMThreshold
        else:
            return 0
        
    def f_VMMarginCall(VMBalanceAntes,VMRequired):
        if (UserVMMinTransfer < VMRequired - VMBalanceAntes < CPVMMinTransfer):
            return 0
        else:
            return round( (VMRequired - VMBalanceAntes) / Round) * Round
     
    
    f_VMMarginCallVec = np.vectorize(f_VMMarginCall)

    print('\rCalculando Margin Calls', end='')
    
    
    l_BBDates = f_datesToBBInterpol(l_referenceDates, dic_CSAParams , MarginLag , numAddCallDates, sessionDate)
    
    df_VMRequired = df_BBMtM.apply(np.vectorize(f_VMRequired))
    
    # df_VMRequired.to_csv(outputCSAFolder + "\\VMRequired.csv")
    
    df_VMBalance    = pd.DataFrame(index=df_BBMtM.index, columns=[sessionDate] + l_BBDates)
    df_VMMarginCall = pd.DataFrame(index=df_BBMtM.index, columns=[sessionDate] + l_BBDates)
    
    df_VMBalance.loc[:,sessionDate] = VMToUser - VMToCpty
    df_VMMarginCall.loc[:, sessionDate] = f_VMMarginCallVec( df_VMBalance.loc[:, sessionDate], df_VMRequired[sessionDate])
    df_VMBalance.loc[:, sessionDate] = df_VMBalance.loc[:, sessionDate] + df_VMMarginCall.loc[:, sessionDate]

    for i_date in l_BBDates:

        prev_call_date = i_date - timedelta(days=int(dic_CSAParams['CallPeriod']))
        # check if MC was calculated for the previous day

        if prev_call_date in [sessionDate] + l_BBDates:

            df_VMMarginCall.loc[:, i_date] = f_VMMarginCallVec(df_VMBalance.loc[:, prev_call_date],
                                                               df_VMRequired.loc[:, i_date])
            df_VMBalance.loc[:, i_date] = df_VMBalance.loc[:, prev_call_date] + df_VMMarginCall.loc[:, i_date]

        else:
            df_VMBalance.loc[:, i_date] = df_VMRequired.loc[:, i_date]  # 0
            df_VMMarginCall.loc[:, i_date] = 0  # df_VMRequired.loc[:, i_date]
        
    print('\rMargin Calls Calculadas para todos los escenarios')
    #df_VMMarginCall.to_csv(outputCSAFolder + "\\VMMarginCall.csv")
    
    # negative MC payments
    df_VMMarginCallNeg = df_VMMarginCall[df_VMMarginCall < 0].fillna(0)
    
    VMMPORStart = df_VMBalance.loc[:, sessionDate].copy()
    
    # output of the function, table with the modelled collateral for each ref date and scenario
    df_VMSmooth = pd.DataFrame(index=df_BBMtM.index, columns=l_referenceDates)
    df_VMFull = pd.DataFrame(index=df_BBMtM.index, columns=l_referenceDates)
    
    print('\rCalculando Variation Margin Full y Smooth', end='')
    i_date =  pd.to_datetime('2031-07-7')
    
    i_date = l_referenceDates[-1]
    
    for i_date in l_referenceDates:
        
        startMPOR = i_date - timedelta(days= int(MPOR) ) # excluido en MC
        endMarginLag = i_date - timedelta( days= int( MPOR-MarginLag) ) # incluido en MC

        if startMPOR < sessionDate:
            VMMPORStart.loc[:] = VMToUser - VMToCpty
        else:

            if startMPOR in df_VMBalance:
                VMMPORStart.loc[:] = df_VMBalance.loc[:, startMPOR]  # + df_VMMarginCall.loc[:, startMPOR]

            else:
                startMPOR = df_VMBalance.columns[df_VMBalance.columns < startMPOR].sort_values(ascending=False)[0]
                VMMPORStart.loc[:] = df_VMBalance.loc[:, startMPOR]

        df_VMSmooth.loc[:, i_date] = VMMPORStart

        if MarginLag > 0:
            df_VMFull.loc[:,i_date] = VMMPORStart + df_VMMarginCallNeg.loc[:, np.logical_and(startMPOR < df_VMMarginCallNeg.columns,
                                                                          df_VMMarginCallNeg.columns <= endMarginLag)].sum(
                axis=1)
        else:
            df_VMFull.loc[:,i_date] = VMMPORStart

    print('\rVariation Margin Full y Smooth calculados', end='')
    
    return df_VMFull, df_VMSmooth


def f_VariationMarginOld(sessionDate, l_referenceDates, df_BBMtM, dic_CSAParams):
    MPOR, CurePeriod, MarginLag, SettleLag, CallPeriod, numAddCallDates, VMToCpty, VMToUser, Round, UserVMMinTransfer, CPVMMinTransfer, UserVMThreshold, CPVMThreshold, userIA, cptyIA = \
        dic_CSAParams['MPOR'], dic_CSAParams['CurePeriod'], dic_CSAParams['MarginLag'], dic_CSAParams['SettleLag'], \
        dic_CSAParams['CallPeriod'], \
            dic_CSAParams['numAddCallDates'], dic_CSAParams['VMToCpty'], dic_CSAParams['VMToUser'], dic_CSAParams[
            'Round'], dic_CSAParams['UserVMMinTransfer'], \
            dic_CSAParams['CPVMMinTransfer'], dic_CSAParams['UserVMThreshold'], dic_CSAParams['CPVMThreshold'], \
        dic_CSAParams['userIA'], dic_CSAParams['cptyIA']

    ###### AUX FUNCTIONS ####################

    def f_VMRequired(mtm):
        if mtm + userIA + cptyIA > CPVMThreshold:
            return mtm + userIA + cptyIA - CPVMThreshold
        elif mtm + userIA + cptyIA < UserVMThreshold:
            return mtm + userIA + cptyIA - UserVMThreshold
        else:
            return 0

    def f_VMMarginCall(VMBalanceAntes, VMRequired):
        if (UserVMMinTransfer < VMRequired - VMBalanceAntes < CPVMMinTransfer):
            return 0
        else:
            return round((VMRequired - VMBalanceAntes) / Round) * Round

    f_VMMarginCallVec = np.vectorize(f_VMMarginCall)

    print('\rCalculando Margin Calls', end='')

    l_BBDates = f_datesToBBInterpol(l_referenceDates, dic_CSAParams, MarginLag, numAddCallDates, sessionDate)

    df_VMRequired = df_BBMtM.apply(np.vectorize(f_VMRequired))

    # df_VMRequired.to_csv(outputCSAFolder + "\\VMRequired.csv")

    df_VMBalance = pd.DataFrame(index=df_BBMtM.index, columns=[sessionDate] + l_BBDates)
    df_VMMarginCall = pd.DataFrame(index=df_BBMtM.index, columns=[sessionDate] + l_BBDates)

    df_VMBalance.loc[:, sessionDate] = VMToUser - VMToCpty
    df_VMMarginCall.loc[:, sessionDate] = f_VMMarginCallVec(df_VMBalance.loc[:, sessionDate],
                                                            df_VMRequired[sessionDate])

    # i_date = l_referenceDates[1]
    # i_date = pd.to_datetime("2020-06-30")

    for i_date in l_BBDates:

        prev_call_date = i_date - timedelta(days=int(dic_CSAParams['CallPeriod']))
        # check if MC was calculated for the previous day

        if prev_call_date in [sessionDate] + l_BBDates:
            df_VMBalance.loc[:, i_date] = df_VMBalance.loc[:, prev_call_date] + df_VMMarginCall.loc[:, prev_call_date]
            df_VMMarginCall.loc[:, i_date] = f_VMMarginCallVec(df_VMBalance.loc[:, i_date],
                                                               df_VMRequired.loc[:, i_date])
        else:
            df_VMBalance.loc[:, i_date] = 0
            df_VMMarginCall.loc[:, i_date] = df_VMRequired.loc[:, i_date]

    print('\rMargin Calls Calculadas para todos los escenarios')
    # df_VMMarginCall.to_csv(outputCSAFolder + "\\VMMarginCall.csv")

    # negative MC payments
    df_VMMarginCallNeg = df_VMMarginCall[df_VMMarginCall < 0].fillna(0)

    VMMPORStart = df_VMBalance.loc[:, sessionDate].copy()

    # output of the function, table with the modelled collateral for each ref date and scenario
    df_VMSmooth = pd.DataFrame(index=df_BBMtM.index, columns=l_referenceDates)
    df_VMFull = pd.DataFrame(index=df_BBMtM.index, columns=l_referenceDates)

    print('\rCalculando Variation Margin Full y Smooth', end='')
    i_date = pd.to_datetime('2031-07-7')

    i_date = l_referenceDates[-1]

    for i_date in l_referenceDates:

        startMPOR = i_date - timedelta(days=int(MPOR))  # excluido en MC
        endMarginLag = i_date - timedelta(days=int(MPOR - MarginLag))  # incluido en MC

        if startMPOR in df_VMBalance:
            VMMPORStart.loc[:] = df_VMBalance.loc[:, startMPOR] + df_VMMarginCall.loc[:, startMPOR]
        # elif (startMPOR > sessionDate):
        #    VMMPORStart.loc[:] = 0

        df_VMSmooth.loc[:, i_date] = VMMPORStart
        df_VMFull.loc[:, i_date] = VMMPORStart + \
                                   df_VMMarginCallNeg.loc[:, np.logical_and(startMPOR < df_VMMarginCallNeg.columns,
                                                                            df_VMMarginCallNeg.columns <= endMarginLag)].sum(
                                       axis=1)

    print('\rVariation Margin Full y Smooth calculados', end='')

    return df_VMFull, df_VMSmooth