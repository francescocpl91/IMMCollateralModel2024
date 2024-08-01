# IMMCollateralModel2024
Counterparty Credit Risk IMM Collateral model to simulate Variation Margin with ultimate intention to calculate Smooth and Full Collateralised Exposures. 

SET-UP INSTRUCTIONS:
1) Create a folder in your local directory and call it "IMM Collateral Model" and within it create 3 folders: "Inputs", "Outputs" and "Python code". On this latter one clone all the Python library available on this master branch. In order to run this Py library you need Python 3.1. (Check library dependencies directly in the code: Pandas, Numpy, TimeDelta etc...). 
2)  In the "Inputs" folder you need to add the following (example available in https://ssctechnologiesinc.sharepoint.com/:f:/r/sites/Algo/Shared%20Documents/Client%20Success/Client%20Projects/EMEA/BBVA/PythonIMMCollateralModel?csf=1&web=1&e=J1tj03):
A)  Static csv data has to be taken from  /unload/au/xkoz/es/dat/in/IMCRData/XXXX/input_data/aidb_data, /unload/au/xkoz/es/dat/in/IMCRData/XXXX/aux_files/XKOZ_udsloader_node_mitigants.CSV  (only if Physical     
   collateral run is needed) and  /unload/au/xkoz/es/dat/in/IMCRData/XXXX/rtce/replace/counterparties/.
   (BE CAREFUL WITH CollateralAllocationSNS.csv - The is only for SNS cases and the information has to be taken from SNS Excel SACCR file after being populated: Allocation factor and IMM VM after applying     
   allocation factor).
B) Static txt data: Intrument cubes to be taken using: $MAG_HOME/util/showCube -c $XKOZ_DATOUT/cubes/XXXX/cubesMerged/YYYY.h5c -n "ZZZZ" -pc 16 -t Instrument
                    TFs cubes to be taken using: $MAG_HOME/util/showCube -c $XKOZ_DATOUT/cubes/XXXX/cubesMerged/YYYY.h5c -n "ZZZZ" -pc 16 -t SettlementFlow
                    CCY Cubes to be taken using: $MAG_HOME/util/showCube -c $XKOZ_DATOUT/cubes/XXXX/cubesMerged/YYYY.h5c -pc 16 -t Currency
                    (PLEASE CHECK THE LINK ABOVE IN ORDER TO KNOW WHERE TO SAVE THE TXT FILES - general rule is that CCY cubes are saved together with the csv 
                    files while for each MA ->CSA or NC node you need to save separately the input data. You have to create all the folders of the nodes that the 
                    code needs to read. if for example you are analysing two master agreements and each one has a CSA below, 
                    you will need to create 1) "MA1" folder and within it "CSA1" and within this last one save the txt instrument and settlement flows there. 2) 
                    "MA2" folder and within it "CSA2" and within this last one save the txt instrument and settlement flows there)

    
HOW TO RUN THE LIBRARY:

1) The whole library is triggered by Exec.py. Within it, WORKPARTH variable has to be updated with your local directory path of "IMM Collateral Model"
2) If you want to run the model with Physical_Collateral se the variable at the beggining of Exec.py to True. If not then False.
3) In variable l_MA at th beggining of Exec.py you need to specify the Master Agreements you want to analise. Obviously they need to be aligned with your "inputs" set-up.
4) All the following functions in Exec.py use the variable "load" = 1 or 0: f_readCcyRWFile, f_readMtMRWFile, f_readCFRWFile, f_AggregatesPerCcyCLS, f_ColModelCFMetrics, f_BrownianBridge_PhysColl, f_BrownianBridge. The code in each one of these functions allows to save intermediate data and read it directly again. this means that first run needs load=1 everywhere then this can be manually changed to 0 saving time in a second run. 
5) All output data is generated automatically. No need to generate output folders manually. 
