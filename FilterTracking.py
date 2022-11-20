from email.utils import parsedate_to_datetime
import os
import pandas as pd

# Need to fill in Cartridge ID manually first for the rest of the columns to be filled in automatically
## many sheets for each location
filepath=f'/Users/babyjourdan/Projects/SPARTANdocs/Network Functioning/Filter tracking_test.xlsx'
filter_tracking=pd.ExcelFile(filepath)
for sheet in filter_tracking.sheet_names:
    def getCartridgeNum():
        CartridgeNum=pd.read_excel(filepath,usecols=['Cartridge ID'],sheet_name=sheet)
        return CartridgeNum
    CartridgeNum=getCartridgeNum()
    pd.set_option('display.max_rows', None)
    print(CartridgeNum)

    def getSitesCode():
            SitesCode=CartridgeNum['Cartridge ID'].str[:4] # a series to store location codes
            return SitesCode
    SitesCode=getSitesCode()
    #print(SitesCode)

    # this should work, but mysterious blank in SitesCode for CAHA, only read  CAH. Come back later...
    def getpartMTL():
        i=0
        Filterset_Offset=8
        partMTL=pd.DataFrame()
        while(i<len(SitesCode)):
            if 'nan' in str(SitesCode[i]):
                partMTL.loc[i,:]='NaN' #add a row of NaN
                i=i+1 #need to leave out the space for NaN or empty
            else:
                MTL_masses_path=f'/Users/babyjourdan/Projects/SPARTANdocs/MTL_weighing_WashU/{SitesCode[i]}_MTL_masses.csv'
                #MTL_masses_path=r'compute1:/storage1/fs1/rvmartin/Active/SPARTAN-shared/Analysis_Data/Filter_Masses/Masses_by_site/MTL_weighing_WashU/'+SitesCode[i]+'_MTL_masses.csv'
                MTL_masses=pd.read_csv(MTL_masses_path) # a dataframe
                if CartridgeNum['Cartridge ID'][i] in MTL_masses['CartridgeID']:
                    partMTL=pd.concat([partMTL,MTL_masses[MTL_masses['CartridgeID']==CartridgeNum['Cartridge ID'][i]]]) #all columns corresponding to Cartridge number given, dataframe
                    i=i+Filterset_Offset
                else:
                    partMTL.loc[i,:]='NaN' #add a row of NaN
                    i=i+1
        partMTL.index=range(0,len(partMTL))
        return partMTL
    partMTL=getpartMTL() #can get Analysis ID, Preweigh_Date, PostWeigh_Date
    print(partMTL)

    def getProjectID():
        SMlist=['ETAD','ILHA','ILNZ','INDH','TWKA','TWTA','USPA','ZAJB','ZAPR']
        ProjectID=pd.Series() # a series to record ProjectID
        SM=pd.Series(['M'])
        SS=pd.Series(['S'])
        i=0
        while(i<len(SitesCode)):
            if SitesCode[i] in SMlist:
                ProjectID=pd.concat([ProjectID,SM])       
            else:
                ProjectID=pd.concat([ProjectID,SS])
            i=i+1
        ProjectID.index=range(0,len(ProjectID))
        return ProjectID
    ProjectID=getProjectID()
    #print(ProjectID)

    ## cannot test right now, need further test
    def getpart_dates_flows():
        i=0
        part_dates_flows=pd.DataFrame()
        while(i<len(SitesCode)): 
            if 'nan' in SitesCode[i]:
                part_dates_flows.loc[i,:]='NaN' #add a row of NaN
                i=i+1
            else:
                #dates_flows_path=r'compute1:/storage1/fs1/rvmartin/Active/SPARTAN-shared/Site_Sampling/symlinks_for_automation/'+SitesCode[i]+'/'+SitesCode[i]+'_dates_flows.xlsx'
                dates_flows_path=f'/Users/babyjourdan/Projects/SPARTANdocs/Site_Sampling/{SitesCode[i]}_dates_flows.xlsx'
                dates_flows=pd.read_excel(dates_flows_path) # a dataframe
                part_dates_flows=pd.concat([part_dates_flows,dates_flows[dates_flows['Analysis_ID']==partMTL['AnalysisID'][i]]]) #all columns corresponding to Analysis ID given, dataframe
                i=i+1
        part_dates_flows.index=range(0,len(part_dates_flows))
        return part_dates_flows
    part_dates_flows=getpart_dates_flows() #should have sampling start and end date, 
    #print(part_dates_flows)

    #not tested
    def getpartDisassemblyElog():
        i=0
        partDisassemblyElog=pd.DataFrame()
        Disassembly_path=f'/Users/babyjourdan/Projects/SPARTANdocs/E-Logs/Cartridge Assembly and Disassembly E-Log.xlsx'
        
        while(i<len(SitesCode)):
            DisassemblyElog=pd.read_excel(Disassembly_path)
            for n in DisassemblyElog.sheet_names:
                if n[:4]==SitesCode[i]:
                    sheetname=n
                    break
            DisassemblyElog=DisassemblyElog.parse(sheetname)
            partDisassemblyElog=pd.concat([partDisassemblyElog,DisassemblyElog[DisassemblyElog['Cartridge Number']==CartridgeNum['Cartridge ID'][i]]])
            i=i+1
        partDisassemblyElog.reindex=range(0,len(partDisassemblyElog))
        return partDisassemblyElog
    partDisassemblyElog=getpartDisassemblyElog() #will give dissambly date

    #not tested
    def getpartSSR():
        i=0
        partSSR=pd.DataFrame()
        Filterset_Offset=8
        while (i<len(SitesCode)):
            if 'nan' in SitesCode[i]:
                partSSR.loc[i,:]='NaN' #add a row of NaN
                i=i+1
            else:
                SSRpath=f'/Users/babyjourdan/Projects/SPARTANdocs/SSR_by_site/{SitesCode[i]}_SSR.xlsx'
                SSR=pd.read_excel(SSRpath)
                partSSR=pd.concat([partSSR,SSR['Cartridge ID']==CartridgeNum['Cartridge ID'][i]])
                i=i+Filterset_Offset
        partSSR.reindex=range(0,len(partSSR))
        return partSSR
    partSSR=getpartSSR() #will have SSR Date

    #not tested
    def getpartSRF():
        i=0
        partSRF=pd.DataFrame()
        while(i<len(SitesCode)):
            if 'nan' in SitesCode[i]:
                partSRF.loc[i,:]='NaN' #add a row of NaN
                i=i+1
            else:
                SRFpath=f'/Users/babyjourdan/Projects/SPARTANdocs/XRF_data/{SitesCode[i]}/{CartridgeNum[i]}.xlsx'
                SRF=pd.read_excel(SRFpath)
                partSRF=pd.concat([partSRF,SRF['Ident']==partMTL['AnalysisID'][i]])
                i=i+1
        partSRF.reindex(0,len(partSRF))
        return partSRF
    partSRF=getpartSRF() #will have SRF date

    #not tested
    def getpartICElog():
        i=0
        partICElog=pd.DataFrame()
        while(i<len(SitesCode)):
            if 'nan' in SitesCode[i]:
                partSSR.loc[i,:]='NaN' #add a row of NaN
                i=i+1
            else:
                ICElogpath=f'/Users/babyjourdan/Projects/SPARTANdocs/E-Logs/IC E-Log.xlsx'
                ICElog=pd.read_excel(ICElogpath)
                for n in ICElog.sheet_names:
                    if n[:4]==SitesCode[i]:
                        sheetname=n
                        break
                ICElog=ICElog.parse(sheetname)
                partICElog=pd.concat([partICElog,ICElog['Analysis ID']==partMTL['AnalysisID'][i]])
                i=i+1
        partICElog.reindex(0,len(partICElog))
        return partICElog
    partICElog=getpartICElog() #will have Extracted for IC Analysis and Ran through IC dates

#need to test first, then write calculations etc. below
#if __name__ == "__main__":
    








        







