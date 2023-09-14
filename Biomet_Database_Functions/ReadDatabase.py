import os
import sys
import numpy as np
import pandas as pd
import configparser
import argparse
import datetime

class MakeCSV():

    def __init__(self,Sites,Years):

        # Create a config file based on the job (Write vs. Read; standard vs. custom)
        config = configparser.ConfigParser()
        config.read('ini/config.ini')
        self.ini = configparser.ConfigParser()
        self.ini.read(config['Read']['ini'])
        self.ini['Database'] = config['Database']

        for Site in Sites:
            self.Site = Site
            for Request in self.ini['Output']['Requests'].split(','):
                print(f'Creating .csv files for {Site}: {Request}')
                self.Request = Request
                if self.ini[self.Request]['by_Year']=='False':
                    self.AllData = pd.DataFrame()
                for Year in Years:
                    self.Year = Year
                    if os.path.exists(self.sub(self.ini['Database']['Path'])+self.ini[self.Request]['Stage']):
                        self.readDB()
                    else:
                        pass
                if self.ini[self.Request]['by_Year']=='False':
                    self.write()
                    
    def readDB(self):
        self.getTime()
        # for self.Request in self.ini['Output']['self.Requests'].split(','):
        self.traces = self.ini[self.Request]['Traces'].split(',')
        D_traces = self.readTrace()
        self.Data = pd.DataFrame(index=self.Time_Trace,data=D_traces)
        self.Data[self.ini[self.Request]['Timestamp']] = self.Data.index.floor('Min').strftime(self.ini[self.Request]['Timestamp_FMT'])
        self.traces.insert(0,self.ini[self.Request]['Timestamp'])
        rn = {}
        for renames in self.ini[self.Request]['Rename'].split(' '):
            r = renames.split('|')
            if len(r)>1:
                rn[r[0]]=r[1]
        self.Data = self.Data.rename(columns=rn)
        if self.ini[self.Request]['by_Year']=='True':
            self.AllData = self.Data
            self.write()
        else:
            self.AllData = pd.concat([self.AllData,self.Data])

    def getTime(self):
        Timestamp = self.ini['Database']['Timestamp']
        Timestamp_alt = self.ini['Database']['Timestamp']
        filename = self.sub(self.ini['Database']['Path'])+self.ini[self.Request]['Stage']+Timestamp
        filename_alt = self.sub(self.ini['Database']['Path'])+self.ini[self.Request]['Stage']+Timestamp_alt
        try:
            with open(filename, mode='rb') as file:
                Time_Trace = np.fromfile(file, self.ini['Database']['Timestamp_dtype'])
        except:
            with open(filename_alt, mode='rb') as file:
                Time_Trace = np.fromfile(file, self.ini['Database']['Timestamp_dtype'])
            pass
        if self.ini['Database']['Timestamp_fmt'] == 'datenum':
            base = float(self.ini['Database']['datenum_base'])
            unit = self.ini['Database']['datenum_base_unit']
            self.Time_Trace_Num = Time_Trace+0
            self.Time_Trace = pd.to_datetime(Time_Trace-base,unit=unit).round('T')
        else:
            # Datenum is depreciated and we should consider upgrading
            warning = 'Revise code for new timestamp format'
            sys.exit(warning)

    def readTrace(self):
        D_traces = {}
        for Trace_Name in self.traces:
            filename = self.sub(self.ini['Database']['Path'])+self.ini[self.Request]['Stage']+Trace_Name
            try:
                with open(filename, mode='rb') as file:
                    trace = np.fromfile(file, self.ini['Database']['Trace_dtype'])
            except:
                print(f'Trace does not exist {filename} , proceeding without')
                trace = np.empty(self.Time_Trace.shape[0])
                trace[:] = np.nan
                pass
            D_traces[Trace_Name]=trace
        return (D_traces)

    def write(self):
        if self.AllData.empty:
            print(f'No data to write for{self.Site}: {self.Year}')
        else:
            output_path = self.sub(self.ini[self.Request]['Output_Paths'])
            if os.path.exists(output_path)==False:
                os.makedirs(output_path)
            output_path = output_path+self.Request+'.csv'
            self.addUnits()
            self.AllData.set_index(self.ini[self.Request]['Timestamp'],inplace=True)
            self.AllData.to_csv(output_path)
        
    def addUnits(self):
        if self.ini[self.Request]['Units_in_Header'].lower() == 'true':
            units = self.ini[self.Request]['Units'].split(',')
            units.insert(0,self.ini[self.Request]['Timestamp_Units'])
            unit_dic = {t:u for t,u in zip(self.traces,units)}
            self.AllData = pd.concat([pd.DataFrame(index=[-1],data=unit_dic),self.AllData])
            
    def sub(self,val):
        v = val.replace('YEAR',str(self.Year)).replace('SITE',self.Site)
        return(v)

        
if __name__ == '__main__':
    # If called from command line ...
    CLI=argparse.ArgumentParser()

    CLI.add_argument(
    "--sites", 
    nargs='+', 
    type=str,
    default=['BB','BB2','BBS','RBM','DSM','HOGG','YOUNG'],
    )

    CLI.add_argument(
    "--years",
    nargs='+',
    type=int,  
    default=np.arange(2014,datetime.datetime.now().year+1),
    )
    # parse the command line
    args = CLI.parse_args()

    MakeCSV(args.sites,args.years)