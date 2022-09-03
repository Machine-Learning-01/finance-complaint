from distutils.util import strtobool
import os,shutil
from six.moves import urllib
import pandas as pd
import wget
class DataIngestion:
    
    def __init__(self,fromDate,toDate,dataDir="data",fileName="complaint",_format="csv"):
        """
        fromDate: "2011-12-01"
        toDate: "2012-12-01"
        data_dir: download data dir
        file_name: file name
        format: csv
        """
        
        self.fromDate=fromDate
        self.toDate=toDate
        self.dataDir=dataDir
        self.fileName=fileName
        self._format=_format



    def downloadMonthlyInterval(self):



        if os.path.exists(self.dataDir):
            shutil.rmtree(self.dataDir)

        monthIntervals = list(pd.date_range(start=self.fromDate,
                                   end=self.toDate,
                                   freq="m").astype('str'))
        
        failedFiles= []
        successFiles = []
        for index in range(1,len(monthIntervals)):
            startMonth = monthIntervals[index-1]
            stopMonth = monthIntervals[index]
            url = self.getUrlToDownloadData(fromDate=startMonth,
            toDate=stopMonth
            )
            fileName = f"{self.fileName}_{startMonth}_{stopMonth}"
            try:

                fileName = self.downloadData(url=url,fileName=fileName)
                successFiles.append(fileName)
            except Exception as e:
                print(e)
                failedFiles.append(fileName)

        print(f"==========success files=====")
        print(successFiles)
        print(f"=========failed files=====")
        print(failedFiles)


    def getUrlToDownloadData(self,fromDate=None,toDate=None):
        """
        fromDate: "2011-12-01"
        toDate: "2012-12-01"
        """
        if fromDate is None:
            fromDate=self.fromDate
        if toDate is None:
            toDate=self.toDate
        url=f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max={toDate}&date_received_min={fromDate}&field=all&format={self._format}"
        return url
    
    
    def downloadData(self,url,dataDir=None,fileName=None):
        """
        url: file will be downloaded from url
        """
        if dataDir is None:
            dataDir=self.dataDir

        if os.path.exists(dataDir):
            shutil.rmtree(dataDir)

        if fileName is None:
            fileName=self.fileName
            
        fileName = f"{fileName}.{self._format}"

        fileName=os.path.join(dataDir,fileName)
        os.makedirs(os.path.join(dataDir),exist_ok=True)
        # urllib.request.urlretrieve(url, fileName)
        wget.download(url,fileName)
        return fileName



    
class Config:
    
    def __init__(self):
        self.fromDate = "2011-12-01"
        self.toDate = "2015-12-01"
        self._format="json"
        self.dataDir="data"
        self.fileName = "complaint"


if __name__=="__main__":
    con=Config()
    dataIngestion  = DataIngestion(fromDate=con.fromDate,toDate=con.toDate,_format=con._format)
    ingestedFilePath = dataIngestion.downloadMonthlyInterval()


    
    
        