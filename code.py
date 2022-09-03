import urllib,os,shutil
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
        if fileName is None:
            fileName=f"{self.fileName}.{self._format}"
        if os.path.exists(dataDir):
            shutil.rmtree(dataDir)
        fileName=os.path.join(dataDir,fileName)
        os.makedirs(os.path.join(dataDir),exist_ok=True)
        urllib.request.urlretrieve(url, fileName)
        return fileName
    
    def initiateDataIngestion(self,):
        """
        initate data ingestion operation
        """
        url = self.getUrlToDownloadData()
        return self.downloadData(url)

    
class Config:
    
    def __init__(self):
        self.fromDate = "2011-12-01"
        self.toDate = "2015-12-01"
        self._format="csv"
        self.dataDir="data"
        self.fileName = "complaint"
        
        
con=Config()
dataIngestion  = DataIngestion(fromDate=con.fromDate,toDate=con.toDate,_format=con._format)
ingestedFilePath = dataIngestion.initiateDataIngestion()        


    
    
        