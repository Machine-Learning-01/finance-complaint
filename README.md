# finance_complaint

docker run -p 8888:8888 -p 4040:4040 -v E:\official\finance_complaint:/home/jovyan -p 4041:4041 jupyter/pyspark-notebook



Assumption:
We will be using all data from min_start_date
We will update date as we receives in each run

### Data Ingestion

Data Ingestion Steps:

Create data ingestion config 


1. Check if meta data file available 




-->Yes:
read information
from_date,to_date
data_dir location

update the incoming configuration

from_date[config]=to_date[meta_info]
start the data ingestion

--->No:
accept the new incoming configuration
start data ingestion 
update the metainfo
```
pip install torch==1.11.0+cu113 torchvision==0.12.0+cu113 -f https://download.pytorch.org/whl/torch_stable.html
```
--
conda install pytorch torchvision torchaudio cudatoolkit=10.2 -c pytorch