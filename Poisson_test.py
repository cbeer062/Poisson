from scipy.stats import poisson
import pandas as pd
from datetime import datetime
FinalData = pd.DataFrame([])
tup_now = datetime.now().timetuple()
dateToday =datetime.today()
today = datetime.strftime(dateToday,'%Y-%m-%d')
# path = "//usoil.local/Production/USV/Apps/PowerBI/US AutoForce/Sales Reports/Poisson/sql_output/InnerEventTimes_Data_test.csv"
data1 = pd.read_csv('InnerEventTimes_Data_test.csv')
data1 = data1.sort_values(by=['CustomerAddressKey', 'Date2'], ascending=False)AllButLastData1 = data1[data1.Date2 < today]
CustGroupABL = AllButLastData1.groupby(
    'CustomerAddressKey').mean().drop(['Row1','Daily_Revenue'],axis=1)
FinalData = data1.merge(
    CustGroupABL, on='CustomerAddressKey').rename(
    index=str, columns={'InnerEventTime_y':'AvgInnerEventTime','InnerEventTime_x':'DaySinceLastPurchase'}).drop('Row1',axis=1)FinalData = FinalData.assign(p = 0).rename(index=str, columns={'p':'Probability'})
	FinalData.Probability = (poisson.cdf(FinalData.DaySinceLastPurchase, FinalData.AvgInnerEventTime, loc=0))
writeFileName = 'innerEventTime_Current.csv'

## RETURN RESULTS TO BLOB
 	# This container and account must already be created, although with more code you could create them in python
 	try:
 	accountKey = "EkMCD1bIrMp8g3+5DOpjFkRl3oDFXG83MmMSBrdVsP2iRa6s4hZSa3yis5l6grdYRPgOkFAUJHf6v+GardbUfw=="
 	accountName = 'iaprod'
 	containerName = 'Poisson_Craig'
 	blobName = writeFileName
 	output = io.StringIO()
 	output = FinalData.to_csv(index_label="idx", encoding = "utf-8", na_rep='NULL')
 	 
 	print(datetime.datetime.now())
 	try:
 	block_blob_service = BlockBlobService(account_name=accountName, account_key=accountKey) 
 	block_blob_service.create_blob_from_text(containerName, blobName, output)
 	except:
 	try:
 	time.sleep(300) # try again in 10 minutes, hoping an internet problem was a passing glitch
 	block_blob_service = BlockBlobService(account_name=accountName, account_key=accountKey) 
 	block_blob_service.create_blob_from_text(containerName, blobName, output) 
 	except:
 	pass
 	except Exception as e:
 	print("Data Upload Error Error: " + str(e))
 	# send email, append to error message
 	fatal = 1
 	error_list.extend(['____Data Upload Error: ' + str(e)])