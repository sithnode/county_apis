print('Importing...')
from sodapy import Socrata
import pandas as pd
import time

t0 = time.time()
cnt,mtch,btch_cnt,btch = 0,0,0,0

def isNaN(num):
    return num != num

def get_county_data_dir(houseno,street,zipcode,direction):
	app_token = 'your-token'
	client = Socrata("data.lacounty.gov", app_token)
	results = client.get("7rjj-f2pv", limit=100, situshouseno=str(houseno),situsstreet=str(street),situszip5=str(zipcode),usetype='SFR',situsdirection=str(direction))
	return results

def get_county_data(houseno,street,zipcode):
	app_token = 'your-token'
	client = Socrata("data.lacounty.gov", app_token)
	results = client.get("7rjj-f2pv", limit=100, situshouseno=str(houseno),situsstreet=str(street),situszip5=str(zipcode),usetype='SFR')
	return results

def elapsed():
	t1 = round(time.time() - t0,5)
	tmin = round(t1/60,2)
	if tmin < 10:
		print("Time elapsed (sec): ", t1)
	print("Time elapsed (min): ", tmin)

data = pd.read_excel (r'no_apns_dir.xlsx', sheet_name='Sheet1')    # Change Input Filename Here

df = pd.DataFrame(data, columns= ['PropertyNo','PropertyDirection','PropertyStreet','PropertyAddress','PropertyCity','PropertyState','PropertyZip','APN'])
df[['APN']] = df[['APN']].astype(str)

elapsed()

print('running...')
for index, row in df.iterrows():
	cnt = cnt + 1
	btch_cnt = btch_cnt + 1
	if btch_cnt == 1000:
		btch = btch + 1
		btch_cnt = 0
		print('Batch Count: ',str(btch))
		elapsed()
		cnt_pct = round((mtch/cnt)*100,2)
		print('Match Rate ' + str(cnt_pct) + '%')
		print('running...')
	if not isNaN(row['PropertyDirection']):
		data = get_county_data_dir(row['PropertyNo'],row['PropertyStreet'].upper(),row['PropertyZip'],row['PropertyDirection'])
	else:
		data = get_county_data(row['PropertyNo'],row['PropertyStreet'].upper(),row['PropertyZip'])
	if len(data) == 1:
		mtch = mtch + 1
		ain = data[0]['assessorid']
		df.at[index,'APN'] = ain
	elif len(data) > 1:
		ain = '*'
		print('More than one PropertyNo and PropertyStreet found: ' + str(len(data)))
		print('Skipping ain for row ' + str(row['PropertyAddress']) + ' '+ str(row['PropertyZip']))
	else:
		print('No data found for: ' + str(row['PropertyNo']) + ' ' + str(row['PropertyDirection']) + ' ' + row['PropertyStreet'].upper() + ' '+ str(row['PropertyZip']) + ', row: ' + str(index))

cnt_pct = round((mtch/cnt)*100,2)
elapsed()
print(str(mtch) + ' Matches Found of ' + str(cnt) + ' Properties. Match Rate ' + str(cnt_pct) + '%')
print(df)
df.to_excel("output_no_apns_dir.xlsx")   # Change Output Filename Here
