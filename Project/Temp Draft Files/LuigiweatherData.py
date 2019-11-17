import luigi
import time
import requests
import os
import zipfile
from urllib.request import urlopen
from bs4 import BeautifulSoup
from zipfile import ZipFile
from io import BytesIO
import shutil
import pandas as pd
import urllib.request
from lxml import html
import gzip
import io
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import csv


		
class WeatherData2016(luigi.Task):

	def run(self):		
		station_invaiid = []
		list_ = []
		frame2016 = pd.DataFrame()
		station = ['DFW', 'DTW', 'SEA', 'JFK', 'SJC', 'ORD', 'PHX', 'STL', 'LAX',
       'MCO', 'DEN', 'MIA', 'IAH','SLC', 'TUS','BOS', 'FLL', 'SFO', 'OGG', 'TPA', 'SNA', 'OKC', 'HNL',
       'PHL', 'LGA', 'RDU', 'DCA', 'RIC', 'ATL', 'LBB', 'CLT', 'ELP',
       'BNA', 'JAC', 'SMF', 'EWR', 'IAD', 'LIH', 'SJU', 'ABQ',
       'ORF', 'JAX', 'MSY','MCI', 'GUC', 'IND', 'PDX', 'BWI',
       'MSP', 'MKE', 'ONT', 'RSW', 'DSM', 'PSP',
       'EGE', 'PBI', 'SDF', 'PIT', 'DAY', 'STX', 'COS',
       'CMH', 'MTJ', 'HDN', 'BDL', 'MEM', 'CLE', 'HOU','OAK',
       'GEG', 'ANC', 'BUF', 'SYR', 'ALB', 'PVD','ILM', 'ICT',
       'PWM', 'GSO', 'CHS', 'MDT', 'BHM', 'ADQ', 'BRW', 'SCC',
       'JNU', 'KTN', 'YAK', 'CDV','PSG', 'WRG', 'OME',
       'OTZ', 'BUR', 'ADK', 'SWF', 'LGB', 'PSE', 'BQN', 'HPN',
       'SAV', 'SRQ', 'BTV', 'ORH','CVG', 'BIS', 'AVL', 'GRR',
       'FNT', 'MYR', 'JAN', 'FAR', 'PNS', 'AGS','LEX',
       'DAL', 'ATW', 'GPT', 'MLB', 'BZN', 'CHO', 'MSN', 'EYW',
       'TRI', 'LFT', 'ROA', 'ECP', 'VPS', 'XNA', 'EVV', 'AVP', 'MDW',
       'HSV', 'FAY', 'TYS', 'TLH', 'MSO','TTN',
       'PHF', 'FSD', 'LBE','BMI', 'CRW','PPG', 'IAG', 'ACT','SHV', 'FSM', 'MAF',
       'SAF', 'JLN', 'LRD', 'BRO', 'TYR', 'GJT', 'DLH',
       'SBA', 'ASE', 'IDA', 'RAP', 'FCA', 'LNK', 'AMA',
       'BFL', 'MLI', 'LSE', 'SBN', 'PSC','FLG', 'ISN', 'GFK',
       'GTF', 'MRY', 'MBS', 'SUN', 'TWF', 'SGF', 'CPR',
       'BTR', 'PBG', 'CRP', 'CID', 'SBP', 'RKS', 'CMX', 'MMH', 'PLN',
       'EKO', 'GCC', 'MFR', 'SMX', 'EUG', 'RST', 'TVC', 'SPI',
       'SGU', 'HLN', 'RDM', 'ACV', 'EAU', 'DVL', 'JMS', 'MKG', 'HYS',
       'COD', 'ITH', 'APN', 'ESC', 'BJI', 'MQT',
       'BGM', 'RHI', 'LWS', 'IMT', 'BRD', 'INL', 'PIH', 'GUM', 'HIB',
       'BTM', 'CDC', 'OTH', 'RDD', 'HRL', 'ISP', 'MHT', 'GNV',
       'MEI', 'PIB', 'BPT',  'AEX', 'TXK', 'ROW', 'CLL',
       'HOB', 'LCH', 'OAJ', 'ELM', 'VLD', 'MGM', 'BGR', 'GTR',
       'CSG', 'BQK', 'DHN', 'EWN', 'ABY', 'SPS', 'SJT', 'GGG',
       'ACK', 'MVY', 'HYA', 'GST', 'AKN', 'DLG', 'GCK', 'MHK',
       'ABI', 'GRI', 'EFD', 'PGD', 'SPN']
		for station_name in station:
			try:
				url = 'https://www.wunderground.com/history/airport/'+station_name+'/2016/1/1/CustomHistory.html?dayend=31&monthend=12&yearend=2016&req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo='
				page = urllib.request.urlopen(url)
				soup = BeautifulSoup(page,"lxml")
				table = soup.find("table", { "id" : "obsTable"})
				column_headers = ['month','high','avg','low','high','avg','low','high','avg','low','high','avg','low','high','avg','low','high','avg','high','Precip','Events']
				player_data_02 = []  # create an empty list to hold all the data
				data_rows = table.findAll('tr')
				for i in range(len(data_rows)):  # for each table row
					player_row = []  # create an empty list for each pick/player

					# for each table data element from each table row
					for td in data_rows[i].findAll('td'):

						# get the text content and append to the row 
						td_text=td.get_text().replace("\n"," ")
						td_text_two=td_text.replace("\t"," ")
						player_row.append(td_text_two)        

				# then append each 
				player_data_02.append(player_row)
				df = pd.DataFrame(player_data_02,columns=column_headers)
				df2 = df.iloc[1:]
				df2.columns = ['date', 'high', 'Temp', 'low', 'high', 'Dew_Point', 'low', 'high', 'Humidity%',
				   'low', 'high', 'Sea_Level_Press', 'low', 'high', 'Visibility', 'low', 'high', 'Wind',
				   'high', 'Precip', 'Events']
				df3 = df2[['date','Temp','Dew_Point','Humidity%','Sea_Level_Press','Visibility','Wind' ,'Precip']]
				df3.columns = ['day','Temp','Dew_Point','Humidity%','Sea_Level_Press','Visibility','Wind' ,'Precip']
				df3.insert(0, 'month','')
				for index, row in df3.iterrows():
					if row['day'] =='Feb':
						break
					else:
						row['month'] = 1
				for index, row in df3.iterrows():
					if row['day'] =='Mar':
						break
					elif row['month'] != 1:
						row['month'] = 2
				for index, row in df3.iterrows():
					if row['day'] =='Apr':
						break
					elif row['month'] != 1 and row['month'] != 2:
						row['month'] = 3
				for index, row in df3.iterrows():
					if row['day'] =='May':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3:
						row['month'] = 4
				for index, row in df3.iterrows():
					if row['day'] =='Jun':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4:
						row['month'] = 5
				for index, row in df3.iterrows():
					if row['day'] =='Jul':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5:
						row['month'] = 6
				for index, row in df3.iterrows():
					if row['day'] =='Aug':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6:
						row['month'] = 7
				for index, row in df3.iterrows():
					if row['day'] =='Sep':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6 and row['month'] != 7:
						row['month'] = 8
				for index, row in df3.iterrows():
					if row['day'] =='Oct':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6 and row['month'] != 7 and row['month'] != 8:
						row['month'] = 9
				for index, row in df3.iterrows():
					if row['day'] =='Nov':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6 and row['month'] != 7 and row['month'] != 8 and row['month'] != 9:
						row['month'] = 10
				for index, row in df3.iterrows():
					if row['day'] =='Dec':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6 and row['month'] != 7 and row['month'] != 8 and row['month'] != 9 and row['month'] != 10:
						row['month'] = 11
				for index, row in df3.iterrows():
					if row['day'] =='Jan':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6 and row['month'] != 7 and row['month'] != 8 and row['month'] != 9 and row['month'] != 10 and row['month'] != 11:
						row['month'] = 12  

				#2017 data denver
				jan_df = df3.loc[df3['month'] == 1]
				jan_df = jan_df.iloc[1:-1]
				no_invalid = jan_df[jan_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				jan_df.Precip = jan_df.Precip.replace({'   - ':mode})
				jan_df.Precip = jan_df.Precip.replace({' T ':0})
				jan_df.Precip = jan_df.Precip.replace({'avg':jan_df.Precip.mode()[0]})
				jan_df.Visibility = jan_df.Visibility.replace({'   - ':jan_df.Visibility.mode()[0]})
				jan_df.Wind = jan_df.Wind.replace({'   - ':jan_df.Visibility.mode()[0]})

				feb_df = df3.loc[df3['month'] == 2]
				feb_df = feb_df.iloc[1:-1]
				no_invalid = feb_df[feb_df.Precip != '   - ']
				mode = no_invalid.Precip.mode()[0]
				feb_df.Precip = feb_df.Precip.replace({'   - ':mode})
				feb_df.Precip = feb_df.Precip.replace({' T ':0})
				feb_df.Precip = feb_df.Precip.replace({'None':mode})
				feb_df.Precip = feb_df.Precip.replace({'avg':mode})
				feb_df.Visibility = feb_df.Visibility.replace({'   - ':feb_df.Visibility.mode()[0]})
				feb_df.Wind = feb_df.Wind.replace({'   - ':feb_df.Visibility.mode()[0]})

				mar_df = df3.loc[df3['month'] == 3]
				mar_df = mar_df.iloc[1:-1]
				no_invalid = mar_df[mar_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				mar_df.Precip = mar_df.Precip.replace({'   - ':mode})
				mar_df.Precip = mar_df.Precip.replace({' T ':0})
				mar_df.Precip = mar_df.Precip.replace({'None':mode})
				mar_df.Precip = mar_df.Precip.replace({'avg':mode})
				mar_df.Visibility = mar_df.Visibility.replace({'   - ':mar_df.Visibility.mode()[0]})
				mar_df.Wind = mar_df.Wind.replace({'   - ':mar_df.Visibility.mode()[0]})

				apr_df = df3.loc[df3['month'] == 4]
				apr_df = apr_df.iloc[1:-1]
				no_invalid = apr_df[apr_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				apr_df.Precip = apr_df.Precip.replace({'   - ':mode})
				apr_df.Precip = apr_df.Precip.replace({' T ':0})
				apr_df.Precip = apr_df.Precip.replace({'None':mode})
				apr_df.Precip = apr_df.Precip.replace({'avg':mode})
				apr_df.Visibility = apr_df.Visibility.replace({'   - ':apr_df.Visibility.mode()[0]})
				apr_df.Wind = apr_df.Wind.replace({'   - ':apr_df.Visibility.mode()[0]})

				may_df = df3.loc[df3['month'] == 5]
				may_df = may_df.iloc[1:-1]
				no_invalid = may_df[may_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				may_df.Precip = may_df.Precip.replace({'   - ':mode})
				may_df.Precip = may_df.Precip.replace({' T ':0})
				may_df.Precip = may_df.Precip.replace({'None':mode})
				may_df.Precip = may_df.Precip.replace({'avg':mode})
				may_df.Visibility = may_df.Visibility.replace({'   - ':may_df.Visibility.mode()[0]})
				may_df.Wind = may_df.Wind.replace({'   - ':may_df.Visibility.mode()[0]})

				jun_df = df3.loc[df3['month'] == 6]
				jun_df = jun_df.iloc[1:-1]
				no_invalid = jun_df[jun_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				jun_df.Precip = jun_df.Precip.replace({'   - ':mode})
				jun_df.Precip = jun_df.Precip.replace({' T ':0})
				jun_df.Precip = jun_df.Precip.replace({'None':mode})
				jun_df.Precip = jun_df.Precip.replace({'avg':mode})
				jun_df.Visibility = jun_df.Visibility.replace({'   - ':jun_df.Visibility.mode()[0]})
				jun_df.Wind = jun_df.Wind.replace({'   - ':jun_df.Visibility.mode()[0]})

				jul_df = df3.loc[df3['month'] == 7]
				jul_df = jul_df.iloc[1:-1]
				no_invalid = jul_df[jul_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				jul_df.Precip = jul_df.Precip.replace({'   - ':mode})
				jul_df.Precip = jul_df.Precip.replace({' T ':0})
				jul_df.Precip = jul_df.Precip.replace({'None':mode})
				jul_df.Precip = jul_df.Precip.replace({'avg':mode})
				jul_df.Visibility = jul_df.Visibility.replace({'   - ':jul_df.Visibility.mode()[0]})
				jul_df.Wind = jul_df.Wind.replace({'   - ':jul_df.Visibility.mode()[0]})

				aug_df = df3.loc[df3['month'] == 8]
				aug_df = aug_df.iloc[1:-1]
				no_invalid = aug_df[aug_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				aug_df.Precip = aug_df.Precip.replace({'   - ':mode})
				aug_df.Precip = aug_df.Precip.replace({' T ':0})
				aug_df.Precip = aug_df.Precip.replace({'None':mode})
				aug_df.Precip = aug_df.Precip.replace({'avg':mode})
				aug_df.Visibility = aug_df.Visibility.replace({'   - ':aug_df.Visibility.mode()[0]})
				aug_df.Wind = aug_df.Wind.replace({'   - ':aug_df.Visibility.mode()[0]})

				sep_df = df3.loc[df3['month'] == 9]
				sep_df = sep_df.iloc[1:-1]
				no_invalid = sep_df[sep_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				sep_df.Precip = sep_df.Precip.replace({'   - ':mode})
				sep_df.Precip = sep_df.Precip.replace({' T ':0})
				sep_df.Precip = sep_df.Precip.replace({'None':mode})
				sep_df.Precip = sep_df.Precip.replace({'avg':mode})
				sep_df.Visibility = sep_df.Visibility.replace({'   - ':sep_df.Visibility.mode()[0]})
				sep_df.Wind = sep_df.Wind.replace({'   - ':sep_df.Visibility.mode()[0]})

				oct_df = df3.loc[df3['month'] == 10]
				oct_df = oct_df.iloc[1:-1]
				no_invalid = oct_df[oct_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				oct_df.Precip = oct_df.Precip.replace({'   - ':mode})
				oct_df.Precip = oct_df.Precip.replace({' T ':0})
				oct_df.Precip = oct_df.Precip.replace({'None':mode})
				oct_df.Precip = oct_df.Precip.replace({'avg':mode})
				oct_df.Visibility = oct_df.Visibility.replace({'   - ':oct_df.Visibility.mode()[0]})
				oct_df.Wind = oct_df.Wind.replace({'   - ':oct_df.Visibility.mode()[0]})

				nov_df = df3.loc[df3['month'] == 11]
				nov_df = nov_df.iloc[1:-1]
				no_invalid = nov_df[nov_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				nov_df.Precip = nov_df.Precip.replace({'   - ':mode})
				nov_df.Precip = nov_df.Precip.replace({' T ':0})
				nov_df.Precip = nov_df.Precip.replace({'None':mode})
				nov_df.Precip = nov_df.Precip.replace({'avg':mode})
				nov_df.Visibility = nov_df.Visibility.replace({'   - ':nov_df.Visibility.mode()[0]})
				nov_df.Wind = nov_df.Wind.replace({'   - ':nov_df.Visibility.mode()[0]})

				dec_df = df3.loc[df3['month'] == 12]
				dec_df = oct_df.iloc[1:-1]
				no_invalid = dec_df[dec_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				dec_df.Precip = dec_df.Precip.replace({'   - ':mode})
				dec_df.Precip = dec_df.Precip.replace({' T ':0})
				dec_df.Precip = dec_df.Precip.replace({'None':mode})
				dec_df.Precip = dec_df.Precip.replace({'avg':mode})
				dec_df.Visibility = dec_df.Visibility.replace({'   - ':dec_df.Visibility.mode()[0]})
				dec_df.Wind = dec_df.Wind.replace({'   - ':dec_df.Visibility.mode()[0]})

				df2016 = pd.concat([jan_df, feb_df, mar_df, apr_df, may_df, jun_df, jul_df, aug_df, sep_df, oct_df, nov_df, dec_df], ignore_index=True)
				df2016 = df2016[['month','day','Visibility','Wind','Precip']]
				df2016[['month','day','Visibility','Wind']] = df2016[['month','day','Visibility','Wind']].astype(int)
				df2016[['Precip']] = df2016[['Precip']].astype(float)
				df2016.insert(0, 'station',station_name)
				df2016.insert(1, 'year',2016)
				list_.append(df2016)
			except:
				station_invaiid.append(station_name)
		frame2016 = pd.concat(list_)
		frame2016.to_csv(self.output().path(), index=False)
			
	def output(self):
		return luigi.LocalTarget('2016WeatherData.csv')

class WeatherData2017(luigi.Task):

	def run(self):		
		station_invaiid = []
		list_ = []
		frame2017 = pd.DataFrame()
		station = ['DFW', 'DTW', 'SEA', 'JFK', 'SJC', 'ORD', 'PHX', 'STL', 'LAX',
       'MCO', 'DEN', 'MIA', 'IAH','SLC', 'TUS','BOS', 'FLL', 'SFO', 'OGG', 'TPA', 'SNA', 'OKC', 'HNL',
       'PHL', 'LGA', 'RDU', 'DCA', 'RIC', 'ATL', 'LBB', 'CLT', 'ELP',
       'BNA', 'JAC', 'SMF', 'EWR', 'IAD', 'LIH', 'SJU', 'ABQ',
       'ORF', 'JAX', 'MSY','MCI', 'GUC', 'IND', 'PDX', 'BWI',
       'MSP', 'MKE', 'ONT', 'RSW', 'DSM', 'PSP',
       'EGE', 'PBI', 'SDF', 'PIT', 'DAY', 'STX', 'COS',
       'CMH', 'MTJ', 'HDN', 'BDL', 'MEM', 'CLE', 'HOU','OAK',
       'GEG', 'ANC', 'BUF', 'SYR', 'ALB', 'PVD','ILM', 'ICT',
       'PWM', 'GSO', 'CHS', 'MDT', 'BHM', 'ADQ', 'BRW', 'SCC',
       'JNU', 'KTN', 'YAK', 'CDV','PSG', 'WRG', 'OME',
       'OTZ', 'BUR', 'ADK', 'SWF', 'LGB', 'PSE', 'BQN', 'HPN',
       'SAV', 'SRQ', 'BTV', 'ORH','CVG', 'BIS', 'AVL', 'GRR',
       'FNT', 'MYR', 'JAN', 'FAR', 'PNS', 'AGS','LEX',
       'DAL', 'ATW', 'GPT', 'MLB', 'BZN', 'CHO', 'MSN', 'EYW',
       'TRI', 'LFT', 'ROA', 'ECP', 'VPS', 'XNA', 'EVV', 'AVP', 'MDW',
       'HSV', 'FAY', 'TYS', 'TLH', 'MSO','TTN',
       'PHF', 'FSD', 'LBE','BMI', 'CRW','PPG', 'IAG', 'ACT','SHV', 'FSM', 'MAF',
       'SAF', 'JLN', 'LRD', 'BRO', 'TYR', 'GJT', 'DLH',
       'SBA', 'ASE', 'IDA', 'RAP', 'FCA', 'LNK', 'AMA',
       'BFL', 'MLI', 'LSE', 'SBN', 'PSC','FLG', 'ISN', 'GFK',
       'GTF', 'MRY', 'MBS', 'SUN', 'TWF', 'SGF', 'CPR',
       'BTR', 'PBG', 'CRP', 'CID', 'SBP', 'RKS', 'CMX', 'MMH', 'PLN',
       'EKO', 'GCC', 'MFR', 'SMX', 'EUG', 'RST', 'TVC', 'SPI',
       'SGU', 'HLN', 'RDM', 'ACV', 'EAU', 'DVL', 'JMS', 'MKG', 'HYS',
       'COD', 'ITH', 'APN', 'ESC', 'BJI', 'MQT',
       'BGM', 'RHI', 'LWS', 'IMT', 'BRD', 'INL', 'PIH', 'GUM', 'HIB',
       'BTM', 'CDC', 'OTH', 'RDD', 'HRL', 'ISP', 'MHT', 'GNV',
       'MEI', 'PIB', 'BPT',  'AEX', 'TXK', 'ROW', 'CLL',
       'HOB', 'LCH', 'OAJ', 'ELM', 'VLD', 'MGM', 'BGR', 'GTR',
       'CSG', 'BQK', 'DHN', 'EWN', 'ABY', 'SPS', 'SJT', 'GGG',
       'ACK', 'MVY', 'HYA', 'GST', 'AKN', 'DLG', 'GCK', 'MHK',
       'ABI', 'GRI', 'EFD', 'PGD', 'SPN']
		for station_name in station:
			try:
				url = 'https://www.wunderground.com/history/airport/'+station_name+'/2017/1/1/CustomHistory.html?dayend=31&monthend=9&yearend=2017&req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo='
				page = urllib.request.urlopen(url)
				soup = BeautifulSoup(page,"lxml")
				table = soup.find("table", { "id" : "obsTable"})
				column_headers = ['month','high','avg','low','high','avg','low','high','avg','low','high','avg','low','high','avg','low','high','avg','high','Precip','Events']
				player_data_02 = []  # create an empty list to hold all the data
				data_rows = table.findAll('tr')
				for i in range(len(data_rows)):  # for each table row
					player_row = []  # create an empty list for each pick/player

					# for each table data element from each table row
					for td in data_rows[i].findAll('td'):

						# get the text content and append to the row 
						td_text=td.get_text().replace("\n"," ")
						td_text_two=td_text.replace("\t"," ")
						player_row.append(td_text_two)        

					# then append each 
					player_data_02.append(player_row)
				df = pd.DataFrame(player_data_02,columns=column_headers)
				df2 = df.iloc[1:]
				df2.columns = ['date', 'high', 'Temp', 'low', 'high', 'Dew_Point', 'low', 'high', 'Humidity%',
				   'low', 'high', 'Sea_Level_Press', 'low', 'high', 'Visibility', 'low', 'high', 'Wind',
				   'high', 'Precip', 'Events']
				df3 = df2[['date','Temp','Dew_Point','Humidity%','Sea_Level_Press','Visibility','Wind' ,'Precip']]
				df3.columns = ['day','Temp','Dew_Point','Humidity%','Sea_Level_Press','Visibility','Wind' ,'Precip']
				df3.insert(0, 'month','')
				for index, row in df3.iterrows():
					if row['day'] =='Feb':
						break
					else:
						row['month'] = 1
				for index, row in df3.iterrows():
					if row['day'] =='Mar':
						break
					elif row['month'] != 1:
						row['month'] = 2
				for index, row in df3.iterrows():
					if row['day'] =='Apr':
						break
					elif row['month'] != 1 and row['month'] != 2:
						row['month'] = 3
				for index, row in df3.iterrows():
					if row['day'] =='May':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3:
						row['month'] = 4
				for index, row in df3.iterrows():
					if row['day'] =='Jun':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4:
						row['month'] = 5
				for index, row in df3.iterrows():
					if row['day'] =='Jul':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5:
						row['month'] = 6
				for index, row in df3.iterrows():
					if row['day'] =='Aug':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6:
						row['month'] = 7
				for index, row in df3.iterrows():
					if row['day'] =='Sep':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6 and row['month'] != 7:
						row['month'] = 8
				for index, row in df3.iterrows():
					if row['day'] =='Oct':
						break
					elif row['month'] != 1 and row['month'] != 2 and row['month'] != 3 and row['month'] != 4 and row['month'] != 5 and row['month'] != 6 and row['month'] != 7 and row['month'] != 8:
						row['month'] = 9
				#2017 data denver
				jan_df = df3.loc[df3['month'] == 1]
				jan_df = jan_df.iloc[1:-1]
				no_invalid = jan_df[jan_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				jan_df.Precip = jan_df.Precip.replace({'   - ':mode})
				jan_df.Precip = jan_df.Precip.replace({' T ':0})
				jan_df.Visibility = jan_df.Visibility.replace({'   - ':jan_df.Visibility.mode()[0]})
				jan_df.Wind = jan_df.Wind.replace({'   - ':jan_df.Visibility.mode()[0]})

				feb_df = df3.loc[df3['month'] == 2]
				feb_df = feb_df.iloc[1:-1]
				no_invalid = feb_df[feb_df.Precip != '   - ']
				mode = no_invalid.Precip.mode()[0]
				feb_df.Precip = feb_df.Precip.replace({'   - ':mode})
				feb_df.Precip = feb_df.Precip.replace({' T ':0})
				feb_df.Precip = feb_df.Precip.replace({'None':mode})
				feb_df.Precip = feb_df.Precip.replace({'avg':mode})
				feb_df.Visibility = feb_df.Visibility.replace({'   - ':feb_df.Visibility.mode()[0]})
				feb_df.Wind = feb_df.Wind.replace({'   - ':feb_df.Visibility.mode()[0]})

				mar_df = df3.loc[df3['month'] == 3]
				mar_df = mar_df.iloc[1:-1]
				no_invalid = mar_df[mar_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				mar_df.Precip = mar_df.Precip.replace({'   - ':mode})
				mar_df.Precip = mar_df.Precip.replace({' T ':0})
				mar_df.Precip = mar_df.Precip.replace({'None':mode})
				mar_df.Precip = mar_df.Precip.replace({'avg':mode})
				mar_df.Visibility = mar_df.Visibility.replace({'   - ':mar_df.Visibility.mode()[0]})
				mar_df.Wind = mar_df.Wind.replace({'   - ':mar_df.Visibility.mode()[0]})

				apr_df = df3.loc[df3['month'] == 4]
				apr_df = apr_df.iloc[1:-1]
				no_invalid = apr_df[apr_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				apr_df.Precip = apr_df.Precip.replace({'   - ':mode})
				apr_df.Precip = apr_df.Precip.replace({' T ':0})
				apr_df.Precip = apr_df.Precip.replace({'None':mode})
				apr_df.Precip = apr_df.Precip.replace({'avg':mode})
				apr_df.Visibility = apr_df.Visibility.replace({'   - ':apr_df.Visibility.mode()[0]})
				apr_df.Wind = apr_df.Wind.replace({'   - ':apr_df.Visibility.mode()[0]})

				may_df = df3.loc[df3['month'] == 5]
				may_df = may_df.iloc[1:-1]
				no_invalid = may_df[may_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				may_df.Precip = may_df.Precip.replace({'   - ':mode})
				may_df.Precip = may_df.Precip.replace({' T ':0})
				may_df.Precip = may_df.Precip.replace({'None':mode})
				may_df.Precip = may_df.Precip.replace({'avg':mode})
				may_df.Visibility = may_df.Visibility.replace({'   - ':may_df.Visibility.mode()[0]})
				may_df.Wind = may_df.Wind.replace({'   - ':may_df.Visibility.mode()[0]})

				jun_df = df3.loc[df3['month'] == 6]
				jun_df = jun_df.iloc[1:-1]
				no_invalid = jun_df[jun_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				jun_df.Precip = jun_df.Precip.replace({'   - ':mode})
				jun_df.Precip = jun_df.Precip.replace({' T ':0})
				jun_df.Precip = jun_df.Precip.replace({'None':mode})
				jun_df.Precip = jun_df.Precip.replace({'avg':mode})
				jun_df.Visibility = jun_df.Visibility.replace({'   - ':jun_df.Visibility.mode()[0]})
				jun_df.Wind = jun_df.Wind.replace({'   - ':jun_df.Visibility.mode()[0]})

				jul_df = df3.loc[df3['month'] == 7]
				jul_df = jul_df.iloc[1:-1]
				no_invalid = jul_df[jul_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				jul_df.Precip = jul_df.Precip.replace({'   - ':mode})
				jul_df.Precip = jul_df.Precip.replace({' T ':0})
				jul_df.Precip = jul_df.Precip.replace({'None':mode})
				jul_df.Precip = jul_df.Precip.replace({'avg':mode})
				jul_df.Visibility = jul_df.Visibility.replace({'   - ':jul_df.Visibility.mode()[0]})
				jul_df.Wind = jul_df.Wind.replace({'   - ':jul_df.Visibility.mode()[0]})

				aug_df = df3.loc[df3['month'] == 8]
				aug_df = aug_df.iloc[1:-1]
				no_invalid = aug_df[aug_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				aug_df.Precip = aug_df.Precip.replace({'   - ':mode})
				aug_df.Precip = aug_df.Precip.replace({' T ':0})
				aug_df.Precip = aug_df.Precip.replace({'None':mode})
				aug_df.Precip = aug_df.Precip.replace({'avg':mode})
				aug_df.Visibility = aug_df.Visibility.replace({'   - ':aug_df.Visibility.mode()[0]})
				aug_df.Wind = aug_df.Wind.replace({'   - ':aug_df.Visibility.mode()[0]})

				sep_df = df3.loc[df3['month'] == 9]
				sep_df = sep_df.iloc[1:-1]
				no_invalid = sep_df[sep_df.Precip != '   - ']
				no_invalid = no_invalid[no_invalid.Precip != 'None']
				no_invalid = no_invalid[no_invalid.Precip != 'avg']
				mode = no_invalid.Precip.mode()[0]
				sep_df.Precip = sep_df.Precip.replace({'   - ':mode})
				sep_df.Precip = sep_df.Precip.replace({' T ':0})
				sep_df.Precip = sep_df.Precip.replace({'None':mode})
				sep_df.Precip = sep_df.Precip.replace({'avg':mode})
				sep_df.Visibility = sep_df.Visibility.replace({'   - ':sep_df.Visibility.mode()[0]})
				sep_df.Wind = sep_df.Wind.replace({'   - ':sep_df.Visibility.mode()[0]})

				df2017 = pd.concat([jan_df, feb_df, mar_df, apr_df, may_df, jun_df, jul_df, aug_df, sep_df], ignore_index=True)
				df2017 = df2017[['month','day','Visibility','Wind','Precip']]
				df2017[['month','day','Visibility','Wind']] = df2017[['month','day','Visibility','Wind']].astype(int)
				df2017[['Precip']] = df2017[['Precip']].astype(float)
				df2017.insert(0, 'station',station_name)
				df2017.insert(1, 'year',2017)
				list_.append(df2017)
			except:
				station_invaiid.append(station_name)
		frame2017 = pd.concat(list_)
		frame2017.to_csv(self.output().path(), index=False)	
	def output(self):
		return luigi.LocalTarget('2017WeatherData.csv')
				
class CombineWeatherData(luigi.Task):

	def requires(self):
		yield WeatherData2016()
		yield WeatherData2017()
	def run(self):		
		concatData2016 = pd.read_csv(WeatherData2016().output().path, encoding= "ISO-8859-1")
		concatData2017 = pd.read_csv(WeatherData2017().output().path, encoding= "ISO-8859-1")
		combined_weather_data = pd.concat([concatData2016, concatData2017])
		combined_weather_data.to_csv(self.output().path, index=False)
	
	def output(self):
		return luigi.LocalTarget('combinedWeatherData.csv')
		
if __name__ == '__main__':
	luigi.run()