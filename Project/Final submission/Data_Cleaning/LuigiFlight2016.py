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


		
class ConcatenateData(luigi.Task):

	def run(self):		
		cur_path = '/Users/madhu/Documents/NEU/Fall 2017/ADS Class/Final project/Data'
		d1 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_1.csv', low_memory=False)
		d2 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_2.csv', low_memory=False)
		d3 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_3.csv', low_memory=False)
		d4 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_4.csv', low_memory=False)
		d5 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_5.csv', low_memory=False)
		d6 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_6.csv', low_memory=False)
		d7 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_7.csv', low_memory=False)
		d8 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_8.csv', low_memory=False)
		d9 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_9.csv', low_memory=False)
		d10 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_10.csv', low_memory=False)
		d11 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_11.csv', low_memory=False)
		d12 = pd.read_csv(cur_path+'/Data2016/On_Time_On_Time_Performance_2016_12.csv', low_memory=False)
		frame2016 = pd.concat([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12])
		frame2016.to_csv(self.output().path(), index=False)

	def output(self):
		return luigi.LocalTarget('Data2016_Uncleaned.csv')

class CombineWeatherData(luigi.Task):

	def requires(self):
		yield ConcatenateData()
	def run(self):		
		df = pd.read_csv(ConcatenateData().output().path, encoding= "ISO-8859-1")
		missing_data = df.isnull().sum().to_frame(name='Missing_Count').reset_index()
		missing_data['Missing_percent'] = (missing_data['Missing_Count']/df.shape[0])*100
		missing_data = missing_data.sort_values(by='Missing_percent', axis=0, ascending=True)
		features = missing_data['index'][(missing_data.Missing_percent<99.38)]
		d = df[features]
		d['Flight_Status'] = 0
		d.loc[df.DepDel15==1.0 ,'Flight_Status'] = 1
		d.loc[df.ArrDel15==1.0,'Flight_Status'] = 1
		d.loc[df.Cancelled==1.0, ['Flight_Status']] = 1
		d['Flight_Status'] = d['Flight_Status'].astype(int)
		d['Status'] = np.where(d['Flight_Status']==1,'Delayed', 'On Time')
		cols = features.tolist()
		p = ['FlightDate','TailNum','Carrier','OriginAirportSeqID','OriginCityMarketID','OriginStateFips','OriginState','OriginWac','DestAirportSeqID','DestCityMArketID','DestStateFips','DestWac','DepDelay','DepDel15','DepTimeBlk','SecurityDelay','LateAircraftDelay', 'WeatherDelay', 'CarrierDelay', 'NASDelay',
			'ArrDelay','ArrDel15','ArrTimeBlk','Diverted','Flights','AirTime','DistanceGroup','CancellationCode']
		for i in p:
			if i in cols:
				cols.remove(i)
		dt = dt[dt['Cancelled']!=1]
		del dt['Cancelled']
		dt = dt[dt['ArrTime'].notnull() & dt['ArrDelayMinutes'].notnull() & dt['ActualElapsedTime'].notnull() & dt['ArrDelayMinutes'].notnull()]		
		dd = dt	
		dd = dd[dd.DepDelayMinutes<1350]
		dd = dd[dd.ArrDelayMinutes<1400]	
		dd = dd.drop(((dd.Month==4) & (dd.ArrDelayMinutes >1200))|((dd.DepDelayMinutes>1200) & (dd.Month==2)) |((dd.DepDelayMinutes>1210) & (dd.Month==5))|((dd.DepDelayMinutes>1200) & (dd.Month==9))| ((dd.DepDelayMinutes>1220) & (dd.Month==10))|((dd.DepDelayMinutes>1200) & (dd.Month==11))|((dd.DepDelayMinutes>1300) & (dd.Month==12)))
		dd = dd[dd.ArrDelayMinutes <= 1325]
		
		dd.to_csv(self.output().path(), index=False)
	def output(self):
		return luigi.LocalTarget('Data2016_Cleaned.csv')
		
if __name__ == '__main__':
	luigi.run()