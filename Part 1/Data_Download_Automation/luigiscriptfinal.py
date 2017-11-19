import luigi
import time
import pandas as pd
import numpy as np
import csv
import requests
import zipfile
from urllib.request import urlopen
from bs4 import BeautifulSoup
from zipfile import ZipFile
from io import BytesIO
import urllib.request
from lxml import html
from ipykernel import kernelapp as app
from sklearn.cross_validation import train_test_split
import matplotlib.pyplot as plt
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn import preprocessing,neighbors
import os

class ScrapeAndMissingLoanData(luigi.Task):
    email=luigi.Parameter()
	password=luigi.Parameter()
		    
    def run(self):
		
        directory = os.path.dirname(os.getcwd()) 
        newpathloanFinal = directory+'/loanstatecsvFinal'
        url = 'https://www.lendingclub.com/account/login.action?'
        postUrl = 'https://www.lendingclub.com/info/download-data.action'
        payload = {'login_email': self.email, 'login_password': self.password}
        with requests.session() as s:
            loginRequest = s.post(url, data=payload)
            finalUrl = s.get(postUrl)
            linkhtml = finalUrl.text
            soup = BeautifulSoup(linkhtml, "html.parser")
            loan_val = soup.find("div", { "id" : "loanStatsFileNamesJS"}).text
            list_loan_csv = loan_val.split('|')
            if not os.path.exists(newpathloanFinal):
                os.makedirs(newpathloanFinal)
            for value in list_loan_csv:
                url="https://resources.lendingclub.com/"+value
                with urlopen(url) as zipresp:
                    with ZipFile(BytesIO(zipresp.read())) as zfile:
                        zfile.extractall(newpathloanFinal)
						
		d1 = pd.read_csv(newpathloanFinal+'/LoanStats3a_securev1.csv', header=1, skipfooter=4, engine='python')
		d2 = pd.read_csv(newpathloanFinal+'/LoanStats3b_securev1.csv', header=1, skipfooter=4, engine='python')
		d3 = pd.read_csv(newpathloanFinal+'/LoanStats3c_securev1.csv', header=1, skipfooter=4, engine='python')
		d4 = pd.read_csv(newpathloanFinal+'/LoanStats3d_securev1.csv', header=1, skipfooter=4, engine='python')
		d5 = pd.read_csv(newpathloanFinal+'/LoanStats_securev1_2016Q1.csv', header=1, skipfooter=4, engine='python')
		d6 = pd.read_csv(newpathloanFinal+'/LoanStats_securev1_2016Q2.csv', header=1, skipfooter=4, engine='python')
		d7 = pd.read_csv(newpathloanFinal+'/LoanStats_securev1_2016Q3.csv', header=1, skipfooter=4, engine='python')
		d8 = pd.read_csv(newpathloanFinal+'/LoanStats_securev1_2016Q4.csv', header=1, skipfooter=4, engine='python')
		d9 = pd.read_csv(newpathloanFinal+'/LoanStats_securev1_2017Q1.csv', header=1, skipfooter=4, engine='python')
		d10 = pd.read_csv(newpathloanFinal+'/LoanStats_securev1_2017Q2.csv', header=1, skipfooter=4, engine='python')
		d11 = pd.read_csv(newpathloanFinal+'/LoanStats_securev1_2017Q3.csv', header=1, skipfooter=4, engine='python')
		loan_data = pd.concat([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10,d11])
		
		missing_data = loan_df.isnull().sum().to_frame(name='Missing_Count').reset_index()
		missing_data['Missing_percent'] = (missing_data['Missing_Count']/loan_data.shape[0])*100
		missing_data.head()
		missing_data = missing_data.sort_values(by='Missing_percent', axis=0, ascending=True)
		features = missing_data['index'][(missing_data.Missing_percent <30)]
		print('Number of columns after removing missing columns -',len(features))
		loan_df = loan_df[features]
		del loan_df['id']
		loan_df = loan_df.reset_index()
		loan_df['index'] = loan_df.index +1
		loan_df=loan_df.rename(columns = {'index':'id'})
		
		f = ['id','debt_settlement_flag','application_type','fico_range_low','fico_range_high','emp_length','dti','annual_inc','grade','sub_grade','int_rate','loan_amnt','issue_d','purpose','addr_state','home_ownership','zip_code','policy_code','term']
		loan_df= loan_df[f]
		loan_df = loan_df[loan_df.addr_state!='WV']
		loan_df = loan_df[loan_df.addr_state!='IA']
		loan_df = loan_df[loan_df.application_type.notnull()]
		
		loan_df['emp_length'].replace(to_replace='[^0-9]+', value='',inplace=True, regex=True)
		loan_df.loc[loan_df.emp_length=='', ['emp_length']] = '0'
		loan_df['emp_length'] = loan_df['emp_length'].astype(int)
		loan_df['zip_code'] = loan_df['zip_code'].astype(str).map(lambda x: x.rstrip('xx'))
		loan_df = loan_df[loan_df.zip_code!='nan']
		loan_df['zip_code'] = loan_df['zip_code'].astype(int)
		
		loan_df['int_rate'] = loan_df['int_rate'].astype(str).map(lambda x: x.rstrip('%'))
		loan_df['int_rate'] = loan_df['int_rate'].astype(float)
		loan_df['Year'] = loan_df['issue_d'].astype(str).map(lambda x: int(x.split('-')[1]))
		loan_df['Month'] = loan_df['issue_d'].astype(str).map(lambda x: x.split('-')[0])
		lookup = {'Dec':12, 'Nov':11, 'Oct':10, 'Sep':9, 'Aug':8, 'Jul':7, 'Jun':6, 'May':5, 'Apr':4,'Mar':3, 'Feb':2, 'Jan':1}
		loan_df['Month'] =  loan_df['Month'].apply(lambda x: lookup[x])
		loan_df[['Year','Month']] = loan_df[['Year','Month']].astype(int)
		
		part = loan_df[['id','emp_length', 'annual_inc']]
		Mode = loan_df['annual_inc'].mode()[0]
		a = list(loan_df['emp_length'].unique())

		for c in a:
			t = part[part.emp_length == c]
			if( t.annual_inc.isnull().sum()==0):
					break
			else:
					if(t.shape == t[t.isnull().any(axis=1)].shape ):
						t['annual_inc'].fillna(Mode, inplace=True)
					else:
						c = t['annual_inc'].mode()[0]
						t['annual_inc'].fillna(c, inplace=True)
					part.loc[part.Id.isin(t.id), ['annual_inc']] = t[['annual_inc']]
		part['annual_inc'].fillna(part['annual_inc'].mode()[0], inplace = True)      
		loan_df.loc[loan_df.id.isin(part.id), ['annual_inc']] = part[['annual_inc']]
		
		loan_df = loan_df[loan_df.dti.notnull()]
		loan_df = loan_df[loan_df['annual_inc'].notnull()]
		
		dti_t = loan_df[['dti', 'emp_length','id']]
		Mode_dti = loan_df['dti'].mode()[0]
		d = list(loan_df['emp_length'].unique())

		for c in d:
			dt = dti_t[dti_t.emp_length == c]
			if( dt.dti.isnull().sum()==0):
					break
			else:
					if(dt.shape == dt[dt.isnull().any(axis=1)].shape ):
						dt['dti'].fillna(Mode_dti, inplace=True)
					else:
						c = dt['dti'].mode()[0]
						dt['dti'].fillna(c, inplace=True)
					dti_t.loc[dti_t.id.isin(dt.id), ['dti']] = dt[['dti']]
		dti_t['dti'].fillna(dti_t['dti'].mode()[0], inplace = True)      
		loan_df.loc[loan_df.id.isin(dti_t.id), ['dti']] = dti_t[['dti']]
		
		loan_df['fico_avg'] = (loan_df['fico_range_high']+loan_df['fico_range_low'])/2	
		loan_df['term'] = loan_df['term'].astype(str).map(lambda x: x.rstrip('months'))
		loan_df['term'] = loan_df['term'].astype(int)		
		loan_df =loan_df[loan_df.dti>-1]		
		loan_df = loan_df[loan_df.fico_avg > 660]
		loan_df[['grade','sub_grade','application_type','addr_state','purpose','home_ownership']]= loan_df[['grade','sub_grade','application_type','addr_state','purpose','home_ownership']].astype(str)
		loan_df['policy_code'] = loan_df['policy_code'].astype(int)
		loan_df['approval'] =1
		loan_df=loan_df.rename(columns = {'fico_avg':'fico'})
		loan_df=loan_df.rename(columns = {'addr_state':'State'})

		loan_df.to_csv(self.output(), index=False)
	def output(self):
		return luigi.LocalTarget('Loan.csv')
		
class ScrapeAndMissingDeclineData(luigi.Task):
	email=luigi.Parameter()
	password=luigi.Parameter()
	def run(self):		
		direct = os.path.dirname(os.getcwd()) 
		newpathdeclineloanFinal = direct+'/declineloanstatecsvFinal'
		url = 'https://www.lendingclub.com/account/login.action?'
		postUrl = 'https://www.lendingclub.com/info/download-data.action'
		payload = {'login_email': self.email, 'login_password': self.password}
		with requests.session() as s:
			loginRequest = s.post(url, data=payload)
			finalUrl = s.get(postUrl)
			linkhtml = finalUrl.text
			soup = BeautifulSoup(linkhtml, "html.parser")
			loan_val = soup.find("div", { "id" : "rejectedLoanStatsFileNamesJS" }).text
			list_loan_csv = loan_val.split('|')
			del list_loan_csv[-1]
			if not os.path.exists(newpathdeclineloanFinal):
				os.makedirs(newpathdeclineloanFinal)
			for value in list_loan_csv:
				url="https://resources.lendingclub.com/"+value
				with urlopen(url) as zipresp:
					with ZipFile(BytesIO(zipresp.read())) as zfile:
						zfile.extractall(newpathdeclineloanFinal)
		
		d1 = pd.read_csv(newpathdeclineloanFinal+'/RejectStatsA.csv', header=1, skipfooter=4, engine='python')
		d2 = pd.read_csv(newpathdeclineloanFinal+'/RejectStatsB.csv', header=1, skipfooter=4, engine='python')
		d3 = pd.read_csv(newpathdeclineloanFinal+'/RejectStatsD.csv', header=1, skipfooter=4, engine='python')
		d4 = pd.read_csv(newpathdeclineloanFinal+'/RejectStats_2016Q1.csv', header=1, skipfooter=4, engine='python')
		d5 = pd.read_csv(newpathdeclineloanFinal+'/RejectStats_2016Q2.csv', header=1, skipfooter=4, engine='python')
		d6 = pd.read_csv(newpathdeclineloanFinal+'/RejectStats_2016Q3.csv', header=1, skipfooter=4, engine='python')
		d7 = pd.read_csv(newpathdeclineloanFinal+'/RejectStats_2016Q4.csv', header=1, skipfooter=4, engine='python')
		d8 = pd.read_csv(newpathdeclineloanFinal+'/RejectStats_2017Q1.csv', header=1, skipfooter=4, engine='python')
		d9 = pd.read_csv(newpathdeclineloanFinal+'/RejectStats_2017Q2.csv', header=1, skipfooter=4, engine='python')
		d10 = pd.read_csv(newpathdeclineloanFinal+'/RejectStats_2017Q3.csv', header=1, skipfooter=4, engine='python')
		concatDataReject = pd.concat([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10])
		
		concatDataReject= concatDataReject[concatDataReject.Risk_Score.notnull()]
		concatDataReject = concatDataReject[concatDataReject.State.notnull()]
		concatDataReject['id'] = concatDataReject.index + 1

		concatDataReject['Debt-To-Income Ratio'] = concatDataReject['Debt-To-Income Ratio'].astype(str).map(lambda x: x.rstrip('%'))
		concatDataReject['Debt-To-Income Ratio'] = concatDataReject['Debt-To-Income Ratio'].astype(float)

		concatDataReject['Year'] = concatDataReject['Application Date'].astype(str).map(lambda x: int(x.split('-')[0]))
		concatDataReject['Month'] = concatDataReject['Application Date'].astype(str).map(lambda x: x.split('-')[1])
		concatDataReject['Day'] = concatDataReject['Application Date'].astype(str).map(lambda x: x.split('-')[2])
		concatDataReject['Month']=concatDataReject['Month'].astype(int)
		concatDataReject['Day']=concatDataReject['Day'].astype(int)
		concatDataReject['Loan Title'].fillna('None', inplace=True)
		concatDataReject['Zip Code'] = concatDataReject['Zip Code'].astype(str).map(lambda x: x.rstrip('xx'))
		Mode = concatDataReject['Policy Code'].mode()[0]
		
		concatDataReject['Policy Code'].fillna(2, inplace=True)
		concatDataReject['Employment Length']=np.where(concatDataReject['Employment Length']=='10+ years','10',concatDataReject['Employment Length'])
		concatDataReject['Employment Length']=np.where(concatDataReject['Employment Length']=='< 1 year','0',concatDataReject['Employment Length'])
		concatDataReject['Employment Length']=np.where(concatDataReject['Employment Length']=='1 year','1',concatDataReject['Employment Length'])
		concatDataReject['Employment Length'] = concatDataReject['Employment Length'].map(lambda x: x.rstrip('years'))
		concatDataReject['Employment Length'] = concatDataReject['Employment Length'].map(lambda x: x.strip())
		concatDataReject.loc[concatDataReject['Employment Length']=='n/',['Employment Length']]=0
		concatDataReject['Employment Length']= concatDataReject['Employment Length'].astype(np.int64)

		concatDataReject['approval']=0
		concatDataReject['State'].fillna('Other',inplace = True)
		
		concatDataReject=concatDataReject.rename(columns = {'Amount Requested':'loan_amnt'})
		concatDataReject=concatDataReject.rename(columns = {'Application Date':'application_date'})
		concatDataReject=concatDataReject.rename(columns = {'Loan Title':'purpose'})
		concatDataReject=concatDataReject.rename(columns = {'Zip Code':'zip_code'}) 
		concatDataReject=concatDataReject.rename(columns = {'Debt-To-Income Ratio':'dti'})
		concatDataReject=concatDataReject.rename(columns = {'Employment Length':'emp_length'})
		concatDataReject=concatDataReject.rename(columns = {'Risk_Score':'fico'})
		concatDataReject=concatDataReject.rename(columns = {'Application Date':'application_date'})
	
		concatDataReject.to_csv(self.output(), index=False)
	def output(self):
        return luigi.LocalTarget('Decline.csv')

class CombineCSVForClassification(luigi.Task):
	email=luigi.Parameter()
	password=luigi.Parameter()
	def requires(self):
		yield ScrapeAndMissingLoanData(loginemail=self.email,loginpassword=self.password)
		yield ScrapeAndMissingDeclineData(loginemail=self.email,loginpassword=self.password)
	def run(self):
		loan_df = pd.read_csv('Loan.csv', low_memory=False) 
		decline_df= pd.read_csv(open('Decline.csv','rU'), encoding='utf-8')
		loan_df=loan_df[['id','loan_amnt', 'purpose', 'fico', 'dti','zip_code', 'State', 'emp_length', 'Year', 'Month', 'approval']]
		decline_df =decline_df[['id','loan_amnt','purpose', 'fico', 'dti','zip_code', 'State', 'emp_length', 'Year', 'Month','approval']]

		df = pd.concat([loan_df,decline_df])
		df.to_csv('Combine_Clean_Data.csv', low_memory=False)
		
	def output(self):
        return luigi.LocalTarget('Combine_Clean_Data.csv')
		
if __name__ == '__main__':
    luigi.run()