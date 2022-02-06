# sep.py
#python -i
#test

import cv2
import base64
import numpy as np
from PIL import Image
import io
import os
import pathlib
import datetime
import time
import platform
import codecs
import pandas as pd

#data = pd.read_csv('./Raw/daidata.csv',names=('Rotation','BB','RB','co1','co2','han1','han2','difference','han3','max','dai','machine','holl','date','kari1','kari2','url','text'), encoding = "utf-16")

rawdata = pd.read_csv('./Raw/daidata.csv', encoding = 'utf-16')
#all chenge columnsname
rawdata = rawdata.set_axis(['Rotation','BB','RB','co1','co2','han1','han2','difference','han3','max','dai','machine','holl','date','kari1','kari2','url','text'], axis='columns')

#Preprocessing
#Remove the comma (Because NaN may be created after removing it)
##important ! 1,234 are treated as strings and replaced by the replace(',','')
#When replacing for one value, it will be removed by A.replace (',',''), but for df, series,use .str.replace.(',','')
#You can change to int type only by removing.

rawdata['Rotation'] = rawdata['Rotation'].str.replace(',','')
rawdata['max'] = rawdata['max'].str.replace(',','')

#Fill in missing values ​​with 0
rawdata = rawdata.fillna(0)

#Judgment whether there is NaN for each column True is returned for columns with missing values
print(rawdata.isnull().any())

#Type conversion
rawdata = rawdata.astype({'Rotation':'int64','BB':'int64','RB':'int64','co1':'int64','co2':'int64','han2':'int64','max':'int64','machine':'str','kari1':'int64','kari2':'int64'})

#y/n date today?
#y/Ndef
def yes_no_input():
	while True:
		choice = input("Please respond with 'today? yes' or 'no' [y/N]: ").lower()
		if choice in ['y', 'ye', 'yes']:
			return True
		elif choice in ['n', 'no']:
			return False
'''
datetime to date
'''
if __name__ == '__main__':
	if yes_no_input():
		d = datetime.datetime.now()
	else:
		d = datetime.datetime.now() - datetime.timedelta(days=1)
#8 digits to int
intdt= int(d.strftime('%Y%m%d'))
print(intdt)
#'date'.values replace intdt all
rawdata['date'] = intdt

#Type confirmation
rawdata.dtypes

#Common up to here

#if Sentence "hollname" in df.values
if 'ゼロタイガー' in rawdata.values:
	zerodf = rawdata.reindex(columns=['dai','Rotation','BB','RB','difference','max','machine','date','holl'])
	zerodf = zerodf[zerodf['holl'] == 'ゼロタイガー']
	zerodf = zerodf.iloc[:,:8]
	#'difference'chenge .astype to int64 
	zerodf['difference'] = zerodf['difference'].str.replace(',','')
	zerodf = zerodf.astype({'difference':'int64'})
	#auto seriesmachine bank
	#pd.Series.unique()
	zeroser = zerodf.loc[:,'machine'].unique()
	#series to df
	zeroserdf = pd.DataFrame(zeroser)
	zeroserdf.insert(0,'namebank', zeroser)
	dainame = pd.read_csv('./tmp/namebank.csv',names=('namebank','neoname'))
	#drop_duplicates(subset=['namebank']
	dainame = dainame.drop_duplicates(subset=['namebank'])
	newdailist = pd.merge(zeroserdf, dainame, how='outer')
	newdailist = newdailist.reindex(columns=['namebank','neoname'])
	newdailist.to_csv('./tmp/namebank.csv', header=False, index=False)
	#name.txt to String conversion
	dainame = pd.read_csv('./tmp/namebank.csv', header=None)
	#tolist
	machinename = (dainame.iloc[:,0]).values.tolist()
	newname = (dainame.iloc[:,1]).values.tolist()
	#replace
	zerocomp = zerodf.replace(machinename,newname)
	zerocomp.to_csv(f'/Users/mac2018/Applications/Collection/linkdata/zero.csv', header=False, index=False)
	print ('zero')
else:
	print ('no zero')

#case ezkana or moro to Calculation
if 'eZone金沢' in rawdata.values or 'オークラ諸江' in rawdata.values:
	Recal = rawdata.reindex(columns=['han2','kari1','kari2','dai','Rotation','BB','RB','difference','max','machine','date','holl'])
	#diff(('kari1'/1000)*6)*('kari2'-'han2')
	#Calculation
	Recal['difference'] = ((Recal['kari1'] / 1000) * 6)  * (Recal['kari2'] - Recal['han2'])
	Recaled = Recal.reindex(columns=['dai','Rotation','BB','RB','difference','max','machine','date','holl',])
	#['difference'] to int64
	Recaled = Recaled.astype({'difference':'int64'})
else:
	print ('fin')

#case ezkana
if 'eZone金沢' in Recaled.values:
	#bloom to df
	#Recaled['holl'] == 'eZone金沢'
	ezkanadf = Recaled[Recaled['holl'] == 'eZone金沢']
	ezkanadf = ezkanadf.iloc[:,:8]
	#dailist
	posdaiez = ezkanadf.loc[:,'dai']
	ezkanadf.insert(0,'posdai',posdaiez)
	dailist = pd.read_csv('./tmp/ezkanadailist.csv',names=('posdai','kuu'))
	ezkanadf = pd.merge(ezkanadf, dailist, how='outer')
	ezkanadf = ezkanadf.reindex(columns=['posdai','Rotation','BB','RB','difference','max','machine','date'])
	#fillna(method='ffill') to 'date'
	ezkanadf['date'] = ezkanadf['date'].fillna(method='ffill')
	ezkanadf= ezkanadf.fillna(0)
	ezkanadf = ezkanadf.astype({'posdai':'int64','Rotation':'int64','BB':'int64','RB':'int64','difference':'int64','max':'int64','machine':'str','date':'int64'})
	ezkanadf = ezkanadf.sort_values('posdai')
	#auto seriesmachine bank
	#pd.Series.unique()
	ezkanaser = ezkanadf.loc[:,'machine'].unique()
	#series to df
	ezkanaserdf = pd.DataFrame(ezkanaser)
	ezkanaserdf.insert(0,'namebank', ezkanaser)
	dainame = pd.read_csv('./tmp/namebank.csv',names=('namebank','neoname'))
	#drop_duplicates(subset=['namebank']
	dainame = dainame.drop_duplicates(subset=['namebank'])
	newdailist = pd.merge(ezkanaserdf, dainame, how='outer')
	newdailist = newdailist.reindex(columns=['namebank','neoname'])
	newdailist.to_csv('./tmp/namebank.csv', header=False, index=False)
	#name.txt to String conversion
	dainame = pd.read_csv('./tmp/namebank.csv', header=None)
	#tolist
	machinename = (dainame.iloc[:,0]).values.tolist()
	newname = (dainame.iloc[:,1]).values.tolist()
	#replace
	ezkanacomp = ezkanadf.replace(machinename,newname)
	ezkanacomp.to_csv(f'/Users/mac2018/Applications/Collection/linkdata/ezkana.csv', header=False, index=False)
	print('ezkana')
else:
	print('no ezkana')

#case moro
if 'オークラ諸江' in Recaled.values:
	#bloom to df
	#Recaled['holl'] == 'オークラ諸江'
	morodf = Recaled[Recaled['holl'] == 'オークラ諸江']
	morodf = morodf.iloc[:,:8]
	#dailist
	posdai = morodf.loc[:,'dai']
	morodf.insert(0,'posdai',posdai)
	dailist = pd.read_csv('./tmp/moroedailist.csv',names=('posdai','kuu'))
	morodf = pd.merge(morodf, dailist, how='outer')
	morodf = morodf.reindex(columns=['posdai','Rotation','BB','RB','difference','max','machine','date'])
	#fillna(method='ffill') to 'date'
	print(morodf['date'].dtype)
	morodf['date'] = morodf['date'].fillna(method='ffill')
	morodf = morodf.fillna(0)
	morodf = morodf.astype({'posdai':'int64','Rotation':'int64','BB':'int64','RB':'int64','difference':'int64','max':'int64','machine':'str','date':'int64'})
	morodf = morodf.sort_values('posdai')
	#auto seriesmachine bank
	#pd.Series.unique()
	moroser = morodf.loc[:,'machine'].unique()
	#series to df
	moroserdf = pd.DataFrame(moroser)
	moroserdf.insert(0,'namebank', moroser)
	dainame = pd.read_csv('./tmp/namebank.csv',names=('namebank','neoname'))
	#drop_duplicates(subset=['namebank']
	dainame = dainame.drop_duplicates(subset=['namebank'])
	newdailist = pd.merge(moroserdf, dainame, how='outer')
	newdailist = newdailist.reindex(columns=['namebank','neoname'])
	newdailist.to_csv('./tmp/namebank.csv', header=False, index=False)
	#name.txt to String conversion
	dainame = pd.read_csv('./tmp/namebank.csv', header=None)
	#tolist
	machinename = (dainame.iloc[:,0]).values.tolist()
	newname = (dainame.iloc[:,1]).values.tolist()
	#replace
	morocomp = morodf.replace(machinename,newname)
	morocomp.to_csv(f'/Users/mac2018/Applications/Collection/linkdata/moroe.csv', header=False, index=False)
	print('moro')
else:
	print('no moro')

print('fin')
quit()