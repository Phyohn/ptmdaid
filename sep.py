# sep.py
#python -i
#test
import base64
import codecs
import cv2
import datetime
import io
import numpy as np
import os
import pandas as pd
import pathlib
from PIL import Image
import platform
import re
import time

def f_int(x):
	return str(x).replace(',','')
#def (ezkana_comp.drop_duplicates(subset=['dai'])).sort_values('dai')

rawdata = pd.read_csv('./Raw/daidata.csv', encoding = 'utf-16' ,dtype=str)
#all chenge columnsname
rawdata = rawdata.set_axis(['Rotation','BB','RB','co1','co2','han1','han2','difference','han3','max','dai','machine','holl','date','kari1','kari2','url','text'], axis='columns')

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

#replace('-','0')
rawdata = rawdata.replace('-','0')

#each holl dai num check
zero_rawdata = rawdata[rawdata['holl'] == 'ゼロタイガー']
zero_rawdata.insert(0,'dai_num_check',zero_rawdata.loc[:,'dai'])
dai_num_check_df = pd.read_csv('./tmp/zerodailist.csv',names=('dai_num_check','empty'),dtype=str)
zero_outer_merged_df = pd.merge(zero_rawdata, dai_num_check_df, how='outer')
zero_inner_merged_df = pd.merge(zero_outer_merged_df, dai_num_check_df, how='inner')
zero_inner_merged_df['holl'] = zero_inner_merged_df['holl'].fillna(method='ffill')

ezkana_rawdata = rawdata[rawdata['holl'] == 'eZone金沢']
ezkana_rawdata.insert(0,'dai_num_check',ezkana_rawdata.loc[:,'dai'])
dai_num_check_df = pd.read_csv('./tmp/ezkanadailist.csv',names=('dai_num_check','empty'),dtype=str)
ezkana_outer_merged_df = pd.merge(ezkana_rawdata, dai_num_check_df, how='outer')
ezkana_inner_merged_df = pd.merge(ezkana_outer_merged_df, dai_num_check_df, how='inner')
ezkana_inner_merged_df['holl'] = ezkana_inner_merged_df['holl'].fillna(method='ffill')


moroe_rawdata = rawdata[rawdata['holl'] == 'オークラ諸江']
moroe_rawdata.insert(0,'dai_num_check',moroe_rawdata.loc[:,'dai'])
dai_num_check_df = pd.read_csv('./tmp/moroedailist.csv',names=('dai_num_check','empty'),dtype=str)
moroe_outer_merged_df = pd.merge(moroe_rawdata, dai_num_check_df, how='outer')
moroe_inner_merged_df = pd.merge(moroe_outer_merged_df, dai_num_check_df, how='inner')
moroe_inner_merged_df['holl'] = moroe_inner_merged_df['holl'].fillna(method='ffill')

#concat
concated_rawdata = pd.concat([zero_inner_merged_df, ezkana_inner_merged_df, moroe_inner_merged_df])
concated_rawdata.dtypes

#auto seriesmachine bank
#pd.Series.drop_duplicates()
machine_name_df = pd.DataFrame(concated_rawdata['machine'].drop_duplicates())
machine_name_df['hoge'] = '0'
rename_list_df = pd.read_csv('./tmp/namebank.csv',names=('machine','renamed_machine_name'))
merged_machine_name_df = pd.merge(machine_name_df, rename_list_df , how='outer')
new_machine_list_df = merged_machine_name_df .reindex(columns=['machine','renamed_machine_name'])
new_machine_list = new_machine_list_df.sort_values('renamed_machine_name', na_position='first')
new_machine_list.to_csv('./tmp/namebank.csv', header=False, index=False)



#trim
trim_rawdata = (concated_rawdata.applymap(f_int)).fillna(0).replace('nan','0')
#trim_rawdata.isnull().any()

#Calculation
rawdata_to_int_df = trim_rawdata.reindex(columns=['han2','kari1','kari2','dai_num_check','Rotation','BB','RB','max','date','difference','machine','holl'])
cal = rawdata_to_int_df
cal_int64 = cal.astype({'han2':'int64','kari1':'int64','kari2':'int64'})
cal_int64['cal_difference'] = ((cal_int64['kari1'] / 1000) * 6) * (cal_int64['kari2'] - cal_int64['han2'])
calculated_df = cal_int64.astype({'cal_difference':'int64'})

#rename
machine = (new_machine_list.iloc[:,0]).values.tolist()
renamed_machine_name = (new_machine_list.iloc[:,1]).values.tolist()
calculated_df['renamed'] = calculated_df['machine'].replace(machine,renamed_machine_name)

comp_df = calculated_df.reindex(columns=['dai_num_check','Rotation','BB','RB','difference','cal_difference','max','renamed','date','holl'])
comp_df['date'] = intdt

#zero
zero_df = comp_df[comp_df['holl'] == 'ゼロタイガー']
zero_comp = zero_df.reindex(columns=['dai_num_check','Rotation','BB','RB','difference','max','renamed','date'])
int_zero_comp = zero_comp.astype({'dai_num_check':'int64','Rotation':'int64','BB':'int64','RB':'int64','difference':'int64','max':'int64','renamed':'str','date':'int64'})
zero_csv = (int_zero_comp.drop_duplicates(subset=['dai_num_check'])).sort_values('dai_num_check')
zero_csv.to_csv(f'/Users/mac2018/Applications/Collection/linkdata/zero.csv', header=False, index=False)


#ezkana
ezkana_df = comp_df[comp_df['holl'] == 'eZone金沢']
ezkana_comp = ezkana_df.reindex(columns=['dai_num_check','Rotation','BB','RB','cal_difference','max','renamed','date'])
int_ezkana_csv = ezkana_comp.astype({'dai_num_check':'int64','Rotation':'int64','BB':'int64','RB':'int64','cal_difference':'int64','max':'int64','renamed':'str','date':'int64'})
ezkana_csv = (int_ezkana_csv.drop_duplicates(subset=['dai_num_check'])).sort_values('dai_num_check')
ezkana_csv.to_csv(f'/Users/mac2018/Applications/Collection/linkdata/ezkana.csv', header=False, index=False)

#moroe
moroe_df = comp_df[comp_df['holl'] == 'オークラ諸江']
moroe_comp = moroe_df.reindex(columns=['dai_num_check','Rotation','BB','RB','cal_difference','max','renamed','date'])
int_moroe_comp = moroe_comp.astype({'dai_num_check':'int64','Rotation':'int64','BB':'int64','RB':'int64','cal_difference':'int64','max':'int64','renamed':'str','date':'int64'})
moroe_csv = (int_moroe_comp.drop_duplicates(subset=['dai_num_check'])).sort_values('dai_num_check')
moroe_csv.to_csv(f'/Users/mac2018/Applications/Collection/linkdata/moroe.csv', header=False, index=False)


#moroe_df.to_csv(f'/Users/mac2018/Applications/Collection/linkdata/moroe.csv', header=False, index=False)

quit()