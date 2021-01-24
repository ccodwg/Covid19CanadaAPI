# load libraries
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import os
import shutil
import glob
from datetime import datetime
import pandas as pd
import git

# define functions

## read in data
def load_data():
    
    ## make data available globally
    global ccodwg, keys_prov, keys_hr, version
    
    ## list data files
    files = glob.glob('data/**/*.csv', recursive=True)
    filenames = [os.path.splitext(os.path.basename(f))[0] for f in files]
    
    ## create dictionary of dataframes
    ccodwg = [pd.read_csv(f) for f in files]
    ccodwg = {filenames[i]: ccodwg[i] for i in range(0, len(ccodwg), 1)}
    
    ## create keys for provinces and health regions
    keys_prov = ccodwg['prov_map'][['province_short', 'province']].set_index(['province_short']).to_dict('index')
    keys_hr = ccodwg['hr_map'][['HR_UID', 'province', 'health_region']]
    keys_hr.loc[keys_hr.HR_UID == 9999, "province"] = "All Provinces"
    keys_hr = keys_hr.drop_duplicates().set_index(['HR_UID'])
    keys_hr.index = keys_hr.index.map(str)
    keys_hr = keys_hr.to_dict('index')
    
    ## read in version - full string and date only
    version = dict.fromkeys(['version', 'date'])
    version['version'] = pd.read_csv('data/update_time.txt', sep='\t', header=None).head().values[0][0]
    version['date'] = pd.to_datetime(version['version'].split()[0])

## update data
def update_data():
    
    ## make data available globally
    global ccodwg, keys_prov, keys_hr, version
    
    ## git pull
    print('Pulling from CCODWG repository...')
    repo = git.Git('data')
    repo.pull('origin', 'master')
    
    ### read in updated data if version has changed
    print('Checking if data have changed...')
    if (pd.read_csv('data/update_time.txt', sep="\t", header=None).head().values[0][0] != version['version']):
        print('Data have changed. Reloading files...')
        load_data()
        print('Data have been updated.')
    else:
        print('Data have not changed. No action required.')

# initial clone of data repository
print('Cloning from CCODWG repository...')
path_data = 'data'
if os.path.isdir(path_data):
    shutil.rmtree(path_data)
os.mkdir(path_data)
git.Repo.clone_from('https://github.com/ccodwg/Covid19Canada.git', 'data', branch='master', depth=1)
print('Clone complete. Reading in data...')
load_data()
print('Data are ready.')

# check for data updates every 5 minutes
scheduler = BackgroundScheduler()
job = scheduler.add_job(update_data, 'interval', minutes=5)
scheduler.start()