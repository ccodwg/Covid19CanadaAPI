# load libraries
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import os
import glob
from datetime import datetime
import pandas as pd
import tempfile
import git

# define functions

## read in data (time series dataset)
def load_data_ts(temp_dir):
    
    ## make data available globally
    global ccodwg, keys_prov, keys_hr, version
    
    ## list data files
    file_paths = glob.glob(temp_dir.name + '/**/*.csv', recursive=True)
    file_names = [os.path.splitext(os.path.basename(f))[0] for f in file_paths]
    
    ## create dictionary: {datasets: paths to datasets}
    ccodwg = {file_names[i]: file_paths[i] for i in range(len(file_names))}
    
    ## load data into memory: province and health region keys
    keys_prov = pd.read_csv(ccodwg['prov_map'])[['province_short', 'province']].set_index(['province_short']).to_dict('index')
    keys_hr = pd.read_csv(ccodwg['hr_map'])[['HR_UID', 'province', 'health_region']]
    keys_hr.loc[keys_hr.HR_UID == 9999, "province"] = "All Provinces"
    keys_hr = keys_hr.drop_duplicates().set_index(['HR_UID'])
    keys_hr.index = keys_hr.index.map(str)
    keys_hr = keys_hr.to_dict('index')
    
    ## load data into memory: version - full string and date only
    version = dict.fromkeys(['version', 'date'])
    version['version'] = pd.read_csv(os.path.join(temp_dir.name, 'update_time.txt'), sep='\t', header=None).head().values[0][0]
    version['date'] = pd.to_datetime(version['version'].split()[0])

## update data (time series dataset)
def update_data_ts(temp_dir):
    
    ## make data available globally
    global ccodwg, keys_prov, keys_hr, version
    
    ## git pull
    print('Pulling from CCODWG repository...')
    repo = git.Git(temp_dir.name)
    repo.pull('origin', 'master')
    
    ### read in updated data if version has changed
    print('Checking if data have changed...')
    if (pd.read_csv(os.path.join(temp_dir.name, 'update_time.txt'), sep="\t", header=None).head().values[0][0] != version['version']):
        print('Data have changed. Reloading files...')
        load_data_ts(temp_dir)
        print('Data have been updated.')
    else:
        print('Data have not changed. No action required.')

# initial clone of data repository
print('Cloning from CCODWG repository...')
temp_dir = tempfile.TemporaryDirectory()
git.Repo.clone_from('https://github.com/ccodwg/Covid19Canada.git', temp_dir.name, branch='master', depth=1)
print('Clone complete. Reading in data...')
load_data_ts(temp_dir)
print('Data are ready.')

# check for data updates
scheduler = BackgroundScheduler()
job_ts = scheduler.add_job(update_data_ts, 'interval', minutes=5, args=[temp_dir])
scheduler.start()
