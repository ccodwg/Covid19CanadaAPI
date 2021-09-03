# import app components
from app import app, data
from flask_cors import CORS
CORS(app) # enable CORS for all routes

# import libraries
from flask import request
import pandas as pd
import re
from datetime import datetime
from functools import reduce

# define functions

## process date args
def date_arg(arg):
    try:
        arg = datetime.strptime(arg, '%d-%m-%Y')
    except:
        try:
            arg = datetime.strptime(arg, '%Y-%m-%d')
        except:
            arg = None
    return arg

## process missing arg
def missing_arg(missing):
    if missing == 'na':
        missing_val = 'NA'    
    elif missing == 'empty':
        missing_val = ''
    elif missing == 'nan':
        missing_val = 'NaN'
    else:
        missing_val = 'NULL'
    return(missing_val)

## get date column
def get_date_col(df):
    return list(filter(re.compile('^date_.*').search, df.columns.values))[0]

# list of dataset by location
data_canada = ['cases_timeseries_canada',
               'mortality_timeseries_canada',
               'recovered_timeseries_canada',
               'testing_timeseries_canada',
               'active_timeseries_canada',
               'vaccine_administration_timeseries_canada',
               'vaccine_distribution_timeseries_canada',
               'vaccine_completion_timeseries_canada']
data_prov = ['cases_timeseries_prov',
            'mortality_timeseries_prov',
            'recovered_timeseries_prov',
            'testing_timeseries_prov',
            'active_timeseries_prov',
            'vaccine_administration_timeseries_prov',
            'vaccine_distribution_timeseries_prov',
            'vaccine_completion_timeseries_prov']
data_hr = ['cases_timeseries_hr',
           'mortality_timeseries_hr']
data_names = ['cases',
              'mortality',
              'recovered',
              'testing',
              'active',
              'avaccine',
              'dvaccine',
              'cvaccine']
data_names_dates = {
    'date_report': 'cases',
    'date_death_report': 'mortality',
    'date_recovered': 'recovered',
    'date_testing': 'testing',
    'date_active': 'active',
    'date_vaccine_administered': 'avaccine',
    'date_vaccine_distributed': 'dvaccine',
    'date_vaccine_completed': 'cvaccine'
}
data_other = {
    'prov': 'prov_map',
    'hr': 'hr_map',
    'age_cases': 'age_map_cases',
    'age_mortality': 'age_map_mortality'
}

@app.route('/')
@app.route('/index')
def index():
    
    # initialize response
    response = {}
    
    # subset dataframes
    dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_canada}
    
    # rename date columns
    for df in dfs.values():
        df.columns = df.columns.str.replace('^date_.*', 'date', regex = True)
    
    # subset active dataframe to avoid duplicate columns
    dfs['active_timeseries_canada'] = dfs['active_timeseries_canada'].drop(columns=['cumulative_cases',
                                                                                   'cumulative_recovered',
                                                                                   'cumulative_deaths'])
    
    # merge dataframes
    df = reduce(lambda left, right: pd.merge(left, right, on=['date', 'province'], how='outer'), dfs.values())
    
    # convert date column and filter to most recent date
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df = df.loc[df['date'] == data.version['date']]
    
    # format output
    df['date'] = df['date'].dt.strftime('%d-%m-%Y')  
    df = df.fillna('NULL')
    response['summary'] = df.to_dict(orient='records')  
    
    # add version to response
    response['version'] = data.version['version']
    
    # return response
    return response

@app.route('/timeseries')
def timeseries():
    
    # initialize response
    response = {}    
    
    # read arguments
    stat = request.args.get('stat')
    loc = request.args.get('loc')
    date = request.args.get('date')
    after = request.args.get('after')
    before = request.args.get('before')
    ymd = request.args.get('ymd')
    missing = request.args.get('missing')
    version = request.args.get('version')
    
    # process date arguments
    if date:
        date = date_arg(date)
    if after:
        after = date_arg(after)
    if before:
        before = date_arg(before)
    
    # process other arguments
    missing_val = missing_arg(missing)
    if not loc:
        loc = 'prov'
    
    # get dataframes
    if loc == 'canada':
        if stat == 'cases':
            data_name = data_canada[0]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'mortality':
            data_name = data_canada[1]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'recovered':
            data_name = data_canada[2]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'testing':
            data_name = data_canada[3]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'active':
            data_name = data_canada[4]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'avaccine':
            data_name = data_canada[5]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'dvaccine':
            data_name = data_canada[6]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'cvaccine':
            data_name = data_canada[7]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        else:
            dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_canada}
            dfs = list(dfs.values()) # convert to list
    elif loc == 'prov' or loc in data.keys_prov.keys():
        if stat == 'cases':
            data_name = data_prov[0]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'mortality':
            data_name = data_prov[1]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'recovered':
            data_name = data_prov[2]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'testing':
            data_name = data_prov[3]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'active':
            data_name = data_prov[4]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'avaccine':
            data_name = data_prov[5]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'dvaccine':
            data_name = data_prov[6]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'cvaccine':
            data_name = data_prov[7]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        else:
            dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_prov}
            dfs = list(dfs.values()) # convert to list
    elif loc == 'hr' or loc in data.keys_hr.keys():
        if stat == 'cases':
            data_name = data_hr[0]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        elif stat == 'mortality':
            data_name = data_hr[1]
            dfs = [pd.read_csv(data.ccodwg[data_name])]
        else:
            dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_canada}
            dfs = list(dfs.values()) # convert to list
    else:
        return "Record not found", 404
    
    # filter by location
    if loc in data.keys_prov.keys():
        for i in range(len(dfs)):
            dfs[i] = dfs[i].loc[dfs[i]['province'] == data.keys_prov[loc]['province']]
    elif loc in data.keys_hr.keys():
        for i in range(len(dfs)):
            dfs[i] = dfs[i].loc[dfs[i]['health_region'] == data.keys_hr[loc]['health_region']]
            if loc != '9999':
                dfs[i] = dfs[i].loc[dfs[i]['province'] == data.keys_hr[loc]['province']]
    
    # convert date column
    for i in range(len(dfs)):
        col_date = get_date_col(dfs[i])
        dfs[i][col_date] = pd.to_datetime(dfs[i][col_date], dayfirst=True)
    
    # filter by date
    for i in range(len(dfs)):
        col_date = get_date_col(dfs[i])
        if date:
            dfs[i] = dfs[i].loc[dfs[i][col_date] == date]
        if after:
            dfs[i] = dfs[i].loc[dfs[i][col_date] >= after]
        if before:
            dfs[i] = dfs[i].loc[dfs[i][col_date] <= before]        
    
    # format output
    for i in range(len(dfs)):
        col_date = get_date_col(dfs[i])
        if ymd == 'true':
            dfs[i][col_date] = dfs[i][col_date].dt.strftime('%Y-%m-%d')
        else:
            dfs[i][col_date] = dfs[i][col_date].dt.strftime('%d-%m-%Y')
        dfs[i] = dfs[i].fillna(missing_val)
        
        # determine response name and add dataframe to response
        resp_name = data_names_dates[col_date]
        response[resp_name] = dfs[i].to_dict(orient='records')
    
    # add version to response
    if version == 'true':
        response['version'] = data.version['version']
    
    # return response
    return response

@app.route('/summary')
def summary():
    
    # initialize response
    response = {}    
    
    # read arguments
    loc = request.args.get('loc')
    date = request.args.get('date')
    after = request.args.get('after')
    before = request.args.get('before')
    ymd = request.args.get('ymd')
    missing = request.args.get('missing')
    version = request.args.get('version')
    
    # process date arguments
    if date:
        date = date_arg(date)
    if after:
        after = date_arg(after)
    if before:
        before = date_arg(before)
    if not date and not after and not before:
        date = data.version['date']    
        
    # process other arguments
    missing_val = missing_arg(missing)
    if not loc:
        loc = 'prov'
    
    # get dataframes and subset by location
    if loc == 'canada':
        dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_canada}
    elif loc == 'prov' or loc in data.keys_prov.keys():
        dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_prov}
    elif loc == 'hr' or loc in data.keys_hr.keys():
        dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_hr}
    else:
        return "Record not found", 404
    
    # rename date columns
    for df in dfs.values():
        df.columns = df.columns.str.replace('^date_.*', 'date')
    
    # subset active dataframe to avoid duplicate columns
    if loc == 'canada':
        dfs['active_timeseries_canada'] = dfs['active_timeseries_canada'].drop(columns=['cumulative_cases',
                                                                                        'cumulative_recovered',
                                                                                        'cumulative_deaths'])
    elif loc == 'prov' or loc in data.keys_prov.keys():
        dfs['active_timeseries_prov'] = dfs['active_timeseries_prov'].drop(columns=['cumulative_cases',
                                                                                    'cumulative_recovered',
                                                                                    'cumulative_deaths'])
    
    # merge dataframes
    if loc == 'hr' or loc in data.keys_hr.keys():
        df = reduce(lambda left, right: pd.merge(left, right, on=['date', 'province', 'health_region'], how='outer'), dfs.values())
    else:
        df = reduce(lambda left, right: pd.merge(left, right, on=['date', 'province'], how='outer'), dfs.values())
    
    # convert dates column
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    
    # filter by location
    if loc in data.keys_prov.keys():
        df = df.loc[df['province'] == data.keys_prov[loc]['province']]
    elif loc in data.keys_hr.keys():
        df = df.loc[df['health_region'] == data.keys_hr[loc]['health_region']]
        print("HI")
        if loc != '9999':
            df = df.loc[df['province'] == data.keys_hr[loc]['province']]
    
    # filter by date
    if date:
        df = df.loc[df['date'] == date]
    if after:
        df = df.loc[df['date'] >= after]
    if before:
        df = df.loc[df['date'] <= before]
    
    # format output
    if ymd == 'true':
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    else:
        df['date'] = df['date'].dt.strftime('%d-%m-%Y')
    df = df.fillna(missing_val)
    response['summary'] = df.to_dict(orient='records')  
    
    # add version to response
    if version == 'true':
        response['version'] = data.version['version']
    
    # return response
    return response

@app.route('/individual')
def individual():
    return "Individual level data return is temporarily disabled, please download from GitHub: https://github.com/ccodwg/Covid19Canada", 404
    
    ## initialize response
    #response = {}
    
    ## read arguments
    #stat = request.args.get('stat')
    #loc = request.args.get('loc')
    #date = request.args.get('date')
    #ymd = request.args.get('ymd')
    #missing = request.args.get('missing')
    #extra = request.args.get('extra')

@app.route('/other')
def other():
    
    # initialize response
    response = {}
    
    # read arguments
    stat = request.args.get('stat')
    missing = request.args.get('missing')
    version = request.args.get('version')
    
    # process other arguments
    missing_val = missing_arg(missing)
    
    # get dataframes
    if stat:
        if (stat == 'prov'):
            dfs = pd.read_csv(data.ccodwg[data_other[stat]])
        elif (stat == 'hr'):
            dfs = pd.read_csv(data.ccodwg[data_other[stat]])
        elif (stat == 'age_cases'):
            dfs = pd.read_csv(data.ccodwg[data_other[stat]])
        elif (stat == 'age_mortality'):
            dfs = pd.read_csv(data.ccodwg[data_other[stat]])
        else:
            return "Record not found", 404
        
        # format output
        dfs = dfs.fillna(missing_val)
        
        # determine response name and add dataframe to response
        response[stat] = dfs.to_dict(orient='records')
        
    else:
        dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_other.values()}
        dfs = list(dfs.values()) # convert to list        
        
        # format output
        for i in range(len(dfs)):
            dfs[i] = dfs[i].fillna(missing_val)
        
            # determine response name and add dataframe to response
            resp_name = list(data_other)[i]
            response[resp_name] = dfs[i].to_dict(orient='records')
    
    # add version to response
    if version == 'true':
        response['version'] = data.version['version']    
    
    # return response
    return response

@app.route('/version')
def version():
    
    # initialize response
    response = {}
    
    # read arguments
    dateonly = request.args.get('dateonly')
    
    # get version
    if dateonly == 'true':
        response['version'] = data.version['version'].split()[0]
    else:
        response['version'] = data.version['version']
    
    # return response
    return response
