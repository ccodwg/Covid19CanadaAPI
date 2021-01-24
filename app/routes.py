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

# list of dataset by location
data_canada = ('cases_timeseries_canada',
               'mortality_timeseries_canada',
               'recovered_timeseries_canada',
               'testing_timeseries_canada',
               'active_timeseries_canada',
               'vaccine_administration_timeseries_canada',
               'vaccine_distribution_timeseries_canada',
               'vaccine_completion_timeseries_canada')
data_prov = ('cases_timeseries_prov',
            'mortality_timeseries_prov',
            'recovered_timeseries_prov',
            'testing_timeseries_prov',
            'active_timeseries_prov',
            'vaccine_administration_timeseries_prov',
            'vaccine_distribution_timeseries_prov',
            'vaccine_completion_timeseries_prov')
data_hr = ('cases_timeseries_hr',
           'mortality_timeseries_hr')

@app.route('/')
@app.route('/index')
def index():
    
    # initialize response
    response = {}
    
    # subset dataframes
    dfs = {k: data.ccodwg[k] for k in data_canada}
    
    # rename date columns
    for df in dfs.values():
        df.columns = df.columns.str.replace('^date_.*', 'date')
    
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

@app.route('/individual')
def individual():
    return "Individual level data return is temporarily disabled, please download from GitHub: https://github.com/ccodwg/Covid19Canada", 404
    #stat = request.args.get('stat')
    #loc = request.args.get('loc')
    #date = request.args.get('date')
    #ymd = request.args.get('ymd')
    #extra = request.args.get('extra')
    #if date:
        #try:
            #date = datetime.strptime(date, '%d-%m-%Y')
        #except:
            #try:
                #date = datetime.strptime(date, '%Y-%m-%d')
            #except:
                #date = None
    #after = request.args.get('after')
    #if after:
        #try:
            #after = datetime.strptime(after, '%d-%m-%Y')
        #except:
            #try:
                #after = datetime.strptime(after, '%Y-%m-%d')
            #except:
                #after = None
    #before = request.args.get('before')
    #if before:
        #try:
            #before = datetime.strptime(before, '%d-%m-%Y')
        #except:
            #try:
                #before = datetime.strptime(before, '%Y-%m-%d')
            #except:
                #before = None
    #version = request.args.get('version')
    #dfs = []
    #response = {}
    #if stat == 'cases':
        #cases = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv")
        #cases['date_report'] = pd.to_datetime(cases['date_report'],dayfirst=True)
        #cases['report_week'] = pd.to_datetime(cases['report_week'],dayfirst=True)
        #if extra and extra=='false':
            #pass
        #else:
            #case_source = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/cases_extra/cases_case_source.csv")[['case_source_short', 'case_source_full']]
            #cases = cases.merge(case_source, left_on='case_source', right_on='case_source_short', how='left').drop(columns=['case_source', 'case_source_short']).rename(columns={'case_source_full': 'case_source'})
        #dfs.append(cases)
    #elif stat =='mortality':
        #mortality = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv")
        #mortality['date_death_report'] = pd.to_datetime(mortality['date_death_report'],dayfirst=True)
        #if extra and extra=='false':
            #pass
        #else:
            #death_source = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/mortality_extra/mortality_death_source.csv")[['death_source_short', 'death_source_full']]
            #mortality = mortality.merge(death_source, left_on='death_source', right_on='death_source_short', how='left').drop(columns=['death_source', 'death_source_short']).rename(columns={'death_source_full': 'death_source'})
        #dfs.append(mortality)
    #else:
        #cases = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv")
        #mortality = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv")
        #cases['date_report'] = pd.to_datetime(cases['date_report'],dayfirst=True)
        #mortality['date_death_report'] = pd.to_datetime(mortality['date_death_report'],dayfirst=True)
        #if extra and extra=='false':
            #pass
        #else:
            #case_source = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/cases_extra/cases_case_source.csv")[['case_source_short', 'case_source_full']]
            #cases = cases.merge(case_source, left_on='case_source', right_on='case_source_short', how='left').drop(columns=['case_source', 'case_source_short']).rename(columns={'case_source_full': 'case_source'})
            #death_source = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/mortality_extra/mortality_death_source.csv")[['death_source_short', 'death_source_full']]
            #mortality = mortality.merge(death_source, left_on='death_source', right_on='death_source_short', how='left').drop(columns=['death_source', 'death_source_short']).rename(columns={'death_source_full': 'death_source'})
        #dfs.append(cases)
        #dfs.append(mortality)
    #for df in dfs:

        #df = df.fillna("NULL")

        #if loc:
            #if loc in data.keys_prov.keys():
                #df = df.loc[df['province'] == data.keys_prov[loc]['province']]
            #elif loc in data.keys_hr.keys():
                #df = df.loc[df['health_region'] == data.keys_hr[loc]['health_region']]
                #if loc != '9999':
                    #df = df.loc[df['province'] == data.keys_hr[loc]['province']]

        #if date:
            #if 'date_report' in df.columns:
                #df = df.loc[df['date']_report == date]
            #if 'date_death_report' in df.columns:
                #df = df.loc[df['date']_death_report == date]

        #if after:
            #if 'date_report' in df.columns:
                #df = df.loc[df['date']_report >= after]
            #if 'date_death_report' in df.columns:
                #df = df.loc[df['date']_death_report >= after]

        #if before:
            #if 'date_report' in df.columns:
                #df = df.loc[df['date']_report <= before]
            #if 'date_death_report' in df.columns:
                #df = df.loc[df['date']_death_report <= before]
        
        #if version:
            #if version=='true':
                #version = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/update_time.txt", sep="\t", header=None)
                #response["version"] = version.head().values[0][0]

        #if 'date_report' in df.columns:
            #if ymd and ymd=='true':
                #df['date_report'] = df['date']_report.dt.strftime('%Y-%m-%d')
                #df['report_week'] = df.report_week.dt.strftime('%Y-%m-%d')
            #else:
                #df['date_report'] = df['date']_report.dt.strftime('%d-%m-%Y')
                #df['report_week'] = df.report_week.dt.strftime('%d-%m-%Y')
        #if 'date_death_report' in df.columns:
            #if ymd and ymd=='true':
                #df['date_death_report'] = df['date']_death_report.dt.strftime('%Y-%m-%d')
            #else:
                #df['date_death_report'] = df['date']_death_report.dt.strftime('%d-%m-%Y')

        #if 'date_report' in df.columns:
            #response["cases"] = df.to_dict(orient='records')
        #if 'date_death_report' in df.columns:
            #response["mortality"] = df.to_dict(orient='records')

    #return response

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
    version = request.args.get('version')
    
    # process date arguments
    if date:
        date = date_arg(date)
    if after:
        after = date_arg(after)
    if before:
        before = date_arg(before)
    
    # default arguments
    if not loc:
        loc = 'prov'
    
    # get dataframes
    if loc == 'canada':
        if stat == 'cases':
            resp_name = data_canada[0]
            dfs = data.ccodwg[resp_name]
        elif stat == 'mortality':
            resp_name = data_canada[1]
            dfs = data.ccodwg[resp_name]
        elif stat == 'recovered':
            resp_name = data_canada[2]
            dfs = data.ccodwg[resp_name]
        elif stat == 'testing':
            resp_name = data_canada[3]
            dfs = data.ccodwg[resp_name]
        elif stat == 'active':
            resp_name = data_canada[4]
            dfs = data.ccodwg[resp_name]
        elif stat == 'avaccine':
            resp_name = data_canada[5]
            dfs = data.ccodwg[resp_name]
        elif stat == 'dvaccine':
            resp_name = data_canada[6]
            dfs = data.ccodwg[resp_name]
        elif stat == 'cvaccine':
            resp_name = data_canada[7]
            dfs = data.ccodwg[resp_name]
        else:
            dfs = {k: data.ccodwg[k] for k in data_canada}
    elif loc == 'prov' or loc in data.keys_prov.keys():
        if stat == 'cases':
            resp_name = data_prov[0]
            dfs = data.ccodwg[resp_name]
        elif stat == 'mortality':
            resp_name = data_prov[1]
            dfs = data.ccodwg[resp_name]
        elif stat == 'recovered':
            resp_name = data_prov[2]
            dfs = data.ccodwg[resp_name]
        elif stat == 'testing':
            resp_name = data_prov[3]
            dfs = data.ccodwg[resp_name]
        elif stat == 'active':
            resp_name = data_prov[4]
            dfs = data.ccodwg[resp_name]
        elif stat == 'avaccine':
            resp_name = data_prov[5]
            dfs = data.ccodwg[resp_name]
        elif stat == 'dvaccine':
            resp_name = data_prov[6]
            dfs = data.ccodwg[resp_name]
        elif stat == 'cvaccine':
            resp_name = data_prov[7]
            dfs = data.ccodwg[resp_name]
        else:
            dfs = {k: data.ccodwg[k] for k in data_prov}
    elif loc == 'hr' or loc in data.keys_hr.keys():
        if stat == 'cases':
            resp_name = data_hr[0]
            dfs = data.ccodwg[resp_name]
        elif stat == 'mortality':
            resp_name = data_hr[1]
            dfs = data.ccodwg[resp_name]
        else:
            dfs = {k: data.ccodwg[k] for k in data_hr}
    else:
        return "Record not found", 404
    
    # the rest of the code depends on whether stat is specified or not (i.e., whether dfs is a DataFrame or a dict)
    if stat is None:
        
        # filter by location
        if loc in data.keys_prov.keys():
            for k in dfs.keys():
                dfs[k] = dfs[k].loc[dfs[k]['province'] == data.keys_prov[loc]['province']]
        elif loc in data.keys_hr.keys():
            for k in dfs.keys():
                dfs[k] = dfs[k].loc[dfs[k]['health_region'] == data.keys_hr[loc]['health_region']]
                if loc != '9999':
                    dfs[k] = dfs[k].loc[dfs[k]['province'] == data.keys_hr[loc]['province']]        
        
        # convert date columns
        for k in dfs.keys():
            col_date = list(filter(re.compile('^date_.*').search, dfs[k].columns.values))[0]
            dfs[k][col_date] = pd.to_datetime(dfs[k][col_date], dayfirst=True)
        
        # filter by date
        for k in dfs.keys():
            col_date = list(filter(re.compile('^date_.*').search, dfs[k].columns.values))[0]
            if date:
                dfs[k] = dfs[k].loc[dfs[k][col_date] == date]
            if after:
                dfs[k] = dfs[k].loc[dfs[k][col_date] >= after]
            if before:
                dfs[k] = dfs[k].loc[dfs[k][col_date] <= before]
        
        # format output
        for k in dfs.keys():
            col_date = list(filter(re.compile('^date_.*').search, dfs[k].columns.values))[0]
            if ymd == 'true':
                dfs[k][col_date] = dfs[k][col_date].dt.strftime('%Y-%m-%d')
            else:
                dfs[k][col_date] = dfs[k][col_date].dt.strftime('%d-%m-%Y')
            dfs[k] = dfs[k].fillna('NULL')
            response[k] = dfs[k].to_dict(orient='records')        
    else:
        
        # determine date column
        col_date = list(filter(re.compile('^date_.*').search, dfs.columns.values))[0]
        
        # filter by location
        if loc in data.keys_prov.keys():
            dfs = dfs.loc[dfs['province'] == data.keys_prov[loc]['province']]
        elif loc in data.keys_hr.keys():
            for k in dfs.keys():
                dfs = dfs.loc[dfs['health_region'] == data.keys_hr[loc]['health_region']]
                if loc != '9999':
                    dfs = dfs.loc[dfs['province'] == data.keys_hr[loc]['province']]         
        
        # convert date column
        dfs[col_date] = pd.to_datetime(dfs[col_date], dayfirst=True)
        
        # filter by date
        if date:
            df = df.loc[df['date'] == date]
            dfs = dfs.loc[dfs[col_date] == date]
        if after:
            dfs = dfs.loc[dfs[col_date] >= after]
        if before:
            dfs = dfs.loc[dfs[col_date] <= before]        
        
        # format output
        if ymd == 'true':
            dfs[col_date] = dfs[col_date].dt.strftime('%Y-%m-%d')
        else:
            dfs[col_date] = dfs[col_date].dt.strftime('%d-%m-%Y')
        dfs = dfs.fillna('NULL')
        response[resp_name] = dfs.to_dict(orient='records')        
        
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
    version = request.args.get('version')
    
    # process date arguments
    if date:
        date = date_arg(date)
    if after:
        after = date_arg(after)
    if before:
        before = date_arg(before)
        
    # default arguments
    if not date and not after and not before:
        date = data.version['date']
    if not loc:
        loc = 'prov'
    
    # get dataframes and subset by location
    if loc == 'canada':
        dfs = {k: data.ccodwg[k] for k in data_canada}
    elif loc == 'prov' or loc in data.keys_prov.keys():
        dfs = {k: data.ccodwg[k] for k in data_prov}
    elif loc == 'hr' or loc in data.keys_hr.keys():
        dfs = {k: data.ccodwg[k] for k in data_hr}
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
    df = reduce(lambda left, right: pd.merge(left, right, on=['date', 'province'], how='outer'), dfs.values())
    
    # convert dates column
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    
    # filter by location
    if loc in data.keys_prov.keys():
        df = df.loc[df['province'] == data.keys_prov[loc]['province']]
    elif loc in data.keys_hr.keys():
        df = df.loc[df['health_region'] == data.keys_hr[loc]['health_region']]
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
    df = df.fillna('NULL')
    response['summary'] = df.to_dict(orient='records')  
    
    # add version to response
    if version == 'true':
        response['version'] = data.version['version']
    
    # return response
    return response

@app.route('/version')
def version():
    
    # initialize response
    response = {}
    
    # get version
    response['version'] = data.version['version']
    
    # return response
    return response
