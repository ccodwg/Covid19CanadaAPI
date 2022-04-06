from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse
from typing import Optional
import re
from functools import reduce
from datetime import datetime
import pandas as pd
from app.data import data

app = FastAPI(
    title="COVID-19 Canada Open Data Working Group API",
    docs_url="/",
    recdoc_url=None
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# define common functions

## add deprecation warning to response
def add_deprecation_warning(response):
    response["deprecation_warning"] = "This dataset is deprecated and will be removed on April 15, 2022. Please see details of our improved replacement dataset here: https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/ccodwg_dataset_announcement.pdf"
    return response

## retired dataset text
def retired_dataset():
    return "This dataset has been retired but an archived version may be available on GitHub: https://github.com/ccodwg/Covid19Canada/tree/master/retired_datasets"

## no results for query text
def query_no_results():
    return "No records match your query. Please try again with different settings."

## UUID not found text
def uuid_not_found():
    return "UUID not found. Please try again with a valid UUID."

## get date column
def get_date_col(df):
    return list(filter(re.compile('^date_.*').search, df.columns.values))[0]

## interpret date arg
def date_arg(arg):
    try:
        arg = datetime.strptime(arg, '%d-%m-%Y')
    except:
        try:
            arg = datetime.strptime(arg, '%Y-%m-%d')
        except:
            arg = None
    return arg

## interpret missing arg
def missing_arg(missing):
    if missing == 'na':
        missing_val = 'NA'    
    elif missing == 'empty':
        missing_val = ''
    elif missing == 'nan':
        missing_val = 'NaN'
    else:
        missing_val = 'NULL'
    return missing_val

# define constants
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
data_sknew = ['sk_new_cases_timeseries_hr_combined',
              'sk_new_mortality_timeseries_hr_combined']
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

# define routes

@app.get('/timeseries')
async def get_timeseries(
    stat: Optional[str] = None,
    loc: str = "prov",
    date: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    ymd: bool = False,
    missing: Optional[str] = None,
    version: bool = False):

    # process date args
    if date:
        date = date_arg(date)
    if after:
        after = date_arg(after)
    if before:
        before = date_arg(before)

    # process arg "missing"
    missing_val = missing_arg(missing)

    # initialize response
    response = {}
    
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
            dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_hr}
            dfs = list(dfs.values()) # convert to list
    else:
        raise HTTPException(status_code=404, detail=query_no_results())
    
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
        if ymd is True:
            dfs[i][col_date] = dfs[i][col_date].dt.strftime('%Y-%m-%d')
        else:
            dfs[i][col_date] = dfs[i][col_date].dt.strftime('%d-%m-%Y')
        dfs[i] = dfs[i].fillna(missing_val)
        
        # determine response name and add dataframe to response
        resp_name = data_names_dates[col_date]
        response[resp_name] = dfs[i].to_dict(orient='records')
    
    # add version to response
    if version is True:
        response['version'] = data.version['version']
    
    # return response
    return add_deprecation_warning(response)

@app.get("/summary")
async def get_summary(
    loc: str = "prov",
    date: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    ymd: bool = False,
    missing: Optional[str] = None,
    version: bool = False):

    # process date args
    if date:
        date = date_arg(date)
    if after:
        after = date_arg(after)
    if before:
        before = date_arg(before)
    # if no date values specified, use latest date
    if not date and not after and not before:
        date = data.version["date"]

    # process arg "missing"
    missing_val = missing_arg(missing)

    # initialize response
    response = {}

    # get dataframes and subset by location
    if loc == 'canada':
        dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_canada}
    elif loc == 'prov' or loc in data.keys_prov.keys():
        dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_prov}
    elif loc == 'hr' or loc in data.keys_hr.keys():
        dfs = {k: pd.read_csv(data.ccodwg[k]) for k in data_hr}
    else:
        raise HTTPException(status_code=404, detail=query_no_results())
    
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
    if ymd is True:
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    else:
        df['date'] = df['date'].dt.strftime('%d-%m-%Y')
    df = df.fillna(missing_val)
    response['summary'] = df.to_dict(orient='records')  
    
    # add version to response
    if version is True:
        response['version'] = data.version['version']
    
    # return response
    return add_deprecation_warning(response)

@app.get('/other')
async def get_other(
    stat: Optional[str] = None,
    missing: Optional[str] = None,
    version: bool = False):
    
    # initialize response
    response = {}
    
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
            raise HTTPException(status_code=404, detail=query_no_results())
        
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
    return add_deprecation_warning(response)

@app.get('/version')
async def get_version(dateonly: bool = False):
    
    # initialize response
    response = {}
    
    # get version
    if dateonly is True:
        response['version'] = data.version['version'].split()[0]
    else:
        response['version'] = data.version['version']
    
    # return response
    return response

@app.get('/datasets')
async def get_datasets(uuid: Optional[str] = None):
    
    # read UUIDs
    if uuid is None:
        return data.datasets['datasets']
    else:
        uuid = uuid.split('|')
    
    # filter dictionary
    response = data.datasets['datasets']
    try:
        response = {k: response[k] for k in uuid}
    except Exception:
        raise HTTPException(status_code=404, detail=uuid_not_found())
    
    # return response
    return(response)

@app.get('/archive')
async def get_archive(
    uuid: Optional[str] = None,
    remove_duplicates: bool = False,
    date: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None):
    
    # read UUIDs
    if uuid is None:
        raise HTTPException(status_code=404, detail="Please specify one or more values for 'uuid', seperated by '|'.")
    else:
        uuid = uuid.split('|')
    
    # process date filters
    if date is None and after is None and before is None:
        # if no filters, return all
        date = 'all'
    else:
        if date and date!= 'all' and date != 'latest' and date != 'first':
            date = date_arg(date)
        if after:
            after = date_arg(after)
        if before:
            before = date_arg(before)
    
    # get dataframe
    df = data.archive['index']
    df = df[df['uuid'].isin(uuid)]
    
    # return 404 if no valid UUIDs
    if len(df) == 0:
        raise HTTPException(status_code=404, detail=uuid_not_found())
    
    # date filtering
    df['file_date_true'] = pd.to_datetime(df['file_date_true'])
    if date:
        # if date is defined, after and before are ignored
        if (date == 'all'):
            pass
        elif (date == 'latest'):
            df = df.groupby('uuid').last()
        elif (date == 'first'):
            df = df.groupby('uuid').first()
        else:
            if date:
                df = df[df['file_date_true'] == date]
    else:
        if after:
            df = df[df['file_date_true'] >= after]
        if before:
            df = df[df['file_date_true'] <= before]
    
    # return 404 if no results found
    if len(df) == 0:
        raise HTTPException(status_code=404, detail=query_no_results())
    
    # filter duplicates in the filtered sample
    # not the same thing as remove file_date_duplicate == 1,
    # since the first instance of a duplicate dataset may not
    # be in the filtered sample
    if (remove_duplicates is True):
        df = df.drop_duplicates(subset=['file_etag'])
    
    # format output
    df['file_date_true'] = df['file_date_true'].dt.strftime('%Y-%m-%d')
    response = df.to_dict(orient='records')
    
    # return response
    return response

@app.get("/sknew")
async def get_sknew():
    raise HTTPException(status_code=404, detail=retired_dataset())

@app.get("/individual")
async def get_individual():
    raise HTTPException(status_code=404, detail=retired_dataset())

@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    return FileResponse("favicon.ico")
