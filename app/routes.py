# import app components
from app import app, data
from flask_cors import CORS
CORS(app) # enable CORS for all routes

# import libraries
from flask import request
import pandas as pd
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
    dfs = []
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
    
    # convert dates and filter to most recent date
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
                #df = df.loc[df.province == data.keys_prov[loc]['province']]
            #elif loc in data.keys_hr.keys():
                #df = df.loc[df.health_region == data.keys_hr[loc]['health_region']]
                #if loc != '9999':
                    #df = df.loc[df.province == data.keys_hr[loc]['province']]

        #if date:
            #if 'date_report' in df.columns:
                #df = df.loc[df.date_report == date]
            #if 'date_death_report' in df.columns:
                #df = df.loc[df.date_death_report == date]

        #if after:
            #if 'date_report' in df.columns:
                #df = df.loc[df.date_report >= after]
            #if 'date_death_report' in df.columns:
                #df = df.loc[df.date_death_report >= after]

        #if before:
            #if 'date_report' in df.columns:
                #df = df.loc[df.date_report <= before]
            #if 'date_death_report' in df.columns:
                #df = df.loc[df.date_death_report <= before]
        
        #if version:
            #if version=='true':
                #version = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/update_time.txt", sep="\t", header=None)
                #response["version"] = version.head().values[0][0]

        #if 'date_report' in df.columns:
            #if ymd and ymd=='true':
                #df['date_report'] = df.date_report.dt.strftime('%Y-%m-%d')
                #df['report_week'] = df.report_week.dt.strftime('%Y-%m-%d')
            #else:
                #df['date_report'] = df.date_report.dt.strftime('%d-%m-%Y')
                #df['report_week'] = df.report_week.dt.strftime('%d-%m-%Y')
        #if 'date_death_report' in df.columns:
            #if ymd and ymd=='true':
                #df['date_death_report'] = df.date_death_report.dt.strftime('%Y-%m-%d')
            #else:
                #df['date_death_report'] = df.date_death_report.dt.strftime('%d-%m-%Y')

        #if 'date_report' in df.columns:
            #response["cases"] = df.to_dict(orient='records')
        #if 'date_death_report' in df.columns:
            #response["mortality"] = df.to_dict(orient='records')

    #return response

@app.route('/timeseries')
def timeseries():
    
    # initialize response
    dfs = []
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

    if stat == 'cases':
        if loc == 'canada':
            cases_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/cases_timeseries_canada.csv")
            cases_can['date_report'] = pd.to_datetime(cases_can['date_report'], dayfirst=True)
            dfs.append(cases_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            cases_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv")
            cases_prov['date_report'] = pd.to_datetime(cases_prov['date_report'], dayfirst=True)
            dfs.append(cases_prov)
        elif loc == 'hr' or loc in data.keys_hr.keys():
            cases_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/cases_timeseries_hr.csv")
            cases_hr['date_report'] = pd.to_datetime(cases_hr['date_report'], dayfirst=True)
            dfs.append(cases_hr)
        else:
            return "Record not found", 404
    elif stat =='mortality':
        if loc == 'canada':
            mortality_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/mortality_timeseries_canada.csv")
            mortality_can['date_death_report'] = pd.to_datetime(mortality_can['date_death_report'], dayfirst=True)
            dfs.append(mortality_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            mortality_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/mortality_timeseries_prov.csv")
            mortality_prov['date_death_report'] = pd.to_datetime(mortality_prov['date_death_report'], dayfirst=True)
            dfs.append(mortality_prov)
        elif loc == 'hr' or loc in data.keys_hr.keys():
            mortality_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/mortality_timeseries_hr.csv")
            mortality_hr['date_death_report'] = pd.to_datetime(mortality_hr['date_death_report'], dayfirst=True)
            dfs.append(mortality_hr)
        else:
            return "Record not found", 404
    elif stat =='recovered':
        if loc == 'canada':
            recovered_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/recovered_timeseries_canada.csv")
            recovered_can['date_recovered'] = pd.to_datetime(recovered_can['date_recovered'], dayfirst=True)
            dfs.append(recovered_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            recovered_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/recovered_timeseries_prov.csv")
            recovered_prov['date_recovered'] = pd.to_datetime(recovered_prov['date_recovered'], dayfirst=True)
            dfs.append(recovered_prov)
        else:
            return "Record not found", 404
    elif stat =='testing':
        if loc == 'canada':
            testing_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/testing_timeseries_canada.csv")
            testing_can['date_testing'] = pd.to_datetime(testing_can['date_testing'], dayfirst=True)
            dfs.append(testing_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            testing_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/testing_timeseries_prov.csv")
            testing_prov['date_testing'] = pd.to_datetime(testing_prov['date_testing'], dayfirst=True)
            dfs.append(testing_prov)
    elif stat =='active':
        if loc == 'canada':
            active_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/active_timeseries_canada.csv")
            active_can['date_active'] = pd.to_datetime(active_can['date_active'], dayfirst=True)
            dfs.append(active_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            active_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/active_timeseries_prov.csv")
            active_prov['date_active'] = pd.to_datetime(active_prov['date_active'], dayfirst=True)
            dfs.append(active_prov)
    elif stat == 'avaccine':
        if loc == 'canada':
            avaccine_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/vaccine_administration_timeseries_canada.csv")
            avaccine_can['date_vaccine_administered'] = pd.to_datetime(avaccine_can['date_vaccine_administered'], dayfirst=True)
            dfs.append(avaccine_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            avaccine_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/vaccine_administration_timeseries_prov.csv")
            avaccine_prov['date_vaccine_administered'] = pd.to_datetime(avaccine_prov['date_vaccine_administered'], dayfirst=True)
            dfs.append(avaccine_prov)
    elif stat == 'dvaccine':
        if loc == 'canada':
            dvaccine_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/vaccine_distribution_timeseries_canada.csv")
            dvaccine_can['date_vaccine_distributed'] = pd.to_datetime(dvaccine_can['date_vaccine_distributed'], dayfirst=True)
            dfs.append(dvaccine_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            dvaccine_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/vaccine_distribution_timeseries_prov.csv")
            dvaccine_prov['date_vaccine_distributed'] = pd.to_datetime(dvaccine_prov['date_vaccine_distributed'], dayfirst=True)
            dfs.append(dvaccine_prov)
    else:
        if loc == 'canada':
            cases_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/cases_timeseries_canada.csv")
            cases_can['date_report'] = pd.to_datetime(cases_can['date_report'], dayfirst=True)
            dfs.append(cases_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            cases_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv")
            cases_prov['date_report'] = pd.to_datetime(cases_prov['date_report'], dayfirst=True)
            dfs.append(cases_prov)
        elif loc == 'hr' or loc in data.keys_hr.keys():
            cases_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/cases_timeseries_hr.csv")
            cases_hr['date_report'] = pd.to_datetime(cases_hr['date_report'], dayfirst=True)
            dfs.append(cases_hr)

        if loc == 'canada':
            mortality_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/mortality_timeseries_canada.csv")
            mortality_can['date_death_report'] = pd.to_datetime(mortality_can['date_death_report'], dayfirst=True)
            dfs.append(mortality_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            mortality_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/mortality_timeseries_prov.csv")
            mortality_prov['date_death_report'] = pd.to_datetime(mortality_prov['date_death_report'], dayfirst=True)
            dfs.append(mortality_prov)
        elif loc == 'hr' or loc in data.keys_hr.keys():
            mortality_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/mortality_timeseries_hr.csv")
            mortality_hr['date_death_report'] = pd.to_datetime(mortality_hr['date_death_report'], dayfirst=True)
            dfs.append(mortality_hr)
        else:
            return "Record not found", 404

        if loc == 'canada':
            recovered_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/recovered_timeseries_canada.csv")
            recovered_can['date_recovered'] = pd.to_datetime(recovered_can['date_recovered'], dayfirst=True)
            dfs.append(recovered_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            recovered_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/recovered_timeseries_prov.csv")
            recovered_prov['date_recovered'] = pd.to_datetime(recovered_prov['date_recovered'], dayfirst=True)
            dfs.append(recovered_prov)

        if loc == 'canada':
            testing_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/testing_timeseries_canada.csv")
            testing_can['date_testing'] = pd.to_datetime(testing_can['date_testing'], dayfirst=True)
            dfs.append(testing_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            testing_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/testing_timeseries_prov.csv")
            testing_prov['date_testing'] = pd.to_datetime(testing_prov['date_testing'], dayfirst=True)
            dfs.append(testing_prov)

        if loc == 'canada':
            active_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/active_timeseries_canada.csv")
            active_can['date_active'] = pd.to_datetime(active_can['date_active'], dayfirst=True)
            dfs.append(active_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            active_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/active_timeseries_prov.csv")
            active_prov['date_active'] = pd.to_datetime(active_prov['date_active'], dayfirst=True)
            dfs.append(active_prov)
            
        if loc == 'canada':
            avaccine_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/vaccine_administration_timeseries_canada.csv")
            avaccine_can['date_vaccine_administered'] = pd.to_datetime(avaccine_can['date_vaccine_administered'], dayfirst=True)
            dfs.append(avaccine_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            avaccine_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/vaccine_administration_timeseries_prov.csv")
            avaccine_prov['date_vaccine_administered'] = pd.to_datetime(avaccine_prov['date_vaccine_administered'], dayfirst=True)
            dfs.append(avaccine_prov)        
        
        if loc == 'canada':
            dvaccine_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/vaccine_distribution_timeseries_canada.csv")
            dvaccine_can['date_vaccine_distributed'] = pd.to_datetime(dvaccine_can['date_vaccine_distributed'], dayfirst=True)
            dfs.append(dvaccine_can)
        elif loc == 'prov' or loc in data.keys_prov.keys():
            dvaccine_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/vaccine_distribution_timeseries_prov.csv")
            dvaccine_prov['date_vaccine_distributed'] = pd.to_datetime(dvaccine_prov['date_vaccine_distributed'], dayfirst=True)
            dfs.append(dvaccine_prov)        

    for df in dfs:

        df = df.fillna("NULL")

        if loc:
            if loc == 'canada':
                df
            elif loc == 'prov':
                df
            elif loc == 'hr':
                df
            elif loc in data.keys_prov.keys():
                df = df.loc[df.province == data.keys_prov[loc]['province']]
            elif loc in data.keys_hr.keys():
                df = df.loc[df.health_region == data.keys_hr[loc]['health_region']]
                if loc != '9999':
                    df = df.loc[df.province == data.keys_hr[loc]['province']]                
            else:
                return "Record not found", 404

        if date:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report == date]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report == date]
            if 'date_active' in df.columns:
                df = df.loc[df.date_active == date]
            if 'date_recovered' in df.columns:
                df = df.loc[df.date_recovered == date]
            if 'date_testing' in df.columns:
                df = df.loc[df.date_testing == date]
            if 'date_vaccine_administered' in df.columns:
                df = df.loc[df.date_vaccine_administered == date]
            if 'date_vaccine_distributed' in df.columns:
                df = df.loc[df.date_vaccine_distributed == date]

        if after:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report >= after]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report >= after]
            if 'date_active' in df.columns:
                df = df.loc[df.date_active >= after]
            if 'date_recovered' in df.columns:
                df = df.loc[df.date_recovered >= after]
            if 'date_testing' in df.columns:
                df = df.loc[df.date_testing >= after]
            if 'date_vaccine_administered' in df.columns:
                df = df.loc[df.date_vaccine_administered>= after]
            if 'date_vaccine_distributed' in df.columns:
                df = df.loc[df.date_vaccine_distributed >= after]
                    
        if before:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report <= before]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report <= before]
            if 'date_active' in df.columns:
                df = df.loc[df.date_active <= before]
            if 'date_recovered' in df.columns:
                df = df.loc[df.date_recovered <= before]
            if 'date_testing' in df.columns:
                df = df.loc[df.date_testing <= before]
            if 'date_vaccine_administered' in df.columns:
                df = df.loc[df.date_vaccine_administered <= before]
            if 'date_vaccine_distributed' in df.columns:
                df = df.loc[df.date_vaccine_distributed <= before]            

        if version:
            if version=='true':
                response['version'] = data.version['version']

        if 'date_report' in df.columns:
            if ymd and ymd=='true':
                df['date_report'] = df.date_report.dt.strftime('%Y-%m-%d')
            else:
                df['date_report'] = df.date_report.dt.strftime('%d-%m-%Y')
        if 'date_death_report' in df.columns:
            if ymd and ymd=='true':
                df['date_death_report'] = df.date_death_report.dt.strftime('%Y-%m-%d')
            else:
                df['date_death_report'] = df.date_death_report.dt.strftime('%d-%m-%Y')
        if 'date_recovered' in df.columns:
            if ymd and ymd=='true':
                df['date_recovered'] = df.date_recovered.dt.strftime('%Y-%m-%d')
            else:
                df['date_recovered'] = df.date_recovered.dt.strftime('%d-%m-%Y')
        if 'date_testing' in df.columns:
            if ymd and ymd=='true':
                df['date_testing'] = df.date_testing.dt.strftime('%Y-%m-%d')
            else:
                df['date_testing'] = df.date_testing.dt.strftime('%d-%m-%Y')
        if 'date_active' in df.columns:
            if ymd and ymd=='true':
                df['date_active'] = df.date_active.dt.strftime('%Y-%m-%d')
            else:
                df['date_active'] = df.date_active.dt.strftime('%d-%m-%Y')
        if 'date_vaccine_administered' in df.columns:
            if ymd and ymd=='true':
                df['date_vaccine_administered'] = df.date_vaccine_administered.dt.strftime('%Y-%m-%d')
            else:
                df['date_vaccine_administered'] = df.date_vaccine_administered.dt.strftime('%d-%m-%Y')
        if 'date_vaccine_distributed' in df.columns:
            if ymd and ymd=='true':
                df['date_vaccine_distributed'] = df.date_vaccine_distributed.dt.strftime('%Y-%m-%d')
            else:
                df['date_vaccine_distributed'] = df.date_vaccine_distributed.dt.strftime('%d-%m-%Y')
                
        if 'date_report' in df.columns:
            response["cases"] = df.to_dict(orient='records')
        if 'date_death_report' in df.columns:
            response["mortality"] = df.to_dict(orient='records')
        if 'date_recovered' in df.columns:
            response["recovered"] = df.to_dict(orient='records')
        if 'date_testing' in df.columns:
            response["testing"] = df.to_dict(orient='records')
        if 'date_active' in df.columns:
            response["active"] = df.to_dict(orient='records')
        if 'date_vaccine_administered' in df.columns:
            response["avaccine"] = df.to_dict(orient='records')
        if 'date_vaccine_distributed' in df.columns:
            response["dvaccine"] = df.to_dict(orient='records')        

    return response

@app.route('/summary')
def summary():
    
    # initialize response
    dfs = []
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
    
    # subset dataframes according to location
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
    
    # convert dates and filter to most recent date
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    
    # filter by location
    if loc in data.keys_prov.keys():
        df = df.loc[df.province == data.keys_prov[loc]['province']]
    elif loc in data.keys_hr.keys():
        df = df.loc[df.health_region == data.keys_hr[loc]['health_region']]
        if loc != '9999':
            df = df.loc[df.province == data.keys_hr[loc]['province']]    
    
    # filter by date
    if date:
        df = df.loc[df.date == date]
    if after:
        df = df.loc[df.date >= after]
    if before:
        df = df.loc[df.date <= before]
    
    # format output
    if ymd == 'true':
        df['date'] = df.date.dt.strftime('%Y-%m-%d')
    else:
        df['date'] = df.date.dt.strftime('%d-%m-%Y')
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
