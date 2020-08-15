from app import app
from flask import request
import pandas as pd

province = {"AB":"Alberta","BC":"BC","MB":"Manitoba","NB":"New Brunswick",
"NL":"NL","NT":"NWT","NS":"Nova Scotia","NU":"Nunavut","ON":"Ontario",
"PE":"PEI","QC":"Quebec","SK":"Saskatchewan","YT":"Yukon"}
health_region = {"4832":"Calgary","4833":"Central","4834":"Edmonton",
"4835":"North","4831":"South","591":"Fraser","592":"Interior",
"593":"Island","594":"Northern","595":"Vancouver Coastal",
"4603":"Interlake-Eastern","4604":"Northern","4602":"Prairie Mountain",
"4605":"Southern Health","4601":"Winnipeg","1301":"Zone 1 (Moncton area)",
"1302":"Zone 2 (Saint John area)","1303":"Zone 3 (Fredericton area)",
"1304":"Zone 4 (Edmundston area)","1305":"Zone 5 (Campbellton area)",
"1306":"Zone 6 (Bathurst area)","1307":"Zone 7 (Miramichi area)",
"1012":"Central","1011":"Eastern","1014":"Labrador-Grenfell",
"1013":"Western","1201":"Zone 1 - Western","1202":"Zone 2 - Northern",
"1203":"Zone 3 - Eastern","1204":"Zone 4 - Central","6201":"Nunavut",
"6101":"NWT","3526":"Algoma","3527":"Brant","3540":"Chatham-Kent",
"3530":"Durham","3558":"Eastern","3533":"Grey Bruce",
"3534":"Haldimand-Norfolk","3535":"Haliburton Kawartha Pineridge",
"3536":"Halton","3537":"Hamilton","3538":"Hastings Prince Edward",
"3539":"Huron Perth","3541":"Kingston Frontenac Lennox & Addington",
"3542":"Lambton","3543":"Leeds Grenville and Lanark",
"3544":"Middlesex-London","3546":"Niagara","3547":"North Bay Parry Sound",
"3549":"Northwestern","3551":"Ottawa","3553":"Peel","3555":"Peterborough",
"3556":"Porcupine","3557":"Renfrew","3560":"Simcoe Muskoka",
"3575":"Southwestern","3561":"Sudbury","3562":"Thunder Bay",
"3563":"Timiskaming","3595":"Toronto","3565":"Waterloo",
"3566":"Wellington Dufferin Guelph","3568":"Windsor-Essex","3570":"York",
"1100":"Prince Edward Island","2408":"Abitibi-Témiscamingue",
"2401":"Bas-Saint-Laurent","2403":"Capitale-Nationale",
"2412":"Chaudière-Appalaches","2409":"Côte-Nord","2405":"Estrie",
"2411":"Gaspésie-Îles-de-la-Madeleine","2414":"Lanaudière",
"2415":"Laurentides","2413":"Laval","2404":"Mauricie",
"2416":"Montérégie","2406":"Montréal","2410":"Nord-du-Québec",
"2417":"Nunavik","2407":"Outaouais","2402":"Saguenay",
"2418":"Terres-Cries-de-la-Baie-James","473":"Central",
"471":"Far North","472":"North","475":"Regina","474":"Saskatoon",
"476":"South","6001":"Yukon"}

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"

@app.route('/individual')
def individual():
    stat = request.args.get('stat')
    loc = request.args.get('loc')
    date = request.args.get('date')
    after = request.args.get('after')
    before = request.args.get('before')
    version = request.args.get('version')
    dfs = []
    response = {}
    if stat == 'cases':
        cases = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv",dayfirst=True)
        dfs.append(cases)
    elif stat =='mortality':
        mortality = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv",dayfirst=True)
        dfs.append(mortality)
    else:
        cases = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv")
        mortality = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv")
        dfs.append(cases)
        dfs.append(mortality)
    for df in dfs:

        df = df.fillna("NULL")

        if loc:
            if loc in province.keys():
                df = df.loc[df.province == province[loc]]
            elif loc in health_region.keys():
                df = df.loc[df.health_region == health_region[loc]]

        if date:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report == date]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report == date]

        if after:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report >= after]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report >= after]

        if before:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report <= before]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report <= before]

        if version:
            if version=='true':
                version = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/update_time.txt", sep="\t", header=None)
                response["version"] = version.head().values[0][0]

        if 'date_report' in df.columns:
            response["cases"] = df.to_dict(orient='records')

        if 'date_death_report' in df.columns:
            response["mortality"] = df.to_dict(orient='records')

    return response



@app.route('/timeseries')
def timeseries():
    stat = request.args.get('stat')
    loc = request.args.get('loc')
    date = request.args.get('date')
    after = request.args.get('after')
    before = request.args.get('before')
    version = request.args.get('version')
    dfs = []
    response = {}
    if stat == 'cases':
        cases_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/cases_timeseries_canada.csv",dayfirst=True)
        cases_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv",dayfirst=True)
        cases_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/cases_timeseries_hr.csv",dayfirst=True)
        cases_hr["province"] = cases_hr["health_region"]
        cases_hr.drop("health_region", axis=1,inplace=True)
        df = pd.concat([cases_can,cases_prov,cases_hr])
        df['date_report'] = pd.to_datetime(df_final['date_report'],dayfirst=True)
        dfs.append(df)
    elif stat =='mortality':
        mortality_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/mortality_timeseries_canada.csv",dayfirst=True)
        mortality_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/mortality_timeseries_prov.csv",dayfirst=True)
        mortality_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/mortality_timeseries_hr.csv",dayfirst=True)
        mortality_hr["province"] = mortality_hr["health_region"]
        mortality_hr.drop("health_region", axis=1,inplace=True)
        df = pd.concat([mortality_can,mortality_prov,mortality_hr])
        df['date_death_report'] = pd.to_datetime(df_final['date_death_report'],dayfirst=True)
        dfs.append(df)
    elif stat =='recovered':
        recovered_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/recovered_timeseries_canada.csv",dayfirst=True)
        recovered_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/recovered_timeseries_prov.csv",dayfirst=True)
        df = pd.concat([recovered_can,recovered_prov])
        df['date_recovered'] = pd.to_datetime(df_final['date_recovered'],dayfirst=True)
        dfs.append(df)
    elif stat =='testing':
        testing_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/testing_timeseries_canada.csv",dayfirst=True)
        testing_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/testing_timeseries_prov.csv",dayfirst=True)
        df = pd.concat([testing_can,testing_prov])
        df['date_testing'] = pd.to_datetime(df_final['date_testing'],dayfirst=True)
        dfs.append(df)
    elif stat =='active':
        active_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/active_timeseries_canada.csv",dayfirst=True)
        active_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/active_timeseries_prov.csv",dayfirst=True)
        df = pd.concat([active_can,active_prov])
        df['date_active'] = pd.to_datetime(df_final['date_active'],dayfirst=True)
        dfs.append(df)
    else:
        cases_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/cases_timeseries_canada.csv",dayfirst=True)
        cases_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv",dayfirst=True)
        cases_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/cases_timeseries_hr.csv",dayfirst=True)
        cases_hr["province"] = cases_hr["health_region"]
        cases_hr.drop("health_region", axis=1,inplace=True)
        df = pd.concat([cases_can,cases_prov,cases_hr])
        df['date_report'] = pd.to_datetime(df_final['date_report'],dayfirst=True)
        dfs.append(df)

        mortality_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/mortality_timeseries_canada.csv",dayfirst=True)
        mortality_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/mortality_timeseries_prov.csv",dayfirst=True)
        mortality_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/mortality_timeseries_hr.csv",dayfirst=True)
        mortality_hr["province"] = mortality_hr["health_region"]
        mortality_hr.drop("health_region", axis=1,inplace=True)
        df = pd.concat([mortality_can,mortality_prov,mortality_hr])
        df['date_death_report'] = pd.to_datetime(df_final['date_death_report'],dayfirst=True)
        dfs.append(df)

        recovered_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/recovered_timeseries_canada.csv",dayfirst=True)
        recovered_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/recovered_timeseries_prov.csv",dayfirst=True)
        df = pd.concat([recovered_can,recovered_prov])
        df['date_recovered'] = pd.to_datetime(df_final['date_recovered'],dayfirst=True)
        dfs.append(df)

        testing_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/testing_timeseries_canada.csv",dayfirst=True)
        testing_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/testing_timeseries_prov.csv",dayfirst=True)
        df = pd.concat([testing_can,testing_prov])
        df['date_testing'] = pd.to_datetime(df_final['date_testing'],dayfirst=True)
        dfs.append(df)

        active_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/active_timeseries_canada.csv",dayfirst=True)
        active_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/active_timeseries_prov.csv",dayfirst=True)
        df = pd.concat([active_can,active_prov])
        df['date_active'] = pd.to_datetime(df['date_active'],dayfirst=True)
        dfs.append(df)

    for df in dfs:

        df = df.fillna("NULL")

        if loc:
            if loc == 'canada':
                df = df.loc[df.province == 'Canada']
            elif loc == 'prov':
                df = df.loc[df.province.isin(province.values())]
            elif loc == 'hr':
                df = df.loc[df.province == health_region[loc]]
            elif loc in province.keys():
                df = df.loc[df.province == province[loc]]
            elif loc in health_region.keys():
                df = df.loc[df.province == health_region[loc]]

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



        if after:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report >= after]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report >= after]
            if 'date_active' in df.columns:
                df = df.loc[df.date_active >= date]
            if 'date_recovered' in df.columns:
                df = df.loc[df.date_recovered >= date]
            if 'date_testing' in df.columns:
                df = df.loc[df.date_testing >= date]

        if before:
            if 'date_report' in df.columns:
                df = df.loc[df.date_report <= before]
            if 'date_death_report' in df.columns:
                df = df.loc[df.date_death_report <= before]
            if 'date_active' in df.columns:
                df = df.loc[df.date_active <= date]
            if 'date_recovered' in df.columns:
                df = df.loc[df.date_recovered <= date]
            if 'date_testing' in df.columns:
                df = df.loc[df.date_testing <= date]

        if version:
            if version=='true':
                version = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/update_time.txt", sep="\t", header=None)
                response["version"] = version.head().values[0][0]

        if 'date_report' in df.columns:
            response["cases"] = df.to_dict(orient='records')

        if 'date_death_report' in df.columns:
            response["mortality"] = df.to_dict(orient='records')

        if 'date_active' in df.columns:
            response["active"] = df.to_dict(orient='records')

        if 'date_recovered' in df.columns:
            response["recovered"] = df.to_dict(orient='records')

        if 'date_testing' in df.columns:
            response["testing"] = df.to_dict(orient='records')

    return response

@app.route('/summary')
def summary():
    loc = request.args.get('loc')
    date = request.args.get('date')
    after = request.args.get('after')
    before = request.args.get('before')
    version = request.args.get('version')
    dfs = []
    response = {}

    cases_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/cases_timeseries_canada.csv",dayfirst=True)
    cases_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv",dayfirst=True)
    cases_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/cases_timeseries_hr.csv",dayfirst=True)
    cases_hr["province"] = cases_hr["health_region"]
    cases_hr.drop("health_region", axis=1,inplace=True)
    df_cases = pd.concat([cases_can,cases_prov,cases_hr])
    df_cases.rename(columns={"date_report":"date"},inplace=True)

    mortality_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/mortality_timeseries_canada.csv",dayfirst=True)
    mortality_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/mortality_timeseries_prov.csv",dayfirst=True)
    mortality_hr = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_hr/mortality_timeseries_hr.csv",dayfirst=True)
    mortality_hr["province"] = mortality_hr["health_region"]
    mortality_hr.drop("health_region", axis=1,inplace=True)
    df_mortality = pd.concat([mortality_can,mortality_prov,mortality_hr])
    df_mortality.rename(columns={"date_death_report":"date"},inplace=True)

    recovered_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/recovered_timeseries_canada.csv",dayfirst=True)
    recovered_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/recovered_timeseries_prov.csv",dayfirst=True)
    df_recovered = pd.concat([recovered_can,recovered_prov])
    df_recovered.rename(columns={"date_recovered":"date"},inplace=True)

    testing_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/testing_timeseries_canada.csv",dayfirst=True)
    testing_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/testing_timeseries_prov.csv",dayfirst=True)
    df_testing = pd.concat([testing_can,testing_prov])
    df_testing.rename(columns={"date_testing":"date"},inplace=True)

    active_can = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_canada/active_timeseries_canada.csv",dayfirst=True)
    active_prov = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/active_timeseries_prov.csv",dayfirst=True)
    df_active = pd.concat([active_can,active_prov])
    df_active.rename(columns={"date_active":"date"},inplace=True)
    df_active = df_active[['province', 'date', 'active_cases','active_cases_change']]

    df_one = pd.merge(df_cases,df_mortality,on=['province','date'], how='outer')
    df_two = pd.merge(df_one,df_recovered,on=['province','date'], how='outer')
    df_three = pd.merge(df_two,df_testing,on=['province','date'], how='outer')
    df_final = pd.merge(df_three,df_active,on=['province','date'], how='outer')
    df_final['date'] = pd.to_datetime(df_final['date'],dayfirst=True)
    df = df_final.fillna("NULL")

    if loc:
        if loc == 'canada':
            df = df.loc[df.province == 'Canada']
        elif loc == 'prov':
            df = df.loc[df.province.isin(province.values())]
        elif loc == 'hr':
            df = df.loc[df.province == health_region[loc]]
        elif loc in province.keys():
            df = df.loc[df.province == province[loc]]
        elif loc in health_region.keys():
            df = df.loc[df.province == health_region[loc]]

    if date:
        df = df.loc[df.date == date]

    if after:
        df = df.loc[df.date >= after]

    if before:
        df = df.loc[df.date <= before]

    if version:
        if version=='true':
            version = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/update_time.txt", sep="\t", header=None)
            response["version"] = version.head().values[0][0]

    response["summary"] = df.to_dict(orient='records')

    return response

@app.route('/version')
def version():
    response = {}
    version = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/update_time.txt", sep="\t", header=None)
    response["version"] = version.head().values[0][0]
    return version
