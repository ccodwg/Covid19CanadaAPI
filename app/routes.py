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
    return "Hello, World!"

@app.route('/summary')
def summary():
    loc = request.args.get('loc')
    date = request.args.get('date')
    version = request.args.get('version')
    return "Hello, World!"

@app.route('/version')
def version():
    return "Hello, World!"
