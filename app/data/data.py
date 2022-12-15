# load libraries
from apscheduler.schedulers.background import BackgroundScheduler
import os
import pandas as pd
from datetime import datetime
from pytz import utc, timezone
import json
import requests
import tempfile
import sqlite3
import git

# define no cache headers
no_cache_headers = {
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}

# function: convert timestamp
def convert_timestamp(v):
    v = datetime.strptime(v, "%a, %d %b %Y %H:%M:%S %Z")
    v = v.replace(tzinfo = utc)
    v = v.astimezone(timezone("America/Toronto"))
    v = v.strftime("%Y-%m-%d %H:%M %Z")
    return v

# function: read CovidTimelineCanada
def load_data_ctc(temp_dir):
    
    # make data available globally
    global ctc, keys_pt, keys_hr, version_ctc

    # define data
    files_hr = [
        ("hr", "cases_hr.csv", "cases_hr"),
        ("hr", "deaths_hr.csv", "deaths_hr")
    ]
    files_pt_can = [
        ("pt", "cases_pt.csv", "cases_pt"),
        ("pt", "deaths_pt.csv", "deaths_pt"),
        ("pt", "hospitalizations_pt.csv", "hospitalizations_pt"),
        ("pt", "icu_pt.csv", "icu_pt"),
        ("pt", "tests_completed_pt.csv", "tests_completed_pt"),
        ("pt", "vaccine_coverage_dose_1_pt.csv", "vaccine_coverage_dose_1_pt"),
        ("pt", "vaccine_coverage_dose_2_pt.csv", "vaccine_coverage_dose_2_pt"),
        ("pt", "vaccine_coverage_dose_3_pt.csv", "vaccine_coverage_dose_3_pt"),
        ("pt", "vaccine_coverage_dose_4_pt.csv", "vaccine_coverage_dose_4_pt"),
        ("pt", "vaccine_administration_total_doses_pt.csv", "vaccine_administration_total_doses_pt"),
        ("pt", "vaccine_administration_dose_1_pt.csv", "vaccine_administration_dose_1_pt"),
        ("pt", "vaccine_administration_dose_2_pt.csv", "vaccine_administration_dose_2_pt"),
        ("pt", "vaccine_administration_dose_3_pt.csv", "vaccine_administration_dose_3_pt"),
        ("pt", "vaccine_administration_dose_4_pt.csv", "vaccine_administration_dose_4_pt"),
        ("can", "cases_can.csv", "cases_can"),
        ("can", "deaths_can.csv", "deaths_can"),
        ("can", "hospitalizations_can.csv", "hospitalizations_can"),
        ("can", "icu_can.csv", "icu_can"),
        ("can", "tests_completed_can.csv", "tests_completed_can"),
        ("can", "vaccine_coverage_dose_1_can.csv", "vaccine_coverage_dose_1_can"),
        ("can", "vaccine_coverage_dose_2_can.csv", "vaccine_coverage_dose_2_can"),
        ("can", "vaccine_coverage_dose_3_can.csv", "vaccine_coverage_dose_3_can"),
        ("can", "vaccine_coverage_dose_4_can.csv", "vaccine_coverage_dose_4_can"),
        ("can", "vaccine_administration_total_doses_can.csv", "vaccine_administration_total_doses_can"),
        ("can", "vaccine_administration_dose_1_can.csv", "vaccine_administration_dose_1_can"),
        ("can", "vaccine_administration_dose_2_can.csv", "vaccine_administration_dose_2_can"),
        ("can", "vaccine_administration_dose_3_can.csv", "vaccine_administration_dose_3_can"),
        ("can", "vaccine_administration_dose_4_can.csv", "vaccine_administration_dose_4_can")
    ]

    # load data
    root = os.path.join(temp_dir, "CovidTimelineCanada")
    ctc = {}
    for i in range(len(files_hr)):
        ctc[files_hr[i][2]] = pd.read_csv(
            os.path.join(root, "data", files_hr[i][0], files_hr[i][1]),
            dtype = {"region": str, "sub_region_1": str}, parse_dates = ["date"])
    for i in range(len(files_pt_can)):
        ctc[files_pt_can[i][2]] = pd.read_csv(
            os.path.join(root, "data", files_pt_can[i][0], files_pt_can[i][1]),
            dtype = {"region": str}, parse_dates = ["date"])
    ctc["pt"] = pd.read_csv(os.path.join(root, "geo", "pt.csv"), dtype = str)
    ctc["hr"] = pd.read_csv(os.path.join(root, "geo", "hr.csv"), dtype = str)
    keys_pt = set(ctc["pt"]["region"].tolist())
    keys_hr = set(ctc["hr"]["hruid"].to_list() + ['9999'])
    version_ctc = pd.read_csv(os.path.join(root, "update_time.txt"), sep="\t", header=None).head().values[0][0]
    
# function: update CovidTimelineCanada data
def update_data_ctc(temp_dir):
    
    # make data available globally
    global ctc, keys_pt, keys_hr, version_ctc
    
    # git pull
    print("Pulling from CovidTimelineCanada repository...")
    repo = git.Git(os.path.join(temp_dir, "CovidTimelineCanada"))
    repo.pull("origin", "main")
    
    # read in updated data if version has changed
    print("Checking if CovidTimelineCanada data have changed...")
    root = os.path.join(temp_dir, "CovidTimelineCanada")
    if (pd.read_csv(os.path.join(root, "update_time.txt"), sep="\t", header=None).head().values[0][0] != version_ctc):
        print("CovidTimelineCanada files have changed. Reloading files...")
        load_data_ctc(temp_dir)
        print("CovidTimelineCanada data have been updated.")
    else:
        print("CovidTimelineCanada data have not changed. No action required.")

# function: read datasets.json
def load_data_datasets():
    
    # make data available globally
    global datasets, version_datasets
    
    # load datasets.json into a single dictionary
    print("Downloading datasets.json...")
    file = requests.get("https://raw.githubusercontent.com/ccodwg/Covid19CanadaArchive/master/datasets.json", headers = no_cache_headers)
    file = json.loads(file.content)
    ds = {}
    for a in file:
        for d in file[a]:
            for i in range(len(file[a][d])):
                    ds[file[a][d][i]["uuid"]] = file[a][d][i]
    datasets = ds
    version_datasets = requests.get("https://api.github.com/repos/ccodwg/Covid19CanadaArchive/commits?path=datasets.json", headers = no_cache_headers).headers["last-modified"]
    version_datasets = convert_timestamp(version_datasets)
    print("File datasets.json ready.")

# function: update datasets.json
def update_data_datasets():
    
    ## make data available globally
    global datasets, version_datasets
    
    # read in updated datasets.json if version has changed
    print("Checking if datasets.json has changed...")
    version_datasets_new = requests.get("https://api.github.com/repos/ccodwg/Covid19CanadaArchive/commits?path=datasets.json", headers = no_cache_headers).headers["last-modified"]
    version_datasets_new = convert_timestamp(version_datasets_new)
    if (version_datasets_new != version_datasets):
        print("File datasets.json has changed. Reloading index...")
        load_data_datasets()
        print("File datasets.json has been updated.")
    else:
        print("File datasets.json has not changed. No action required.")

# function: read archive file index
def load_data_archive_index():
    
    ## make data available globally
    global archive, version_archive_index, datasets
    
    ## load file index into "archive" dictionary
    print("Downloading archive file index...")
    ## download database as temporary file
    temp = tempfile.NamedTemporaryFile()
    with open(temp.name, "wb") as f:
        f.write(requests.get("https://data.opencovid.ca/archive/index.db", headers = no_cache_headers).content)
    ## read database into DataFrame
    archive["index"] = pd.read_sql("SELECT * FROM archive", sqlite3.connect(temp.name))
    ## download latest version of datasets.json
    load_data_datasets()
    ## extract uuid, dir_parent, dir_file from datasets.json for each dataset
    datasets_meta = pd.DataFrame.from_dict(datasets, orient = "index")[["uuid", "dir_parent", "dir_file"]]
    ## merge dataset metadata with archive index
    archive["index"] = pd.merge(archive["index"], datasets_meta, on = "uuid", how = "left")
    ## for each non-duplicate file, calculate file URL
    archive_unique = archive["index"][archive["index"]["file_duplicate"] == 0]
    # temporarily disable pandas chained assignment warning
    # otherwise, a sprurious warning will be printed
    pd_option = pd.get_option('chained_assignment') # save previous value
    pd.set_option('chained_assignment', None) # disable
    archive_unique["file_url"] = "https://data.opencovid.ca/archive/" + archive_unique["dir_parent"] + "/" + archive_unique["dir_file"] + "/" + archive_unique["file_name"]
    # reset pandas chained assignment warning option
    pd.set_option('chained_assignment', pd_option) # reset
    ## join file URL back to archive index
    archive["index"] = pd.merge(archive["index"], archive_unique[["uuid", "file_md5", "file_size", "file_url"]], on = ["uuid", "file_md5", "file_size"], how = "left")
    ## calculate whether file is the final unique file for a given UUID and date
    archive["index"]["file_final_for_date"] = archive["index"].groupby(["uuid", "file_date"])["file_timestamp"].transform(max) == archive["index"]["file_timestamp"]
    archive["index"]["file_final_for_date"] = archive["index"]["file_final_for_date"].astype(int)
    ## reorder columns
    archive["index"] = archive["index"][[
        "uuid", "dir_parent", "dir_file", "file_name", "file_timestamp", "file_date", "file_duplicate", "file_final_for_date", "file_md5", "file_size", "file_url"]]
    ## update version
    version_archive_index = requests.get("https://data.opencovid.ca/archive/update_time.txt", headers = no_cache_headers).content
    print("File index ready.")
    
## function: update archive file index
def update_data_archive_index():
    
    ## make data available globally
    global archive, version_archive_index
    
    ## read in updated file index if version has changed
    print("Checking if archive file index has changed...")
    version_archive_index_new = requests.get("https://data.opencovid.ca/archive/update_time.txt", headers = no_cache_headers).content
    if (version_archive_index_new != version_archive_index):
        print("Archive file index has changed. Reloading index...")
        load_data_archive_index()
        print("Archive file index has been updated.")
    else:
        print("Archive file index has not changed. No action required.")
    
# initial data pulls

## define temporary directory
temp_dir = tempfile.TemporaryDirectory().name

## CovidTimelineCanada
print("Cloning from CovidTimelineCanada repository...")
git.Repo.clone_from(
    "https://github.com/ccodwg/CovidTimelineCanada.git",
    os.path.join(temp_dir, "CovidTimelineCanada"), branch = "main", depth = 1)
print("Clone complete. Reading in data from CovidTimelineCanada...")
load_data_ctc(temp_dir)
print("CovidTimelineCanada data are ready.")

## datasets.json
global datasets, version_datasets
datasets = {}
version_datasets = requests.get("https://api.github.com/repos/ccodwg/Covid19CanadaArchive/commits?path=datasets.json", headers = no_cache_headers).headers["last-modified"]
load_data_datasets()

## archive file index
global archive, version_archive_index
archive = {}
version_archive_index = requests.get("https://data.opencovid.ca/archive/update_time.txt", headers = no_cache_headers).content
load_data_archive_index()

# schedule data updates
scheduler = BackgroundScheduler()
job_ctc = scheduler.add_job(update_data_ctc, "interval", minutes=5, args=[temp_dir])
job_datasets = scheduler.add_job(update_data_datasets, "interval", minutes=5)
job_archive_index = scheduler.add_job(update_data_archive_index, "interval", minutes=5)
scheduler.start()
