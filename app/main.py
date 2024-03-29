from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse, StreamingResponse
from datetime import date, datetime
import pytz
import re
import io
import csv
import pandas as pd
from app.data import data

tags_metadata = [
    {"name": "CovidTimelineCanada", "description": "New Canadian COVID-19 dataset from the COVID-19 Canada Open Data Working Group (see https://github.com/ccodwg/CovidTimelineCanada)"},
    {"name": "Archive of Canadian COVID-19 Data", "description": "Canadian COVID-19 Data Archive (see https://github.com/ccodwg/Covid19CanadaArchive)"},
    {"name": "Version", "description": "Update times corresponding to the data available from each route"}
]

app = FastAPI(
    title = "COVID-19 Canada Open Data Working Group API",
    version = "0.2.0",
    docs_url = "/",
    openapi_tags = tags_metadata,
    recdoc_url = None
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# define routes
routes = ["timeseries", "summary", "datasets", "archive", "version"]

# define common functions

## no results for query text
def query_no_results():
    return "No records match your query. Please try again with different settings."

## UUID not found text
def uuid_not_found():
    return "UUID not found. Please try again with a valid UUID."

## get current datetime in America/Toronto
def get_datetime():
    return datetime.now(pytz.timezone("America/Toronto"))

# define constants

## metrics available for each geo level
stats_hr = ["cases", "deaths"]
stats_pt = [
    "cases",
    "deaths",
    "hospitalizations",
    "icu",
    "tests_completed",
    "vaccine_coverage_dose_1",
    "vaccine_coverage_dose_2",
    "vaccine_coverage_dose_3",
    "vaccine_coverage_dose_4",
    "vaccine_administration_total_doses",
    "vaccine_administration_dose_1",
    "vaccine_administration_dose_2",
    "vaccine_administration_dose_3",
    "vaccine_administration_dose_4"
    ]
stats_can = [
    "cases",
    "deaths",
    "hospitalizations",
    "icu",
    "tests_completed",
    "vaccine_coverage_dose_1",
    "vaccine_coverage_dose_2",
    "vaccine_coverage_dose_3",
    "vaccine_coverage_dose_4",
    "vaccine_administration_total_doses",
    "vaccine_administration_dose_1",
    "vaccine_administration_dose_2",
    "vaccine_administration_dose_3",
    "vaccine_administration_dose_4"
    ]

# define common query parameters
query_geo = Query(
    "pt",
    description = "Geographic level of data to return. Can be 'pt' (province/territory, the default), 'hr' (health region), or 'can' (Canada). Note that not all metrics are available at all levels.",
    enum = ["can", "pt", "hr"])
query_loc = Query(
    None,
    description = (
        "Specific geography to filter to. "
        "Can be one or more two-letter province/territory codes (https://github.com/ccodwg/CovidTimelineCanada/blob/main/geo/pt.csv) and/or health region unique identifiers (https://github.com/ccodwg/CovidTimelineCanada/blob/main/geo/hr.csv; 9999 is 'Unknown'). "
        "If not provided, all available data will be returned."))
query_date = Query(
    None,
    description = "Filter to include data only from this date (YYYY-MM-DD). Alternatively, a positive integer will return the latest x days of data and a negative integer will return the earliest x days of data. This is calculated separately for each geographic group, so the dates returned may differ between provinces/territories.")
query_after = Query(
    None,
    description = "Filter to include data only from after and including this date (YYYY-MM-DD).")
query_before = Query(
    None,
    description = "Filter to include data only from before and including this date (YYYY-MM-DD).")
query_version = Query(
    True,
    description = "Return update date and time of the dataset? Default: true.")
query_pt_names = Query(
    "short",
    description = "How should provinces be named? Can be 'short' (two-letter province/territory codes), 'canonical' (full official names), 'pruid' (unique identifiers) or 'ccodwg' (names used in the legacy CCODWG dataset).",
    enum = ["short", "canonical", "pruid", "ccodwg"]
)
query_hr_names = Query(
    "hruid",
    description = "How should health regions be named? Can be 'hruid' (unique identifiers), 'canonical' (full official names), 'short' (short names) or 'ccodwg' (names used in the legacy CCODWG dataset).",
    enum = ["hruid", "canonical", "short", "ccodwg"]
)
query_fmt = Query(
    "json",
    description = "Format to return data in. Can be 'json' (default) or 'csv'. Note that 'csv' will not return any metadata such as the update time.",
    enum = ["json", "csv"]
)

# define common functions

# function: filter by date
def date_filter(d, date, after, before, date_invalid_returns_latest = False):
    # HACK: ensure date column is right type
    if not pd.api.types.is_datetime64_ns_dtype(d["date"]):
        d["date"] = pd.to_datetime(d["date"])
    # date: specific date or latest/earlier x days
    if date:
        # case: date is date
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            d = d[d["date"].dt.date == pd.to_datetime(date).date()]
        # case: date is positive integer
        elif date.isdigit():
            # get grouping columns
            geo_cols = [x for x in ['region', 'sub_region_1', 'sub_region_2'] if x in d.columns]
            d = d.groupby(geo_cols).tail(int(date))
        # case: date is negative integer
        elif date.startswith("-") and date[1:].isdigit():
            geo_cols = [x for x in ['region', 'sub_region_1', 'sub_region_2'] if x in d.columns]
            d = d.groupby(geo_cols).head(-int(date))
        # case: date is invalid
        else:
            if date_invalid_returns_latest:
                # if invalid value for date, return latest (default behaviour for summary route)
                geo_cols = [x for x in ['region', 'sub_region_1', 'sub_region_2'] if x in d.columns]
                d = d.groupby(geo_cols).tail(1)
            else:
                # if invalid value for date, ignore parameter (default behaviour for timeseries route)
                pass
    if after:
        d = d[d["date"].dt.date >= after]
    if before:
        d = d[d["date"].dt.date <= before]
    return d

# function: filter by location
def loc_filter(d, geo, loc):
    for i in range(len(loc)):
        loc[i] = loc[i].upper()
    if geo == "hr":
        k_pt = [x for x in loc if x in data.keys_pt]
        k_hr = [x for x in loc if x in data.keys_hr]
        if len(k_pt) == 0 and len(k_hr) == 0:
            raise HTTPException(status_code = 400, detail = "Invalid loc")
        return d[d["region"].isin(k_pt) | d["sub_region_1"].isin(k_hr)]
    elif geo == "pt":
        k_pt = [x for x in loc if x in data.keys_pt]
        if len(k_pt) == 0:
            raise HTTPException(status_code = 400, detail = "Invalid loc")
        return d[d["region"].isin(k_pt)]
    elif geo == "can":
        return d
    else:
        raise HTTPException(status_code = 400, detail = "Invalid geo")

# function: convert names (hr, pt, can)
def convert_names(d, geo, pt_names = "short", hr_names = "hruid"):
    if geo in ["hr", "pt"]:
        # convert pt names
        pt = data.ctc["pt"]
        if pt_names == "short":
            col_pt = None
        elif pt_names == "canonical":
            col_pt = "name_canonical"
        elif pt_names == "pruid":
            col_pt = "pruid"
        elif pt_names == "ccodwg":
            col_pt = "name_ccodwg"
        else:
            raise HTTPException(status_code = 400, detail = "Invalid pt_names")
        if col_pt:
            pt = pt[["region", col_pt]]
            d = d.merge(pt, on = "region", how = "left")
            d["region"] = d[col_pt]
            d = d.drop(col_pt, axis = 1)
        # convert hr names
        if geo == "hr":
            hr = data.ctc["hr"]
            if hr_names == "hruid":
                col_hr = None
            elif hr_names == "canonical":
                col_hr = "name_canonical"
                unknown = "Unknown"
            elif hr_names == "short":
                col_hr = "name_short"
                unknown = "Unknown"
            elif hr_names == "ccodwg":
                col_hr = "name_ccodwg"
                unknown = "Not Reported"
            else:
                raise HTTPException(status_code = 400, detail = "Invalid hr_names")
            if col_hr:
                hr = hr[["hruid", col_hr]]
                d = d.merge(hr, left_on = "sub_region_1", right_on = "hruid", how = "left")
                d["sub_region_1"] = d[col_hr]
                d = d.drop(["hruid", col_hr], axis = 1)
                d["sub_region_1"] = d["sub_region_1"].fillna(unknown)
    elif geo == "can":
        if pt_names == "short":
            d["region"] = "CAN"
        elif pt_names.isin(["canonical", "ccodwg"]):
            d["region"] = "Canada"
        elif pt_names == "pruid":
            d["region"] = 1
        else:
            raise HTTPException(status_code = 400, detail = "Invalid pt_names")
    # return data
    return d

# function: sort rows by region, date, lower geography
def sort_rows(d, geo, legacy):
    if geo == "hr":
        if legacy:
            d = d.sort_values(["region", "sub_region_1", "date"])
        else:
            d = d.sort_values(["region", "date", "sub_region_1"])
    elif geo == "pt":
        d = d.sort_values(["region", "date"])
    elif geo == "can":
        d = d.sort_values(["date"])
    else:
        raise HTTPException(status_code = 400, detail = "Invalid geo")
    return d

# function: format date column for response
def format_date_col(d, date_format = "%Y-%m-%d"):
    d["date"] = d["date"].dt.strftime(date_format)
    return d

# fill missing dates
def fill_dates(d, geo):
    if geo == "hr":
        cols = ["name", "region", "sub_region_1", "date"]
        stats = list(stats_hr)
    elif geo == "pt":
        cols = ["name", "region", "date"]
        stats = list(stats_pt)
    elif geo == "can":
        cols = ["name", "region", "date"]
        stats = list(stats_can)
    else:
        raise HTTPException(status_code = 400, detail = "Invalid geo")
    if "name" not in d.columns:
        cols.remove("name")
    df = d[cols[0:len(cols) - 1]].drop_duplicates()
    df_rows = len(df)
    dates = pd.date_range(
        d["date"].min(), get_datetime().date(), freq = "d").to_list()
    df = pd.concat([df] * len(dates), ignore_index = True)
    df = df.sort_values(cols[0:len(cols) - 1])
    df["date"] = dates * df_rows
    # HACK: ensure date column is correct type
    if not pd.api.types.is_datetime64_ns_dtype(d["date"]):
        d["date"] = pd.to_datetime(d["date"])
    if not pd.api.types.is_datetime64_ns_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    d = pd.merge(df, d, how = "left", on = cols)
    d = d.sort_values(by = cols)
    daily_cols = [x for x in d.columns if re.match("_daily$", x)]
    d[daily_cols] = d[daily_cols].fillna(0) # daily values should be 0
    if "value" in d.columns:
        # for timeseries route
        d["value"] = d.groupby(cols[0:len(cols) - 1])["value"].transform(lambda x: x.ffill())
    else:
        # for summary route
        d[stats] = d.groupby(cols[0:len(cols) - 1])[stats].transform(lambda x: x.ffill()) # carry cumulative values forward
    d = d.fillna(value = 0) # fill cumulative values not filled above
    return d

# function: convert to legacy format
def convert_to_legacy(d, geo, stat):
    # format date column
    d = format_date_col(d, "%d-%m-%Y")
    # drop value name column
    d = d.drop(columns = ["name"])
    d[["value", "value_daily"]] = d[["value", "value_daily"]].astype(int)
    # convert column names
    d = d.rename(columns = {"region": "province"})
    if geo == "hr":
        d = d.rename(columns = {"sub_region_1": "health_region"})
    if stat == ["cases"]:
        d = d.rename(columns = {
            "date": "date_report", "value": "cumulative_cases", "value_daily": "cases"})
    elif stat == ["deaths"]:
        d = d.rename(columns = {
            "date": "date_death_report", "value": "cumulative_deaths", "value_daily": "deaths"})
    else:
        raise HTTPException(status_code = 400, detail = "Invalid stat")
    # swap order of final two columns so value_daily appears first
    cols = list(d)
    cols[len(d.columns) - 1], cols[len(d.columns) - 2] = cols[len(d.columns) - 2], cols[len(d.columns) - 1]
    d = d.loc[:, cols]
    # return data
    return d

# function: format response as CSV
def fmt_response_csv(d, file_name):
    d = d.to_csv(
        index = False, quoting = csv.QUOTE_NONNUMERIC, float_format = "%.0f")
    response = StreamingResponse(
        io.StringIO(d),
            media_type = "text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=" + file_name + ".csv"
    return response

# define routes

@app.get("/timeseries", tags=["CovidTimelineCanada"])
async def get_timeseries(
    stat: list[str] = Query(
        ["all"],
        description = "One or more metrics to return. By default, all available metrics for the specified geographic level will be returned. Can be 'all' or one or more of the metrics listed below.\n* cases\n* deaths\n* hospitalizations\n* icu\n* tests_completed\n* vaccine_coverage_dose_1\n* vaccine_coverage_dose_2\n* vaccine_coverage_dose_3\n* vaccine_coverage_dose_4\n* vaccine_administration_total_doses\n* vaccine_administration_dose_1\n* vaccine_administration_dose_2\n* vaccine_administration_dose_3\n* vaccine_administration_dose_4",
        enum=[
            "all",
            "cases",
            "deaths",
            "hospitalizations",
            "icu",
            "tests_completed",
            "vaccine_coverage_dose_1",
            "vaccine_coverage_dose_2",
            "vaccine_coverage_dose_3",
            "vaccine_coverage_dose_4",
            "vaccine_administration_total_doses",
            "vaccine_administration_dose_1",
            "vaccine_administration_dose_2",
            "vaccine_administration_dose_3",
            "vaccine_administration_dose_4"
            ]
    ),
    geo: str = query_geo,
    loc: list[str] | None = query_loc,
    date: str | None = query_date,
    after: date | None = query_after,
    before: date | None = query_before,
    fill: bool = Query(
        False,
        description = (
            "Fill in data such that every location has an observation for every date. "
            "For example, infrequently updated locations will not have an entry for recent dates if this parameter is false. "
            "Default: false.")
    ),
    version: bool = query_version,
    pt_names: str = query_pt_names,
    hr_names: str = query_hr_names,
    legacy: bool = Query(
        False,
        description = (
            "If true, convert the output format to that used by the old COVID-19 Canada Open Data Working Group dataset. "
            "Default: false. "
            "Note that you MUST specify either cases or deaths (stat=cases or stat=deaths, not both) or else this parameter will be ignored. "
            "This parameter also forces the values of several other parameters: fill=true, pt_names=ccodwg, hr_names=ccodwg."
        )
    ),
    fmt: str = query_fmt
):

    # initialize response
    response = {"data": {}}

    # force parameters if legacy parameter is true and stat is cases or deaths
    if legacy and (stat == ["cases"] or stat == ["deaths"]):
        fill = True
        pt_names = "ccodwg"
        hr_names = "ccodwg"
    else:
        # turn off legacy parameter if requirements are not met
        legacy = False

    # get and process data
    if geo == "hr":
        if "all" in stat:
            stats = stats_hr
        else:
            stats = [x for x in stat if x in stats_hr]
            if len(stats) == 0:
                raise HTTPException(status_code = 400, detail = "Invalid stat")
        for s in stats:
            d = data.ctc[s + "_hr"]
            # fill missing dates (before loc filter so not locations are excluded)
            if fill:
                d = fill_dates(d, geo)
            # filter by location
            if loc:
                d = loc_filter(d, geo, loc)
            # filter by date
            d = date_filter(d, date, after, before)
            # convert pt, hr names
            d = convert_names(d, "hr", pt_names = pt_names, hr_names = hr_names)
            # sort rows
            d = sort_rows(d, geo, legacy)
            # format date column and convert to legacy format if requested
            if legacy:
                d = convert_to_legacy(d, geo, stat)
            else:
                # format date column
                d = format_date_col(d)
            # add data to response
            response["data"][s] = d.to_dict(orient = "records")

    elif geo == "pt":
        if "all" in stat:
            stats = stats_pt
        else:
            stats = [x for x in stat if x in stats_pt]
            if len(stats) == 0:
                raise HTTPException(status_code = 400, detail = "Invalid stat")
        for s in stats:
            d = data.ctc[s + "_pt"]
            # fill missing dates (before loc filter so not locations are excluded)
            if fill:
                d = fill_dates(d, geo)
            # filter by location
            if loc:
                d = loc_filter(d, geo, loc)
            # filter by date
            d = date_filter(d, date, after, before)
            # convert pt names
            d = convert_names(d, "pt", pt_names = pt_names)
            # sort rows
            d = sort_rows(d, geo, legacy)
            # format date column and convert to legacy format if requested
            if legacy:
                d = convert_to_legacy(d, geo, stat)
            else:
                # format date column
                d = format_date_col(d)
            # add data to response
            response["data"][s] = d.to_dict(orient = "records")
    
    elif geo == "can":
        if "all" in stat:
            stats = stats_can
        else:
            stats = [x for x in stat if x in stats_can]
            if len(stats) == 0:
                raise HTTPException(status_code = 400, detail = "Invalid stat")
        for s in stats:
            d = data.ctc[s + "_can"]
            # fill missing dates and loc filter won't do anything here
            # filter by date
            d = date_filter(d, date, after, before)
            # convert Canada names
            d = convert_names(d, "can", pt_names = pt_names)
            # sort rows
            d = sort_rows(d, geo, legacy)
            # format date column and convert to legacy format if requested
            if legacy:
                d = convert_to_legacy(d, geo, stat)
            else:
                # format date column
                d = format_date_col(d)
            # add data to response
            response["data"][s] = d.to_dict(orient = "records")
    else:
        raise HTTPException(status_code = 400, detail = "Invalid geo")

    # add version to response
    if version is True:
        response["version"] = data.version_ctc
    
    # convert response to requested format
    if fmt == "csv":
        # combine stats into single list
        out = []
        for s in response["data"]:
            for i in response["data"][s]:
                out.append(i)
        # format response as CSV
        response = fmt_response_csv(
            pd.DataFrame.from_records(out), "timeseries")
    
    # return response
    return response

@app.get("/summary", tags=["CovidTimelineCanada"])
async def get_summary(
    geo: str = query_geo,
    loc: list[str] | None = query_loc,
    date: str = Query(
        None,
        description = "Return summary for this date (YYYY-MM-DD). By default, the latest available date. Alternatively, a positive integer will return the latest x days of data and a negative integer will return the earliest x days of data."
    ),
    after: date | None = query_after,
    before: date | None = query_before,
    fill: bool = Query(
        True,
        description = (
            "Fill in data such that every location has an observation for every date."
            "For example, infrequently updated locations will not have an entry for recent dates if this parameter is false."
            "Default: true.")
    ),
    version: bool = query_version,
    pt_names: str = query_pt_names,
    hr_names: str = query_hr_names,
    fmt: str = query_fmt
):

    # if no date values specified, use latest date
    if not date and not after and not before:
        date = str(1) # latest date of data

    # initialize response
    response = {"data": {}}

    # get data and process data
    if geo == "hr":
        d = pd.DataFrame(columns = ["region", "sub_region_1", "date"])
        for s in stats_hr:
            df = data.ctc[s + "_hr"].rename(
                columns = {"value": s, "value_daily": s + "_daily"})
            df = df.drop("name", axis = 1)
            d = pd.merge(d, df, on = ["region", "sub_region_1", "date"], how = "outer")
    elif geo == "pt":
        d = pd.DataFrame(columns = ["region", "date"])
        for s in stats_pt:
            df = data.ctc[s + "_pt"].rename(
                columns = {"value": s, "value_daily": s + "_daily"})
            df = df.drop("name", axis = 1)
            d = pd.merge(d, df, on = ["region", "date"], how = "outer")
    elif geo == "can":
        d = pd.DataFrame(columns = ["region", "date"])
        for s in stats_can:
            df = data.ctc[s + "_can"].rename(
                columns = {"value": s, "value_daily": s + "_daily"})
            df = df.drop("name", axis = 1)
            d = pd.merge(d, df, on = ["region", "date"], how = "outer")
    else:
        raise HTTPException(status_code = 400, detail = "Invalid geo")
    # fill missing dates (before loc filter so not locations are excluded)
    if fill:
        d = fill_dates(d, geo)
    # filter by location
    if loc:
        d = loc_filter(d, geo, loc)
    # filter by date
    d = date_filter(d, date, after, before, date_invalid_returns_latest = True)
    # format date column
    d = format_date_col(d)
    # convert pt, hr names
    d = convert_names(d, geo, pt_names = pt_names, hr_names = hr_names)
    # fill missing values
    if not fill:
        d = d.fillna(value = "")

    # sort and summarize data by date
    if geo == "hr":
        d = d.sort_values(by = ["date", "region", "sub_region_1"])
    else:
        d = d.sort_values(by = ["date", "region"])
    response["data"] = d.to_dict(orient = "records")  

    # add version to response
    if version is True:
        response["version"] = data.version_ctc
    
    # convert response to requested format
    if fmt == "csv":
        response = fmt_response_csv(
            pd.DataFrame.fr(response["data"]), "summary")
    
    # return response
    return response

@app.get("/datasets", tags=["Archive of Canadian COVID-19 Data"])
async def get_datasets(
    uuid: str = Query(
        None,
        description = "UUID of dataset from datasets.json"),
    version: bool = query_version
):
    
    # read UUIDs
    if uuid is not None:
        uuid = uuid.split("|")
    
    # initialize response
    response = {"data": {}}

    # filter dictionary
    try:
        if uuid is None:
            response["data"] = data.datasets
        else:
            response["data"] = {k: data.datasets[k] for k in uuid}
    except Exception:
        raise HTTPException(status_code=400, detail=uuid_not_found())
    
    # add version to response
    if version is True:
        response["version"] = data.version_datasets

    # return response
    return(response)

@app.get("/archive", tags=["Archive of Canadian COVID-19 Data"])
async def get_archive(
    uuid: str = Query(
        None,
        description = "UUID of dataset from datasets.json"),
    remove_duplicates: bool = Query(
        False,
        description = "Keep only the first instance of each unique data file? Default: false."
    ),
    keep_only_final_for_date: bool = Query(
        True,
        description = "Keep only the final file downloaded on each date? Default: true. Generally, only one file per date exists, but in some cases there are multiple unique files on a single date."
    ),
    date: str | None = Query(
        None,
        description = "One of 'all', 'latest', 'first' or a date in YYYY-MM-DD format. If not specified, 'all' is used."),
    after: date | None = query_after,
    before: date | None = query_before,
    version: bool = query_version
):
    
    # read UUIDs
    if uuid is None:
        raise HTTPException(status_code=400, detail="Please specify one or more values for 'uuid', seperated by '|'.")
    else:
        uuid = uuid.split("|")
    
    # process date filters
    if date is None and after is None and before is None:
        # if no filters, return all
        date = "all"
    
    # intialize response
    response = {"data": {}}

    # get dataframe
    df = data.archive["index"]
    df = df[df["uuid"].isin(uuid)]
    
    # return 400 if no valid UUIDs
    if len(df) == 0:
        raise HTTPException(status_code=400, detail=uuid_not_found())
    
    # date filtering
    df["file_date"] = pd.to_datetime(df["file_date"])
    if date:
        # if date is defined, after and before are ignored
        if (date == "all"):
            pass
        elif (date == "latest"):
            df = df.groupby("uuid").last()
        elif (date == "first"):
            df = df.groupby("uuid").first()
        else:
            if date:
                df = df[df["file_date"].dt.date == pd.to_datetime(date, format = "%Y-%m-%d").date()]
    else:
        if after:
            df = df[df["file_date"].dt.date >= after]
        if before:
            df = df[df["file_date"].dt.date <= before]
    
    # return 400 if no results found
    if len(df) == 0:
        raise HTTPException(status_code=400, detail=query_no_results())
    
    # filter duplicates in the filtered sample
    # not the same thing as remove file_date_duplicate == 1,
    # since the first instance of a duplicate dataset may not
    # be in the filtered sample
    if (remove_duplicates is True):
        df = df.drop_duplicates(subset=["file_md5"])
    
    # filter files to keep only the last file downloaded on each date
    if (keep_only_final_for_date is True):
        df = df[df["file_final_for_date"] == 1]
    
    # format output
    df["file_date"] = df["file_date"].dt.strftime("%Y-%m-%d")
    response["data"] = df.to_dict(orient="records")
    
    # add version to response
    if version is True:
        response["version"] = data.version_archive_index

    # return response
    return response

@app.get("/version", tags=["Version"])
async def get_version(
    route: str | None = Query(
        None,
        description = "Route to get update time for. If not specified, returns all update times for all routes.",
        enum = routes[0:len(routes) - 1]),
    date_only: bool = Query(
        False,
        description = "Return only the date component of the update time? Default: false.")
):
    
    # initialize response
    response = {}

    # get versions
    response["timeseries"] = data.version_ctc
    response["summary"] = data.version_ctc
    response["datasets"] = data.version_datasets
    response["archive"] = data.version_archive_index

    # filter to specific route (if valid)
    if route:
        if route in routes[0:len(routes) - 1]:
            response = {route: response[route]}
    
    # truncate to date only
    if date_only is True:
        for k in response.keys():
            response[k] = response[k].split()[0]
    
    # return response
    return response

@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    return FileResponse("favicon.ico")
