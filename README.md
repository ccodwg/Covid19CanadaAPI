# API for the COVID-19 Open Data Working Group dataset

This repository contains the code underlying the API for the [COVID-19 Open Data Working Group](https://opencovid.ca/) [dataset](https://github.com/ccodwg/CovidTimelineCanada). The API has been retired as of February 1, 2024, but it was previously available at the following URL: [https://api.opencovid.ca/](https://api.opencovid.ca/).

## Running the API locally

Run the API locally using the following command from the root directory:

```
uvicorn app.main:app --reload
```

To run tests, simply call `pytest` from the root directory:

```
python -m pytest
```
