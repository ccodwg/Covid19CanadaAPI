# API for the COVID-19 Open Data Working Group dataset

This repository contains the code underlying the API for the [COVID-19 Open Data Working Group](https://opencovid.ca/) [dataset](https://github.com/ccodwg/CovidTimelineCanada) available at the following URL: [https://api.opencovid.ca/](http://api.opencovid.ca/)

Full API documentation is available at the following URL: [https://opencovid.ca/api/](https://opencovid.ca/api/)

## Running the API

Run the API locally using the following command from the root directory:

```
uvicorn app.main:app --reload
```
