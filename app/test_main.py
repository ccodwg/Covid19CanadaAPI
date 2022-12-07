from fastapi.testclient import TestClient

from .main import app

client = TestClient(app)

def test_timeseries():
    resp_1 = client.get("/timeseries?stat=cases&geo=hr&loc=3595&date=2022-01-01&hr_names=short&version=true")
    assert resp_1.status_code == 200
    assert resp_1.json().keys() == {"data", "version"}
    assert resp_1.json()["data"].keys() == {"cases"}
    assert len(resp_1.json()["data"]["cases"]) == 1
    assert resp_1.json()["data"]["cases"][0].keys() == {
        "name", "region", "sub_region_1", "date", "value", "value_daily"}
    assert resp_1.json()["data"]["cases"][0]["name"] == "cases"
    assert resp_1.json()["data"]["cases"][0]["region"] == "ON"
    assert resp_1.json()["data"]["cases"][0]["sub_region_1"] == "Toronto"
    assert resp_1.json()["data"]["cases"][0]["date"] == "2022-01-01"
    assert isinstance(resp_1.json()["data"]["cases"][0]["value"], int)
    assert isinstance(resp_1.json()["data"]["cases"][0]["value_daily"], int)
    resp_2 = client.get("/timeseries?loc=UNKNOWN")
    assert resp_2.status_code == 400
    assert resp_2.json() == {"detail": "Invalid loc"}