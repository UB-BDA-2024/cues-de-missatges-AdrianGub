from fastapi.testclient import TestClient
import pytest
from app.main import app

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_db():
    from app.database import SessionLocal, engine
    from app.sensors import models
    models.Base.metadata.drop_all(bind=engine)
    models.Base.metadata.create_all(bind=engine)
    
def test_create_sensor():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor 1", "latitude": 1.0, "longitude": 1.0})
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 1
    assert json["name"] == "Sensor 1"

def test_get_sensor_by_id():
    """We can get a sensor by its id"""
    response = client.get("/sensors/1")
    assert response.status_code == 200
    
def test_get_all_sensors():
    """🙋🏽‍♀️ First assignment: make this test pass, it should return a list with the sensor created in the previous test"""
    response = client.get("/sensors")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 1
    assert json[0]["id"] == 1
    assert json[0]["name"] == "Sensor 1"

def test_post_sensor_data():
    """Sets the data for a sensor"""
    response = client.post("/sensors/1/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 1
    assert json["name"] == "Sensor 1"
    assert json["temperature"] == 1.0
    assert json["humidity"] == 1.0
    assert json["battery_level"] == 1.0
    assert json["last_seen"] == "2020-01-01T00:00:00"
    
def test_delete_sensor():
    """Deletes a sensor"""
    response = client.delete("/sensors/1")
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 1
    assert json["name"] == "Sensor 1"
    assert json["temperature"] == 1.0
    assert json["humidity"] == 1.0
    assert json["battery_level"] == 1.0