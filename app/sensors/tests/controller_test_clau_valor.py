from fastapi.testclient import TestClient
import pytest
from app.main import app
from app.redis_client import RedisClient

client = TestClient(app)



@pytest.fixture(scope="session", autouse=True)
def clear_db():
     from app.database import SessionLocal, engine
     from app.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)

@pytest.fixture(scope="session", autouse=True)   

def create_sensor():
     """A sensor can be properly created"""
     response = client.post("/sensors", json={"name": "Sensor 1", "latitude": 1.0, "longitude": 1.0})
     assert response.status_code == 200
     json = response.json()
     assert json["id"] == 1
     assert json["name"] == "Sensor 1"

def test_redis_connection():
    redis_client = RedisClient(host="redis")
    assert redis_client.ping()
    redis_client.close()

def test_post_sensor_data():
    response = client.post("/sensors/1/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

def test_get_sensor_data():
    """We can get a sensor by its id"""
    response = client.get("/sensors/1/data")
    assert response.status_code == 200
    json = response.json()
    assert json["id"] == 1
    assert json["name"] == "Sensor 1"
    assert json["temperature"] == 1.0
    assert json["humidity"] == 1.0
    assert json["battery_level"] == 1.0
    assert json["last_seen"] == "2020-01-01T00:00:00.000Z"
    
def test_post_sensor_data_not_exists():
    response = client.post("/sensors/2/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 404
    assert "Sensor not found" in response.text

def test_get_sensor_data_not_exists():
    response = client.get("/sensors/2/data")
    assert response.status_code == 404
    assert "Sensor not found" in response.text