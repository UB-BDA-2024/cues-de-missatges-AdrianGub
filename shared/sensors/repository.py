from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

import json

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale
from shared.elasticsearch_client import ElasticsearchClient
from shared.cassandra_client import CassandraClient


# Get sensor by id
def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

# Get sensor by name
def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

# Get all sensors
def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

# Create a sensor
def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient, es: ElasticsearchClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    
    # Insert sensor info
    mongodb_client.getDatabase("sensors")
    mongo_info = sensor.dict()
    mongodb_client.getCollection("sensors")
    mongodb_client.insert(mongo_info)
    sensor_info = {
        "id": db_sensor.id,
        "name": db_sensor.name,
        "latitude": mongo_info["latitude"],
        "longitude": mongo_info["longitude"],
        "type": mongo_info["type"],
        "mac_address": mongo_info["mac_address"],
        "manufacturer": mongo_info["manufacturer"],
        "model": mongo_info["model"],
        "serie_number": mongo_info["serie_number"],
        "firmware_version": mongo_info["firmware_version"],
        "description": mongo_info["description"]
    }
    
    # Index sensors
    index = "sensors"
    created = es.create_index(index)
    
    # Define the mapping for the index
    if created:
        mapping = {
            'properties': {
                'name': {'type': 'text'},
                'description': {'type': 'text'},
                'type': {'type': 'text'}
            }
        }
        
        es.create_mapping(index, mapping)
    
    # Create document on ElasticNet
    es_doc = {
        'name': sensor_info["name"],
        'description': sensor_info["description"],
        'type': sensor_info["type"]
    }
    
    es.index_document(index, es_doc)
    return sensor_info

# Record sensor data
def record_data(cassandra: CassandraClient, redis: RedisClient, sensor_id: int, data: schemas.SensorData, timescale: Timescale) -> schemas.Sensor:
    # Save dynamic data in Redis
    redis.set(sensor_id, data.json())
    
    data_insert = data.dict()
    data_insert['sensor_id'] = sensor_id
    
    # Insert data
    ts = timescale.insert_query('sensor_data', data_insert)
    
    # Execute the insert query
    tse = timescale.execute(ts)
    
    # Refresh materialized views
    timescale.execute("REFRESH MATERIALIZED VIEW table_hour;")
    timescale.execute("REFRESH MATERIALIZED VIEW table_day;")
    timescale.execute("REFRESH MATERIALIZED VIEW table_month;")
    timescale.execute("REFRESH MATERIALIZED VIEW table_week;")
    timescale.execute("REFRESH MATERIALIZED VIEW table_year;")
    battery_level = data_insert["battery_level"]
    last_seen = data_insert["last_seen"]
    if data_insert["temperature"]:
        temperature = data_insert["temperature"]
        cassandra.execute(f"INSERT INTO sensor.sensor_temperature (sensor_id, temperature, last_seen) VALUES ({sensor_id}, {temperature}, '{last_seen}');")
        cassandra.execute(f"INSERT INTO sensor.sensor_type (sensor_id, type) VALUES ({sensor_id}, 'temperature');")
        cassandra.execute(f"INSERT INTO sensor.sensor_battery (sensor_id, battery, last_seen) VALUES ({sensor_id}, {battery_level}, '{last_seen}');")

    elif data_insert["velocity"] != None:
        cassandra.execute(f"INSERT INTO sensor.sensor_type (sensor_id, type) VALUES ({sensor_id}, 'velocity');")
        cassandra.execute(f"INSERT INTO sensor.sensor_battery (sensor_id, battery, last_seen) VALUES ({sensor_id}, {battery_level}, '{last_seen}');")
    return data

# Get data
def get_data(timescale: Timescale, sensor_id: int, rfrom: str, rto: str, rbucket: str) -> schemas.Sensor:
    # Check the bucket
    if rbucket == 'year':
        table = 'table_year'
    if rbucket == 'month':
        table = 'table_month'
    if rbucket == 'week':
        table = 'table_week'
    if rbucket == 'day':
        table = 'table_day'
    if rbucket == 'hour':
        table = 'table_hour'
    
    query = f"SELECT * FROM {table} WHERE sensor_id = {sensor_id} AND bucket >= '{rfrom}' AND bucket <= '{rto}'"
    return timescale.execute(query, True)


# Delete a sensor
def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

# Get sensors near to a latitude and longitude given a radius
def get_sensors_near(mongodb_client: MongoDBClient, db: Session, redis_client: Session, latitude: float, longitude: float, radius: float):
    db_sensors = get_sensors(db=db)
    near_sensors = []
    # For each sensor, get data, specifically latitud and longitude
    for sensor in db_sensors:
        sensor_data = get_data(redis=redis_client, sensor_id=sensor.id, mongodb_client=mongodb_client, sensor_name=sensor.name)
        sensor_latitude = sensor_data['latitude']
        sensor_longitude = sensor_data['longitude']
        # If the latitud and longitude are inside the range, add the sensor to a list
        if (latitude - radius) <= sensor_latitude <= (latitude + radius):
            if (longitude - radius) <= sensor_longitude <= (longitude + radius):
                near_sensors.append(sensor_data)    
    return near_sensors

# Search sensors
def search_sensors(db: Session, mongodb: MongoDBClient, query: str, size: int, search_type: str, es: ElasticsearchClient):
    db_sensors = get_sensors(db=db)
    
    mongodb.getDatabase("sensors")
    mongodb.getCollection("sensors")
    
    query = json.loads(query)

    # Define the name of the index to search
    index_name = "sensors"

    if search_type == 'prefix':
        search_type = 'match_phrase_prefix'

    # Query body
    query_body = {
        "query": {
            search_type: {}
        }
    }

    # If similar search
    if search_type == 'similar':
        queries = []
        for key, value in query.items():
            queries.append({"match": {key: {"query": value, "fuzziness": "AUTO", "minimum_should_match": "100%"}}})
        query_body = {
            "query": {
                "bool": {
                    "should": queries
                }
            }
        }
        
    # If match/prefix search
    else:
        if 'type' in query:
            query_body["query"][search_type]["type"] = query['type']
        elif 'name' in query:
            query_body["query"][search_type]["name"] = query['name']
        else:
            query_body["query"][search_type]["description"] = query['description']

    # Get results
    results = es.search(index_name, query_body)

    db_sensors = []
    
    # Append hits
    for hit in results['hits']['hits']:
        db_sensor = get_sensor_by_name(db=db, name=hit['_source']['name'])
        db_sensors.append(get_sensor(db=db, sensor_id=db_sensor.id, mongodb_client=mongodb))

    # Get size sensors
    return db_sensors[:size]

def get_sensor_data(mongodb_client: MongoDBClient, sensor_name: str) -> schemas.Sensor:
    # Get Mongo data and pop _id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensors")
    mongo_info = mongodb_client.findOne({'name': sensor_name})
    mongo_info.pop('_id', None)
    return mongo_info

def get_temperature_values(db: Session, cassandra: CassandraClient, mongodb: MongoDBClient):
    result = cassandra.execute("SELECT sensor_id FROM sensor.sensor_temperature;")
    id_set = set(row.sensor_id for row in result)
    output = {"sensors": []}
    for i in id_set:
        values = {}
        temperatures = cassandra.execute(f"SELECT temperature FROM sensor.sensor_temperature WHERE sensor_id = {i};")
        temperatures = [row.temperature for row in temperatures]
        values["max_temperature"] = max(temperatures)
        values["min_temperature"] = min(temperatures)
        values["average_temperature"] = sum(temperatures) / len(temperatures)
        sensor_ = get_sensor(db=db, sensor_id=i)
        sensor = get_sensor_data(mongodb_client=mongodb, sensor_name=sensor_.name)
        sensor["id"] = i
        sensor["values"] = [values]
        output["sensors"].append(sensor)
    return output

def get_sensors_quantity(db: Session, cassandra: CassandraClient):
    result = cassandra.execute("SELECT sensor_id FROM sensor.sensor_type;")
    id_set = set(row.sensor_id for row in result)
    output = {"sensors": []}
    sensor_types = {}
    for i in id_set:
        types = cassandra.execute(f"SELECT type FROM sensor.sensor_type WHERE sensor_id = {i};")
        for row in types:
            sensor_type = row.type
            if sensor_type == 'temperature':
                sensor_type = "Temperatura"
            elif sensor_type == 'velocity':
                sensor_type = "Velocitat"
            
            if sensor_type not in sensor_types:
                sensor_types[sensor_type] = 1
            else:
                sensor_types[sensor_type] += 1
    for sensor_type, quantity in sensor_types.items():
        output["sensors"].append({"type": sensor_type, "quantity": quantity})
    return output


def get_low_battery_sensors(db: Session, cassandra: CassandraClient, mongo_db: MongoDBClient):
    result = cassandra.execute("SELECT sensor_id FROM sensor.sensor_battery;")
    id_set = set(row.sensor_id for row in result)
    output = {"sensors": []}
    for i in id_set:
        batteries = cassandra.execute(f"SELECT battery FROM sensor.sensor_battery WHERE sensor_id = {i};")
        batteries = [round(row.battery, 2) for row in batteries if round(row.battery, 2) < 0.2]
        if batteries:
            for b in batteries:
                sensor_ = get_sensor(db=db, sensor_id=i)
                sensor = get_sensor_data(mongodb_client=mongo_db, sensor_name=sensor_.name)
                sensor["id"] = i
                sensor["battery_level"] = b
                output["sensors"].append(sensor)
    return output
    
    