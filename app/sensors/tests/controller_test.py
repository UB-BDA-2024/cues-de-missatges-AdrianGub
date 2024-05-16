from fastapi.testclient import TestClient
import pytest
from app.main import app
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.cassandra_client import CassandraClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.timescale import Timescale
import time

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
     from shared.database import engine
     from shared.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)
     redis = RedisClient(host="redis")
     redis.clearAll()
     redis.close()
     mongo = MongoDBClient(host="mongodb")
     mongo.clearDb("sensors")
     mongo.close()

     #cassandra = CassandraClient(["cassandra"])
     #cassandra.get_session().execute("DROP KEYSPACE IF EXISTS sensor")
     #cassandra.close()
     while True:
        try:
            cassandra = CassandraClient(["cassandra"])
            #cassandra.get_session().execute("DROP KEYSPACE sensor")
            cassandra.close()
            break
        except Exception as e:
            time.sleep(5)
