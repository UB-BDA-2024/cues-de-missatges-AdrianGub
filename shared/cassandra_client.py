from cassandra.cluster import Cluster

class CassandraClient:
    created = False
    
    def __init__(self, hosts):        
        self.cluster = Cluster(hosts, protocol_version=4)
        self.session = self.cluster.connect()

        if not CassandraClient.created:
            CassandraClient.created = True
            try:
                # Create the 'sensor' keyspace if it doesn't exist
                self.execute("CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
                self.session.set_keyspace('sensor')
                # Switch to the 'sensor' keyspace
                self.execute("USE sensor;")
                # Create tables if they don't exist
                self.execute("CREATE TABLE IF NOT EXISTS sensor_temperature (sensor_id int, last_seen timestamp, temperature float, PRIMARY KEY (sensor_id, last_seen));")
                self.execute("CREATE TABLE IF NOT EXISTS sensor_type (sensor_id int PRIMARY KEY, type text);")
                self.execute("CREATE TABLE IF NOT EXISTS sensor_battery (sensor_id int, last_seen timestamp, battery float, PRIMARY KEY (sensor_id, last_seen));")
                print("Cassandra tables created!")
            except Exception as e:
                print("EXCEPTION: ", e)
            
    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)