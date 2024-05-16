import psycopg2
import os


class Timescale:
    _migrations_applied = False
    
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor()
        
        # Do migrations only once
        if not Timescale._migrations_applied:
            Timescale._migrations_applied = True
            self.migrations(self.conn)
        
    def getCursor(self):
            return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        return self.conn.ping()
    
    # Ping function
    def test(self):
        try:
            self.cursor.execute("SELECT 1")
            self.conn.commit()
            return True
        except Exception as e:
            print("Ping error:", e)
            return False
    
    def execute(self, query, fetch=False):
        #return self.cursor.execute(query)
        self.cursor.execute(query)
        if fetch:
            return self.cursor.fetchall()
        self.conn.commit()
    
    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()

    def insert_query(self, table_name, data):
        columns = []
        values = []
        
        # Data is a dictionary
        for key, value in data.items():
            # Avoid None values
            if value is not None:
                columns.append(key)
                if isinstance(value, str):
                    values.append(f"'{value}'")
                else:
                    values.append(str(value))
        
        all_columns = ", ".join(columns)
        all_values = ", ".join(values)
        return f"INSERT INTO {table_name} ({all_columns}) VALUES ({all_values})"
     
    # Apply migrations
    def migrations(self, conn):
        migrations_dir = os.path.join(os.getcwd(), 'migrationsTS')
        migration_files = sorted(os.listdir(migrations_dir))
        for migration_file in migration_files:
            # Check the files are sql
            if migration_file.endswith('.sql'):
                with open(os.path.join(migrations_dir, migration_file), 'r') as f:
                    migration_sql = f.read()
                    cursor = conn.cursor()
                    
                    try:
                        # Transactions work different
                        if "-- transactional: false" in migration_sql:
                            conn.autocommit = True
                            cursor.execute(migration_sql)
                            conn.autocommit = False
                        else:
                            cursor.execute("BEGIN;")
                            cursor.execute(migration_sql)
                            cursor.execute("COMMIT;")
                    except Exception as e:
                        conn.rollback()
                        
                    cursor.close()
                    conn.commit()
    
    