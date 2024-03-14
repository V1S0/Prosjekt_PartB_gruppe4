#from math import prod
import sqlite3
from typing import Optional
from smarthouse.domain import measurement
from smarthouse.domain import SmartHouse
from smarthouse.domain import sensor
from smarthouse.domain import actuator

class SmartHouseRepository:
    """
    Provides the functionality to persist and load a _SmartHouse_ object 
    in a SQLite database.
    """

    def __init__(self, file: str) -> None:
        self.file = file 
        self.conn = sqlite3.connect(file)

    def __del__(self):
        self.conn.close()

    def cursor(self) -> sqlite3.Cursor:
        """
        Provides a _raw_ SQLite cursor to interact with the database.
        When calling this method to obtain a cursors, you have to 
        rememeber calling `commit/rollback` and `close` yourself when
        you are done with issuing SQL commands.
        """
        return self.conn.cursor()

    def reconnect(self):
        self.conn.close()
        self.conn = sqlite3.connect(self.file)

    
    def load_smarthouse_deep(self): 
        """
        This method retrives the complete single instance of the _SmartHouse_ 
        object stored in this database. The retrieval yields a _deep_ copy, i.e.
        all referenced objects within the object structure (e.g. floors, rooms, devices) 
        are retrieved as well. 
        """
        cursor = self.cursor()


        #henter alle floors
        cursor.execute("SELECT DISTINCT  floor from rooms;")
        floors = cursor.fetchall()

        allfloors = list(floors)
    
        #henter alle rooms
        cursor.execute("select * from rooms r;")
        rooms = cursor.fetchall()
        allrooms = list(rooms)

        #henter alle devices
        cursor.execute("select * from devices d, rooms r where d.room = r.id;")
        devices= cursor.fetchall()
        alldevices = list(devices)

        smarthjem = SmartHouse()

        for f in allfloors:
            smarthjem.register_floor(f)

        
        for r in rooms:
            name = r[3]
            area = r[2]
            floor = r[1]
            smarthjem.register_room(floor, area, name)


        for d in alldevices:
            id = d[0]
            room = d[9]
            for allerom in smarthjem.rooms:
                if room == allerom.room_name:
                    room = allerom

            kind = d[2]
            category = d[3]     #actuator or sensor
            supplier = d[4]
            product = d[5]

            if category == 'sensor':
                sensorToAdd = sensor(id, supplier, product, kind, kind,room,kind)
                smarthjem.register_device(room, sensorToAdd)
            if category == 'actuator':

                cursor.execute(f"select state from Actuators a where id = '{id}';")
                state = cursor.fetchone()
                if state is not None:
    # Convert the database state (integer) to a boolean and then pass it
                    actual_state = bool(state[0])
                else:
                    actual_state = False  # Default state if none is found
                actuatorToAdd = actuator(id, supplier, product, kind, kind, room, actual_state)
                smarthjem.register_device(room, actuatorToAdd)



        return smarthjem
        
        


    def get_latest_reading(self, sensor) -> Optional[measurement]:
        """
        Retrieves the most recent sensor reading for the given sensor if available.
        Returns None if the given object has no sensor readings.
        """
        # TODO: After loading the smarthouse, continue here
        id = sensor.id
        cursor = self.cursor()
        cursor.execute(f"select * from measurements where device = '{id}';")
        allSensorMeasurements = cursor.fetchall()
        if allSensorMeasurements:
            lastMeasurement = allSensorMeasurements[-1]
            sensor.addMeasurement(lastMeasurement[2], lastMeasurement[3], lastMeasurement[1])
            
            return sensor.last_measurement()

        else:
            return None


        


    def update_actuator_state(self, actuator):
        """
        Saves the state of the given actuator in the database. 
        """
        # TODO: Implement this method. You will probably need to extend the existing database structure: e.g.
        #       by creating a new table (`CREATE`), adding some data to it (`INSERT`) first, and then issue
        #       and SQL `UPDATE` statement. Remember also that you will have to call `commit()` on the `Connection`
        #       stored in the `self.conn` instance variable.

        cursor = self.cursor()
        
        
        id = actuator.id


        print("dette er status på "+actuator.nickname)
        print(actuator.state)

        cursor.execute(f"update Actuators set state = {actuator.state} where id = '{id}';")

        # Commit the changes to the database
        self.conn.commit()



    
    def calc_avg_temperatures_in_room(self, room, from_date: Optional[str] = None, until_date: Optional[str] = None) -> dict:
        """Calculates the average temperatures in the given room for the given time range by
        fetching all available temperature sensor data (either from a dedicated temperature sensor 
        or from an actuator, which includes a temperature sensor like a heat pump) from the devices 
        located in that room, filtering the measurement by given time range.
        The latter is provided by two strings, each containing a date in the ISO 8601 format.
        If one argument is empty, it means that the upper and/or lower bound of the time range are unbounded.
        The result should be a dictionary where the keys are strings representing dates (iso format) and 
        the values are floating point numbers containing the average temperature that day.
        """
        # TODO: This and the following statistic method are a bit more challenging. Try to design the respective 
        #       SQL statements first in a SQL editor like Dbeaver and then copy it over here.  

        roomName = room.room_name

        cursor = self.cursor()
        
        
        cursor.execute(f"select id from rooms r where name ='{roomName}';")   
        roomid = cursor.fetchone()


         # Build the WHERE clause based on optional date arguments.
        date_filter = "AND"
        if from_date and until_date:
         date_filter += f" m.ts >= '{from_date}' AND m.ts <= '{until_date}'"
        elif from_date:
            date_filter += f" m.ts >= '{from_date}'"
        elif until_date:
            date_filter += f" m.ts <= '{until_date}"
            date_filter += " 23:59:59'"
            
        else:
            date_filter = ""  # No date filter if both are None

        # SQL query to fetch average temperatures.
        query = f"""
            SELECT DATE(m.ts) as DAG, AVG(m.value) AS avg_temp
            FROM measurements m
            JOIN devices d ON m.device = d.id
            JOIN rooms r ON d.room = r.id
            WHERE m.unit = '°C' AND r.id = {roomid[0]}
            {date_filter}
            GROUP BY DAG
            ORDER BY DAG;
         """

        cursor.execute(query)
        results = cursor.fetchall()

        # Build the result dictionary.
        avg_temperatures = {str(result[0]): result[1] for result in results}

        return avg_temperatures

    
    def calc_hours_with_humidity_above(self, room, date: str) -> list:
        """
        This function determines during which hours of the given day
        there were more than three measurements in that hour having a humidity measurement that is above
        the average recorded humidity in that room at that particular time.
        The result is a (possibly empty) list of number representing hours [0-23].
        """
        
        room_name = room.room_name

        # Connect to the database and prepare the cursor
        cursor = self.cursor()

        # Get the room ID for the given room name
        cursor.execute(f"SELECT id FROM rooms WHERE name = '{room_name}';")
        room_id = cursor.fetchone()
        if room_id is None:
            return []  # No such room
        room_id = room_id[0]

        # Calculate the average humidity for the room on the specified date
        cursor.execute(f"""
            SELECT AVG(m.value) as avg_humidity
            FROM measurements m
            JOIN devices d ON m.device = d.id
            WHERE d.room = {room_id} AND m.unit = '%' AND DATE(m.ts) = '{date}';
        """)
        avg_humidity = cursor.fetchone()
        if avg_humidity is None:
            return []  # No measurements
        avg_humidity = avg_humidity[0]

        # Find hours where more than three measurements exceed this average humidity
        cursor.execute(f"""
            SELECT STRFTIME('%H', m.ts) as hour
            FROM measurements m
            JOIN devices d ON m.device = d.id
            WHERE d.room = {room_id} AND m.unit = '%' AND DATE(m.ts) = '{date}'
            AND m.value > {avg_humidity}
            GROUP BY hour
            HAVING COUNT(m.device) > 3;
        """)
        results = cursor.fetchall()

        # Convert results to a list of hours
        hours = [int(result[0]) for result in results]

        return hours

