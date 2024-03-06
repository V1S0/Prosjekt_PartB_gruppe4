from math import prod
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

        # Fetch all rooms
        #cursor.execute("select * from rooms r , devices d where d.room = r.id")
        #rooms = cursor.fetchall()

        #henter alle floors
        cursor.execute("SELECT DISTINCT  floor from rooms;")
        floors = cursor.fetchall()

        allfloors = list(floors)
    
        #henter alle rooms
        cursor.execute("select * from rooms r;")
        rooms = cursor.fetchall()
        allrooms = list(rooms)

        #henter vi alle devices
        cursor.execute("select * from devices d, rooms r where d.room = r.id;")
        devices= cursor.fetchall()
        alldevices = list(devices)

        smarthjem = SmartHouse()

        for f in allfloors:
            smarthjem.register_floor(f)

        
        ##må finne en måte å hente ut all info pr rom for å bruke register_room
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
#problem her fordi vi får rom som nummer og ikke som navn
            #fikset nå får vi romnavnet men ikke romobjektet
            if category == 'sensor':
                sensorToAdd = sensor(id, supplier, product, kind, kind,room,kind)
                smarthjem.register_device(room, sensorToAdd)
            if category == 'actuator':
                actuatorToAdd = actuator(id, supplier, product, kind, kind,room, False)
                smarthjem.register_device(room, actuatorToAdd)


       ## smarthjem.rooms = allrooms
        ##smarthjem.devices = alldevices


        return smarthjem
        
        


    def get_latest_reading(self, sensor) -> Optional[measurement]:
        """
        Retrieves the most recent sensor reading for the given sensor if available.
        Returns None if the given object has no sensor readings.
        """
        # TODO: After loading the smarthouse, continue here
        return NotImplemented


    def update_actuator_state(self, actuator):
        """
        Saves the state of the given actuator in the database. 
        """
        # TODO: Implement this method. You will probably need to extend the existing database structure: e.g.
        #       by creating a new table (`CREATE`), adding some data to it (`INSERT`) first, and then issue
        #       and SQL `UPDATE` statement. Remember also that you will have to call `commit()` on the `Connection`
        #       stored in the `self.conn` instance variable.
        pass


    # statistics

    
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
        return NotImplemented

    
    def calc_hours_with_humidity_above(self, room, date: str) -> list:
        """
        This function determines during which hours of the given day
        there were more than three measurements in that hour having a humidity measurement that is above
        the average recorded humidity in that room at that particular time.
        The result is a (possibly empty) list of number representing hours [0-23].
        """
        # TODO: implement
        return NotImplemented

