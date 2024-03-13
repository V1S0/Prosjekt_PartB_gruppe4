import datetime
from smarthouse.domain import Device, SmartHouse, sensor, actuator

DEMO_HOUSE = SmartHouse()


# Building house structure
ground_floor = DEMO_HOUSE.register_floor(1)
entrance = DEMO_HOUSE.register_room(ground_floor, 13.5, "Entrance")
garage = DEMO_HOUSE.register_room(ground_floor, 19.0, "Garage")
guestRoom1 = DEMO_HOUSE.register_room(ground_floor, 8.0, "Guest Room 1")
Bathroom1 = DEMO_HOUSE.register_room(ground_floor, 6.3, "Bathroom 1")
LivingRoomKitchen = DEMO_HOUSE.register_room(ground_floor, 39.75, "LivingRoom/Kitchen")

first_floor = DEMO_HOUSE.register_floor(2)
Hallway = DEMO_HOUSE.register_room(first_floor, 10.0, "Hallway")
guestRoom2 = DEMO_HOUSE.register_room(first_floor, 8.0, "Guest Room 2")
Bathroom2 = DEMO_HOUSE.register_room(first_floor, 9.25, "Bathroom 2")
Office = DEMO_HOUSE.register_room(first_floor, 11.75, "Office")
guestRoom3 = DEMO_HOUSE.register_room(first_floor, 10.0, "Guest Room 3")
dressingRoom = DEMO_HOUSE.register_room(first_floor, 4.0, "dressing Room")
Masterbedroom = DEMO_HOUSE.register_room(first_floor, 17.0, "Master bedroom")

#devices


motion_sensor = sensor("cd5be4e8-0e6b-4cb5-a21f-819d06cf5fc5", "NebulaGuard Innovations", "MoveZ Detect 69", "Motion Sensor", "Motion sensor",LivingRoomKitchen,"motion")
DEMO_HOUSE.register_device(LivingRoomKitchen, motion_sensor)

smartlock = actuator("4d5f1ac6-906a-4fd1-b4bf-3a0671e4c4f1", "MythicalTech", "Guardian Lock 7000", "actuator", "Smart lock",entrance, False)
DEMO_HOUSE.register_device(entrance, smartlock)

c02_sensor = sensor("8a43b2d7-e8d3-4f3d-b832-7dbf37bf629e", "ElysianTech", "Smoke Warden 1000","sensor","CO2 sensor",LivingRoomKitchen,"co2")
DEMO_HOUSE.register_device(LivingRoomKitchen, c02_sensor)

electricity_meter= sensor("a2f8690f-2b3a-43cd-90b8-9deea98b42a7", "MysticEnergy Innovations", "Volt Watch Elite","sensor","Electricity Meter",entrance,"kw")
DEMO_HOUSE.register_device(entrance, electricity_meter)


heat_pump= actuator("5e13cabc-5c58-4bb3-82a2-3039e4480a6d", "ElysianTech", "Thermo Smart 6000","actuator","Heat Pump",LivingRoomKitchen, False)
DEMO_HOUSE.register_device(LivingRoomKitchen, heat_pump)

humidity_sensor = sensor("3d87e5c0-8716-4b0b-9c67-087eaaed7b45", "AetherCorp", "Aqua Alert 800","sensor","Humidity Sensor",Bathroom1,"humidity")
DEMO_HOUSE.register_device(Bathroom1, humidity_sensor)

smart_oven1 = actuator("8d4e4c98-21a9-4d1e-bf18-523285ad90f6", "AetherCorp", "Pheonix HEAT 333","actuator","Smart oven",guestRoom1,False)
DEMO_HOUSE.register_device(guestRoom1, smart_oven1)

Automatic_garage_door = actuator("9a54c1ec-0cb5-45a7-b20d-2a7349f1b132", "MythicalTech", "Guardian Lock 9000","actuator","Automatic Garage Door",garage,False)
DEMO_HOUSE.register_device(garage, Automatic_garage_door)

smart_oven2 = actuator("c1e8fa9c-4b8d-487a-a1a5-2b148ee9d2d1", "IgnisTech Solutions", "Ember Heat 3000","actuator","Smart oven",Masterbedroom,False)
DEMO_HOUSE.register_device(Masterbedroom, smart_oven2)

Temperature_sensor = sensor("4d8b1d62-7921-4917-9b70-bbd31f6e2e8e", "AetherCorp", "SmartTemp 42","sensor","Temperature sensor",Masterbedroom,"°C")
DEMO_HOUSE.register_device(Masterbedroom, Temperature_sensor)
Temperature_sensor.addMeasurement(0.0,"°C",'t')

Air_Quality_Sensor = sensor("7c6e35e1-2d8b-4d81-a586-5d01a03bb02c", "CelestialSense Technologies", "AeroGuard Pro","sensor","Air Quality Sensor",guestRoom3,"co2")
DEMO_HOUSE.register_device(guestRoom3, Air_Quality_Sensor)

Smart_plug = actuator("1a66c3d6-22b2-446e-bf5c-eb5b9d1a8c79", "MysticEnergy Innovations", "FlowState X","actuator","Smart Plug",Office,False)
DEMO_HOUSE.register_device(Office, Smart_plug)

Dehumidifier = actuator("9e5b8274-4e77-4e4e-80d2-b40d648ea02a", "ArcaneTech Solutions", "Hydra Dry 8000","actuator","Dehumidifier",Bathroom2,False)
DEMO_HOUSE.register_device(Bathroom2, Dehumidifier)

Light_bulp = actuator("6b1c5f6b-37f6-4e3d-9145-1cfbe2f1fc28", "Elysian Tech", "Lumina Glow 4000","Light Bulp","Light Bulp",guestRoom2,False)
DEMO_HOUSE.register_device(guestRoom2, Light_bulp)


#SmartLock = DEMO_HOUSE.register_device(entrance, smartlock )
# TODO: continue registering the remaining floor, rooms and devices
