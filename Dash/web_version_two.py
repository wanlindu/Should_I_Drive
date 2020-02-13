from email.headerregistry import Address
from itertools import count
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from geopy.geocoders import Nominatim
from config import user, password, host, port, database

#connection to postgreSQL
connection = psycopg2.connect(user=user,
                             password=password,
                             host=host,
                             port=port,
                             database=database)
cursor = connection.cursor()
print("Connected!")

#Get the address to geolocation data
geolocator = Nominatim(user_agent="my-application")
loc = geolocator.geocode("1455 Market St, san francisco, CA")
distance = '50'
day = 'Mon'
time = '11:00:00'
lat = str(loc.latitude)
lon = str(loc.longitude)

#Query for average occupany number
def query_for_compute_occupancy(lat,lon, distance, day, time):
    location = """'Point(""" + lat + " " + lon + """)'"""
    postgreSQL_query_for_occupied = """ SELECT AVG(c)FROM(SELECT processed_events_test."EventDate" , count(*) as c FROM processed_events_test WHERE ST_DWithin(where_is,ST_GeomFromText(""" + location +""", 4326),"""+ distance + """) AND processed_events_test."Day_Of_Week" = '""" + day + """' AND processed_events_test."StartTime" < '""" + time + """' AND processed_events_test."EndTime" > '""" + time + """' GROUP BY processed_events_test."EventDate")as temp; """
    cursor.execute(postgreSQL_query_for_occupied)
    numbers_of_occupied = cursor.fetchall()
    return numbers_of_occupied

#Query for total meter number and location
def query_for_meter_base(lat,lon, distance):
    location = """'Point(""" + lat + " " + lon + """)'"""
    print(location)
    postgreSQL_query_for_meter = """ SELECT parking_meters."LATITUDE", parking_meters."LONGITUDE" FROM parking_meters WHERE ST_DWithin(where_is,ST_GeomFromText(""" + location + """, 4326),""" + distance +  """);"""
    cursor.execute(postgreSQL_query_for_meter)
    meter_base = cursor.fetchall()
    meter_base_df = pd.DataFrame(meter_base, columns = ['LATITUDE' , 'LONGITUDE'])
    return meter_base_df


occupied_meters = query_for_compute_occupancy(lat,lon, distance, day, time)[0][0]
if(occupied_meters != None):
    occupied_meters = round(occupied_meters);

total_meter_locations = query_for_meter_base(lat,lon, distance)

total_meter_number = total_meter_locations["LATITUDE"].count()

result = "Historical Occupancy = " + str(occupied_meters) + "/" + str(total_meter_number)
print(result)

fig = px.scatter_mapbox(total_meter_locations, lat="LATITUDE", lon="LONGITUDE",color_discrete_sequence=["fuchsia"], zoom=18, height=450)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()
