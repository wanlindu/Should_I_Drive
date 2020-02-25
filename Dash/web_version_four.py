from config import user, password, host, port, database
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from geopy.geocoders import Nominatim
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash.dependencies import Input, Output,State
from dash.exceptions import PreventUpdate
from itertools import count
import datetime

#connection to postgreSQL
connection = psycopg2.connect(user=user,
                             password=password,
                             host=host,
                             port=port,
                             database=database)
cursor = connection.cursor()
print("Connected!Origin")

connection_2 = psycopg2.connect(user=user,
                             password=password,
                             host="34.223.254.35",
                             port=port,
                             database=database)
cursor_2 = connection_2.cursor()

#Modify user inout time + find time window
def process_time(time):  
    formed_time = time.split(':')
    hour = formed_time[0]
    return hour



#Query for average occupancy (duplicate dropped)


#Query for total meter number and location
def query_for_meter_base(lat,lon, distance):
    location = """'Point(""" + lat + " " + lon + """)'"""
    postgreSQL_query_for_meter = """ SELECT parking_meters."LATITUDE", parking_meters."LONGITUDE", parking_meters."POST_ID" FROM parking_meters WHERE ST_DWithin(where_is,ST_GeomFromText(""" + location + """, 4326),""" + distance +  """);"""
    cursor.execute(postgreSQL_query_for_meter)
    meter_base = cursor.fetchall()
    meter_base_df = pd.DataFrame(meter_base, columns = ['LATITUDE' , 'LONGITUDE','POST_ID'])
    return meter_base_df

def query_for_compute_occupancy(meters, processed_time):
    
    processed_time_col = 'full_processing_9to16."{}"'.format(processed_time)
    
    postgreSQL_query_for_occupied = """ SELECT full_processing_9to16."POST_ID", """ + processed_time_col + """ FROM full_processing_9to16"""
    print(postgreSQL_query_for_occupied)
    t0 = datetime.datetime.now().timestamp()
    cursor_2.execute(postgreSQL_query_for_occupied)
    t1 = datetime.datetime.now().timestamp()
    print("from database")
    print(t1 - t0)
    occupied_df = pd.DataFrame(cursor_2.fetchall(), columns = ['POST_ID','processed_time'])            
    
    print(occupied_df)
    #Find unique POST_IDs
    occupied = pd.merge(meters,occupied_df,left_on=['POST_ID'],right_on=['POST_ID'])
    
    return occupied


# map 
def map(address, distance, day, time_pre):
    geolocator = Nominatim(user_agent="my-application")
    loc = geolocator.geocode(address)
    lat = str(loc.latitude)
    lon = str(loc.longitude)
    time = process_time(time_pre)
    total_meter_locations_pre = query_for_meter_base(lat,lon, distance)
    total_meter_number = total_meter_locations_pre["LATITUDE"].count()
    stats_for_each_meters = query_for_compute_occupancy(total_meter_locations_pre, time)
    stats_for_each_meters["occupied_possibility"] = stats_for_each_meters["processed_time"].apply(lambda x: 1 if x >= 553 else round(float(x/553), 2))
    stats_for_each_meters_final = stats_for_each_meters[stats_for_each_meters.occupied_possibility > 0]
    label = str(round(stats_for_each_meters_final["occupied_possibility"].mean(),2))
    print(stats_for_each_meters)
    print(label)
    
    #show all the meters in the range
    fig = px.scatter_mapbox(stats_for_each_meters_final, lat="LATITUDE", lon="LONGITUDE", zoom=16, height=450, hover_data = ["occupied_possibility"])
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    
    #show the destination 
    fig.add_trace(
       go.Scattermapbox(mode= "markers+text", lat=[lat],lon=[lon],marker=go.scattermapbox.Marker(size=25),text = [label], hoverinfo = "text")
       )
    fig.update_layout(showlegend=False)
    
    return fig


#-------------------------------------------------------

#Original display
distance_o = '100'
day_o = 'Mon'
time_o = '11:00' #this give a drop down in 10mins and give without seconds
address_o = 'Financial District, San Francisco, CA'
geolocator = Nominatim(user_agent="my-application")
loc_o = geolocator.geocode(address_o)
lat_o = str(loc_o.latitude)
lon_o = str(loc_o.longitude)
fig = px.scatter_mapbox(query_for_meter_base(lat_o, lon_o, distance_o), lat="LATITUDE", lon="LONGITUDE",color_discrete_sequence=["fuchsia"], zoom=15, height=450)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":5,"t":5,"l":5,"b":5})

#-------------------------------------------------------

#Create App

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
colors = {
    'background': '#A8D0E6',
    'text': 'blue'
}
#https://raw.githubusercontent.com/wanlindu/Should_I_Drive/master/image/san-francisco-tram.jpg
app.layout = html.Div(
    style={'background-image': 'url("https://raw.githubusercontent.com/wanlindu/Should_I_Drive/master/image/san-francisco-tram.jpg")'},
    children=[
    html.H1(children='Should I Drive',
            style={
            'textAlign': 'center',
            'color': colors['text']
            }
    ),

    html.Div(children='''
        The one million dollar question.
    ''', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    
    
    
    
    html.Div([
        dcc.Markdown('''##### Enter Time:''', ),
        dcc.Input(id='time', type='text', placeholder='9:00 - 18:00', value=''),
        
        dcc.Markdown('''##### Enter Range(m):'''),
        dcc.Input(id='ran', type='text', placeholder='50 ...', value='')
        ], style={'width': '48%', 'display': 'inline-block'}),
    
    html.Div([
        dcc.Markdown('''##### Day in Week:'''),
        dcc.Dropdown(
        id='day',
        style={'height': '30px', 'width': '190px'},
        options=[
            {'label': 'Mon', 'value': 'mon'},
            {'label': 'Tue', 'value': 'tue'},
            {'label': 'Wed', 'value': 'wed'},
            {'label': 'Thu', 'value': 'thu'},
            {'label': 'Fri', 'value': 'fri'},
            {'label': 'Sat', 'value': 'sat'},
        ],
        placeholder= 'Mon - Sat',
        value=''
        ),
        
        dcc.Markdown('''##### Enter Destination:'''),
        dcc.Input(id='loc', type='text', placeholder='Destination...', value='')
        ],style={'width': '48%', 'display': 'inline-block'}),

    html.Button('Submit', id='button'),
    html.Div(id='output-container-button'),
    
    dcc.Graph(
        id='map',
        figure=fig
    )
])
@app.callback(
    Output(component_id='map', component_property='figure'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('day', 'value'),dash.dependencies.State('time', 'value'), dash.dependencies.State('ran', 'value'), dash.dependencies.State('loc', 'value')])
def update_output(n_clicks, day, time, ran,loc ):
    if n_clicks is None:
        raise PreventUpdate
    else:
        day_u = ''.join(day)
        time_u = ''.join(time)
        distance_u = ''.join(ran)
        address_u = ''.join(loc)
        fig = map(address_u, distance_u, day_u, time_u)
    return fig
    
    
    
if __name__ == '__main__':
    app.run_server(debug=True)