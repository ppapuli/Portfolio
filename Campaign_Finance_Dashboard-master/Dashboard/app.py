# Import required libraries
import dash
import datetime as dt
import pandas as pd
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import pygeoj
from geopy.geocoders import Nominatim
import json
import numpy as np
import plotly.express as px
import dash_table



app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server
### Load data from Google Big Query
##This function returns a dataframe that is imported from Google BQ
def GetDatafromGBQ(credentials_file, project_id, folder, limit):
    credentials = service_account.Credentials.from_service_account_file(credentials_file)

    project_id = project_id

    if limit > 0:
        query = "SELECT * FROM" + ' ' + str(project_id) + '.' +  str(folder) + ' ' +  "LIMIT " + str(limit)
    else:
        query = "SELECT * FROM" + ' ' + str(project_id) + '.' + str(folder)

    return pandas_gbq.read_gbq(query, project_id= project_id, credentials= credentials)

#Importing the NYS Voter FOIL Data
# MAC IMPORT CODE:
dfoil = GetDatafromGBQ('GCP_M24_ServiceAccount.json', 'mfor24', 'central_voter_data.central_voters_', 1000) #remove test after data upload

#PC IMPORT CODE:
#dfoil = GetDatafromGBQ('Dashboard/GCP_M24_ServiceAccount.json', 'mfor24', 'central_voter_data.central_voters_', 1000) #remove test after data upload


#Import the central contribution dataset from GBQ
df_dataTable = GetDatafromGBQ('GCP_M24_ServiceAccount.json', 'mfor24', 'central_voter_data.central_contributions', 1000)

#Importing NYC Candidate Contributions Data
dfinance = GetDatafromGBQ('GCP_M24_ServiceAccount.json', 'mfor24', 'campaign_finance.contributions', 1000)

# GEO Data as datafrom (in order to later get all NYC Zip Codes)
json_df = pd.read_json("nyc_zip_code_tabulation_areas_polygons.geojson")

#GEO Data as a geojson file
geodata = pygeoj.load("nyc_zip_code_tabulation_areas_polygons.geojson")


# Getting list of all NYC Zip Codes from the Geo Data
zips_list = []
for i in range(len(json_df['features'])):
    zips_list.append(json_df['features'][i]['properties']['postalcode'])


"""Here we begin to clean all the dataframes for use in the Dash Board"""
#This creatons a election year column using the "ELECTION" Column from the df

#We don't really need this functino anymore since Chris cleaned up that column in GBQ
def GetElectionYear(data_frame):

    election_year = []
    for election in data_frame['ELECTION']:
        election_year.append(int(election[:4]))
    data_frame['ELECTION_YEAR'] = election_year
    return data_frame

#Cleaning up the Election Type column to more cohesively identify which election district
##The city council or burough president elections

for i in range(len(dfinance['Election_District'])):
    if dfinance['Election_District'][i].isdigit():
        CCDistrict = dfinance['Election_District'][i]
        dfinance["Election_District"][i] = "City Council District " + str(CCDistrict)
    else:
        pass

for i in range(len(df_dataTable['Election_District'])):
    if df_dataTable['Election_District'][i].isdigit():
        CCDistrict = df_dataTable['Election_District'][i]
        df_dataTable["Election_District"][i] = "City Council District " + str(CCDistrict)
    else:
        pass

## Create a Dictionary of all Years for use in the dashboard year slider
year_options = []
max = max(df_dataTable['ELECTION_YEAR'].unique())
min = min(df_dataTable['ELECTION_YEAR'].unique())
year_dict = {}
year_list = [min]
for change_year in range(int(max) - int(min)):
    year_list.append(min + change_year + 1)
for each_year in year_list:
    year_options.append({'label': str(each_year), 'value': each_year})
    if each_year % 2 == 1:
        year_dict.update({str(each_year): {'label': str(each_year), 'style': {'transform': 'rotate(-45deg)', 'font-size': '12px' }}})

"""
for year in df_dataTable["ELECTION_YEAR"].sort_values().unique():
    year_options.append({'label': str(year), 'value': year})
    year_dict.update({str(year): str(year)})
    year_list.append(year)
"""
#print(year_options, year_dict, year_list)


#Cleaning the Zip Codes column to match the Geo Json Zip codes
clean_zips = []
for i in dfinance['ZIP']:
    i = str(i)
    if len(i) >= 5:
        if i[:5].isdigit():
            clean_zips.append(str(i[:5]))
        else:
            clean_zips.append('')
    else:
        clean_zips.append('')
dfinance['CLEAN_ZIP'] = clean_zips

#Filtering the DF for all contribution made by individuals in New York City
nyc_df = dfinance[dfinance['CLEAN_ZIP'].isin(zips_list)]

#creates a global mapbox token that allows us to use all mapbox map templates
mapbox_access_token = "pk.eyJ1IjoibnlzdHJhdGVneSIsImEiOiJja2VuNmp6N2MwYW03Mnp0ZzRmOTdobjhiIn0.sFAOxY5hhZCd4Qv0AP_BhA"
px.set_mapbox_access_token(mapbox_access_token)

## Why is this following fig randomly placed here? Let's place this somewhere more suitable.

fig = px.scatter_mapbox(
    dfoil[dfoil['latitude'] != 0.0],
    lat = 'latitude',
    lon = 'longitude',
    center = {'lat' : 40.7628, 'lon': -73.92},
    height = 700
    )

fig.update_layout(mapbox_style = 'satellite')
fig.update_layout(margin = {'pad': 200, 't': 0, 'b': 0, 'l': 0, 'r': 0})


def filterDF(data_frame, year_selector, elec_type, cand):
    filtered_df = data_frame[data_frame['ELECTION_YEAR'].isin(year_selector)]
    filtered_df2 = filtered_df[filtered_df['Election_District'].isin(elec_type)]
    filtered_dff = filtered_df2[filtered_df2['RECIPNAME'].isin(cand)]

    return filtered_dff

# vvv We need this to filter the DataTable
"""
def filterDT(data_frame, year_selector, elec_type, cand):
    filtered_df = data_frame[data_frame['ELECTION_YEAR'].isin(year_selector)]
    filtered_df2 = filtered_df[filtered_df['Election_District'].isin(elec_type)]
    filtered_dff = filtered_df2[filtered_df2['RECIPNAME'].isin(cand)]

    return filtered_dff
"""

##Setting the layout design
layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(l=30, r=30, b=20, t=20),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
    title="Satellite Overview",
    mapbox=dict(
        accesstoken=mapbox_access_token,
        style="light",
        center=dict(lon=-78.05, lat=42.54),
        zoom=7,
    ),
)

#Creating the App Layout
app.layout = html.Div(
    [
        dcc.Store(id="aggregate_data"),
        # empty Div to trigger javascript file for graph resizing

        #html.Div(id="output-clientside"),
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("NYS logo.png"),
                            id="plotly-image",
                            style={
                                "height": "60px",
                                "width": "auto",
                                "margin-bottom": "25px",
                            },
                        )
                    ],
                    className="one-third column",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3(
                                    "Campaign Finance and Voter Turnout",
                                    style={"margin-bottom": "0px",
                                            'color': '#f9f9f9'}
                                ),
                                html.H5(
                                    "A Dashboard by New York Strategy",
                                    style={"margin-top": "0px",
                                            'color': '#f9f9f9'}
                                ),
                            ]
                        )
                    ],
                    className="one-half column",
                    id="title",
                ),
            ],
            id="header",
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P(
                            "Filter by Election Year:",
                            className="control_label",
                        ),
                        dcc.RangeSlider(    #Creates the html Division for the year slider
                            id='year-picker',
                            min = min,
                            max = max,
                            value= [2010, 2021],
                            marks = year_dict
                        # {
                        #        'label' : str(year_dict),
                        #        'style' : {
                        #            'font-size':'10px',
                        #            'transform':'rotate(-45deg)'
                        #        }
                        #    }

                            #step=None
                        ),
                        html.P("Filter by Race Type:", className="control_label"),
                        dcc.Dropdown(   #Creates a dropdown for potential candidates in a selectedrange of years
                            id="election_type",
                            multi=True,
                            value='',
                            className="dcc_control",
                        ),
                        html.P("Filter by Candidate Name:", className="control_label"),
                        dcc.Dropdown(   #Creates a Dropdown for all election types in a selected year range
                            id="cand_picker",
                            className="dcc_control",
                            value=[],
                            multi = True # take Out?
                        ),
                    ],
                    className="pretty_container four columns",
                    id="cross-filter-options",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [html.H6(id="candidate_text"), html.P("Running Candidates")],
                                    id="well",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="tConText"), html.P("In Total Contributions")],
                                    id="oil",
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="conText"), html.P("Average Contributions")],
                                    id="gas",
                                    className="mini_container",
                                ),
                            ],
                            id="info-container",
                            className="row container-display",
                        ),
                        html.Div(
                            [dcc.Graph(id="contributions_bar_graph")],
                            id="countGraphContainer",
                            className="pretty_container",
                        ),
                    ],
                    id="right-column",
                    className="eight columns",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="nyc_choromap")],
                    className="pretty_container six columns",
                ),
                html.Div(
                    [dcc.Graph(id="nyc_map_table")],
                    className="pretty_container six columns",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="scatter_map", figure=fig)],
                    className="pretty_container six columns",
                ),
                html.Div(
                    [dcc.Graph(id="scatter_map_table")],
                    className="pretty_container six columns",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [dash_table.DataTable(
                        id="datatable",
                        columns=[{"name": i, "id": i} for i in ['CONTRID', 'NAME', 'ENROLLMENT','RECIPNAME', 'AMNT']],
                        editable=True,              # allow editing of data inside all cells
                        filter_action="native",     # allow filtering of data by user ('native') or not ('none')
                        sort_action="native",       # enables data to be sorted per-column by user or not ('none')
                        sort_mode="single",         # sort across 'multi' or 'single' columns
                        column_selectable="multi",  # allow users to select 'multi' or 'single' columns
                        row_selectable="multi",     # allow users to select 'multi'
                    )],
                    className="pretty_container twelve columns",

                ),
            ],
            className="row flex-display",
        ),



    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)


#Creates a function to return a list of years between the selected years
def year_filler(selected_year):
    years_between = selected_year[-1] - selected_year[0]
    new_years = [selected_year[0]]
    for i in range(int(years_between)):
        new_years.append(selected_year[0] + (i+1))
    new_years.sort()
    return new_years

##Start of All Callbacks!!!
# Create callbacks
'''
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="resize"),
    Output("output-clientside", "children"),
    [Input("count_graph", "figure")],
)
'''
#ELECTION TYPE CALLBACK
# Callback to populate the election_type dropdown menu with all Elections that have happened in specified year
@app.callback([Output(component_id='election_type', component_property='options'),
            Output(component_id ='election_type', component_property='value')],
            [Input(component_id='year-picker', component_property='value')])
def update_election_type(selected_year):
    selected_year = year_filler(selected_year)
    filtered_df =df_dataTable[df_dataTable['ELECTION_YEAR'].isin(selected_year)]
    default = filtered_df['Election_District'].unique()
    elec_types =[]
    for every_election in default:
        elec_types.append({'label':every_election, 'value':every_election})
    return [elec_types, default]

# CANDIDATE SELECTOR CALLBACK
# Callback for populating the cand_picker dropdown with all candidates in specifed election year and election type
@app.callback([Output(component_id='cand_picker', component_property='options'),
            Output(component_id ='cand_picker', component_property='value')],
            [Input(component_id='year-picker', component_property='value'),
            Input(component_id = 'election_type', component_property='value')])
def update_Cand_Picker(selected_year, election_type):
    selected_year = year_filler(selected_year)
    filtered_df = df_dataTable[df_dataTable['ELECTION_YEAR'].isin(selected_year)]
    filtered_dff = filtered_df[filtered_df['Election_District'].isin(election_type)]
    default = filtered_dff['RECIPNAME'].unique()
    candidates = []
    for every_cand in default:
        candidates.append({'label':every_cand, 'value':every_cand})
    return [candidates, default]

# Updates the top left card 'running candidates' above the bar graph
@app.callback(
    Output("candidate_text", "children"),
    [Input('year-picker', 'value'),
     Input('election_type', 'value'),
     Input('cand_picker', 'value')]
)
def update_candidate_text(selected_year,selected_elec_type, selected_cand):
    selected_year = year_filler(selected_year)
    filtered_df = filterDF(df_dataTable, selected_year, selected_elec_type, selected_cand)
    candidates = filtered_df['RECIPNAME'].unique()
    return len(candidates)

# Updates the top middle card 'total contribution' above the bar graph
@app.callback(
    Output("tConText", "children"),
    [Input('year-picker', 'value'),
     Input('election_type', 'value'),
     Input('cand_picker', 'value')]
)
def update_total_contribution_text(selected_year,selected_elec_type, selected_cand):
    selected_year = year_filler(selected_year)
    filtered_df = filterDF(df_dataTable, selected_year, selected_elec_type, selected_cand)
    commas = "{:,.0f}".format(sum(filtered_df['AMNT']))
    return "$" + str(commas)

# Updates the top right card 'average contribution' above the bar graph
@app.callback(
        Output("conText", "children"),
    [Input('year-picker', 'value'),
     Input('election_type', 'value'),
     Input('cand_picker', 'value')]
)
def update_average_contribution_text(selected_year, selected_elec_type, selected_cand):
    selected_year = year_filler(selected_year)
    filtered_df = filterDF(df_dataTable, selected_year, selected_elec_type, selected_cand)
    commas = "{:,.0f}".format(sum(filtered_df['AMNT']) / len(filtered_df['AMNT']))
    return "$" + str(commas)

#CONTRIBUTION BAR CHART
# Updates Candidate Contributions Bar Graph
@app.callback(Output(component_id = 'contributions_bar_graph', component_property = 'figure'),
              [Input(component_id = 'year-picker', component_property = 'value'),
               Input(component_id = 'election_type', component_property = 'value'),
               Input(component_id = 'cand_picker', component_property = 'value')])
def update_figure(selected_year, election_type, candidate_list):
    selected_year = year_filler(selected_year)
    filtered_df = filterDF(df_dataTable, selected_year, election_type, candidate_list)
    specific_candidates = filtered_df.groupby('RECIPNAME', as_index = False).agg({'AMNT':'sum'}).sort_values("AMNT", ascending = False)
    fig_data = []
    fig_data.append(go.Bar(x = specific_candidates['RECIPNAME'],
                           y = specific_candidates['AMNT'],
                           ))
    fig = {
        'data':fig_data,
        'layout': go.Layout(
            title = 'Value of Contributions per Candidate Across Selected Election Years',
            annotations = [   # Set the x axis title using a custom annotation
                dict(
                    x=-0.04,
                    y=-0.20,
                    showarrow=True,
                    #text="Candidate",
                    textangle= -90,
                    xref="paper",
                    yref="paper",
                    font= {
                        'size': 16,
                        'color': 'White'
                    }
                )
            ],
            yaxis = {
                'title': {
                    'text': 'Total Contributions',
                    'font': {
                        'size': 16,
                        'color': 'white'
                    }
                }
            },
            xaxis = {
                'title': {
                    'text': 'Candidate',
                    'font': {
                        'size': 16,
                        'color': 'white'
                    }
                }
            },
           #plot_bgcolor = '#79667a',   # dark purple
           #paper_bgcolor = '#79667a',  # dark purple
            font = {'color': 'white',
                    'size': 18,
            },
            height = 540,
            margin = {'pad': 200, 't': 75, 'b': 75, 'l': 75, 'r': 75},
            plot_bgcolor = '#101010',
        )
    }
    return fig

# Updates Contributions Choropleth by Zip
@app.callback(Output(component_id = 'nyc_choromap', component_property = 'figure'),
              [Input(component_id = 'year-picker', component_property = 'value'),
               Input(component_id = 'election_type', component_property = 'value'),
               Input(component_id = 'cand_picker', component_property = 'value')]
              )
def update_choromap(selected_year, election_type, cand_picker):
    if not selected_year:
        return {'data':[], 'layout':[]}
    selected_year = year_filler(selected_year)
    filtered_df = filterDF(nyc_df, selected_year, election_type, cand_picker) #THIS WILL SIMPLIFY THE DATATABLE FILTRATION PROCESS
    grouped_df = filtered_df.groupby("CLEAN_ZIP", as_index=False).agg({"AMNT":"sum"}) #grouping by Zip Code and suming all contributions from each zip code
    grouped_df["LOG_AMNT"] = np.log(grouped_df["AMNT"])
    text_list = []
    for i in range(len(grouped_df)):
        text_list.append('Zip Code: ' + str(grouped_df['CLEAN_ZIP'][i]) + '<br>' + \
                        'Total Contributions: ' + str(grouped_df['AMNT'][i]) + '<br>' + 'Total Number of Contributions: ' + \
                        str(len(grouped_df['AMNT'])))
    grouped_df['text'] = text_list
    fig_data = []
    fig_data = px.choropleth_mapbox(
        grouped_df,
        geojson=geodata,
        locations='CLEAN_ZIP',
        featureidkey = 'properties.postalcode',
        #scope = 'usa',
        color='LOG_AMNT',
        labels={'LOG_AMNT':'Log Contributions', 'CLEAN_ZIP':'Zip Code', 'AMNT':'Total Contribution'},
        color_continuous_scale = 'haline',
        hover_data = ['AMNT'],
        opacity = 0.75,
        center = {'lat' : 40.7628, 'lon': -73.92},
        zoom = 8,
        height = 700
    )

    fig_data.update_layout(mapbox_style = 'mapbox://styles/nystrategy/ckereniwd5zcr19p2q6gzv4rt')
    fig_data.update_geos(fitbounds = 'locations')
    fig_data.update_layout(margin = {'pad': 200, 't': 0, 'b': 0, 'l': 0, 'r': 0})
    return fig_data

#Scatter Map #callback errors here re. input. Solution: define new
#@app.callback(Output(component_id = 'datatable', component_property = 'figure'),
#    [Input(component_id = 'year-picker', component_property = 'value'))
###def scattermap():
###    fig = px.scatter_mapbox(sm_dff[sm_dff['LAT'] != ''], lat = 'LAT', lon = 'LONG')
###    fig.update_layout(mapbox_style = 'carto-darkmatter')
###    return fig

#Dash Datatable
@app.callback([Output(component_id = 'datatable', component_property = 'data')],
                #Output(component_id = 'datatable', component_property = 'columns')],
            [Input(component_id = 'year-picker', component_property = 'value'),
             Input(component_id = 'election_type', component_property = 'value'),
            Input(component_id = 'cand_picker', component_property = 'value')])
def update_table(selected_year, election_type, candidate_list):
    #print('dataframe', df_dataTable)
    selected_year = year_filler(selected_year)
    filtered_data = filterDF(df_dataTable, selected_year, election_type, candidate_list)
    filtered_df = filtered_data.groupby(['CONTRID', 'Election_District'], as_index = False).agg({'AMNT':'sum', 'NAME':'first', 'ENROLLMENT':'first','RECIPNAME':'first'})
    #print('filtered df\n\n', filtered_df)
    #print('filtered_data\n\n', filtered_data)
    #filtered_df = pd.DataFrame(filtered_df)
    #print(filtered_df.head())
    #columns = [{"name": i, "id": i} for i in filtered_df.columns]
    #filtered_datatable = filtered_data[['NAME', 'RECIPNAME', 'DATE']]
    #columns=[{"name": i, "id": i} for i in filtered_datatable.columns]
    data=filtered_df.to_dict('records')
    #print(data.head())
    #data=filtered_data.to_dict('records')
    return [data]


# Main
if __name__ == "__main__":
    app.run_server(debug=True)
