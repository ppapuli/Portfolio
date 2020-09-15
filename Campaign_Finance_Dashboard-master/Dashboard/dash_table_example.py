import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import dash  #(version 1.12.0)
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

credentials = service_account.Credentials.from_service_account_file('Dashboard/GCP_M24_ServiceAccount.json')
project_id = "mfor24"
query = """
SELECT * FROM `mfor24.nys_foil.nys_foil_original` LIMIT 1000
"""
sm_df = pandas_gbq.read_gbq(query, project_id= project_id, credentials= credentials)


#df = pd.read_csv("C:/Users/Peter/Desktop/PoliPod/Dash/dash-oil-and-gas/data/Mayoral Contributions.csv")

# App layout
app = dash.Dash(__name__, prevent_initial_callbacks=True) # this was introduced in Dash version 1.12.0


app.layout = html.Div([
    go.Table(header=dict(values=['A Scores', 'B Scores']),
                 cells=dict(values=[[100, 90, 80, 90], [95, 85, 75, 95]]))
])



if __name__ == '__main__':
    app.run_server(debug=True)
