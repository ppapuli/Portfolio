import plotly.graph_objects as go

fig = go.Figure(go.Choroplethmapbox(geojson=counties,
                                    locations=df.fips,
                                    z=df.unemp,
                                    colorscale="Viridis",
                                    zmin=0,
                                    zmax=12,
                                    marker_line_width=0))