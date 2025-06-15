import pandas as pd
import dash_leaflet as dl
import plotly.express as px
from dash import Dash, html, dcc, dash_table
from dash_extensions.javascript import assign

from library.ui.register_ui_callbacks import register_callbacks
from library.ui.leaflet_maps import main_map, mirror_map
from library.io_interfaces.db_io import DatabaseInterface
from library.io_interfaces.filestore_io import FileInterface


def generate_data_labelling_dash_app(
    db_io: DatabaseInterface, file_io: FileInterface, config: dict
) -> Dash:

    point_to_layer = assign(
        """function(feature, latlng, context){
        const p = feature.properties;
        if(p.type === 'circlemarker'){return L.circleMarker(latlng, radius=p._radius)}
        if(p.type === 'circle'){return L.circle(latlng, radius=p._mRadius)}
        return L.marker(latlng);
    }"""
    )

    app = Dash()

    register_callbacks(db_io=db_io, file_io=file_io, config=config)

    app.layout = html.Div(
        [
            html.Div(
                [
                    # Setup a map with the edit control.
                    main_map,
                    # Setup another map to that mirrors the edit control geometries using the GeoJSON component.
                    mirror_map,
                    dcc.Dropdown(
                        id="aoi_interests",
                        value=[],
                        options=[],
                        clearable=False,
                        multi=True,
                    ),
                    # Buttons for triggering actions from Dash.
                    html.Button("Draw maker", id="draw_marker"),
                    html.Button("Remove -> Clear all", id="clear_all"),
                    html.Button("Load Table", id="unlabeled_posts_tbl_btn"),
                ]
            ),
            html.Div(
                children=[
                    dcc.Markdown(id="selected_post_id"),
                    html.Div(
                        children=[
                            dcc.Markdown(
                                id="selected_post_json_fields",
                                style={
                                    "whiteSpace": "pre-wrap",
                                    "overflowX": "auto",
                                    "width": "50%",
                                    "marginRight": "1rem",
                                    "border color": "black",
                                },
                            ),
                            html.Pre(
                                id="current_map_extents",
                                style={
                                    "whiteSpace": "pre-wrap",
                                    "overflowX": "auto",
                                    "width": "50%",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "flexDirection": "row",
                            "width": "100%",
                        },
                    ),
                    html.Div(
                        children=[
                            dcc.Textarea(
                                id="label_comment",
                                style={
                                    "width": "100%",
                                    "height": "50px",
                                    "padding": "0.75rem",
                                    "fontSize": "1rem",
                                    "border": "1px solid #ccc",
                                    "borderRadius": "5px",
                                    "resize": "vertical",
                                },
                            ),
                            html.Button(
                                "Add Label to post",
                                style={
                                    "marginTop": "0.5rem",
                                    "padding": "0.75rem 1.5rem",
                                    "backgroundColor": "#4CAF50",
                                    "color": "white",
                                    "border": "none",
                                    "borderRadius": "4px",
                                    "fontSize": "1rem",
                                    "cursor": "pointer",
                                },
                            ),
                        ]
                    ),
                ],
                style={"margin": "1rem"},
            ),
            dash_table.DataTable(
                id="unlabeled_posts_tbl",
                data=None,
                style_cell={
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                    "maxWidth": 0,
                },
            ),
        ]
    )

    return app
