import json
import dash
import pprint
import pandas as pd
from dash import Output, Input, State
from library.io_interfaces.db_io import DatabaseInterface
from library.io_interfaces.filestore_io import FileInterface


def register_callbacks(db_io: DatabaseInterface, file_io: FileInterface, config: dict):

    # Copy data from the edit control to the geojson component.
    @dash.callback(Output("geojson", "data"), Input("edit_control", "geojson"))
    def mirror(x):
        return x

    # Trigger mode (draw marker).
    @dash.callback(
        Output("edit_control", "drawToolbar"), Input("draw_marker", "n_clicks")
    )
    def trigger_mode(n_clicks):
        return dict(
            mode="marker", n_clicks=n_clicks
        )  # include n_click to ensure prop changes

    # Trigger mode (edit) + action (remove all)
    @dash.callback(
        Output("edit_control", "editToolbar"), Input("clear_all", "n_clicks")
    )
    def trigger_action(n_clicks):
        return dict(
            mode="remove", action="clear all", n_clicks=n_clicks
        )  # include n_click to ensure prop changes

    @dash.callback(
        Output("unlabeled_posts_tbl", "data"),
        Output("unlabeled_posts_tbl", "columns"),
        Input("unlabeled_posts_tbl_btn", "n_clicks"),
    )
    def render_all_unlabeled_posts(n_clicks):
        unlabeled_posts_df: pd.DataFrame | None = db_io.get_all_unlabeled_posts(
            db_engine=config["db_engine"], config=config
        )
        return (
            unlabeled_posts_df.to_dict(orient="records"),
            [{"name": i, "id": i} for i in unlabeled_posts_df.columns],
        )

    @dash.callback(
        Output("selected_post_id", "children"),
        Output("selected_post_json_fields", "children"),
        Output("current_map_extents", "children"),
        Input("unlabeled_posts_tbl", "active_cell"),
        Input("edit_control", "geojson"),
        State("unlabeled_posts_tbl", "data"),
    )
    def unlabeled_post_tbl_onclick(
        active_cell: dict, current_geojson: dict, posts_data: list[dict]
    ):

        if active_cell is None:
            raise dash.exceptions.PreventUpdate

        selected_tbl_row = posts_data[active_cell["row"]]

        fields = selected_tbl_row["fields"]
        if isinstance(fields, str):
            try:
                fields = json.loads(fields)
                core_url = fields["url"]
            except json.JSONDecodeError:
                pass

        print(core_url)

        return (
            f"## Selected post: {selected_tbl_row['post_id']}",
            json.dumps(fields, indent=2).replace('"', ""),
            json.dumps(current_geojson, indent=2),
        )
