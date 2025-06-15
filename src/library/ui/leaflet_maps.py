import dash_leaflet as dl
from dash_extensions.javascript import assign

point_to_layer = assign(
    """function(feature, latlng, context){
    const p = feature.properties;
    if(p.type === 'circlemarker'){return L.circleMarker(latlng, radius=p._radius)}
    if(p.type === 'circle'){return L.circle(latlng, radius=p._mRadius)}
    return L.marker(latlng);
}"""
)

main_basemap_layers = [
    dl.LayersControl(
        [
            dl.BaseLayer(
                dl.TileLayer(url="http://tile.openstreetmap.org/{z}/{x}/{y}.png"),
                name="Open Street Map",
                checked=True,
            ),
            dl.BaseLayer(
                dl.TileLayer(url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}"),
                name="Google Street Map",
                checked=False,
            ),
            dl.Overlay(
                dl.LayerGroup(
                    dl.GeoJSON(
                        url="https://raw.githubusercontent.com/EugeneBorshch/ukraine_geojson/refs/heads/master/UA_FULL_Ukraine.geojson",
                        zoomToBounds=True,
                        zoomToBoundsOnClick=True,
                        onEachFeature=assign(
                            """
                            function(feature, layer) {
                                if (feature.properties && feature.properties["name:en"]) {
                                    layer.bindTooltip(feature.properties["name:en"], {sticky: true});
                                }
                            }
                            """
                        ),
                        style={"color": "blue", "weight": 2, "fillOpacity": 0.2},
                    )
                ),
                name="Ukraine",
                checked=True,
            ),
            dl.Overlay(
                dl.LayerGroup(
                    dl.GeoJSON(
                        url="https://raw.githubusercontent.com/georgique/world-geojson/refs/heads/develop/countries/russia.json",
                        zoomToBoundsOnClick=True,
                        style={"color": "red", "weight": 2, "fillOpacity": 0.2},
                    )
                ),
                name="Russia",
                checked=True,
            ),
        ]
    )
]

mirror_basemap_layers = [
    dl.LayersControl(
        [
            dl.BaseLayer(
                dl.TileLayer(url="http://tile.openstreetmap.org/{z}/{x}/{y}.png"),
                name="Open Street Map",
                checked=False,
            ),
            dl.BaseLayer(
                dl.TileLayer(url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}"),
                name="Google Street Map",
                checked=True,
            ),
        ]
    )
]


main_map = dl.Map(
    center=[56, 10],
    zoom=4,
    children=main_basemap_layers
    + [
        dl.FullScreenControl(),
        dl.FeatureGroup([dl.EditControl(id="edit_control")]),
    ],
    style={"width": "50%", "height": "50vh", "display": "inline-block"},
    id="map",
)

mirror_map = dl.Map(
    center=[56, 10],
    zoom=4,
    children=mirror_basemap_layers
    + [
        dl.GeoJSON(id="geojson", pointToLayer=point_to_layer, zoomToBounds=True),
    ],
    style={"width": "50%", "height": "50vh", "display": "inline-block"},
    id="mirror",
)
