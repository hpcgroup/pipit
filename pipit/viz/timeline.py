import numpy as np
import spatialpandas as sp
import pandas as pd


def sampleRanks(df, num_ranks_displayed):
    """Filters events by their rank to limit number of ranks displayed."""
    num_ranks = len(df.Rank.unique())

    if num_ranks > num_ranks_displayed:
        divisor = int(num_ranks / num_ranks_displayed)
        df = df[df.Rank.astype("int") % divisor == 0]

    N = min(num_ranks_displayed, num_ranks)

    return (N, df)


def eventsToPolygons(numEvents, left, right, y):
    """Converts events to polygon coordinates for graphing."""
    left = left.to_numpy()
    right = right.to_numpy()
    y = y.to_numpy()

    top = y + 0.45
    bottom = y - 0.45

    arr = np.array([right, bottom, right, top, left, top, left, bottom, right, bottom])

    arr = arr.transpose()
    arr = arr.reshape(numEvents, 1, 1, 10)

    polygons = sp.geometry.MultiPolygonArray(arr.tolist())

    return polygons

def calculateHeight(N, numLegendItems):
    """Calculate the appropriate height of the Bokeh plot (in notebook)"""
    return min(N * 25 + 175, numLegendItems * 20 + 100)


def timeline(trace, rank=None, rasterization_threshold=40000):
    """Draws a timeline of events in trace.
    If rank=None then draws timeline in overview mode (all ranks), otherwise draws
    timeline in depth mode for the specified rank.
    """
    import holoviews as hv
    import colorcet as cc
    from bokeh.models import PrintfTickFormatter
    from holoviews.operation.datashader import datashade, inspect_polygons
    import datashader as ds
    from bokeh.palettes import Category20b_20

    # Set holoviews to use bokeh as its backend
    hv.extension("bokeh", logo=False)

    df = trace.events

    # Filter DF if necessary
    if rank is None:
        (N, df) = sampleRanks(df, 16)
    else:
        df = df[df.Rank == rank]
        N = len(df.Level.unique())

    # Convert events to 2D polygons
    polygons = eventsToPolygons(
        df.shape[0],
        df["Enter"],
        df["Leave"],
        df["Rank"] if rank is None else df["Level"],
    )

    # Create a SpatialDataframe out of polygons
    sdf = sp.GeoDataFrame({"polygons": polygons})

    # Copy necessary columns from df to sdf
    cols = ["Name", "Enter", "Leave", "Rank", "Level", "Inc Time (s)", "Exc Time (s)"]

    for col in cols:
        sdf[col] = df[col].to_numpy()

    sdf.Name = sdf.Name.astype("category")

    # Create polygon specification in HoloViews
    polys = hv.Polygons(sdf, vdims=cols, datatype=["spatialpandas"])

    # Configure color mapping and legend
    # See https://examples.pyviz.org/nyc_buildings/nyc_buildings.html
    cats = pd.unique(df.Name).to_numpy()
    colors = cc.glasbey_bw_minc_20
    color_key = {cat: tuple(int(e*255.) for e in colors[i]) for i, cat in enumerate(cats)}
    legend    = hv.NdOverlay({k: hv.Points([0,0], label=str(k)).opts(
                                            color=cc.rgb_to_hex(*v), size=0, apply_ranges=False) 
                            for k, v in color_key.items()}, 'Name')
    
    def hook(plot, element):
        plot.state.legend.spacing = 0

    # Configure plot options
    common_opts = {
        "height": calculateHeight(N, len(cats)),
        "xaxis": "top",
        "xlabel": "Time (sec)",
        "yaxis": "left" if rank is None else None,
        "invert_yaxis": rank is not None and df.shape[0] < rasterization_threshold,
        "ylabel": "Process ID",
        "yticks": list(range(N)),
        "yformatter": PrintfTickFormatter(format="Process %d"),
        "responsive": True,
        "legend_position": "right",
        "fontsize": {"legend": 8}
    }

    title = (
        "Overview Timeline" if rank is None else "Depth Timeline, Process " + str(rank)
    )

    # print("Plotting " + str(df.shape[0]) + " events...")

    # If numEvents < threshold then draw normally, otherwise rasterize
    if df.shape[0] < rasterization_threshold:
        return (
            polys.opts(
                **common_opts,
                tools=["hover"],
                default_tools=["xpan", "xwheel_zoom"],
                active_tools=["xwheel_zoom"],
                line_width=0,
                cmap=color_key,
                title=title
            )
            * legend.opts(hooks=[hook])
        )
    else:
        import panel as pn

        shaded = datashade(
            polys,
            aggregator=ds.by("Name", ds.any()),
            width=900,
            height=common_opts["height"],
            color_key=color_key,
        )
        hover = inspect_polygons(shaded).opts(tools=["hover"], active_tools=["xwheel_zoom"])

        shaded = shaded.opts(default_tools=["xpan", "xwheel_zoom"], active_tools=["xwheel_zoom"])
        hover = hover.opts(default_tools=["xpan", "xwheel_zoom"], active_tools=["xwheel_zoom"], fill_color="none")

        row = (shaded * hover * legend).opts(
            **common_opts, title=title + " (rasterized)", hooks=[hook]
        )

        bokeh_server = pn.Row(row).show(port=12345)
        # bokeh_server.stop()


# # If rank is specified then depth mode else overview mode
# def basicTimeline(trace, rank=None):
#     # Import libraries
#     from bokeh.plotting import figure, show
#     from bokeh.io import output_notebook
#     from bokeh.models import HoverTool
#     from bokeh.models import ColumnDataSource
#     from bokeh.transform import factor_cmap
#     from bokeh.models import Arrow, NormalHead
#     from bokeh.plotting import figure, show
#     from bokeh.palettes import Dark2_8, Viridis10, Spectral10
#     from IPython.display import display, HTML

#     output_notebook(hide_banner=True)

#     df = trace.events.copy()

#     # Filter events
#     if rank is None:
#         num_ranks = len(df.Rank.unique())
#         num_ranks_displayed = 16

#         if num_ranks > num_ranks_displayed:
#             divisor = int(num_ranks / num_ranks_displayed)
#             df = df[df.Rank.astype("int") % divisor == 0]

#         N = min(num_ranks_displayed, num_ranks)
#     else:
#         df = df[df.Rank == rank]
#         N = len(df.Level.unique())
        
#     df.Rank = df.Rank.astype('str').astype('category')
#     df.Level = df.Level.astype('str').astype('category')

#     # Convert DF into a ColumnDataSource that Bokeh expects
#     source = ColumnDataSource(df)

#     # Generate color palette for functions
#     tiled = np.tile(Dark2_8, 5)
#     index_cmap = factor_cmap(
#         "Name", palette=tuple(tiled), factors=sorted(df.Name.unique())
#     )

#     # Add custom hover interaction
#     hover = HoverTool(
#         point_policy="follow_mouse",
#         line_policy="next",
#         tooltips=[
#             ("Func", "@Name (@Rank)"),
#             ("Time", "@{Inc Time (s)}"),
#         ]
#     )

#     display(
#         HTML(
#             """
#         <style>
#             div.bk-tooltip.bk-right>div.bk>div:not(:last-child) {
#                 display:none !important;
#             }
#             div.bk-tooltip.bk-left>div.bk>div:not(:last-child) {
#                 display:none !important;
#             }
#         </style>
#         """
#         )
#     )
    
#     # Determine y-range
#     if rank is None:
#         y_range=[str(x) for x in sorted(df.Rank.astype('int').unique(), reverse=True)]
#     else:
#         y_range=[str(x) for x in sorted(df.Level.astype('float').unique(), reverse=True)]

#     # Create Bokeh figure
#     p = figure(
#         y_range=y_range,
#         title="Overview Timeline" if rank == None else ("Depth Timeline (Process " + str(rank) + ")"),
#         tools=["xpan", "xwheel_zoom", "reset", "save", hover],
#         active_scroll="xwheel_zoom",
# #         height=max(300, (N * 30) + 200),
#         height=(N*30) + 90,
# #         width=900,
#         sizing_mode="stretch_width",
#         x_axis_location="above",
#         x_axis_label="Time (sec)",
#         output_backend="webgl",
#         toolbar_location="right",
#     )
#     p.yaxis.visible = rank is None

#     # Draw horizontal bars for events
#     p.hbar(
#         y="Rank" if rank == None else "Level",
#         left="Enter",
#         right="Leave",
#         source=source,
#         fill_color=index_cmap,
#         line_width=0,
#         line_color="black",
#         height=0.9,
# #         legend_field="Name",
#     )

#     # Configure legend
# #     p.legend.orientation = "vertical"
# #     p.legend.location = "top"
# #     p.legend.label_text_font_size = "11px"
# #     p.legend.spacing = 4
# #     p.add_layout(p.legend[0], "right")

#     # Render plot to screen
#     show(p)