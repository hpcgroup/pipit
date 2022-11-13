import random

import holoviews as hv
import numpy as np
from bokeh.models import (
    HoverTool,
    AdaptiveTicker,
    PrintfTickFormatter,
)
from holoviews import opts
import pandas as pd

from .util import (
    DEFAULT_PALETTE,
    generate_cmap,
    in_notebook,
    time_series,
    clamp,
    fake_time_profile,
)

hv.extension("bokeh", logo=False)


class Vis:
    """Contains visualization data and functions for a Trace"""

    def __init__(self, trace):
        """Initialize environment for visualization"""
        self.trace = trace

        # Apply css customizations, remove multiple tooltips for overlapping glyphs
        if in_notebook():
            from IPython.display import HTML, display

            display(
                HTML(
                    """
                <style>
                    .container { width:90% !important; }
                    div.bk-tooltip > div.bk > div.bk:not(:last-child) {
                        display:none !important;
                    }
                    div.bk { cursor: default !important; }
                    .bk.bk-tooltip-row-label {
                        color: black;
                        font-weight: bold;
                    }
                    .bk.bk-tooltip-row-value {
                        font-family: monospace;
                        padding-left: 3px;
                    }
                </style>
                """
                )
            )

        # Set some properties for easy access
        self.functions = trace.events[trace.events["Event Type"] == "Entry"][
            "Name"
        ].unique()
        self.ranks = trace.events["Process ID"].unique()

        # Initialize color map
        random.shuffle(DEFAULT_PALETTE)
        self.cmap = generate_cmap(self.functions, DEFAULT_PALETTE)

        # Apply default opts for HoloViews elements
        # See https://holoviews.org/user_guide/Applying_Customizations.html#session-specific-options # noqa: 501
        def customize_plot(plot, _):
            plot.state.toolbar_location = "above"
            plot.state.ygrid.visible = False
            if plot.state.legend:
                plot.state.legend.label_text_font_size = "8pt"
                plot.state.legend.spacing = 0
                plot.state.legend.location = "top"

        defaults = dict(
            fontsize={
                "title": 10,
                "legend": 8,
            },
            hooks=[customize_plot],
            responsive=True,
        )

        opts.defaults(
            opts.Area(**defaults),
            opts.Bars(**defaults),
            opts.Bivariate(**defaults),
            opts.BoxWhisker(**defaults),
            opts.Chord(**defaults),
            opts.Contours(**defaults),
            opts.Curve(**defaults),
            opts.Distribution(**defaults),
            opts.Graph(**defaults),
            opts.Image(**defaults),
            opts.Labels(**defaults),
            opts.Points(**defaults),
            opts.Polygons(**defaults),
            opts.Rectangles(**defaults),
            opts.Sankey(**defaults),
            opts.Segments(**defaults),
        )

    def timeline(self, rects=True, segments=False, points=True):
        # Calculate matching rows and inc time
        self.trace.match_rows()
        self.trace.calc_inc_time()

        # Get events dataframe
        events = self.trace.events.copy(deep=False)
        events = events.drop("Attributes", axis=1)

        # TODO: filter ranks

        # Construct element-specific dataframes
        if rects:
            func = events[events["Event Type"] == "Entry"].copy(deep=False)
            func["y"] = func["Process ID"].astype("int")
            func["y0"] = func["y"] - 0.5
            func["y1"] = func["y"] + 0.5

        if points:
            inst = events[-events["Event Type"].isin(["Entry", "Exit"])]

        if segments:
            sends = events[events["Event Type"].isin(["MpiSend", "MpiIsend"])]
            recvs = events[events["Event Type"].isin(["MpiRecv", "MpiIrecv"])]
            comm = pd.DataFrame()
            comm["x0"] = sends["Timestamp (ns)"].values
            comm["y0"] = sends["Process ID"].values
            comm["x1"] = recvs["Timestamp (ns)"].values
            comm["y1"] = recvs["Process ID"].values

        def _callback(x_range):
            if x_range is None or pd.isna(x_range[0]) or pd.isna(x_range[1]):
                x_range = (
                    events["Timestamp (ns)"].min(),
                    events["Timestamp (ns)"].max(),
                )

            x_min, x_max = x_range
            viewport_size = x_max - x_min

            x_min_buff = x_min - (viewport_size * 0.25)
            x_max_buff = x_max + (viewport_size * 0.25)
            min_width = viewport_size * (1 / 1920)

            # Filter dataframes constructed above based on current
            # x_range, and generate HoloViews elements
            if rects:
                func_filtered = func[
                    (func["Matching Timestamp"] > x_min_buff)
                    & (func["Timestamp (ns)"] < x_max_buff)
                    & (func["Inc Time"] > min_width)
                ]

                if len(func_filtered) > 5000:
                    func_filtered = func_filtered.sample(n=5000)

                hv_rects = hv.Rectangles(
                    func_filtered, ["Timestamp (ns)", "y0", "Matching Timestamp", "y1"]
                )

            if points:
                inst_filtered = inst[
                    (inst["Timestamp (ns)"] > x_min_buff)
                    & (inst["Timestamp (ns)"] < x_max_buff)
                ]

                if len(inst_filtered) > 5000:
                    inst_filtered = inst_filtered.sample(n=5000)

                hv_points = hv.Points(inst_filtered, ["Timestamp (ns)", "Process ID"])

            if segments:
                comm_filtered = comm[
                    ((comm["x0"] < x_max_buff) & (comm["x1"] > x_min_buff))
                    & (comm["x1"] - comm["x0"] > min_width)
                ]

                hv_segments = hv.Segments(comm_filtered, ["x0", "y0", "x1", "y1"])

            # Generate HoloViews elements from filtered dataframes
            return (
                (hv_rects if rects else hv.Curve([]))
                * (hv_points if points else hv.Curve([]))
                * (hv_segments if segments else hv.Curve([]))
            )

        return (
            hv.DynamicMap(_callback, streams=[hv.streams.RangeX()])
            .opts(
                opts.Points(
                    size=6,
                    color="rgba(60,60,60,0.3)",
                    tools=[
                        HoverTool(
                            tooltips={
                                "Event Type": "@{Event_Type}",
                                "Timestamp": "@Timestamp",
                            },
                        )
                    ],
                ),
                opts.Segments(color="k"),
                opts.Rectangles(
                    cmap=self.cmap,
                    line_width=0.2,
                    line_color="white",
                    tools=[
                        HoverTool(
                            point_policy="follow_mouse",
                            tooltips={
                                "Name": "@Name",
                                "Inc Time": "@{Inc_Time}",
                            },
                        ),
                        "xbox_zoom",
                        "tap",
                    ],
                    fill_color="Name",
                    active_tools=["xwheel_zoom"],
                    default_tools=["xpan", "xwheel_zoom"],
                ),
                opts(
                    height=clamp(len(self.ranks) * 30 + 100, 150, 1000),
                    invert_yaxis=True,
                    responsive=True,
                    padding=(0, (0, 0.5)),
                    xaxis="top",
                    xlabel="",
                    ylabel="",
                    yformatter=PrintfTickFormatter(format="Process %d"),
                    yticks=self.ranks.astype("int"),
                    legend_position="right",
                    show_grid=True,
                ),
            )
            .relabel("Overview timeline")
        )

    def utilization(self):
        """Shows value of certain metric over time (timeseries)"""

        data = time_series()

        area = hv.Area(data)
        curve = hv.Curve(data)

        return (
            (curve * area)
            .opts(
                opts.Curve(
                    tools=[
                        HoverTool(
                            mode="vline",
                            tooltips={"Time": "@x", "Utilization": "@y{0.}%"},
                        )
                    ]
                ),
                opts(xlabel="Time", ylabel="% utilization", height=300),
            )
            .relabel("% CPU utilization over time (process 0)")
        )

    def time_profile(self):
        # Compute time profile from API function
        profile = fake_time_profile(samples=100, num_bins=64, functions=self.functions)

        # Generate bars
        return (
            hv.Bars(profile, kdims=["bin", "function"])
            .sort()
            .aggregate(function=np.sum)
            .opts(
                stacked=True,
                xlabel="Time interval",
                ylabel="Time contribution (ms)",
                cmap=self.cmap,
                legend_position="right",
                line_width=0.2,
                line_color="white",
                xformatter=None,
                tools=[
                    HoverTool(
                        tooltips={
                            "Name": "@function",
                            "Time spent": "@time ns",
                            "Time interval": "@bin",
                        }
                    )
                ],
                height=300,
            )
            .relabel("Excl. time contributed by each function per time interval")
        )

    def process_summary(self):
        """Bar graph of total time spent by function per process"""

        # Get function summary
        funcs = self.trace.flat_profile(
            metric="Exc Time", groupby_column=["Name", "Process ID"]
        ).reset_index()

        # Generate bars
        # See https://holoviews.org/reference/elements/bokeh/Bars.html
        return (
            hv.Bars(funcs, kdims=["Process ID", "Name"])
            .aggregate(function=np.sum)
            .opts(
                height=len(self.ranks) * 90,
                stacked=True,
                cmap=self.cmap,
                legend_position="right",
                invert_axes=True,
                invert_yaxis=True,
                yformatter=PrintfTickFormatter(format="Process %d"),
                tools=[
                    HoverTool(
                        tooltips={
                            "Name": "@{Name}",
                            "Total time": "@{Exc_Time}",
                            "Process ID": "@{Process_ID}",
                        },
                        point_policy="follow_mouse",
                    )
                ],
                default_tools=["xpan", "xwheel_zoom"],
                active_tools=["xpan", "xwheel_zoom"],
                line_width=0.2,
                line_color="white",
                ylabel="Time",
                xlabel="",
                show_grid=True,
            )
            .relabel("Total excl. time per function per process")
        )

    def function_summary(self):
        """Bar graph of total time spent by function per process"""

        # Get function summary
        funcs = self.trace.flat_profile(
            metric="Exc Time", groupby_column=["Name"]
        ).reset_index()

        # Generate bars
        # See https://holoviews.org/reference/elements/bokeh/Bars.html
        return (
            hv.Bars(funcs)
            .opts(
                height=len(self.ranks) * 90,
                cmap=self.cmap,
                color="Name",
                legend_position="right",
                invert_axes=True,
                invert_yaxis=True,
                tools=[
                    HoverTool(
                        tooltips={
                            "Name": "@{Name}",
                            "Total time": "@{Exc_Time}",
                        },
                        point_policy="follow_mouse",
                    )
                ],
                default_tools=["xpan", "xwheel_zoom"],
                active_tools=["xpan", "xwheel_zoom"],
                line_width=0.2,
                line_color="white",
                ylabel="Time",
                xlabel="",
                show_grid=True,
            )
            .relabel("Total excl. time per function")
        )

    def histogram(self):
        pass

    def comm_heatmap(self, comm_type="counts", label_threshold=16, cmap="blues"):
        """Heatmap of process-to-process message volume"""
        comm_matrix = self.trace.comm_matrix(comm_type)

        num_ranks = comm_matrix.shape[0]
        bounds = (-0.5, -0.5, num_ranks - 0.5, num_ranks - 0.5)

        # Generate heatmap image
        image = hv.Image(comm_matrix, bounds=bounds).opts(
            width=clamp(160 + num_ranks * 35, 300, 850),
            height=clamp(250 + num_ranks * 25, 200, 650),
            responsive=False,
            colorbar=True,
            colorbar_position="bottom",
            cmap=cmap,
            tools=[
                HoverTool(
                    tooltips={
                        "Process IDs": "$x{0.} → $y{0.}",
                        "Count": "@image messages",
                    }
                )
            ],
            xlabel="Sender",
            ylabel="Receiver",
            xticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
            yticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
            padding=0,
            title="Communication Heatmap",
            yformatter=PrintfTickFormatter(format="Process %d"),
            xformatter=PrintfTickFormatter(format="Process %d"),
            xaxis="top",
            invert_yaxis=True,
            xrotation=60,
        )

        if num_ranks > label_threshold:
            return image

        # Generate labels
        max_val = np.amax(comm_matrix)
        labels = hv.Labels(image).opts(
            text_color="z",
            color_levels=[0, max_val / 2, max_val],
            cmap=["black", "white"],
            text_font_size="8pt",
        )

        return image * labels

    def tree(self):
        pass
