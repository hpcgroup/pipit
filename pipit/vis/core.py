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
    time_series,
    clamp,
    fake_time_profile,
    in_notebook,
)

import numpy as np


class Vis:
    """Contains visualization data and functions for a Trace"""

    # TODO: aggregate common opts in __init__ (`defaults`)
    def __init__(self, trace, server=(not in_notebook())):
        """Initialize environment for visualization"""
        self.trace = trace
        self.server = server

        # Initialize holoviews and custom css
        self.css = """
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
        """

        hv.extension("bokeh", logo=False, css=self.css)

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
            height=300,
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
            opts.Histogram(**defaults),
            opts.Image(**defaults),
            opts.Labels(**defaults),
            opts.Points(**defaults),
            opts.Polygons(**defaults),
            opts.Rectangles(**defaults),
            opts.Sankey(**defaults),
            opts.Segments(**defaults),
        )

    def _view(self, element):
        """Launches server if `self.server`, else returns holoviews element"""
        if self.server:
            import panel as pn

            pn.extension(raw_css=[self.css])
            pn.panel(element).show()
            return

        return element

    def timeline(self, rects=True, segments=False, points=True):
        # Calculate matching rows and inc time
        self.trace.match_rows()
        self.trace.calc_inc_time()

        # Get events dataframe
        events = self.trace.events.copy(deep=False)
        events = events.drop("Attributes", axis=1)
        events = events.drop("Graph_Node", axis=1)

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

        return self._view(
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

        return self._view(
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
        return self._view(
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
        return self._view(
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
        return self._view(
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

    def comm_summary(self):
        """Bar graph of total message size sent by process

        Similar to comm_heatmap but aggregated per process
        """

        # Get communication summary
        messages = self.trace.events[
            self.trace.events["Event Type"].isin(["MpiSend", "MpiIsend"])
        ].copy(deep=False)
        messages["size"] = messages["Attributes"].apply(lambda x: x["msg_length"])
        processes = messages.groupby("Process ID")[["size"]].sum().reset_index()

        # Generate bars
        # See https://holoviews.org/reference/elements/bokeh/Bars.html
        return self._view(
            hv.Bars(processes)
            .opts(
                height=len(self.ranks) * 90,
                cmap=self.cmap,
                # color="Name",
                # legend_position="right",
                invert_axes=True,
                invert_yaxis=True,
                tools=[
                    HoverTool(
                        tooltips={
                            "Process ID": "@{Process_ID}",
                            "Total message size": "@{size}",
                        },
                        point_policy="follow_mouse",
                    )
                ],
                default_tools=["xpan", "xwheel_zoom"],
                active_tools=["xpan", "xwheel_zoom"],
                line_width=0.2,
                line_color="white",
                ylabel="Total message",
                xlabel="",
                show_grid=True,
                padding=2,
                yformatter=PrintfTickFormatter(format="Process %d"),
            )
            .relabel("Total message size sent per process")
        )

    def message_size_hist(self):
        messages = self.trace.events[
            self.trace.events["Event Type"].isin(["MpiSend", "MpiIsend"])
        ]
        sizes = messages["Attributes"].map(lambda x: x["msg_length"])

        freq, edges = np.histogram(sizes, 64)
        return self._view(
            hv.Histogram((edges, freq))
            .opts(xlabel="Message size", ylabel="Number of messages", tools=["hover"])
            .relabel("Histogram of message sizes")
        )

    def comm_over_time(self, weighted=True):
        messages = self.trace.events[
            self.trace.events["Event Type"].isin(["MpiSend", "MpiIsend"])
        ]
        times = messages["Timestamp (ns)"]
        sizes = messages["Attributes"].map(lambda x: x["msg_length"])

        freq, edges = np.histogram(times, 64, weights=(sizes if weighted else None))
        return self._view(
            hv.Histogram((edges, freq))
            .opts(xlabel="Time", ylabel="Number of messages", tools=["hover"])
            .relabel("Communication over time")
        )

    def occurence_over_time(self, name="MpiSend"):
        events_filtered = self.trace.events[self.trace.events["Name"] == name]
        times = events_filtered["Timestamp (ns)"]

        freq, edges = np.histogram(times, 64)
        return self._view(
            hv.Histogram((edges, freq))
            .opts(xlabel="Time", ylabel="Frequency", tools=["hover"])
            .relabel("Occurence over time")
        )

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

        return self._view(image * labels)

    def cct(self):
        self.trace.match_rows()
        self.trace.calc_inc_time()
        self.trace.calc_exc_time()

        df = self.trace.events[self.trace.events["Process ID"] == 0]
        df = df[df["Event Type"] == "Entry"][["Name", "Depth"]]
        df["Depth"] = df["Depth"].astype("int")
        df = df.groupby(["Name"]).max().reset_index()
        df = df.dropna()

        df.loc[len(df.index)] = ["main", -1]

        df = df.sort_values(by="Depth", ignore_index=True)

        idx = df.groupby("Depth").cumcount()
        count = df.groupby("Depth")["Depth"].transform("count")

        df["x"] = idx - (count / 2) + 0.5

        source = [0, 0, 0, 0]
        target = [1, 2, 3, 4]

        x = df["x"].values
        y = df["Depth"].values
        i = np.arange(0, 5)
        label = df["Name"].values

        nodes = hv.Nodes((x, y, i, label), vdims="Name")
        graph = hv.Graph(((source, target), nodes))
        label = hv.Labels(graph.nodes, ["x", "y"], "Name")

        return self._view(
            (graph * label)
            .opts(
                opts.Graph(
                    invert_yaxis=True,
                    responsive=True,
                    height=200,
                    yaxis=None,
                    xaxis=None,
                    padding=(1, 0.5),
                    cmap=self.cmap,
                    node_color="Name",
                    edge_line_width=1,
                ),
                opts.Labels(
                    text_font_size="9pt",
                    xoffset=0.08,
                    text_align="left",
                ),
            )
            .relabel("Calling context tree")
        )
