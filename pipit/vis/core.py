import random

import holoviews as hv
import numpy as np
from bokeh.models import (
    HoverTool,
    AdaptiveTicker,
    PrintfTickFormatter,
    NumeralTickFormatter,
)
from holoviews import opts
import pandas as pd

from .util import (
    DEFAULT_FUNCTION_PALETTE,
    generate_cmap,
    time_series,
    clamp,
    fake_time_profile,
    in_notebook,
    time_tick_formatter,
    time_hover_formatter,
    size_tick_formatter,
    size_hover_formatter,
    process_tick_formatter,
    get_height,
)


class Vis:
    """Contains visualization data and functions for a Trace"""

    def __init__(self, trace, server=(not in_notebook())):
        """Initialize environment for visualization."""
        self.trace = trace
        self.server = server

        # Initialize HoloViews with and Bokeh backend and custom css
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
        random.shuffle(DEFAULT_FUNCTION_PALETTE)
        self.cmap = generate_cmap(self.functions, DEFAULT_FUNCTION_PALETTE)

        # Apply default opts for HoloViews elements
        # https://holoviews.org/user_guide/Applying_Customizations.html#session-specific-options # noqa: 501
        # https://holoviews.org/user_guide/Customizing_Plots.html#plot-hooks
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

        self.default_color = "#E5AE38"

        opts.defaults(
            opts.Area(**defaults, color=self.default_color),
            opts.Bars(**defaults, color=self.default_color),
            opts.Bivariate(**defaults),
            opts.BoxWhisker(**defaults),
            opts.Chord(**defaults),
            opts.Contours(**defaults),
            opts.Curve(**defaults, color=self.default_color),
            opts.Distribution(**defaults),
            opts.Graph(**defaults),
            opts.Histogram(**defaults, color=self.default_color),
            opts.Image(**defaults),
            opts.Labels(**defaults),
            opts.Points(**defaults),
            opts.Polygons(**defaults),
            opts.Rectangles(**defaults),
            opts.Sankey(**defaults),
            opts.Segments(**defaults),
        )

    def _view(self, element):
        """Internal function used to wrap return values in vis functions.

        Launches server for element if `self.server`, otherwise returns element.
        """
        if self.server:
            import panel as pn

            pn.extension(raw_css=[self.css])
            pn.panel(element).show()
            return

        return element

    def timeline(self, max_num_ranks=17, rects=True, segments=False, points=True):
        """Generates overview timeline of trace events.

        Requires live Python kernel for full functionality.

        Args:
            max_num_ranks (int): Maximum number of ranks to display
            rects (bool): Whether to generate hv.Rectangles for functions
            segments (bool): Whether to generate hv.Segments for messages
            points (bool): Whether to generate hv.Points for instant events
        """
        # Calculate matching rows and inc time
        self.trace.match_rows()
        self.trace.calc_inc_time()

        # Get events dataframe
        events = self.trace.events.copy(deep=False)
        events = events.drop("Attributes", axis=1)
        events = events.drop("Graph_Node", axis=1)

        # Sample ranks if needed
        if len(self.ranks) > max_num_ranks:
            idx = np.round(np.linspace(0, len(self.ranks), max_num_ranks)).astype(int)
            idx[-1] -= 1
            events = events[events["Process ID"].isin(self.ranks[idx])]

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

        # https://holoviews.org/reference/containers/bokeh/DynamicMap.html
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

            # Filter dataframes based on x_range, generate HoloViews elements
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

        # https://holoviews.org/user_guide/Custom_Interactivity.html
        return self._view(
            hv.DynamicMap(_callback, streams=[hv.streams.RangeX()])
            .opts(
                opts.Points(
                    size=6,
                    color="rgba(60,60,60,0.3)",
                    tools=[
                        HoverTool(
                            tooltips={
                                "Event type": "@{Event_Type}",
                                "Timestamp": "@{Timestamp (ns)}{custom}",
                            },
                            formatters={"@{Timestamp (ns)}": time_hover_formatter},
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
                                "Incl. time": "@{Inc_Time}{custom}",
                            },
                            formatters={"@{Inc_Time}": time_hover_formatter},
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
                    padding=(0, (0, 1)),
                    xaxis="top",
                    xformatter=time_tick_formatter,
                    xlabel="",
                    ylabel="",
                    yformatter=process_tick_formatter,
                    yticks=self.ranks.astype("int"),
                    legend_position="right",
                    show_grid=True,
                ),
            )
            .relabel("Overview timeline")
        )

    def utilization(self):
        """Generates line chart showing CPU utilization over time."""
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
                            tooltips={"Time": "@x{custom}", "Utilization": "@y{0.}%"},
                            formatters={"@x": time_hover_formatter},
                        )
                    ],
                ),
                opts.Area(
                    alpha=0.2,
                ),
                opts(
                    xlabel="Time",
                    ylabel="% utilization",
                    xformatter=time_tick_formatter,
                    yformatter=NumeralTickFormatter(format="0%"),
                    show_grid=True,
                ),
            )
            .relabel("% CPU utilization over time (process 0)")
        )

    def time_profile(self):
        """Generates bar graph of function durations per time interval.

        Uses :func:`~pipit.Trace.time_profile` for calculation.
        """
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
                ylabel="Time contribution",
                yformatter=time_tick_formatter,
                cmap=self.cmap,
                legend_position="right",
                line_width=0.2,
                line_color="white",
                xformatter=PrintfTickFormatter(format=""),
                tools=[
                    HoverTool(
                        tooltips={
                            "Name": "@function",
                            "Time spent": "@time{custom}",
                            "Time interval": "@bin",
                        },
                        formatters={"@time": time_hover_formatter},
                    )
                ],
            )
            .relabel("Excl. time contributed by each function per time interval")
        )

    def process_summary(self):
        """Generates bar graph of function durations per process.

        Uses :func:`~pipit.Trace.flat_profile` for calculation.
        """
        # Get function summary
        funcs = self.trace.flat_profile(
            metric="Exc Time", groupby_column=["Name", "Process ID"]
        ).reset_index()

        # Generate bars
        return self._view(
            hv.Bars(funcs, kdims=["Process ID", "Name"])
            .aggregate(function=np.sum)
            .opts(
                height=get_height(len(self.ranks)),
                stacked=True,
                cmap=self.cmap,
                legend_position="right",
                invert_axes=True,
                invert_yaxis=True,
                yformatter=process_tick_formatter,
                tools=[
                    HoverTool(
                        tooltips={
                            "Name": "@{Name}",
                            "Total time": "@{Exc_Time}{custom}",
                            "Process ID": "@{Process_ID}",
                        },
                        formatters={"@{Exc_Time}": time_hover_formatter},
                        point_policy="follow_mouse",
                    )
                ],
                default_tools=["xpan", "xwheel_zoom"],
                active_tools=["xpan", "xwheel_zoom"],
                line_width=0.2,
                line_color="white",
                ylabel="Time",
                xlabel="",
                xformatter=time_tick_formatter,
                show_grid=True,
            )
            .relabel("Total excl. time per function per process")
        )

    def function_summary(self):
        """Generates bar graph of total duration for each function.

        Uses :func:`~pipit.Trace.flat_profile` for calculation.
        """
        # Get function summary
        funcs = self.trace.flat_profile(
            metric="Exc Time", groupby_column=["Name"]
        ).reset_index()

        # Generate bars
        return self._view(
            hv.Bars(funcs)
            .sort(["Exc Time"], reverse=True)
            .opts(
                height=get_height(len(self.functions)),
                cmap=self.cmap,
                color="Name",
                legend_position="right",
                invert_axes=True,
                invert_yaxis=True,
                tools=[
                    HoverTool(
                        tooltips={
                            "Name": "@{Name}",
                            "Total time": "@{Exc_Time}{custom}",
                        },
                        formatters={"@{Exc_Time}": time_hover_formatter},
                        point_policy="follow_mouse",
                    )
                ],
                default_tools=["xpan", "xwheel_zoom"],
                active_tools=["xpan", "xwheel_zoom"],
                line_width=0.2,
                line_color="white",
                ylabel="Time",
                xlabel="",
                xformatter=time_tick_formatter,
                show_grid=True,
            )
            .relabel("Total excl. time per function")
        )

    def comm_summary(self):
        """Generates bar graph of total message size sent per process"""
        # Get communication summary
        messages = self.trace.events[
            self.trace.events["Event Type"].isin(["MpiSend", "MpiIsend"])
        ].copy(deep=False)
        messages["size"] = messages["Attributes"].apply(lambda x: x["msg_length"])
        processes = messages.groupby("Process ID")[["size"]].sum().reset_index()

        # Generate bars
        return self._view(
            hv.Bars(processes)
            .opts(
                height=get_height(len(self.ranks)),
                cmap=self.cmap,
                # color="Name",
                # legend_position="right",
                invert_axes=True,
                invert_yaxis=True,
                tools=[
                    HoverTool(
                        tooltips={
                            "Process ID": "@{Process_ID}",
                            "Total sent": "@{size}{custom}",
                        },
                        formatters={"@{size}": size_hover_formatter},
                        point_policy="follow_mouse",
                    )
                ],
                default_tools=["xpan", "xwheel_zoom"],
                active_tools=["xpan", "xwheel_zoom"],
                line_width=0.2,
                line_color="white",
                ylabel="Size",
                xlabel="",
                xformatter=size_tick_formatter,
                show_grid=True,
                padding=2,
                yformatter=process_tick_formatter,
            )
            .relabel("Total message size sent per process")
        )

    def message_size_hist(self):
        """Generates histogram of message frequency per size."""
        messages = self.trace.events[
            self.trace.events["Event Type"].isin(["MpiSend", "MpiIsend"])
        ]
        sizes = messages["Attributes"].map(lambda x: x["msg_length"])

        freq, edges = np.histogram(sizes, 64)
        return self._view(
            hv.Histogram((edges, freq))
            .opts(
                xlabel="Message size",
                xformatter=size_tick_formatter,
                ylabel="Number of messages",
                yformatter=NumeralTickFormatter(format="0a"),
                tools=[
                    HoverTool(
                        tooltips={
                            "Message size": "@x{custom}",
                            "Frequency": "@Frequency",
                        },
                        formatters={
                            "@x": size_hover_formatter,
                        },
                    )
                ],
            )
            .relabel("Histogram of message sizes")
        )

    def comm_over_time(self, weighted=True):
        """Generates histogram of message frequency per time interval.

        Args:
            weighted (bool): Whether to weigh histogram by message size.
        """
        messages = self.trace.events[
            self.trace.events["Event Type"].isin(["MpiSend", "MpiIsend"])
        ]
        times = messages["Timestamp (ns)"]
        sizes = messages["Attributes"].map(lambda x: x["msg_length"])

        freq, edges = np.histogram(times, 64, weights=(sizes if weighted else None))
        return self._view(
            hv.Histogram((edges, freq))
            .opts(
                xlabel="Time",
                xformatter=time_tick_formatter,
                ylabel="Message size",
                yformatter=size_tick_formatter,
                tools=[
                    HoverTool(
                        tooltips={
                            "Bin": "@x{custom}",
                            "Message size": "@Frequency{custom}",
                        },
                        formatters={
                            "@x": time_hover_formatter,
                            "@Frequency": size_hover_formatter,
                        },
                    )
                ],
            )
            .relabel("Communication over time")
        )

    def occurence_over_time(self):
        """Generates histogram of event occurence per time interval."""
        events_filtered = self.trace.events
        times = events_filtered["Timestamp (ns)"]

        freq, edges = np.histogram(times, 64)
        return self._view(
            hv.Histogram((edges, freq))
            .opts(
                xlabel="Time",
                xformatter=time_tick_formatter,
                ylabel="Frequency",
                yformatter=NumeralTickFormatter(format="0a"),
                tools=[
                    HoverTool(
                        tooltips={"Bin": "@x{custom}", "Frequency": "@Frequency"},
                        formatters={"@x": time_hover_formatter},
                    )
                ],
            )
            .relabel("Occurence over time")
        )

    def comm_heatmap(self, comm_type="counts", label_threshold=16, cmap="YlOrRd"):
        """Generates heatmap of process-to-process message volume.

        Uses :func:`~pipit.Trace.comm_matrix` function for calculation.

        Args:
            comm_type (str): "counts" or "bytes"
            label_threshold (int): Number of ranks, above which labels are not displayed
            cmap (str): Name of HoloViews colormap to use
        """
        comm_matrix = self.trace.comm_matrix(comm_type)

        num_ranks = comm_matrix.shape[0]
        bounds = (-0.5, -0.5, num_ranks - 0.5, num_ranks - 0.5)

        # Generate heatmap image
        image = hv.Image(comm_matrix, bounds=bounds).opts(
            width=clamp(300 + num_ranks * 35, 400, 850),
            height=clamp(250 + num_ranks * 25, 200, 650),
            responsive=False,
            colorbar=True,
            colorbar_position="bottom",
            cmap=cmap,
            tools=[
                HoverTool(
                    tooltips={
                        "Sender": "Process $x{0.}",
                        "Receiver": "Process $y{0.}",
                        "Count": "@image messages",
                    }
                )
            ],
            xlabel="Sender",
            ylabel="Receiver",
            xticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
            yticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
            title="Total message counts per process pair",
            yformatter=process_tick_formatter,
            xformatter=process_tick_formatter,
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
            text_font_size="9.5pt",
        )

        return self._view(image * labels)

    def cct(self):
        """Generates visualization of trace's calling context tree.

        Uses :py:`~pipit.Trace.cct`.
        """
        # Nodes
        name = []
        depth = []
        index = []
        num_calls = []

        # Edges
        source = []
        target = []

        # Perform depth-first search to populate above arrays
        visited = set()

        def dfs(node):
            if node not in visited:
                name.append(node.name)
                depth.append(node.get_level())
                index.append(node.name_id)
                num_calls.append(len(node.calling_context_ids))

                for child in node.children:
                    if child != node:
                        source.append(node.name_id)
                        target.append(child.name_id)

                        dfs(child)

                visited.add(node)

        for root in self.trace.cct.roots:
            dfs(root)

        # Construct df for HoloViews `Nodes` element
        nodes = pd.DataFrame()
        nodes["Name"] = name
        nodes["Depth"] = depth
        nodes["index"] = index
        nodes["num_calls"] = num_calls

        # Calculate position for each node
        group_index = nodes.groupby("Depth").cumcount()
        group_size = nodes.groupby("Depth")["Depth"].transform("count")
        nodes["x"] = (group_index - (group_size / 2) + 0.5) * (1 / group_size) * 10

        nodes = nodes.set_index("index")

        # Generate hv elements
        # https://holoviews.org/user_guide/Network_Graphs.html
        hv_nodes = hv.Nodes(nodes, ["x", "Depth", "index"])
        graph = hv.Graph(((source, target), hv_nodes))
        label = hv.Labels(graph.nodes, ["x", "Depth"], "Name")

        max_row_count = group_size.max().item()

        return self._view(
            (graph * label)
            .opts(
                opts.Graph(
                    invert_yaxis=True,
                    yaxis=None,
                    xaxis=None,
                    padding=(0.05, 0.3),
                    node_color="Name",
                    edge_line_width=1,
                    invert_axes=True,
                    cmap=self.cmap,
                    height=clamp(max_row_count * 35 + 10, 270, 600),
                    edge_color="gray",
                    tools=[
                        HoverTool(
                            tooltips={"Name": "@Name", "Call count": "@num_calls"}
                        )
                    ],
                ),
                opts.Labels(
                    text_font_size="8pt",
                    yoffset=3 / max_row_count + 0.1,
                ),
            )
            .relabel("Calling context tree")
        )
