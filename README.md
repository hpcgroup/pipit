# Pipit

[![Build Status](https://github.com/hpcgroup/pipit/actions/workflows/unit-tests.yaml/badge.svg)](https://github.com/hpcgroup/pipit/actions)
[![docs](https://readthedocs.org/projects/pipit/badge/?version=latest)](https://pipit.readthedocs.io/en/latest/?badge=latest)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A Python-based library for analyzing execution traces from parallel programs.

## Visual Interface

Pipit's visual interface requires you to install some Python libraries:

```
pip install bokeh==2.4.3 datashader
```

Also, we highly recommend using Pipit in a Jupyter notebook. You can install Jupyter with pip:

```
pip install notebook
```

Launching the notebook:
```
jupyter-notebook
```

**Note: there are some known issues using with VSCode/Firefox. Only use Chrome/Safari for now.**

Once these are installed, you should be good to go:

```
trace = pp.Trace.from_otf2("path/to/trace")

trace.plot_timeline()
```

If you are using a notebook, the plot will be displayed in the output cell.

If not, Pipit will launch an HTTP server and display the plot in a new browser tab. To stop the server, press `Ctrl+C` in the Python shell.

### Roadmap
#### Timeline:
- [ ] Add a dotted line
- [ ] Fix nsight reader bug (num procs = 1)
- [x] ~~Trim legend labels~~
- [x] ~~Click --> draw arrow (display messages on click)~~
- [x] ~~Truncate function names~~
- [x] ~~Get rid of extra processes~~
- [x] ~~Box zoom~~
- [x] ~~Make it 2/3 the height~~
- [x] ~~Have some space between process 0 and 1 (20px)~~
- [x] ~~Call "calc_exc_time" explicitly, show all metrics on hover~~
- [x] ~~Fix hover bug (stacked tooltips)~~
- [x] ~~Display legend (maybe on top or bottom), 2D list (not possible with bokeh 2.4.3)~~

#### Comm matrix:
- [ ] Get rid of unit from labels
- [x] ~~Don't display zero on labels~~
- [x] ~~Diverging colormap	~~

#### Histogram:
- [ ] Messages by size and frequency, and by histogram			
- [x] ~~Make units constant instead of changing~~
- [x] ~~Show tick in the ends instead of the middle~~
- [x] ~~Get nicer ticks~~
- [x] ~~Count >> number of messages~~

#### Time profile:
- [x] ~~Trim legend labels~~
- [x] ~~x-axis labels messed up with high num_bins~~
- [x] ~~Use numbers instead of categories on x axis~~
- [x] ~~Remove spacing between bars~~
- [x] ~~Ticks on edges instead of middle~~

#### Flat profile:
- [x] ~~Trim y-axis labels~~

#### All:
- [ ] Test everything for multiple traces (# procs, # threads, filtered, OTF/HPCT/Projections, etc)
- [ ] Test everything for kripke-8 (large # of functions)
- [x] ~~Don't display title (for paper)~~
- [x] ~~Use consistent color palette across different plots/traces~~
- [x] ~~Send isend same color~~
- [x] ~~Recv irecv same color~~

### Contributing

Pipit is an open source project. We welcome contributions via pull requests,
and questions, feature requests, or bug reports via issues.

### License

Pipit is distributed under the terms of the MIT License.

All contributions must be made under the the MIT license. Copyrights in the
Pipit project are retained by contributors.  No copyright assignment is
required to contribute to Pipit.

See [LICENSE](https://github.com/pssg-int/trace-analysis/blob/develop/LICENSE)
for details.

SPDX-License-Identifier: MIT
