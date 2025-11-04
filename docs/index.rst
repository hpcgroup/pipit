.. Copyright 2022-2023 Parallel Software and Systems Group, University of
   Maryland. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

.. pipit documentation master file, created by
   sphinx-quickstart on Sun Nov 13 14:19:38 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

#####
Pipit
#####

Pipit is an open-source Python library for analyzing parallel execution
traces. It supports various trace formats, including OTF2, HPCToolkit, and
Nsight, and implements several operations for in-depth exploration and
analysis of trace data. Built on top of Pandas, Pipit is highly scalable and
extensible for custom analysis needs.

You can get pipit from its `GitHub repository
<https://github.com/hpcgroup/pipit>`_:

.. code-block:: console

  $ git clone https://github.com/hpcgroup/pipit.git


.. toctree::
   :maxdepth: 2
   :caption: User Docs

   getting_started
   user_guide

.. toctree::
   :maxdepth: 2
   :caption: Developer Docs

   developer_guide

.. toctree::
   :maxdepth: 2
   :caption: API Docs

   Pipit API Docs <source/pipit>


##################
Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
