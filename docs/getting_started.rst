.. Copyright 2022-2023 Parallel Software and Systems Group, University of
   Maryland. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

***************
Getting Started
***************

Introduction
=============


Pipit is an open-source Python library for analyzing parallel execution
traces.

It supports various trace formats, including OTF2, HPCToolkit, and
Nsight. Pipit implements several operations for in-depth exploration and
analysis of trace data. Built on top of Pandas, Pipit is highly scalable
and extensible for custom analysis needs.

Compared to other trace analysis/visualization tools, Pipit:

-  Provides a unified interface for outputs of many different tracing
   tools, which can be extended by writing custom *readers*

-  Provides a programmatic API, allowing users to write simple code for
   trace analysis, allowing for flexibility of exploration, scalability,
   reproducibility, and automation/ saving of workflows using Jupyter
   notebooks

-  Automates certain common performance analysis tasks for analyzing
   single and multiple executions

-  Provides a visualization API that can be used to display analysis
   results

Prerequisites
=============

Pipit has the following minimum requirements, which must be installed before
pipit is run:

#. Python 2 (2.7) or 3 (3.5 - 3.10)
#. pandas

Pipit is available on `GitHub <https://github.com/hpcgroup/pipit>`_


Installation
============

You can get pipit from its `GitHub repository
<https://github.com/hpcgroup/pipit>`_ using this command:

.. code-block:: console

  $ git clone https://github.com/hpcgroup/pipit.git

This will create a directory called ``pipit``.

Install and Build Pipit
-----------------------



Check Installation
------------------

Once pipit is installed, you should be able to import it into the Python interpreter and use it in interactive mode:

.. code-block:: console

  $ python
  Python 3.8.12 (default, Jul 10 2022, 16:35:51)
  [GCC 9.4.0] on linux
  Type "help", "copyright", "credits" or "license" for more information.
  >>>

Typing ``import pipit`` at the prompt should succeed without any error
messages:

.. code-block:: console

  >>> import pipit
  >>>


Supported data formats
======================

Currently, pipit supports the following data formats as input:

* `HPCToolkit <http://hpctoolkit.org/index.html>`_ trace
* OTF2
* Nsight
* Projections
