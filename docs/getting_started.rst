.. Copyright 2022-2023 Parallel Software and Systems Group, University of
   Maryland. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

***************
Getting Started
***************

Introduction
=============


Pipit is an open-source Python library for analyzing parallel execution
traces. It supports various trace formats, including OTF2, HPCToolkit, and
Nsight, and implements several operations for in-depth exploration and
analysis of trace data. Built on top of Pandas, Pipit is highly scalable and
extensible for custom analysis needs.

Compared to other tools, Pipit offers the following benefits:

- Provides a unified API for reading/analyzing outputs of different tracing tools, which can be extended with a custom reader

- Offers a variety of high-performance functions for in-depth performance analysis, including flat profile, pattern detection, outlier analysis, and load imbalance

- Offers interactive visualizations to display the results of its analysis functions

- Enables easy comparison of results across multiple traces or processes, with features like cross-trace performance and communication analysis

- Is built on top of Pandas, allowing users to perform custom data analysis and visualization without being limited to implemented operations and views


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
