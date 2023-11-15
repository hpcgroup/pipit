# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from setuptools import setup

setup(
    name="pipit",
    version="0.1.0",
    description="A Python library for analyzing parallel execution traces",
    url="https://github.com/hpcgroup/pipit",
    author="Abhinav Bhatele",
    author_email="bhatele@cs.umd.edu",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
    ],
    keywords="distributed computing, parallel computing, GPU traces",
    packages=["pipit", "pipit.readers", "pipit.tests", "pipit.util", "pipit.writers"],
    install_requires=[
        "numpy",
        "otf2",
        "pandas",
    ],
)
