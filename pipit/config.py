# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# Private dict that stores global config values
_config = {
    "LOG_LEVEL": "INFO",
    "NOTEBOOK_URL": "http://localhost:8888",
}


# Public function to get config value
def get_config(key: str, default: any = None):
    return _config.get(key, default)


# Public function to set config value
def set_config(key: str, value: any):
    _config[key] = value
