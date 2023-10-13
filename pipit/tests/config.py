# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pipit as pp


def test_get_config():
    # Test getting an existing config value
    assert pp.get_config("LOG_LEVEL") == "INFO"

    # Test getting a non-existent config value with default
    assert pp.get_config("NON_EXISTENT", "default") == "default"

    # Test getting a non-existent config value without default
    assert pp.get_config("NON_EXISTENT") is None


def test_set_config():
    # Test setting a new config value
    pp.set_config("NEW_CONFIG", "new_value")
    assert pp.get_config("NEW_CONFIG") == "new_value"

    # Test setting an existing config value
    pp.set_config("LOG_LEVEL", "DEBUG")
    assert pp.get_config("LOG_LEVEL") == "DEBUG"
