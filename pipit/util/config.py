# Copyright 2023 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT


# Validator to check if the value entered is of type bool
def bool_validator(key, value):
    if type(value) is not bool:
        raise TypeError(
            (
                'Error loading configuration: The Value "{}" for Configuration "{}"'
                + "must be of type Bool"
            ).format(value, key)
        )
    else:
        return True


# Validator to check if the value entered is of type string
def str_validator(key, value):
    if type(value) is not str:
        raise TypeError(
            (
                'Error loading configuration: The Value "{}" for Configuration "{}"'
                + "must be of type string"
            ).format(value, key)
        )
    else:
        return True


# Validator to check if the value entered is of type int
def int_validator(key, value):
    if type(value) is not int:
        raise TypeError(
            (
                'Error loading configuration: The Value "{}" for Configuration "{}"'
                + "must be of type int"
            ).format(value, key)
        )
    if key == "depth" and value < 1:
        raise ValueError("Depth must be greater than 1")
    return True


# Validator to check if the value entered is of type float
def float_validator(key, value):
    if type(value) is not float:
        raise TypeError(
            (
                'Error loading configuration: The Value "{}" for Configuration "{}"'
                + "must be of type float"
            ).format(value, key)
        )
    else:
        return True


# Validator to check if the value entered is a valid log level
def log_level_validator(key, value):
    if value not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        raise ValueError(
            (
                'Error loading configuration: The Value "{}" for Configuration "{}"'
                + "must be a valid log level"
            ).format(value, key)
        )
    else:
        return True


# Validator to check if the value entered is a valid URL
def url_validator(key, value):
    if value.startswith("http://") or value.startswith("https://"):
        return True
    else:
        raise ValueError(
            (
                'Error loading configuration: The Value "{}" for Configuration "{}"'
                + "must be a valid URL"
            ).format(value, key)
        )


# Validator to check if theme is valid YAML
def theme_validator(key, value):
    import yaml

    try:
        yaml.safe_load(value)
    except yaml.YAMLError:
        raise ValueError(
            (
                'Error loading configuration: The Value "{}" for Configuration "{}"'
                + "must be a valid YAML"
            ).format(value, key)
        )
    else:
        return True


registered_options = {
    "log_level": {
        "default": "INFO",
        "validator": log_level_validator,
    },
    "notebook_url": {
        "default": "http://localhost:8888",
        "validator": url_validator,
    },
    "theme": {
        "default": """
            attrs:
                Plot:
                    height: 350
                    width: 700
                    background_fill_color: "#fafafa"
                Axis:
                    axis_label_text_font_style: "bold"
                    minor_tick_line_color: null
                Toolbar:
                    autohide: true
                    logo: null
                HoverTool:
                    point_policy: "follow_mouse"
                Legend:
                    label_text_font_size: "8.5pt"
                    spacing: 10
                    border_line_color: null
                    glyph_width: 16
                    glyph_height: 16
                Scatter:
                    size: 9
                DataRange1d:
                    range_padding: 0.05
        """,
        "validator": theme_validator,
    },
}

global_config = {key: registered_options[key]["default"] for key in registered_options}


# Returns the current value of the specific config key
def get_option(key):
    if not key or key not in registered_options:
        raise ValueError("No such keys(s)")
    else:
        return global_config[key]


# Updates the value of the specified key
def set_option(key, val):
    if not key or key not in registered_options:
        raise ValueError("No such keys(s)")

    validator = registered_options[key]["validator"]

    if validator(key, val):
        global_config[key] = val


# Resets the value of the specfied key
# If "all" is passed in, resets values of all keys
def reset_option(key):
    if not key:
        raise ValueError("No such keys(s)")

    if key in registered_options:
        global_config[key] = registered_options[key]["default"]
    elif key == "all":
        for k in registered_options:
            global_config[k] = registered_options[k]["default"]
    else:
        raise ValueError(
            "You must specify a valid key. Or, use the special keyword "
            '"all" to reset all the options to their default value'
        )
