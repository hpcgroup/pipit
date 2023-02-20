# TODO: ensure changes are reflected in vis
config = {
    "vis": {
        "theme": "light",
        "initialized": False,
        "launch_server": False,
        "default_color": "#E5AE38",
        "shuffle_colors": True,
        "colors": [
            "rgb(138,113,152)",
            "rgb(175,112,133)",
            "rgb(127,135,225)",
            "rgb(93,81,137)",
            "rgb(116,143,119)",
            "rgb(178,214,122)",
            "rgb(87,109,147)",
            "rgb(119,155,95)",
            "rgb(114,180,160)",
            "rgb(132,85,103)",
            "rgb(157,210,150)",
            "rgb(148,94,86)",
            "rgb(164,108,138)",
            "rgb(139,191,150)",
            "rgb(110,99,145)",
            "rgb(80,129,109)",
            "rgb(125,140,149)",
            "rgb(93,124,132)",
            "rgb(140,85,140)",
            "rgb(104,163,162)",
            "rgb(132,141,178)",
            "rgb(131,105,147)",
            "rgb(135,183,98)",
            "rgb(152,134,177)",
            "rgb(141,188,141)",
            "rgb(133,160,210)",
            "rgb(126,186,148)",
            "rgb(112,198,205)",
            "rgb(180,122,195)",
            "rgb(203,144,152)",
        ],
        "css": """
            /* Increase output width */
            .container {
                width: 90% !important;
            }

            /* Remove tooltip overlap */
            div.bk-tooltip>div.bk>div.bk:not(:last-child) {
                display: none !important;
            }

            /* Change hover cursor */
            div.bk {
                cursor: default !important;
            }

            /* Tooltip text styling */
            .bk.bk-tooltip-row-label {
                color: black;
                font-weight: bold;
            }

            .bk.bk-tooltip-row-value {
                font-family: monospace;
                padding-left: 3px;
            }""",
    }
}
