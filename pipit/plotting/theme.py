DEFAULT = """
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
"""

PAPER = """
    attrs:
        Plot:
            height: 350
            width: 700
            toolbar_location: "above"
            outline_line_width: 0
        Title:
            text_font_size: "0pt"
            text_font: "Gill Sans"
        Axis:
            axis_label_text_font_style: "bold"
            axis_label_text_font_size: "18pt"
            axis_label_text_font: "Gill Sans"
            major_label_text_font_size: "16pt"
            major_label_text_font: "Gill Sans"
            minor_tick_line_color: null
        ColorBar:
            major_label_text_font_size: "16pt"
            major_label_text_font: "Gill Sans"
        Toolbar:
            autohide: true
            logo: null
        HoverTool:
            point_policy: "follow_mouse"
        Legend:
            label_text_font_size: "15pt"
            label_text_font: "Gill Sans"
            spacing: -1
            padding: 0
            border_line_color: null
            glyph_width: 16
            glyph_height: 16
            margin: 5
        Scatter:
            size: 12
        DataRange1d:
            range_padding: 0.05
"""

themes = {
    "default": DEFAULT,
    "paper": PAPER,
}
