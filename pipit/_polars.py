import polars as pl

def __repr__(self):
    data_dict = pl.concat(
        [self.head().select(pl.exclude("Attributes")), self.tail().select(pl.exclude("Attributes"))]
    ).collect().to_dict()

    # Extract headers and data
    headers = list(data_dict.keys())
    rows = list(zip(*[data_dict[key] for key in headers]))

    # Find the maximum width for each column
    max_widths = [max(map(len, map(str, col))) for col in zip(headers, *rows)]

    # Create the table with ellipses
    result = []
    result.append('┌' + '┬'.join('─' * (width + 2) for width in max_widths) + '┐')
    result.append('┆' + '┆'.join(f' {header:<{width}} ' for header, width in zip(headers, max_widths)) + '┆')
    result.append('╞' + '╪'.join('═' * (width + 2) for width in max_widths) + '╡')

    # Add the first 3 rows
    for row in rows[:5]:
        row_str = '┆' + '┆'.join(f' {str(item):>{width}} ' for item, width in zip(row, max_widths)) + '┆'
        result.append(row_str)

    # Add ellipses in the middle
    ellipsis_str = '┆' + '┆'.join(f' {"…":>{width}} ' for width in max_widths) + '┆'
    result.append(ellipsis_str)

    # Add the last 3 rows
    for row in rows[-5:]:
        row_str = '┆' + '┆'.join(f' {str(item):>{width}} ' for item, width in zip(row, max_widths)) + '┆'
        result.append(row_str)

    result.append('└' + '┴'.join('─' * (width + 2) for width in max_widths) + '┘')

    result.append(f"{self.select(pl.count()).collect()[0,0]} rows")

    return "\n".join(result)

def _repr_html_(self):
    data_dict = pl.concat(
        [self.head().select(pl.exclude("Attributes")), self.tail().select(pl.exclude("Attributes"))]
    ).collect().to_dict()

    # Extract headers and data
    headers = list(data_dict.keys())
    rows = list(zip(*[data_dict[key] for key in headers]))

    # Start the HTML table
    result = ['<table>']

    # Add the headers
    result.append('<thead><tr>')
    for header in headers:
        result.append(f'<th>{header}</th>')
    result.append('</tr></thead>')

    # Add the first 5 rows
    result.append('<tbody>')
    for row in rows[:5]:
        result.append('<tr>')
        for item in row:
            result.append(f'<td>{str(item)}</td>')
        result.append('</tr>')

    # Add ellipses in the middle
    result.append('<tr>')
    for _ in headers:
        result.append('<td>…</td>')
    result.append('</tr>')

    # Add the last 5 rows
    for row in rows[-5:]:
        result.append('<tr>')
        for item in row:
            result.append(f'<td>{str(item)}</td>')
        result.append('</tr>')

    result.append('</tbody></table>')

    # Add the row count
    result.append(f"<p>{self.select(pl.count()).collect()[0,0]} rows</p>")

    return "".join(result)

pl.LazyFrame.__repr__ = __repr__
pl.LazyFrame._repr_html_ = _repr_html_
pl.LazyFrame.__str__ = __repr__