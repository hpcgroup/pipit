import polars as pl

def preview(self):
    data_dict = pl.concat(
        [self.head().select(pl.exclude("Attributes")), self.tail().select(pl.exclude("Attributes"))]
    ).collect().to_dict()

    # Extract headers and data
    headers = list(data_dict.keys())
    rows = list(zip(*[data_dict[key] for key in headers]))

    # Find the maximum width for each column
    max_widths = [max(map(len, map(str, col))) for col in zip(headers, *rows)]

    # Print the table with ellipses
    border = '+' + '+'.join('-' * (width + 2) for width in max_widths) + '+'
    print(border)

    header_str = '|' + '|'.join(f' {header:>{width}} ' for header, width in zip(headers, max_widths)) + '|'
    print(header_str)
    print(border)

    # Print the first 3 rows
    for row in rows[:5]:
        row_str = '|' + '|'.join(f' {item:>{width}} ' for item, width in zip(row, max_widths)) + '|'
        print(row_str)

    # Print ellipses in the middle
    ellipsis_str = '|' + '|'.join(f' {"…":>{width}} ' for width in max_widths) + '|'
    print(ellipsis_str)

    # Print the last 3 rows
    for row in rows[-5:]:
        row_str = '|' + '|'.join(f' {item:>{width}} ' for item, width in zip(row, max_widths)) + '|'
        print(row_str)

    print(border)

    print(f"{self.select(pl.count()).collect()[0,0]} rows")

pl.LazyFrame.preview = preview