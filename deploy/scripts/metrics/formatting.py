# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Terminal table formatting and cell wrapping utilities for metrics reports."""

from __future__ import annotations

import re


MAX_TERMINAL_WIDTH = 120


def _wrap_single_line(line: str, width: int) -> list[str]:
    """Wraps a single line to at most `width` characters at natural boundaries."""
    if len(line) <= width or width <= 0:
        return [line]

    # 1. Prefer wrapping comma-separated lists (e.g. labels "{a=1, b=2}" or buckets "<=1s:5, <=10s:2")
    if ", " in line:
        raw_parts = line.split(", ")
        parts = [p + "," if i < len(raw_parts) - 1 else p for i, p in enumerate(raw_parts)]
        lines: list[str] = []
        cur = ""
        for part in parts:
            candidate = f"{cur} {part}" if cur else part
            if len(candidate) <= width:
                cur = candidate
            else:
                if cur:
                    lines.extend(_wrap_single_line(cur, width))
                if len(part) <= width:
                    cur = part
                else:
                    lines.extend(_wrap_single_line(part, width))
                    cur = ""
        if cur:
            lines.extend(_wrap_single_line(cur, width))
        return lines

    # 2. Next prefer wrapping on spaces (e.g. SECTION names like "ClusterQueue Resources (Optional)")
    if " " in line:
        words = line.split(" ")
        lines = []
        cur = ""
        for word in words:
            candidate = f"{cur} {word}" if cur else word
            if len(candidate) <= width:
                cur = candidate
            else:
                if cur:
                    lines.extend(_wrap_single_line(cur, width))
                if len(word) <= width:
                    cur = word
                else:
                    lines.extend(_wrap_single_line(word, width))
                    cur = ""
        if cur:
            lines.extend(_wrap_single_line(cur, width))
        return lines

    # 3. Next wrap on identifier/label delimiters (_, =, -, /, :) so metric names
    #    and key=value pairs break cleanly at token boundaries.
    tokens = re.findall(r"[^_=\-/:]+[_=\-/:]*", line)
    if len(tokens) > 1:
        lines = []
        cur = ""
        for tok in tokens:
            if len(cur) + len(tok) <= width:
                cur += tok
            else:
                if cur:
                    lines.append(cur)
                if len(tok) <= width:
                    cur = tok
                else:
                    for idx in range(0, len(tok), width):
                        chunk = tok[idx : idx + width]
                        if len(chunk) == width:
                            lines.append(chunk)
                        else:
                            cur = chunk
        if cur:
            lines.append(cur)
        return lines

    # 4. Fallback hard slice for single contiguous tokens exceeding `width`.
    return [line[idx : idx + width] for idx in range(0, len(line), width)]


def wrap_cell(cell: str, width: int) -> list[str]:
    """Wraps a potentially multi-line cell string so every line is <= width chars."""
    if not cell:
        return [""]
    out: list[str] = []
    for raw_line in cell.splitlines():
        out.extend(_wrap_single_line(raw_line, width))
    return out or [""]


def print_table(headers: list[str], rows: list[list[str]], max_width: int = MAX_TERMINAL_WIDTH) -> None:
    if not rows:
        return
    num_cols = len(headers)
    border_overhead = 2 + 3 * (num_cols - 1)
    available_width = max(num_cols * 4, max_width - border_overhead)

    def compute_actual_widths(limits: list[int]) -> list[int]:
        widths = [0] * num_cols
        for i in range(num_cols):
            widths[i] = max(len(line) for line in wrap_cell(headers[i], limits[i]))
        for row in rows:
            for i in range(num_cols):
                widths[i] = max(widths[i], max(len(line) for line in wrap_cell(row[i], limits[i])))
        return widths

    natural_widths = [max(len(line) for line in h.splitlines()) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            cell_max = max((len(line) for line in cell.splitlines()), default=0)
            natural_widths[idx] = max(natural_widths[idx], cell_max)

    col_widths = list(natural_widths)

    # First wrap SECTION (column 0) at word boundaries (<= 16 chars) if the table
    # exceeds available_width.
    if sum(col_widths) > available_width and col_widths[0] > 16:
        col_widths[0] = 16
        col_widths = compute_actual_widths(col_widths)

    # Protect compact columns (numeric stats, short headers <= 12 chars) from
    # wrapping, and iteratively reduce the widest column until actual rendered
    # width fits within `available_width`.
    min_widths = [
        col_widths[i]
        if natural_widths[i] <= 12
        else max(10, min(max(len(line) for line in headers[i].splitlines()), 14))
        for i in range(num_cols)
    ]
    while sum(col_widths) > available_width:
        reducible = [i for i in range(num_cols) if col_widths[i] > min_widths[i]]
        if not reducible:
            reducible = [i for i in range(num_cols) if col_widths[i] > 4]
            if not reducible:
                break
        widest_idx = max(reducible, key=lambda i: col_widths[i])
        col_widths[widest_idx] -= 1
        col_widths = compute_actual_widths(col_widths)

    wrapped_headers = [wrap_cell(headers[i], col_widths[i]) for i in range(num_cols)]
    wrapped_rows = [[wrap_cell(row[i], col_widths[i]) for i in range(num_cols)] for row in rows]
    actual_widths = compute_actual_widths(col_widths)

    fmt = "  " + " | ".join(f"{{:<{w}}}" for w in actual_widths)
    sep = "  " + "-+-".join("-" * w for w in actual_widths)

    header_height = max(len(cell_lines) for cell_lines in wrapped_headers)
    for line_idx in range(header_height):
        line_cells = [
            wrapped_headers[col_idx][line_idx] if line_idx < len(wrapped_headers[col_idx]) else ""
            for col_idx in range(num_cols)
        ]
        print(fmt.format(*line_cells).rstrip())
    print(sep)

    has_multiline_rows = any(len(cell_lines) > 1 for wrow in wrapped_rows for cell_lines in wrow)
    for row_idx, wrow in enumerate(wrapped_rows):
        row_height = max(len(cell_lines) for cell_lines in wrow)
        for line_idx in range(row_height):
            line_cells = [
                wrow[col_idx][line_idx] if line_idx < len(wrow[col_idx]) else ""
                for col_idx in range(num_cols)
            ]
            print(fmt.format(*line_cells).rstrip())
        if has_multiline_rows and row_idx < len(wrapped_rows) - 1:
            print(sep)
