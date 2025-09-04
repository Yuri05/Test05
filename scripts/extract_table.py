#!/usr/bin/env python3
"""
PDF Table Extractor Script

This script extracts tables from a PDF document and saves them to a CSV file.
It accepts command line arguments for PDF URL, pages to extract, output CSV filename,
and options to handle multi-row table headers (multi-level columns).

Examples:
- Treat the first 2 rows of each table as headers and keep them as a MultiIndex:
  python scripts/extract_table.py --pdf-url URL --pages all --output-csv out.csv --header-rows 2

- Treat the first 2 rows of each table as headers but flatten them into a single row:
  python scripts/extract_table.py --pdf-url URL --pages all --output-csv out.csv --header-rows 2 --flatten-headers --header-sep " - "
"""

import argparse
import sys
import tempfile
import os
from pathlib import Path
from typing import List

try:
    import requests
    import pandas as pd
    import tabula
    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    DEPENDENCIES_AVAILABLE = False
    print(f"Warning: Missing dependencies: {e}")


def download_pdf(url, temp_dir):
    """Download PDF from URL to temporary directory."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        pdf_path = os.path.join(temp_dir, "temp_pdf.pdf")
        with open(pdf_path, 'wb') as f:
            f.write(response.content)
        
        return pdf_path
    except Exception as e:
        print(f"Error downloading PDF: {e}")
        sys.exit(1)


def _normalize_header_values(values: List[object]) -> List[str]:
    """Convert potential header cell values to clean strings, replacing None/NaN with empty strings."""
    out = []
    for v in values:
        s = "" if v is None else str(v)
        s = s.strip()
        if s.lower() in ("nan", "none"):
            s = ""
        out.append(s)
    return out


def _apply_multirow_header(df: "pd.DataFrame", header_rows: int, flatten: bool, sep: str) -> "pd.DataFrame":
    """
    Use the first `header_rows` rows of df as column headers.
    - If flatten=False: create a MultiIndex header.
    - If flatten=True: join header levels into a single level using `sep`.
    Returns a new DataFrame with those header rows removed from the data.
    """
    if header_rows <= 0:
        return df

    if len(df) < header_rows:
        # Not enough rows to form headers; return as-is
        return df

    headers = df.iloc[:header_rows].copy()

    # Clean NaNs/None, then forward-fill horizontally to handle cells spanning across columns
    headers = headers.apply(_normalize_header_values, axis=1, result_type="expand")
    headers = headers.ffill(axis=1)

    # Build column labels from header rows
    arrays = [headers.iloc[i].tolist() for i in range(header_rows)]

    # Ensure all arrays are the same length as number of columns in the data slice
    ncols = df.shape[1]
    arrays = [arr + [""] * (ncols - len(arr)) if len(arr) < ncols else arr[:ncols] for arr in arrays]

    data = df.iloc[header_rows:].reset_index(drop=True).copy()

    if flatten:
        # Join non-empty parts for each column
        flat_cols = []
        for col_idx in range(ncols):
            parts = [arrays[level][col_idx] for level in range(header_rows)]
            parts = [p for p in parts if p != ""]
            flat_cols.append(sep.join(parts) if parts else f"col_{col_idx}")
        data.columns = flat_cols
    else:
        # Create a MultiIndex from header arrays
        try:
            data.columns = pd.MultiIndex.from_arrays(arrays)
        except Exception:
            # Fallback: flatten if MultiIndex creation fails
            flat_cols = []
            for col_idx in range(ncols):
                parts = [arrays[level][col_idx] for level in range(header_rows)]
                parts = [p for p in parts if p != ""]
                flat_cols.append(sep.join(parts) if parts else f"col_{col_idx}")
            data.columns = flat_cols

    return data


def extract_tables(pdf_path, pages, output_csv, header_rows=0, flatten_headers=False, header_sep=" | "):
    """Extract tables from PDF pages and save to CSV."""
    try:
        # Parse pages parameter
        if isinstance(pages, str) and pages.lower() == "all":
            pages_list = "all"
        else:
            # Handle page ranges like "1-3" or individual pages like "1,3,5"
            pages_list = []
            for page_spec in str(pages).split(","):
                page_spec = page_spec.strip()
                if "-" in page_spec:
                    start, end = map(int, page_spec.split("-"))
                    pages_list.extend(range(start, end + 1))
                else:
                    pages_list.append(int(page_spec))
        
        print(f"Extracting tables from pages: {pages_list}")
        
        # Extract tables using tabula
        tables = tabula.read_pdf(pdf_path, pages=pages_list, multiple_tables=True)

        if not tables:
            print("No tables found in the specified pages.")
            sys.exit(1)

        processed_tables = []
        for idx, df in enumerate(tables, start=1):
            if header_rows and header_rows > 0:
                print(f"Applying {header_rows} header row(s) to table {idx} (flatten={flatten_headers})")
                df = _apply_multirow_header(df, header_rows, flatten_headers, header_sep)
            processed_tables.append(df)

        # Combine all tables into a single DataFrame
        combined_df = pd.concat(processed_tables, ignore_index=True)

        # Save to CSV (pandas will output multiple header rows if MultiIndex columns are used and flatten_headers=False)
        combined_df.to_csv(output_csv, index=False)
        print(f"Tables extracted and saved to: {output_csv}")
        print(f"Total rows extracted: {len(combined_df)}")
        
    except Exception as e:
        print(f"Error extracting tables: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Extract tables from PDF and save to CSV")
    parser.add_argument("--pdf-url", required=True, help="URL of the PDF to process")
    parser.add_argument("--pages", required=True, help="Pages to extract (e.g., 'all', '1', '1-3', '1,3,5')")
    parser.add_argument("--output-csv", required=True, help="Output CSV filename")

    # New options for multi-row headers
    parser.add_argument(
        "--header-rows",
        type=int,
        default=0,
        help="Number of top rows in each table to treat as header rows (default: 0 = no special handling)"
    )
    parser.add_argument(
        "--flatten-headers",
        action="store_true",
        help="Flatten multi-level headers into a single header row by joining levels"
    )
    parser.add_argument(
        "--header-sep",
        type=str,
        default=" | ",
        help="Separator to use when flattening multi-level headers (default: ' | ')"
    )
    
    args = parser.parse_args()
    
    if not DEPENDENCIES_AVAILABLE:
        print("Error: Required dependencies are not installed. Please install: pandas, tabula-py, requests")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Download PDF to temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Downloading PDF from: {args.pdf_url}")
        pdf_path = download_pdf(args.pdf_url, temp_dir)
        
        print(f"Extracting tables from pages: {args.pages}")
        extract_tables(
            pdf_path=pdf_path,
            pages=args.pages,
            output_csv=args.output_csv,
            header_rows=args.header_rows,
            flatten_headers=args.flatten_headers,
            header_sep=args.header_sep,
        )

if __name__ == "__main__":
    main()
