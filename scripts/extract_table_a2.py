#!/usr/bin/env python3
"""
PDF Table Extractor Script

This script extracts tables from a PDF document and saves them to a CSV file.
It accepts command line arguments for PDF URL, pages to extract, and output CSV filename.
"""

import argparse
import sys
import tempfile
import os
from pathlib import Path

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


def extract_tables(pdf_path, pages, output_csv):
    """Extract tables from PDF pages and save to CSV."""
    try:
        # Parse pages parameter
        if pages.lower() == "all":
            pages_list = "all"
        else:
            # Handle page ranges like "1-3" or individual pages like "1,3,5"
            pages_list = []
            for page_spec in pages.split(","):
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
        
        # Combine all tables into a single DataFrame
        combined_df = pd.concat(tables, ignore_index=True)
        
        # Save to CSV
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
        extract_tables(pdf_path, args.pages, args.output_csv)


if __name__ == "__main__":
    main()