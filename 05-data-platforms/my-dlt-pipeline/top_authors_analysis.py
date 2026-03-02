"""
marimo notebook for analyzing top 10 authors by book count.

Uses ibis to access dlt-loaded Open Library data from DuckDB.
Reference: https://dlthub.com/docs/general-usage/dataset-access/marimo
"""

import marimo as mo
import ibis
import ibis.expr.types as t
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path

__generated_with = "marimo"

@mo.cache
def connect_to_duckdb():
    """Connect to the DuckDB database created by open_library_pipeline."""
    # Determine the path to the DuckDB file
    # dlt creates it in a default location
    db_path = ".dlt/pipelines/open_library_pipeline/open_library_pipeline.duckdb"
    
    # Connect using ibis
    con = ibis.duckdb.connect(db_path)
    return con


def get_top_authors(con, limit: int = 10):
    """Get top N authors by book count."""
    # Access the books table
    books = con.table("books")
    
    # Display available columns for debugging
    columns = books.columns
    mo.toast(f"Available columns: {', '.join(columns)}")
    
    # Query to extract authors and count books
    # Open Library returns authors as a nested structure
    # We need to handle the JSON data appropriately
    try:
        # Try to group by a simple author field if it exists
        if "authors" in columns:
            # Create a basic aggregation
            result = books.group_by("authors").aggregate(
                book_count=books.count()
            ).order_by(ibis.desc("book_count")).limit(limit)
            
            return result.to_pandas()
        else:
            # Fallback: show what columns are available
            return books.limit(10).to_pandas()
    except Exception as e:
        mo.toast(f"Error processing authors: {str(e)}")
        return pd.DataFrame()


def create_visualization(data: pd.DataFrame):
    """Create a bar chart visualization of top authors."""
    if data.empty or len(data) == 0:
        return go.Figure().add_annotation(text="No data available")
    
    # Prepare data for visualization
    if "authors" in data.columns and "book_count" in data.columns:
        fig = go.Figure(
            data=[
                go.Bar(
                    x=data["book_count"],
                    y=data["authors"].astype(str),
                    orientation="h",
                    marker=dict(color="rgb(26, 118, 255)"),
                )
            ]
        )
        
        fig.update_layout(
            title="Top 10 Authors by Book Count",
            xaxis_title="Number of Books",
            yaxis_title="Author",
            height=500,
            showlegend=False,
            yaxis=dict(autorange="reversed"),
        )
    else:
        # Fallback for different data structure
        fig = go.Figure().add_annotation(
            text=f"Data structure: {list(data.columns)}"
        )
    
    return fig


app = mo.App()

with app.batch():
    title = mo.md("# Top 10 Authors by Book Count")
    description = mo.md(
        """
        This notebook analyzes Open Library book data loaded via `dlt` into DuckDB.
        It uses `ibis` for data access and displays the top 10 authors by book count.
        
        **Data Source:** Open Library API via dlt pipeline
        **Database:** DuckDB (open_library_pipeline)
        """
    )
    
    # Connect to database
    mo.toast("Connecting to DuckDB...")
    con = connect_to_duckdb()
    mo.toast("✓ Connected successfully")
    
    # Fetch data
    mo.toast("Fetching top authors...")
    top_authors_data = get_top_authors(con, limit=10)
    mo.toast("✓ Data fetched")
    
    # Create visualization
    fig = create_visualization(top_authors_data)
    
    chart = mo.as_html(fig)
    
    # Data table
    table_title = mo.md("## Data Table")
    table = mo.ui.table(top_authors_data, selection="single")
    
    stats = mo.md(f"""
    ## Statistics
    - **Total Authors Shown:** {len(top_authors_data)}
    - **Data Shape:** {top_authors_data.shape[0]} rows × {top_authors_data.shape[1]} columns
    """)

app += [title, description, chart, table_title, table, stats]

if __name__ == "__main__":
    app.run()
