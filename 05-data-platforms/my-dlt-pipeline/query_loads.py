import duckdb
con = duckdb.connect('open_library_pipeline.duckdb')
print(con.execute('SELECT * FROM open_library_pipeline_dataset._dlt_loads').fetchall())
print(con.execute('DESCRIBE open_library_pipeline_dataset._dlt_loads').fetchall())
