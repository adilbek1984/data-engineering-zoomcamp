import duckdb
con = duckdb.connect('open_library_pipeline.duckdb')
print(con.execute('SELECT table_schema, table_name FROM information_schema.tables;').fetchall())
