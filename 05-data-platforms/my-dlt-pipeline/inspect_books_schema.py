import duckdb
con = duckdb.connect('open_library_pipeline.duckdb')
print('SCHEMA FOR open_library_pipeline_dataset.books:')
for row in con.execute('DESCRIBE open_library_pipeline_dataset.books').fetchall():
    print(row)
