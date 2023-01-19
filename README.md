
This project is an example of using Dagster to conditionally write new data to a table. 

TODO 
- [x] write sensor to watch for new files in a local directory 
- [x] add dbt to create a monthly summary table from the raw data
- [ ] make the asset conditional based on a schema check (using graph-backed asset)
- [ ] enable GCP versions of the sensor, file reader, and table
