
This project is an example of using Dagster to conditionally write new data to a table. 

## Getting Started

To install the dependencies and run Dagster:

```
pip install -e ".[dev]"
dagster dev
```

Once running, open the Dagster user interface. Navigate to "Overview > Sensors" and then turn on the sensor `watch_for_new_plant_data` by clicking the sensor toggle. Then click on `watch_for_new_plant_data` to view the sensor output. 

You will see the sensor launch two runs, one for the file `data/parts.csv` and one run for `data/bad_parts.csv`. 

The run for `data/parts.csv` will parse the data and pass the data checks. As a result, the downstream dbt assets will run to load the data into the warehouse and create a monthly summary table.

The run for `data/bad_parts.csv` will fail the data checks. As a result, the data will be written to a failure location and a warning will be logged. (This warning could be an alert email in production).



## Project TODOs:  

*Local*
- [x] write an asset that represents data from plants for a vehicle manufacturer
- [x] write sensor to watch for new files in a local directory 
- [x] add dbt to create a monthly summary table from the raw data
- [x] make the asset conditional based on a schema check (using graph-backed asset)  

*Production on GCP*  
- [ ] write a GCP resource that can authenticate GCP clients from an env var by deserializing a service account auth json file
- [ ] write a resource the sensor can use to list a GCP bucket instead of file system
- [ ] write a resource the asset ops can use to read a GCP file 
- [ ] bigquery IO manager instead of duckdb
- [ ] GCP pickle io manager instead of fs_io_manager for failure case 
