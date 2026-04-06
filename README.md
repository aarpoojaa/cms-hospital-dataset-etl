# CMS Data ETL 

## Overview
This project is designed to analyze and process CSV data. The provided dataset contains information about Medicare hospital spending per patient, including scores, national medians, and date ranges.

### Example Row
```csv
measure_id,measure_name,score,footnote_score,national_median,footnote_national_median,start_date,end_date
MSPB_1,Medicare hospital spending per patient (Medicare Spending per Beneficiary),0.99,,"$27,416.63",,01/01/2024,12/31/2024
```

### Check:
1. `metastore_api_logs.txt` for log details regarding the etl run.
2. `requirements.txt` for additional modules.
3. `scheduler.py` for optional scheduler implementation in-house, a trigger with cron schedule is recommended.
4. `previous_run_data.json` which store the last processed information (metadata).
5. `cms_dataset_etl.py` for the python script that runs the ETL process.
