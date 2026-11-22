# Charlotte Week 10 Python Projects

Class exercises and capstone work.
# Production Data Capstone

This project uses Python, Pandas, and NumPy to clean the supplied well production data and create daily summaries.

## Input

The input file is `data/historical_production_capstone.csv`. It contains well IDs, timestamps, production readings, pressure, status, and ingestion times.

## Cleaning

Rows with invalid timestamps are rejected. For duplicate well and timestamp pairs, the latest valid ingestion time is kept. Negative or non-numeric readings are treated as missing; real zero readings are kept.
Missing oil, water, and gas readings are left blank. Pressure is filled only for an internal gap with a valid reading on each side. The pipeline does not fill gaps at the beginning or end.

## Run the project

Open `notebook/Week10_Class5_Student_Capstone.ipynb` and run its cells. The results are saved in `outputs/`.
To run the tests from the project folder:
```bash
python -m pytest tests -v