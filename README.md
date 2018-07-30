# Appbase-Stat-Report

This is a script to allow user to automatically stream app data from server in a json format to generate summary statistics output. By running main.py with options specified, it will generate output to the directory.


It has one main.py, extract_col.py, sdk.py, iwatch.py, categories.py, deviceset.py, inapp.py. Basically, what it does is to generate aggregated summary report separately for google, amazon, and apple from Appbase for user specified one choice or multiple choice in command line. It is a 30 GB compressed json file. This project involves data streaming, data cleaning, pandas, data merging, pivot table, and parser, and some special case handling. The output looks like the index is categories and the columns are apple_active_deviceset and the value is count data. 
