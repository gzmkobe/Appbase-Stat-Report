#!/usr/bin/python
import pandas as pd
import os 
##########################################################################
#### Processing and adjusting for watch_compatibility
def process_watch(col):
    if col == 'True':
        return 1 
    elif col == 'False':
        return 0
    else:
    	return -999


def clean(data,outfile):

    col = ['store', 'deviceset', 'active', 'watch_com']
    out = data.loc[data.store == 1,col]

    df = pd.pivot_table(out, index = "watch_com", columns = ['deviceset', 'active'], aggfunc = len, fill_value=0)
    df.columns = ["_".join(map(str, v[1:3])) for v in df.columns.values] 
    df.reset_index(inplace = True)

    if not outfile:
        outfile = ""
    file_ = "iWatch_counts_%s.csv" % outfile
    outdir = './data/iWatch'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, file_) 

    return df.to_csv(fullname, encoding='utf-8', index = None)


def get_report(data, outfile):
    clean(data, outfile)
    print("iwatch finished!")
    print("###############################")