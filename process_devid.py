#! /usr/bin/env python
import sys
import pandas as pd
import os 
##########################################################################
#### Processing and adjusting for counting developer id by store by deviceset by country
def process_country(country):
    if country == None:
        return 'Unknown'
    else:
        return country

def unactive(ll):
    if sum(ll) == 0:
        return 1
    else:
        return 0        

def active(ll):
    if sum(ll) > 0:
        return 1
    else:
        return 0

def clean(store_id,data,outfile):
    if store_id == 1:
        name = 'Apple'
    elif store_id == 2:
        name = 'Google'
    else:
        name = 'Amazon' 

    col = ['product_id','store', 'deviceset', 'active', 'dev_id','dev_country']
    out = data[col]

   
    s = out.loc[(out.store == store_id), col]
    if s.empty == True:
    	print "No store_id = %s is detected when output dev_id!" % store_id 
    	return 1
    df = pd.pivot_table(s, values = 'active', index = ['dev_country','dev_id'],
                       columns = ['store','deviceset'], aggfunc = [active,unactive], fill_value=0)

    df.columns = [str(col[2]) + "_" + col[0] for col in df.columns.values]
    df.reset_index(inplace = True)
    df = df.groupby('dev_country').sum().reset_index()

    if not outfile:
    	outfile = ""
    file_ = "Dev_id_counts_%s_%s.csv" % (name, outfile)
    outdir = './data/Devid'
    if not os.path.exists(outdir):
    	os.mkdir(outdir)
    fullname = os.path.join(outdir, file_)

    return df.to_csv(fullname, encoding = 'utf-8', index = None)

def get_report(data, outfile):
    clean(1, data, outfile)
    clean(2, data, outfile)
    clean(3, data, outfile)
    print("dev_id finished!")
    print("###############################")