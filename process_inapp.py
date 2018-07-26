#!/usr/bin/python
import pandas as pd
import os 
##########################################################################
#### Processing and adjusting for has_inapps.EN
def process_in_app(in_app):
    if in_app == 'true':
        return 1
    elif in_app == 'false':
        return 0
    else:
        return -999

        
def clean(data, outfile):

	col = ['store','deviceset','active','in_app']
	out = data[col]
	df = pd.pivot_table(out, index = 'store', aggfunc = len, columns = ['deviceset','active','in_app'])
	df.columns = [str(col[0])+"_"+str(col[1])+"_"+str(col[2]) for col in df.columns.values]
	df.reset_index(inplace = True)
	if not outfile:
		outfile = ""
	file_ = "InApp_counts_%s.csv" % (outfile)
	outdir = './data/InApp'
	if not os.path.exists(outdir):
		os.mkdir(outdir)
	fullname = os.path.join(outdir, file_)

	return df.to_csv(fullname, encoding = 'utf-8', index = None)


def get_report(data, outfile):
    clean(data, outfile)
    print("InApps finished!")
    print("###############################")
