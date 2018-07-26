#!/usr/bin/python
import sys
import argparse
import traceback
import pandas as pd
import ujson as json
import process_sdk
import process_categories
import extract_col
import process_inapp
import process_devid
import process_iwatch

##########################################################################
#### Streaming from Appbase
def stream_rows(r, choice):
    row = 0
    total_num = 100000
    print "%s number of rows streamed from Appbase...\n\n\n" % total_num
    for line in r:
        row += 1
        if row % total_num == 0:
            break

        try:
            out = json_2_csvrow(line, choice)
            if out is not None:
                yield out
        except:
            traceback.print_exc()


def json_2_csvrow(line, choice):

    js = json.loads(line)
    return extract_col.get_colvalues(js, choice)


def stream_jsonlines(f, outfile = None, choice = ['sdk']):
    columns = extract_col.get_colname(choice)

    print 'columns extracted are:',columns
    

    data = pd.DataFrame(stream_rows(f, choice), columns=columns)

    if 'sdk' in choice:
        print "running sdk..."
        print "......"
        process_sdk.get_report(data, outfile)
    if 'cat' in choice:
        print "running cat..."
        print "......"
        process_categories.get_report(data, outfile)
    if 'app' in choice:
        print "running app..."
        print "......"
        process_inapp.get_report(data, outfile)
    if 'devid' in choice:
        print "running devid..."
        print "......"
        process_devid.get_report(data, outfile)
    if 'iwatch' in choice:
        print "running iwatch..."
        print "......"
        process_iwatch.get_report(data, outfile)       
    return None 


if __name__ == '__main__':
	
    parser = argparse.ArgumentParser(
            description="Appbase summary statistics")
    parser.add_argument("-o", "--outfile", type=str, default=None,
                        help="""Supply additional naming convention to the outputed files.
                                (e.g. Sdk_counts_[store]_[outfile])""")
    parser.add_argument("-c","--choice", nargs = "*", type = str, choices = ['sdk','cat','app','devid','iwatch'], 
                        default = ['sdk'], help = "Choose expected output report")
    args = parser.parse_args()
    print "You choose:",' and '.join(str(c) for c in args.choice) 
    stream_jsonlines(sys.stdin, outfile=args.outfile, choice = args.choice)