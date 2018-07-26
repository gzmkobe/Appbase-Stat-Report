#!/usr/bin/python
import pandas as pd
import os 
##########################################################################
#### Processing and adjusting for SDK
def process_sdk(sdk):
    if sdk == []:
        out = 'Unknown'
    else:
        out = "-".join([str(a) for a in sdk])
    return out


def process_active(active):
    if active:
        return 1 
    else:
        return 0

    
def split_sdks(df):
    '''
    Splitting 'sdks' column whose row has multilabel on sdks into multiple rows woth single label
    '''
    new_df = pd.DataFrame(df.sdks.str.split('-', expand = True).stack().reset_index(level = 1, drop = True),columns = ['sdks'])
    result = df.drop(['sdks'], axis=1).join(new_df).reset_index(drop=True)
    return result


def sdk_id2name(sdk_id):
    '''
    sdk_id = the file whose sdk column is in id format
    Replace sdk_id by sdk_name. Example: replace sdk_id = 214 by sdk_name = 'Cocos2d'
    '''
    sdk_name = pd.read_csv('./data/Input/sdk_categories.csv')
    sdk_name.rename(columns={'code':'sdks'}, inplace=True)
    sdk_name.sdks = sdk_name.sdks.astype('str')
    result = pd.merge(sdk_name, sdk_id, on = ['sdks'], how = 'right')
    result.drop(['sdks'], axis = 1, inplace = True)
    result.rename(columns = {'name':'sdks'}, inplace = True)
    result.fillna('Unknown',inplace = True)
    return result

def clean(store_id,data,outfile):
    if store_id == 1:
        name = 'Apple'
    elif store_id == 2:
        name = 'Google'
    else:
        name = 'Amazon' 

    col = ['store', 'deviceset', 'active', 'sdks']
    out = data[col]
    out = split_sdks(out)
    out = sdk_id2name(out)

    s = out.loc[(out.store == store_id), col]
    df = pd.pivot_table(s, index = 'sdks', columns = ['deviceset','active'], aggfunc = len)
    df.columns = [name + "_" + str(col[1]) + "_" + str(col[2]) for col in df.columns.values]
    df.reset_index(inplace = True)
    df.fillna(0.0, inplace=True)

    if not outfile:
        outfile = ""
    file_ = "Sdk_counts_%s_%s.csv" % (name, outfile)
    outdir = './data/Sdk'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, file_) 

    return df.to_csv(fullname, encoding='utf-8', index = None)

def get_report(data, outfile):
    clean(1, data, outfile)
    clean(2, data, outfile)
    clean(3, data, outfile)
    print("sdk finished!")
    print("###############################")
