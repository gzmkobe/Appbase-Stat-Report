#!/usr/bin/python
import pandas as pd
import os
##########################################################################
#### Processing and adjusting for Categories
def import_categories():
    dat = pd.read_csv("./data/Input/store_categories.csv")
    d = dict(zip(dat['category_id'], dat['name']))
    p_name = [row.name if row.parent_id == -999 else d[row.parent_id]
                for row in dat.itertuples()]    
    dat['parent_name'] = p_name
    dat.loc[dat['store'] == 'apple', 'store'] = 1
    dat.loc[dat['store'] == 'google_play', 'store'] = 2
    dat.loc[dat['store'] == 'amazon_appstore', 'store'] = 3
    dat_abbre = dat[['category_id','store','parent_name']].copy()
    dat_abbre['category_id'] = dat_abbre['category_id'].apply(lambda x: int(x))
    return dat_abbre


### Determine category from category_main and category_all
def get_category(main, all_list):
    TOP = [1000, 12, 25204, 35204,252042, 100, 200, 80000,90000, 101]
    if not pd.isnull(main):
        return int(main)
    else:
        if all_list == None:
            return 'No Category'
        elif len(all_list) == 1:
            if all_list[0] not in TOP:
                return int(all_list[0])
            else:
                return 'No Category'
        else:
            for cat in all_list:
                if cat not in TOP:
                    return int(cat)
            return 'No Category'


def get_price_level(price):
    if not price:
        return "Unknown"   ### the entire price dictionary is empty
    for k in price.keys():
        if price[k].split(' ')[0] == '(null)':
            del price[k]
    price_list = [float(i.split(' ')[0].encode('utf-8')) for i in price.values()]
    if all(p == 0 for p in price_list):
        return "Free"
    elif all(p == None for p in price_list):
        return "Unknown"
    else:
        return "Paid"   
    
    
def clean(store_id, data, outfile):
    if store_id == 1:
        name = 'Apple'
    elif store_id == 2:
        name = 'Google'
    else:
        name = 'Amazon' 

    col = ['store','deviceset', 'price_level','category','active']
    out = data[col]
    dtfCats = import_categories()
    ### Merge dat_abbre and data
    result = pd.merge(dtfCats, out, left_on=  ["category_id", "store"],
                       right_on= ['category', 'store'], how = 'right')
    result = result[['store','deviceset', 'parent_name','active','price_level']]
    result.rename(columns={'parent_name':'category'}, inplace=True)
    result['category'].fillna('No Category', inplace=True)
    s = result.loc[(result.store == store_id), col]
    df = pd.pivot_table(s, index = ['category'], aggfunc = len, columns=['deviceset', 'active', 'price_level'])
    df.columns = [str(col[0]) + "_" + str(col[1]) + "_" + str(col[2]) for col in df.columns.values]
    df.reset_index(inplace= True)
    df.fillna(0.0, inplace=True)

    if not outfile:
        outfile = ""
    file_ = "Categories_counts_%s_%s.csv" % (name, outfile)
    outdir = './data/Category'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, file_) 

    return df.to_csv(fullname, encoding='utf-8', index = None)
    

def get_report(data, outfile):
    clean(1, data, outfile)
    clean(2, data, outfile)
    clean(3, data, outfile)
    print("categories finished!")
    print("###############################")       