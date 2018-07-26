#!/usr/bin/python
import process_sdk
import process_categories
import process_inapp
import process_devid
import process_iwatch

def get_colname(choice):
    sdk_col = []
    cat_col = []   
    inapp_col = []
    devid_col = []
    iwatch_col = []

    if 'sdk' in choice:
        sdk_col = ['store', 'deviceset', 'active', 'sdks']
    if 'cat' in choice:
        cat_col = ['store','deviceset', 'price_level','category','active']
    if 'app' in choice:
        inapp_col = ['store', 'deviceset', 'active','in_app']
    if 'devid' in choice:
        devid_col = ['product_id','store','deviceset','dev_id','dev_country','active']
    if 'iwatch' in choice:
        iwatch_col = ['store', 'deviceset','active', 'watch_com']

    union_col = list(set().union(sdk_col, cat_col, inapp_col, devid_col, iwatch_col)) 
    return union_col


def get_colvalues(js, choice):

    def K(k):
        t = len(k)
        out = js.get(k[0])
        if t > 1 and out is not None:
            for i in range(1, t):
                out = out.get(k[i])
        return out

    def iK(k):
        out = K(k)
        if out:
            return out
        else:
            return K([s.lower() for s in k]) 

    ##### Common columns
    store = K(['store'])

    if (choice == ['iwatch'] and store != 1):
        return None

    deviceset = K(['deviceset'])
    active = process_sdk.process_active(K(['active']))
    ##### Sdk Column
    if 'sdk' in choice:
        sdks = process_sdk.process_sdk(K(['sdks'])) 
    ##### Category Column
    if 'cat' in choice:
        price = K(['prices']) 
        category_main = K(['categories','main']) ## take the categories type as main
        category_all = K(['categories','all'])
        category = process_categories.get_category(category_main, category_all)
        price_level = process_categories.get_price_level(price)
    ##### has_inapp column
    if 'app' in choice:
        in_app = process_inapp.process_in_app(iK(['meta','has_inapps.EN']))
    ##### developer_id column
    if 'devid' in choice:
        product_id = K(['product_id'])
        dev_id = K(['developer_id'])
        dev_country = process_devid.process_country(K(['developer_country']))
    ##### watch_compatible.EN column
    if 'iwatch' in choice:
        watch_com = process_iwatch.process_watch(K(["meta","watch_compatible.EN"]))

    union_col = get_colname(choice)
    gl = locals()

    col_values = tuple(eval(col, gl) for col in union_col)
    return col_values