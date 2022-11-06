import finnhub
import requests
import datetime
import holidays
import pandas as pd
import sklearn
from math import radians, cos, sin, asin, sqrt
#import datetime'
from datetime import datetime

# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay


# Making a GET request for iss
r = requests.get('http://api.open-notify.org/iss-now.json')
iss_loc=r.json()
# check status code for response received

from datetime import datetime
def retrive_date(ss):
    return datetime.fromtimestamp(ss).date()

def weekday(ss):
    return datetime.fromtimestamp(ss).strftime('%A')


def single_pt_haversine(lat, lng, degrees=True):
    """
    'Single-point' Haversine: Calculates the great circle distance
    between a point on Earth and the (0, 0) lat-long coordinate
    """
    r = 6371 # Earth's radius (km). Have r = 3956 if you want miles

    # Convert decimal degrees to radians
    if degrees:
        lat, lng = map(radians, [lat, lng])

    # 'Single-point' Haversine formula
    a = sin(lat/2)**2 + cos(lat) * sin(lng/2)**2
    d = 2 * r * asin(sqrt(a)) 

    return d


iss_loc_haversine=single_pt_haversine(float(iss_loc['iss_position']['latitude']),float(iss_loc['iss_position']['longitude']))

# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
from flask import Flask,request, jsonify,make_response
#from flask_module import Module
#import flask.ext.excel 
 
# Flask constructor takes the name of
# current module (__name__) as argument.
app = Flask(__name__)
 
# The route() function of the Flask class is a decorator,
# which tells the application which URL should call
# the associated function.
@app.route("/iss_stock/", methods=['GET']) #,<rtype>:<'csv'>,<date>:<'Today'>, <stock_id>:<'AAPL'>, <interval>:<'15'>}")
# ‘/’ URL is bound with hello_world() function.
def iss_stock(): #corr_type='pearson',rtype='csv',date='Today', stock_id='AAPL', interval='15'):
    corr_type = request.args.get('corr_type', 'pearson') # use default value repalce 'None'
    rtype = request.args.get('rtype', 'csv')
    date = request.args.get('date', 'Today')
    stock_id = request.args.get('stock_id', 'AAPL')
    interval = request.args.get('interval', '15')
    
    
    if date=='Today' or date=='today':
        date_str=datetime.today()
    else:
        date_str = datetime.strptime(date, '%Y-%m-%d').date()
        if date_str>=datetime.today().date():
            date_str=datetime.today()
        
    if date_str>=datetime.today():
        date_str=datetime.today()
    date_object=date_str
    print(date_object)
    north_america = holidays.CA() + holidays.US() + holidays.MX()
    date_list=[]
    i=0
    count=0
    while 1:
        datee=date_object - BDay(i)
        if datee<=date_object and datee not in north_america:
            count=count+1
            date_list.append(datee)
        if count==5:
            break
        i=i+1
    print(date_list)
    from_=date_list[-1].timestamp() # FARTHEST BUSINESS DAY  
    to_=date_list[0].timestamp()        # CLOSEST BUSINESS DAY 
    #print(from_,to_)
    
    finnhub_client = finnhub.Client(api_key="cdiltaiad3i9g9pvsj60cdiltaiad3i9g9pvsj6g")
    try:
        load=finnhub_client.stock_candles(stock_id,interval, int(from_),int(to_))
        stock_data=pd.DataFrame(load)
        #print(load)
    except Exception as e:
        #print(load)
        return load
    print(stock_data.shape)
    stock_data['single_pt_haversine']=iss_loc_haversine
    
    stock_data['date']=stock_data['t'].apply(retrive_date)
    stock_data['weekday']=stock_data['t'].apply(weekday)
    del stock_data['s']
    del stock_data['t']
    unique_date=list(stock_data['date'].unique())
    
    new_df=pd.DataFrame(columns=['date','Day','Iss_location','Single_pt_haversine','Correleation_with_close','Correleation_with_high',
                               'Correleation_with_low','Correleation_with_open','Correleation_with_volume'])
    
    print(stock_data.shape)
    for j in unique_date:
        dict_={}
        temp_df=stock_data[(stock_data['date']==j)]
        print(temp_df.shape)
        day=temp_df['weekday'].iloc[0]
        #print(temp_df.dtypes())
        del temp_df['date']
        del temp_df['weekday']
        temp_df=temp_df.astype(float)
        #print(temp_df)
        closing=temp_df['c'].corr(temp_df['single_pt_haversine'],method=corr_type)
        #print(closing)
        high=temp_df['h'].corr(temp_df['single_pt_haversine'],method=corr_type)
        low=temp_df['l'].corr(temp_df['single_pt_haversine'],method=corr_type)
        open_=temp_df['o'].corr(temp_df['single_pt_haversine'],method=corr_type)
        volume=temp_df['v'].corr(temp_df['single_pt_haversine'],method=corr_type)
        #print(closing,high,low,open_,volume)
        dict_['date']=j
        dict_['Day']=day
        dict_['Iss_location']= (float(iss_loc['iss_position']['latitude']),float(iss_loc['iss_position']['longitude']))
        dict_['Single_pt_haversine']=iss_loc_haversine
        dict_['Correleation_with_close']='{:.20f}'.format(closing)
        dict_['Correleation_with_high']='{:.20f}'.format(high)
        dict_['Correleation_with_low']='{:.20f}'.format(low)
        dict_['Correleation_with_open']='{:.20f}'.format(open_)
        dict_['Correleation_with_volume']='{:.20f}'.format(volume)
        
        new_df=new_df.append([dict_],ignore_index=True)
    if rtype=='HTML' or rtype=='html'or rtype=='Html':
        return new_df.to_html()
    else:
        resp = make_response(new_df.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp 

        #return new_df.to_dict()
 
# main driver function
if __name__ == '__main__':
    
 
    # run() method of Flask class runs the application
    # on the local development server.
    app.run(debug=True,use_reloader=False)
    
    
    
    
    
    
    
    
