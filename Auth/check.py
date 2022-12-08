

# Import required libraries
import datetime
import time
from polygon import RESTClient
from sqlalchemy import create_engine 
from sqlalchemy import text
import sqlalchemy as db
import pandas as pd
from math import sqrt
from math import isnan
import matplotlib.pyplot as plt
from numpy import mean
from numpy import std
from math import floor
# from pkg2.get_key import key_fun
import csv

# We can buy, sell, or do nothing each time we make a decision.
# This class defies a nobject for keeping track of our current investments/profits for each currency pair
class portfolio(object):
    def __init__(self,from_,to):
        # Initialize the 'From' currency amont to 1
        self.from_ = from_
        self.to = to
        # We want to keep track of state, to see what our next trade should be
        self.Prev_Action_was_Buy = False
    


def main(currency_pairs):
    # The api key given by the professor
    # key = key_fun() # Fetch the API key from the library
    currency_pairs = [["AUD","USD",[],portfolio("AUD","USD"), -1],
                  ["GBP","EUR",[],portfolio("GBP","EUR"), -1],
                  ["USD","CAD",[],portfolio("USD","CAD"), -1],
                  ["USD","JPY",[],portfolio("USD","JPY"), -1],
                  ["USD","MXN",[],portfolio("USD","MXN"), -1],
                  ["EUR","USD",[],portfolio("EUR","USD"), 1],
                  ["USD","CNY",[],portfolio("USD","CNY"), 1],
                  ["USD","CZK",[],portfolio("USD","CZK"), 1],
                  ["USD","PLN",[],portfolio("USD","PLN"), 1],
                  ["USD","INR",[],portfolio("USD","INR"), 1]]
    
    key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"

    
    # Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
    engine = create_engine("sqlite+pysqlite:///sqlite/final_input.db", echo=False, future=True)
    
    
    # Open a RESTClient for making the api calls
    client = RESTClient(key)

    for curr in self.currency_pairs:
    key = curr[0] + curr[1]
    result = conn.execute(text("SELECT avgfxrate as a,vol_val as v , fd as fd ,return as ret from " + key + "_agg;"))

    for row in avgfxrate:
        a = row.a
        v = row.v
        fd = row.fd
        r = row.ret
        
        print(a,v,fd,r)


    
    
    
    
#     for currency in currency_pairs:
#         # Set the input variables to the API
#         from_ = currency[0]
#         to = currency[1]
        
#         # write the final db to csv
        
#         with engine.begin() as conn:        
#             cursor = conn.execute(text("SELECT * FROM "+str(from_)+str(to)+"_agg;"))
#             outfile = open('data_vec_'+str(from_)+str(to)+'.csv', 'w', newline='')
#             outcsv = csv.writer(outfile)
#             header = ['inserttime', 'avgfxrate', 'stdfxrate', 'prev_avg', 'min_val', 'max_val', 'vol_val', 'fd', 'return']
#             outcsv.writerow(header)
#             outcsv.writerows(cursor.fetchall())
#             outfile.close()


# A dictionary defining the set of currency pairs we will be pulling data for
currency_pairs = [["AUD","USD",[],portfolio("AUD","USD"), -1],
                  ["GBP","EUR",[],portfolio("GBP","EUR"), -1],
                  ["USD","CAD",[],portfolio("USD","CAD"), -1],
                  ["USD","JPY",[],portfolio("USD","JPY"), -1],
                  ["USD","MXN",[],portfolio("USD","MXN"), -1],
                  ["EUR","USD",[],portfolio("EUR","USD"), 1],
                  ["USD","CNY",[],portfolio("USD","CNY"), 1],
                  ["USD","CZK",[],portfolio("USD","CZK"), 1],
                  ["USD","PLN",[],portfolio("USD","PLN"), 1],
                  ["USD","INR",[],portfolio("USD","INR"), 1]]



# Run the main data collection loop
main(currency_pairs)
