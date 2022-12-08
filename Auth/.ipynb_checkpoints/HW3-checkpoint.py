# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 09:16:41 2022

@author: pbhav
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 17:47:39 2022

@author: pbhav
"""

#!/usr/bin/env python
# coding: utf-8

# In[16]:


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
# In[17]:


# The following 10 blocks of code define the classes for storing the the return data, for each
# currency pair.
        
# Define the AUDUSD_return class - each instance will store one row from the dataframe
class AUDUSD_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if AUDUSD_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - AUDUSD_return.last_price) / AUDUSD_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            AUDUSD_return.run_sum = 0
        else:
            # Increment the counter
            if AUDUSD_return.num < 5:
                AUDUSD_return.num += 1
            AUDUSD_return.run_sum += hist_return
        AUDUSD_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            AUDUSD_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            AUDUSD_return.run_sum -= pop_value
            avg = AUDUSD_return.run_sum/(AUDUSD_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(AUDUSD_return.run_squared_sum/(AUDUSD_return.num))
            self.std_return = std
            AUDUSD_return.run_sum_of_std += std
            AUDUSD_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            AUDUSD_return.run_sum_of_std -= pop_value
            avg_std = AUDUSD_return.run_sum_of_std/(AUDUSD_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[18]:


# Define the GBPEUR_return class - each instance will store one row from the dataframe
class GBPEUR_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if GBPEUR_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - GBPEUR_return.last_price) / GBPEUR_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            GBPEUR_return.run_sum = 0
        else:
            # Increment the counter
            if GBPEUR_return.num < 5:
                GBPEUR_return.num += 1
            GBPEUR_return.run_sum += hist_return
        GBPEUR_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            GBPEUR_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            GBPEUR_return.run_sum -= pop_value
            avg = GBPEUR_return.run_sum/(GBPEUR_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(GBPEUR_return.run_squared_sum/(GBPEUR_return.num))
            self.std_return = std
            GBPEUR_return.run_sum_of_std += std
            GBPEUR_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            GBPEUR_return.run_sum_of_std -= pop_value
            avg_std = GBPEUR_return.run_sum_of_std/(GBPEUR_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[19]:


# Define the USDCAD_return class - each instance will store one row from the dataframe
class USDCAD_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):

        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDCAD_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDCAD_return.last_price) / USDCAD_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDCAD_return.run_sum = 0
        else:
            # Increment the counter
            if USDCAD_return.num < 5:
                USDCAD_return.num += 1
            USDCAD_return.run_sum += hist_return
        USDCAD_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDCAD_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCAD_return.run_sum -= pop_value
            avg = USDCAD_return.run_sum/(USDCAD_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDCAD_return.run_squared_sum/(USDCAD_return.num))
            self.std_return = std
            USDCAD_return.run_sum_of_std += std
            USDCAD_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCAD_return.run_sum_of_std -= pop_value
            avg_std = USDCAD_return.run_sum_of_std/(USDCAD_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[20]:


# Define the USDJPY_return class - each instance will store one row from the dataframe
class USDJPY_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDJPY_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDJPY_return.last_price) / USDJPY_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDJPY_return.run_sum = 0
        else:
            # Increment the counter
            if USDJPY_return.num < 5:
                USDJPY_return.num += 1
            USDJPY_return.run_sum += hist_return
        USDJPY_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDJPY_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDJPY_return.run_sum -= pop_value
            avg = USDJPY_return.run_sum/(USDJPY_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDJPY_return.run_squared_sum/(USDJPY_return.num))
            self.std_return = std
            USDJPY_return.run_sum_of_std += std
            USDJPY_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDJPY_return.run_sum_of_std -= pop_value
            avg_std = USDJPY_return.run_sum_of_std/(USDJPY_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[21]:


# Define the USDMXN_return class - each instance will store one row from the dataframe
class USDMXN_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDMXN_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDMXN_return.last_price) / USDMXN_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDMXN_return.run_sum = 0
        else:
            # Increment the counter
            if USDMXN_return.num < 5:
                USDMXN_return.num += 1
            USDMXN_return.run_sum += hist_return
        USDMXN_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDMXN_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDMXN_return.run_sum -= pop_value
            avg = USDMXN_return.run_sum/(USDMXN_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDMXN_return.run_squared_sum/(USDMXN_return.num))
            self.std_return = std
            USDMXN_return.run_sum_of_std += std
            USDMXN_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDMXN_return.run_sum_of_std -= pop_value
            avg_std = USDMXN_return.run_sum_of_std/(USDMXN_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[22]:


# Define the EURUSD_return class - each instance will store one row from the dataframe
class EURUSD_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if EURUSD_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - EURUSD_return.last_price) / EURUSD_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            EURUSD_return.run_sum = 0
        else:
            # Increment the counter
            if EURUSD_return.num < 5:
                EURUSD_return.num += 1
            EURUSD_return.run_sum += hist_return
        EURUSD_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            EURUSD_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            EURUSD_return.run_sum -= pop_value
            avg = EURUSD_return.run_sum/(EURUSD_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(EURUSD_return.run_squared_sum/(EURUSD_return.num))
            self.std_return = std
            EURUSD_return.run_sum_of_std += std
            EURUSD_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            EURUSD_return.run_sum_of_std -= pop_value
            avg_std = EURUSD_return.run_sum_of_std/(EURUSD_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[23]:


# Define the USDCNY_return class - each instance will store one row from the dataframe
class USDCNY_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDCNY_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDCNY_return.last_price) / USDCNY_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDCNY_return.run_sum = 0
        else:
            # Increment the counter
            if USDCNY_return.num < 5:
                USDCNY_return.num += 1
            USDCNY_return.run_sum += hist_return
        USDCNY_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDCNY_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCNY_return.run_sum -= pop_value
            avg = USDCNY_return.run_sum/(USDCNY_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDCNY_return.run_squared_sum/(USDCNY_return.num))
            self.std_return = std
            USDCNY_return.run_sum_of_std += std
            USDCNY_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCNY_return.run_sum_of_std -= pop_value
            avg_std = USDCNY_return.run_sum_of_std/(USDCNY_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[24]:


# Define the USDCZK_return class - each instance will store one row from the dataframe
class USDCZK_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDCZK_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDCZK_return.last_price) / USDCZK_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDCZK_return.run_sum = 0
        else:
            # Increment the counter
            if USDCZK_return.num < 5:
                USDCZK_return.num += 1            
            USDCZK_return.run_sum += hist_return
        USDCZK_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDCZK_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCZK_return.run_sum -= pop_value
            avg = USDCZK_return.run_sum/(USDCZK_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDCZK_return.run_squared_sum/(USDCZK_return.num))
            self.std_return = std
            USDCZK_return.run_sum_of_std += std
            USDCZK_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCZK_return.run_sum_of_std -= pop_value
            avg_std = USDCZK_return.run_sum_of_std/(USDCZK_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[25]:


# Define the USDPLN_return class - each instance will store one row from the dataframe
class USDPLN_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDPLN_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDPLN_return.last_price) / USDPLN_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDPLN_return.run_sum = 0
        else:
            # Increment the counter
            if USDPLN_return.num < 5:
                USDPLN_return.num += 1
            USDPLN_return.run_sum += hist_return
        USDPLN_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDPLN_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDPLN_return.run_sum -= pop_value
            avg = USDPLN_return.run_sum/(USDPLN_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDPLN_return.run_squared_sum/(USDPLN_return.num))
            self.std_return = std
            USDPLN_return.run_sum_of_std += std
            USDPLN_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDPLN_return.run_sum_of_std -= pop_value
            avg_std = USDPLN_return.run_sum_of_std/(USDPLN_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[26]:


# Define the USDINR_return class - each instance will store one row from the dataframe
class USDINR_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDINR_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDINR_return.last_price) / USDINR_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDINR_return.run_sum = 0
        else:
            # Increment the counter
            if USDINR_return.num < 5:
                USDINR_return.num += 1
            USDINR_return.run_sum += hist_return
        USDINR_return.last_price = avg_price
    
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDINR_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDINR_return.run_sum -= pop_value
            avg = USDINR_return.run_sum/(USDINR_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDINR_return.run_squared_sum/(USDINR_return.num))
            self.std_return = std
            USDINR_return.run_sum_of_std += std
            USDINR_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDINR_return.run_sum_of_std -= pop_value
            avg_std = USDINR_return.run_sum_of_std/(USDINR_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[27]:


# We can buy, sell, or do nothing each time we make a decision.
# This class defies a nobject for keeping track of our current investments/profits for each currency pair
class portfolio(object):
    def __init__(self,from_,to):
        # Initialize the 'From' currency amont to 1
        self.from_ = from_
        self.to = to
        # We want to keep track of state, to see what our next trade should be
        self.Prev_Action_was_Buy = False
    
    # This defines a function to buy the 'To' currency. It will always buy the max amount, in whole number
    # increments
    def buy_curr(self, price, num_to_buy, ret_loss, amount, curr2):
        print("buy_cur")
        if amount >= 1 and amount <=100:
            amount -= num_to_buy
            self.Prev_Action_was_Buy = True
            curr2 += num_to_buy*price
            print("Bought %d worth of the target currency (%s). Our current profits and losses in the original currency (%s) are: %f." % (num_to_buy,self.to,self.from_,(amount-1)))
        else:
            print("There was not enough of the original currency (%s) to make another buy." % self.from_)
        f = open('balances_'+str(self.from_)+str(self.to)+'.csv', 'a', newline='')
        writer = csv.writer(f)
        # ['Balances', 'prof_loss', 'return_pf']
        writer.writerow([num_to_buy, amount-1, ret_loss])
        f.close()
    # This defines a function to sell the 'To' currency. It will always sell the max amount, in a whole number
    # increments
    def sell_curr(self, price, num_to_sell, ret_loss, amount, curr2):
        if curr2 >= 1 and curr2 <=100:
            amount += num_to_sell * (1/price)
            self.Prev_Action_was_Buy = False
            curr2 -= num_to_sell
            print("Sold %d worth of the target currency (%s). Our current profits and losses in the original currency (%s) are: %f." % (num_to_sell,self.to,self.from_,(amount-1)))
        else:
            print("There was not enough of the target currency (%s) to make another sell." % self.to)   
        f = open('balances_'+str(self.from_)+str(self.to)+'.csv', 'a', newline='')
        # ['Balances', 'prof_loss', 'return_pf']
        writer = csv.writer(f)
        writer.writerow([num_to_sell, amount-1, ret_loss])
        f.close()
# In[28]:


# Function slightly modified from polygon sample code to format the date string 
def ts_to_datetime(ts) -> str:
    return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

# Function which clears the raw data tables once we have aggregated the data in a 6 minute interval
def reset_raw_data_tables(engine,currency_pairs):
    with engine.begin() as conn:
        for curr in currency_pairs:
            conn.execute(text("DROP TABLE "+curr[0]+curr[1]+"_raw;"))
            conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text);"))

# This creates a table for storing the raw, unaggregated price data for each currency pair in the SQLite database
def initialize_raw_data_tables(engine,currency_pairs):
    with engine.begin() as conn:
        for curr in currency_pairs:
            conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text);"))

# This creates a table for storing the (6 min interval) aggregated price data for each currency pair in the SQLite database            
def initialize_aggregated_tables(engine,currency_pairs):
    with engine.begin() as conn:
        for curr in currency_pairs:
            conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_agg(inserttime text, avgfxrate numeric, stdfxrate numeric, prev_avg numeric, min_val numeric, max_val numeric, vol_val numeric, fd numeric, return numeric);"))

# This function is called every 6 minutes to aggregate the data, store it in the aggregate table, 
# and then delete the raw data
def aggregate_raw_data_tables(engine,currency_pairs, count):
    with engine.begin() as conn:
        print("1:aggregate_raw_data_tables")
        for curr in currency_pairs:
            result = conn.execute(text("SELECT AVG(fxrate) as avg_price, COUNT(fxrate) as tot_count FROM "+curr[0]+curr[1]+"_raw;"))

            for row in result:
                avg_price = row.avg_price
                tot_count = row.tot_count
            std_res = conn.execute(text("SELECT SUM((fxrate - "+str(avg_price)+")*(fxrate - "+str(avg_price)+"))/("+str(tot_count)+"-1) as std_price FROM "+curr[0]+curr[1]+"_raw;"))
            for row in std_res:
                std_price = sqrt(row.std_price)
            date_res = conn.execute(text("SELECT MAX(ticktime) as last_date FROM "+curr[0]+curr[1]+"_raw;"))
            for row in date_res:
                last_date = row.last_date
                
            ## Extract minimum, maximum, max-min and average value of 6 min data from raw table
            values = conn.execute(text("SELECT AVG(fxrate) as avg_price, MIN(fxrate) as min_price, MAX(fxrate) as max_price, MAX(fxrate)-MIN(fxrate) as vol FROM "+curr[0]+curr[1]+"_raw;"))
            for vls in values: 
                avg_price1 = vls.avg_price
                min_price = vls.min_price
                max_price = vls.max_price
                vol = vls.vol
                
            # During thr first period (first 6 min) fd is None
            if count <= 120:
                fd = None
                ret = 0
            
            # FOr periods 2 to 100 fetch values and calculate fd and update values
            if count > 120:
                #Get the number of the last entry of the aggregate table
                cnt = conn.execute(text("SELECT COUNT(prev_avg) as cnts FROM "+curr[0]+curr[1]+"_agg;"))
                for cn in cnt:
                    cnts = cn.cnts
                #Fetch the previous average and volatility values using the number of the last row
                prev_vals = conn.execute(text("SELECT prev_avg, vol_val FROM "+curr[0]+curr[1]+"_agg LIMIT "+str(cnts)+"-10, 10;"))

                for vls1 in prev_vals:
                    prev_avg = vls1.prev_avg
                    prev_vol = vls1.vol_val
                
                ret = (avg_price1 - prev_avg)/prev_avg
                print("2:aggregate_raw_data_tables :: return ",ret,"avg_price1",avg_price1,"prev_avg",prev_avg)

                #Calculate keltner bands using fetched previous values
                prev_upp_kel_band = [prev_avg + n*0.025*prev_vol for n in range(1, 101)]
                prev_loww_kel_band = [prev_avg - n*0.025*prev_vol for n in range(1, 101)]
                
                #Calculate fractral dimension using the bands
                fd_count = sum(ii < max_price for ii in prev_upp_kel_band) + sum(ii > min_price for ii in prev_loww_kel_band)
                print("fd:", fd)

                if vol != 0:
                    fd = vol/count
            
            #Save the values into the database table
            conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_agg(inserttime, avgfxrate, stdfxrate, prev_avg, min_val, max_val, vol_val, fd, return) VALUES (:inserttime, :avgfxrate, :stdfxrate, :prev_avg, :min_val, :max_val, :vol_val, :fd, :return);"),[{"inserttime": last_date, "avgfxrate": avg_price, "stdfxrate": std_price, "prev_avg": avg_price1, "min_val": min_price, "max_val": max_price, "vol_val": vol, "fd": fd, "return": ret}])
            print("count in aggrigate table ",count)           
            ###################################################################
            # This calculates and stores the return values
            exec("curr[2].append("+curr[0]+curr[1]+"_return(last_date,avg_price))")
            
            # counter for each hour
            t1 = 360
            
            # At every hour
            if count%t1 == 0:
                print("3 :aggregate_raw_data_tables :: After one hour")
                print("Hours spent: ", (count//t1))
                # At first hour
                if count <= t1:
                    print("4 :aggregate_raw_data_tables :: 1st one hour :: cur[4]", curr[4])
                    avg_ret = 0
                    if curr[4] == -1: 
                        num_to_sell = 100
                        amount = num_to_sell
                        curr2 = num_to_sell
                        curr[3].sell_curr(avg_price1, num_to_sell, avg_ret, amount, curr2)
                    if curr[4] == 1:
                        num_to_buy = 100
                        amount = num_to_buy
                        curr2 = num_to_buy
                        print("5b :aggregate_raw_data_tables :: 1st one hour :: cur[4]", curr[4])
                        print("5a avg_price1",avg_price1, "num_to_sell",num_to_sell, "avg_ret",avg_ret ,"amount",amount, "curr2", curr2)
                        curr[3].buy_curr(avg_price1, num_to_buy, avg_ret, amount, curr2)
                
                # At every hour other than the first
                if count > t1:
                    try:
                        cnt1 = conn.execute(text("SELECT COUNT(return) as cnts FROM AUDUSD_agg;"))
    
                        for cn in cnt1:
                            cnts1 = cn.cnts
                        
                        #Fetch the previous 10 return values
                        prev_ret = conn.execute(text("SELECT return as prev_return FROM AUDUSD_agg LIMIT "+str(cnts1)+"-10, 10;"))
                        prev_rets = []
                        
                        for vls2 in prev_ret:
                            prev_rets.append(vls2.prev_return)
                        
                        #Calculate the average return
                        avg_ret = sum(prev_rets)
                        print("Average return: ", avg_ret)
                        
                        # losses for each hour
                        losses = [0.250, 0.150, 0.100, 0.050, 0.050, 0.050, 0.050, 0.050, 0.050, 0.050]
                        
                        lim_loss = losses[(count//t1)-1]
                        
                        # condition to buy/sell/stop trade
                        if abs(avg_ret) <= lim_loss: #  (return_value > 0) and (return_value_1 > 0) and   
                            if curr[4] == -1:
                                num_to_sell = 100*(1+(count//t1))
                                amount = num_to_sell
                                curr2 = num_to_sell

                                curr[3].sell_curr(avg_price1, num_to_sell, avg_ret, amount, curr2)
                            if curr[4] == 1:
                                num_to_buy = 100*(1+(count//t1))
                                amount = num_to_buy
                                curr2 = num_to_buy
                                curr[3].buy_curr(avg_price1, num_to_buy, avg_ret, amount, curr2)
                            if curr[4] == 0:
                                print("Trade closed")
                        else:
                            curr[4] = 0
                            print("There was not enough of the target currency to make another trade. Trade closed")
                            
                    except:
                        pass
            
            
            
# This main function repeatedly calls the polygon api every 1 seconds for 24 hours 
# and stores the results.
def main(currency_pairs):
    # The api key given by the professor
    # key = key_fun() # Fetch the API key from the library
    key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
    for currency in currency_pairs:
        # Set the input variables to the API
        from_ = currency[0]
        to = currency[1]
    
        f = open('output2_'+str(from_)+str(to)+'.csv', 'w', newline='')
        writer = csv.DictWriter(f, fieldnames=['Balances', 'prof_loss', 'return_pf'])
        writer.writeheader()
        
        # writer.writerow(['Balances', 'prof_loss', 'return_pf'])
        f.close()
    # Number of list iterations - each one should last about 1 second
    count = 0
    agg_count = 0
    
    # Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
    engine = create_engine("sqlite+pysqlite:///sqlite/newfinal.db", echo=False, future=True)
    
    # Create the needed tables in the database
    initialize_raw_data_tables(engine,currency_pairs)
    initialize_aggregated_tables(engine,currency_pairs)
    
    # Open a RESTClient for making the api calls
    client = RESTClient(key)
    # with RESTClient(key) as client:
        # Loop that runs until the total duration of the program hits 24 hours. 
    while count <= 36000: # 86400 seconds = 24 hours
        # Make a check to see if 6 minutes has been reached or not

        if agg_count == 360:
            # Aggregate the data and clear the raw data tables
            aggregate_raw_data_tables(engine,currency_pairs, count)
            
            reset_raw_data_tables(engine,currency_pairs)
            agg_count = 0
        
        # Only call the api every 1 second, so wait here for 0.75 seconds, because the 
        # code takes about .15 seconds to run
        time.sleep(0.75)
        
        # Increment the counters
        count += 1
        agg_count +=1

        # Loop through each currency pair
        for currency in currency_pairs:
            # Set the input variables to the API
            from_ = currency[0]
            to = currency[1]

            # Call the API with the required parameters
            try:
                resp = client.get_real_time_currency_conversion(from_, to, amount=100, precision=2)
    
            except:
                continue

            # This gets the Last Trade object defined in the API Resource
            last_trade = resp.last
            
            # Format the timestamp from the result
            dt = ts_to_datetime(last_trade.timestamp)

            # Get the current time and format it
            insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Calculate the price by taking the average of the bid and ask prices
            avg_price = (last_trade.bid + last_trade.ask)/2

            # Write the data to the SQLite database, raw data tables
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO "+from_+to+"_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"),[{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])
    
    for currency in currency_pairs:
        # Set the input variables to the API
        from_ = currency[0]
        to = currency[1]
        
        # write the final db to csv
        
        with engine.begin() as conn:        
            cursor = conn.execute(text("SELECT * FROM "+str(from_)+str(to)+"_agg;"))
            outfile = open('data_vec_'+str(from_)+str(to)+'.csv', 'w', newline='')
            outcsv = csv.writer(outfile)
            header = ['inserttime', 'avgfxrate', 'stdfxrate', 'prev_avg', 'min_val', 'max_val', 'vol_val', 'fd', 'return']
            outcsv.writerow(header)
            outcsv.writerows(cursor.fetchall())
            outfile.close()


# from file_name import fun_name
# 

# In[29]:


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


# # The following code blocks were used on historical data to fomulate a strategy

# In[ ]:



# In[ ]:




