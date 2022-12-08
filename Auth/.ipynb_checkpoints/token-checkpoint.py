# '''
# @author: Nithya Shathish
# @NetID: nss9899
# '''

import collections
import datetime
import time
import csv
from math import sqrt
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy import text

class Token:
    """
    Fetch Data from polygon API and store in sqllite database

    """
    # Init all the necessary variables when instantiating the class
    def __init__(self):
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
        
    def get_key(self):
        return self.key

