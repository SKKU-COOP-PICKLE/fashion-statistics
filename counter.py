import os
import argparse
import pickle
import pymysql
import pandas as pd
import numpy as np
from tqdm import tqdm

from itertools import permutations, product
from collections import Counter
import logging

from config import config
from util import attr2keys, DELIM
from db_handler import DatabaseHandler

class FashionCounter:
    def __init__(self, attributes):
        self.counter = Counter()
        self.attributes = attributes
    
    def update(self, df: object, group_name = 'fashion_id') -> None:
        """
        count all item relations in dataframe group by group_name
        """
        for indexes in tqdm(df.groupby(group_name).groups.values()):
            for idx1, idx2 in permutations(indexes, 2):
                string1 = attr2keys(dict(df.loc[idx1]), self.attributes, divide=False)
                string2 = attr2keys(dict(df.loc[idx2]), self.attributes, divide=False)
                
                if string1.split(DELIM)[1].split('>')[0] == string2.split(DELIM)[1].split('>')[0]:
                    continue
                
                for p in product([string1], [string2]):
                    self.counter[p] += 1
                    
    
    def save(self, output_path: str) -> None:
        with open(output_path, 'wb') as f:
            pickle.dump({
                'counter': self.counter,
                'attributes': self.attributes
                }, f)

if __name__=='__main__':
    ATTRIBUTES = [
        'sex', 'category', 'color', 'pattern', 'style', 'fit', 'materials', 'length', 'neckline', 'sleeve_length'
    ]
    db = DatabaseHandler(
        host=config.DB_HOST,
        user=config.DB_USERNAME,
        password=config.DB_PASSWORD,
        port=config.DB_PORT,
        db=config.DB_DBNAME
    )
    data = db.execute("SELECT * FROM `vw_fashion_items`")
    counter = FashionCounter(attributes=ATTRIBUTES)
    counter.update(pd.DataFrame(data))
    counter.save('data/counter.pkl')
