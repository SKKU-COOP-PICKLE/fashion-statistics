from typing import *
import numpy as np
from itertools import product


DELIM = '#'

def attr2keys(attr: dict, filter_attrs: List[str], divide=False) -> List[str]:
    """
    convert attributes in dictionary into string list
    
    ex. if divide:
            {'sex': 'MEN', 'style': 'Casual,Trendy'} -> ['MEN#Casual', 'MEN#Trendy']
        else:
            {'sex': 'MEN', 'style': 'Casual,Trendy'} -> ['MEN#Casual,Trendy']
    """
    attr = {k: attr[k] for k in filter_attrs}
    if divide:
        keys = []
        for comb in product(*tuple([x.split(',') if x else [''] for x in attr.values()])):
            keys.append(DELIM.join(comb))
        return keys
    else:
        return DELIM.join([','.join(sorted(x.split(','))) if x else '' for x in attr.values()])


def key2attr(key: str, filter_attrs: List[str]) -> dict:
    return {k: v if v != '' else None for k, v in zip(filter_attrs, key.split(DELIM))}
