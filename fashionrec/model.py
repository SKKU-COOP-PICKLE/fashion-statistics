import os
import logging
import json
import pickle
import heapq
from typing import *
from collections import defaultdict, OrderedDict
import numpy as np


from .util import *
from .db_handler import DatabaseHandler

class FashionRec:
    def __init__(self):
        self.index = None
        self.attributes = None
        self.db = None
        
    def load(self, index_path: str):
        with open(index_path, 'rb') as f:
            self.index = pickle.load(f) 
    
    def save(self, save_path: str):
        with open(save_path, 'wb') as f:
            pickle.dump(self.index, f)
    
    def connect_db(self, host, password, user, db, port):
        self.db = DatabaseHandler(
                                host=host,
                                user=user,
                                password=password,
                                db=db,
                                port=port
                                )
                                
    def build_index(self, counter_path: str, max_heap_size: int):
        """
        Make index based on saved counter.pkl file
        
        :param counter_path : path of counter pickle file 
        :param max_heap_size : size of heap. if set -1, no limitation
        """
        
        with open(counter_path, 'rb') as f:
            data = pickle.load(f)
            counter = data['counter']
            self.attributes = data['attributes']
        
        self.index = defaultdict(list)

        for (item1, item2), count in counter.items():
            if max_heap_size == -1 or len(self.index[item1]) < max_heap_size:
                heapq.heappush(self.index[item1], (count, item2))
            else:
                heapq.heappushpop(self.index[item1], (count, item2))
                
        for item in self.index.keys():
            self.index[item].sort(key=lambda x: x[0], reverse=True)

    def get_answers(self, id: str, filter_category: List[str] = []) -> List[dict]:
        assert self.db, "Not connected to DB"
        
        answer_items_sql = """
                SELECT * FROM `vw_items`
                JOIN (
                    SELECT * FROM `fashion_to_item`
                    WHERE `fashion_to_item`.fashion_id in (
                        SELECT `fashion_id`
                        FROM `fashion_to_item`
                        WHERE `item_id` = %s AND `fashion_id` LIKE %s )
                    AND `item_id` != %s
                    ) AS f
                ON f.item_id = vw_items.id
            """
            
        img_sql = """
            SELECT img_url
            FROM `vw_items`
            WHERE vw_items.id=%s
        """
        
        _answers = self.db.execute(answer_items_sql, args=(id, 'W%', id))
        
        answers = []
        answers_ids = set()
        for _answer in _answers:
            if any([x in _answer['category'] for x in filter_category]):
                continue
            if _answer['id'] in answers_ids:
                continue
            answers_ids.update(_answer['id'])
            _answers_fashion = self.db.execute(img_sql, args=(_answer['fashion_id'][1:],), fetch_one=True)
            answers.append(_answer)
        
        return answers

    def recommend(self, input_item: dict, ngroups_max: int = None, nitems_max: int = None, filter_category: List[str] = []) -> (List[dict], List[dict]):
        """
        :param input_item: item dictionary
            input_item = {
                'id': ...,
                'attrs': {'category': '스커트', 'sex': 'WOMEN', ...}
                'brand': ...,
                'img_url': ...,
                ...
            }
            
        :ngroups_max: 추천될 속성 셋 최대 개수. None이면 전부 반환
        
        :nitems_max: 속성 셋 별 최대 아이템 개수. None이면 전부 반환
        
        :param filter_category: 추천에서 제외하고 싶은 category list
        
        returns (Top items, Top attributes with percentage) 
        """
        assert self.db, "Not connected to DB"
        assert self.index, 'Index is not initialized'
        
        # Select Related Keys
        key_counter = Counter()
        key = attr2keys(input_item['attrs'], self.attributes)
        for count, topkey in self.index[key]:
            category = key2attr(topkey, self.attributes)['category']
            if not category:
                continue
            if any([x in category for x in filter_category]):
                continue
            key_counter[topkey] += count
            
        # Count frequency of attributes
        attr_counters = defaultdict(Counter)
        for key, count in key_counter.items():
            for attr, value in key2attr(key, self.attributes).items():
                if not value:
                    continue
                attr_counters[attr].update(value.split(',')*count)
        
        answers = self.get_answers(id, filter_category=filter_category)
        answers_ids = set([answer['id'] for answer in answers])
        
        predictions = []
        top_attributes = {}
        
        if len(key_counter) == 0:
            return predictions, top_attributes
        
        # top attribute 통계 값 계산
        for attr in self.attributes:
            if attr not in attr_counters:
                continue
            top = attr_counters[attr].most_common(n=1)[0]
            top_attributes[attr] = {'name': top[0], 'percentage': round(top[1]/sum(attr_counters[attr].values()), 2)}
    
        if not ngroups_max:
            ngroups_max = len(key_counter)

        for key, count in key_counter.most_common(n=ngroups_max):
            attr = {k:v for k,v in key2attr(key, self.attributes).items() if v}
            
            # 속성에 해당하는 item을 DB에서 가져오기
            items = self.db.execute(
                        f"SELECT `id`, `brand`, `name`, `detail_url`, `img_url`, `wish`, `price` \
                            FROM `vw_items` WHERE " + \
                            " AND ".join([
                                f"{column} = %s" 
                                if column in attr.keys() 
                                else f"{column} IS NULL" 
                                for column in self.attributes]) + " ORDER BY wish DESC",
                        args=tuple(attr.values()))
            
            for idx, item in enumerate(items):
                item['show_rank'] = idx
            
            for k, v in attr.items():
                attr[k] = [{
                    'percentage': round(attr_counters[k][name]/sum(key_counter.values()),2),
                    'name': name 
                    } for name in v.split(',')]
            
            item_group = {
                'attrs': attr,
                'items': items
            }
            
            predictions.append(item_group)
        
        for pred in predictions:
            for item in pred['items']:
                if item['id'] in answers_ids:
                    item['show_rank'] = -1 # 정답을 최상위로 올리기 위해
                    
            pred['items'].sort(key=lambda item: item['show_rank']) # show rank 순으로 아이템 정렬
            
            if nitems_max:
                pred['items'] = pred['items'][:nitems_max]
            
            for item in pred['items']:
                del item['show_rank']
                
        return predictions, top_attributes
