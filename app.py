import os
import re
import json
import flask
import traceback
from flask import Flask, request, Response
from collections import OrderedDict

from config import config
from model import Recommender


app = Flask(__name__)

model = Recommender()
model.build_index(config.MODEL_COUNTER_PATH, config.MODEL_MAX_HEAPSIZE)
model.connect_db(
    host=config.DB_HOST,
    user=config.DB_USERNAME,
    password=config.DB_PASSWORD,
    port=config.DB_PORT,
    db=config.DB_DBNAME
)

@app.route('/statistics/id/<id>')
def recommend_by_id(id):
    input_item = model.db.execute("SELECT * FROM `vw_items` WHERE `id`=%s", args=(id,), fetch_one=True)
    if not input_item:
        return "Item doesn't exist", 404
    
    input_item['attrs'] = {attr: input_item[attr] if attr in input_item else None for attr in model.attributes}
    for attr in input_item['attrs']:
        del input_item[attr]

    ngroups_max = request.args.get('ngroups_max', type=int)
    nitems_max = request.args.get('nitems_max', type=int)
    
    try:
        predictions, top_attributes = model.recommend(input_item=input_item, ngroups_max=ngroups_max, nitems_max=nitems_max, filter_category=['신발','패션잡화','가방','아우터'])
    except Exception as e:
        traceback.print_exc()
        app.logger.error(e)
        return 'Internal Module Error', 500

    # 프론트 요청
    # TODO data reformatting 
    # will be removed
    for attr in model.attributes:
        input_item[attr] = input_item['attrs'][attr]
    
    for pred in predictions:
        for attr in pred['attrs']:
            pred[attr] = pred['attrs'][attr]
    #####
    
    def change_img_number(url, number):
        return re.sub('_(\d)_ORGINL', f'_{number}_ORGINL', url)
    
    input_item['fashion_url'] = change_img_number(input_item['img_url'], number=2)
    input_item['item_url'] = change_img_number(input_item.pop('img_url'), number=1)
    
    for pred in predictions:
        for item in pred['items']:
            item['fashion_url'] = change_img_number(item['img_url'], number=2)
            item['item_url'] = change_img_number(item.pop('img_url'), number=1)
    
    return {
        'input_info': input_item,
        'predictions_info': predictions,
        'top_attributes': top_attributes
    }
