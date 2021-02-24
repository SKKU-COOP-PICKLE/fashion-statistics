# fashion statistics

Statistical fashion recommender system.

통계기반 패션 아이템 추천 시스템. Top K개의 아이템과 총평 확인 가능

## Requirements
    flask pymysql numpy tqdm pandas

## Mysql table info

- `vw_fashion_items`: (fashion_id, item_id, attributes...)

- `vw_items`: (id, attributes...)

- `fashion_to_item`: (fashion_id, item_id)


## Workflow
![](workflow.png)


