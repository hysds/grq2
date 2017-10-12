#!/bin/bash
export DMB_IDX="grq_v01_dumby-product"

curl -XDELETE "http://localhost:9200/${DMB_IDX}"
python create_index.py ${DMB_IDX} dumby-product
curl -XPOST 'http://localhost:9200/_aliases' -d "
{
    \"actions\" : [
        { \"add\" : { \"index\" : \"${DMB_IDX}\", \"alias\" : \"grq\" } },
        { \"add\" : { \"index\" : \"${DMB_IDX}\", \"alias\" : \"grq_demo\" } },
        { \"add\" : { \"index\" : \"${DMB_IDX}\", \"alias\" : \"grq_dumby-product\" } }
    ]
}"
