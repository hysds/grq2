#!/bin/bash
export OCO2_IDX="grq_v02_oco2-l2fp_sounding_group"

curl -XDELETE "http://localhost:9200/${OCO2_IDX}"
python create_index.py ${OCO2_IDX} oco2
curl -XPOST 'http://localhost:9200/_aliases' -d "
{
    \"actions\" : [
        { \"add\" : { \"index\" : \"${OCO2_IDX}\", \"alias\" : \"grq\" } },
        { \"add\" : { \"index\" : \"${OCO2_IDX}\", \"alias\" : \"grq_oco2\" } }
    ]
}"
