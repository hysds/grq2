#!/bin/bash
curl -XDELETE "http://localhost:9200/grq_v01_input_sounding_list"
curl -XDELETE "http://localhost:9200/grq_v01_input_tar"
python create_index.py grq_v01_input_sounding_list oco2
python create_index.py grq_v01_input_tar oco2
curl -XPOST 'http://localhost:9200/_aliases' -d "
{
    \"actions\" : [
        { \"add\" : { \"index\" : \"grq_v01_input_sounding_list\", \"alias\" : \"grq\" } },
        { \"add\" : { \"index\" : \"grq_v01_input_sounding_list\", \"alias\" : \"grq_oco2\" } },
        { \"add\" : { \"index\" : \"grq_v01_input_sounding_list\", \"alias\" : \"grq_input_sounding_list\" } },
        { \"add\" : { \"index\" : \"grq_v01_input_tar\", \"alias\" : \"grq\" } },
        { \"add\" : { \"index\" : \"grq_v01_input_tar\", \"alias\" : \"grq_oco2\" } },
        { \"add\" : { \"index\" : \"grq_v01_input_tar\", \"alias\" : \"grq_input_tar\" } }
    ]
}"
