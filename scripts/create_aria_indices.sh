#!/bin/bash

./create_index.py grq_v02_csk InSAR; curl -XPOST 'http://localhost:9200/_aliases' -d "
{
    \"actions\" : [
        { \"add\" : { \"index\" : \"grq_v02_csk\", \"alias\" : \"grq\" } },
        { \"add\" : { \"index\" : \"grq_v02_csk\", \"alias\" : \"grq_csk\" } },
        { \"add\" : { \"index\" : \"grq_v02_csk\", \"alias\" : \"grq_aria\" } } ] }"
./create_index.py grq_v02_interferogram InSAR
curl -XPOST 'http://localhost:9200/_aliases' -d "
{
    \"actions\" : [
        { \"add\" : { \"index\" : \"grq_v02_interferogram\", \"alias\" : \"grq\" } },
        { \"add\" : { \"index\" : \"grq_v02_interferogram\", \"alias\" : \"grq_csk\" } },
        { \"add\" : { \"index\" : \"grq_v02_interferogram\", \"alias\" : \"grq_interferogram\" } } ] }"
