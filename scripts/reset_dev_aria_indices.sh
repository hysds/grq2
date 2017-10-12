#!/bin/bash
export CSK_IDX="grq_dev_v10_csk"
export INT_IDX="grq_dev_v10_interferogram"
export TS_IDX="grq_dev_v10_time-series"
export S1_IDX="grq_dev_v10_s1"
export S1_SWATH_IDX="grq_dev_v10_s1-swath"
export S1_IFG_IDX="grq_dev_v10_s1-ifg"
export S1_COH_IDX="grq_dev_v10_s1-coh"
export EF_IDX="grq_dev_v10_features"
export PRED_MOD_IDX="grq_dev_v10_predictor_model"
export INC_IDX="grq_dev_v01_incoming"
export AOI_IDX="grq_dev_v10_area_of_interest"

curl -XDELETE "http://localhost:9200/${CSK_IDX}"
curl -XDELETE "http://localhost:9200/${INT_IDX}"
curl -XDELETE "http://localhost:9200/${TS_IDX}"
curl -XDELETE "http://localhost:9200/${S1_IDX}"
curl -XDELETE "http://localhost:9200/${S1_SWATH_IDX}"
curl -XDELETE "http://localhost:9200/${S1_IFG_IDX}"
curl -XDELETE "http://localhost:9200/${S1_COH_IDX}"
curl -XDELETE "http://localhost:9200/${EF_IDX}"
curl -XDELETE "http://localhost:9200/${PRED_MOD_IDX}"
curl -XDELETE "http://localhost:9200/${INC_IDX}"
curl -XDELETE "http://localhost:9200/${AOI_IDX}"
python create_index.py ${CSK_IDX} InSAR
python create_index.py ${INT_IDX} InSAR
python create_index.py ${TS_IDX} InSAR
python create_index.py ${S1_IDX} InSAR
python create_index.py ${S1_SWATH_IDX} InSAR
python create_index.py ${S1_IFG_IDX} InSAR
python create_index.py ${S1_COH_IDX} InSAR
python create_index.py ${EF_IDX} features
python create_index.py ${PRED_MOD_IDX} predictor_model
python create_index.py ${INC_IDX} incoming
python create_index.py ${AOI_IDX} area_of_interest
curl -XPOST 'http://localhost:9200/_aliases' -d "
{
    \"actions\" : [
        { \"add\" : { \"index\" : \"${AOI_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${AOI_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${AOI_IDX}\", \"alias\" : \"grq_dev_area_of_interest\" } },
        { \"add\" : { \"index\" : \"${INC_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${INC_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${INC_IDX}\", \"alias\" : \"grq_dev_incoming\" } },
        { \"add\" : { \"index\" : \"${S1_IFG_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${S1_IFG_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${S1_IFG_IDX}\", \"alias\" : \"grq_dev_s1-ifg\" } },
        { \"add\" : { \"index\" : \"${S1_COH_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${S1_COH_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${S1_COH_IDX}\", \"alias\" : \"grq_dev_s1-coh\" } },
        { \"add\" : { \"index\" : \"${S1_SWATH_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${S1_SWATH_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${S1_SWATH_IDX}\", \"alias\" : \"grq_dev_s1-swath\" } },
        { \"add\" : { \"index\" : \"${S1_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${S1_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${S1_IDX}\", \"alias\" : \"grq_dev_s1\" } },
        { \"add\" : { \"index\" : \"${EF_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${EF_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${EF_IDX}\", \"alias\" : \"grq_dev_features\" } },
        { \"add\" : { \"index\" : \"${PRED_MOD_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${PRED_MOD_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${PRED_MOD_IDX}\", \"alias\" : \"grq_dev_predictor_model\" } },
        { \"add\" : { \"index\" : \"${TS_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${TS_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${TS_IDX}\", \"alias\" : \"grq_dev_time-series\" } },
        { \"add\" : { \"index\" : \"${CSK_IDX}\", \"alias\" : \"grq_dev\" } },
        { \"add\" : { \"index\" : \"${CSK_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${CSK_IDX}\", \"alias\" : \"grq_dev_csk\" } },
        { \"add\" : { \"index\" : \"${INT_IDX}\", \"alias\" : \"grq_dev\" }},
        { \"add\" : { \"index\" : \"${INT_IDX}\", \"alias\" : \"grq_dev_aria\" } },
        { \"add\" : { \"index\" : \"${INT_IDX}\", \"alias\" : \"grq_dev_interferogram\" } }
    ]
}"
