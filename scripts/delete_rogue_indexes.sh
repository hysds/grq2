#!/bin/bash

ROGUE_INDEXES=(
  admin
  bin
  blazeds
  cgi-bin
  docs
  flex2gateway
  gw
  *.php  
  *.html  
  *.cgi
  formmail*
  lcds
  messagebroker
  perl
  phppath
  samba
  scripts
  servlet
  *.asp
  *.pl
  spipe
  topic
  webui
)

for i in ${ROGUE_INDEXES[@]}; do
  curl -XDELETE "http://localhost:9200/$i/"
  echo "Deleted $i"
done
