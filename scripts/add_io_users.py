#!/usr/bin/env python
from __future__ import print_function
import json, requests, sys, traceback
import re
import fnmatch

from grq2 import app

USER_RE = re.compile("^([a-zA-Z0-9-_.@]+\s*)+$")
es_url = app.config['ES_URL']
def get_matching_hysdsios():
    # get connection and create destination index

    # index all docs from source index to destination index
    query = {
      "fields": "_source",
      "query": {
        "match_all": {}
      },
      "sort": [{"_id":{"order":"asc"}}]
    }
    r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' % (es_url, "hysds_ios"), data=json.dumps(query))
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    users = {}
    print("Available HySDS IOs:")
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            ident = hit["_id"]
            accounts = hit.get("_source",{}).get("allowed_accounts",[])
            print("\t{0}: {1}".format(ident, json.dumps(accounts)))
            users[ident] = accounts
    #Enter in valid HySDS - IO
    print("-----------")
    hysdsios = []
    while len(hysdsios) == 0:
        pattern = raw_input("Enter in hysds-io id glob pattern (from above):")
        hysdsios = fnmatch.filter(users.keys(),pattern)
        if len(hysdsios) == 0:
            print("No matching hysds-ios for '{0}'".format(pattern))
    print()
    print()
    print("Selected hysds-io(s): {0}".format(json.dumps(hysdsios)))
    print("----------")
    return (hysdsios, users)
def get_add_users():
    #User input
    user = ""
    while not USER_RE.match(user):
        if user != "":
            print("Invalid username(s): '{0}'".format(user))
        user = raw_input("Enter in space-separated user(s) to add:")
    user = user.split()
    return user
def add_to_ios(users, hysdsio, existing):
    '''
    '''
    if set(users).union(set(existing[hysdsio])) == set(existing[hysdsio]):
        print("Supplied users already have permission to run {0}, nothing to do, Skipping.".format(hysdsio))
        return
    existing[hysdsio] = list(set(existing[hysdsio]).union(set(users)))
    print()
    print()
    print("------------")
    sure = ""
    while sure == "":
        sure = raw_input("Are you sure you want to add {0} to allowed users for '{1}' for a final user set of {2}?" .format(users,hysdsio,json.dumps(existing[hysdsio])))
        if not sure.startswith("y") and sure != "":
            print("User showed weakness, skipping")
            return
    doc = {
        "doc": {
            "allowed_accounts": existing[hysdsio]
        },
        "doc_as_upsert":True
    }
    try:
        r = requests.post('%s/%s/%s/%s/_update' % (es_url, "hysds_ios","hysds_io",hysdsio), data=json.dumps(doc))
        r.raise_for_status()
    except Exception as e:
        print("[ERROR] Failed to update hysds-io. Resolve IMMEDIATELY. {0}:{1}\n{2}".format(type(e), str(e), traceback.format_exc()), file=sys.stderr)
        sys.exit(-1)
if __name__ == "__main__":
    hysdsios, existing = get_matching_hysdsios()
    users = get_add_users()
    for hysdsio in hysdsios:
        add_to_ios(users, hysdsio, existing)
