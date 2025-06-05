#!/usr/bin/env python

from future import standard_library
standard_library.install_aliases()
import json
import requests
import sys
import traceback
import re
import fnmatch

from grq2 import app

USER_RE = re.compile(r"^([a-zA-Z0-9-_.@]+\s*)+$")
es_url = app.config['ES_URL']


def get_matching_hysdsios():
    # get connection and create destination index

    # index all docs from source index to destination index
    query = {
        "fields": "_source",
        "query": {
            "match_all": {}
        },
        "sort": [{"_id": {"order": "asc"}}]
    }
    r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' %
                      (es_url, "hysds_ios"), data=json.dumps(query))
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    users = {}
    print("Available HySDS IOs:")
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' %
                          es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0:
            break
        for hit in res['hits']['hits']:
            ident = hit["_id"]
            accounts = hit.get("_source", {}).get("allowed_accounts", [])
            print(f"\t{ident}: {json.dumps(accounts)}")
            users[ident] = accounts
    # Enter in valid HySDS - IO
    print("-----------")
    hysdsios = []
    while len(hysdsios) == 0:
        pattern = eval(input("Enter in hysds-io id glob pattern (from above):"))
        hysdsios = fnmatch.filter(list(users.keys()), pattern)
        if len(hysdsios) == 0:
            print(f"No matching hysds-ios for '{pattern}'")
    print()
    print()
    print(f"Selected hysds-io(s): {json.dumps(hysdsios)}")
    print("----------")
    return (hysdsios, users)


def get_rm_users():
    # User input
    user = ""
    while not USER_RE.match(user):
        if user != "":
            print(f"Invalid username(s): '{user}'")
        user = eval(input("Enter in space-separated user(s) to remove:"))
    user = user.split()
    return user


def rm_to_ios(users, hysdsio, existing):
    '''
    '''
    if set(existing[hysdsio]) - set(users) == set(existing[hysdsio]):
        print("Supplied users don't have permission to run {}, nothing to do, Skipping.".format(
            hysdsio))
        return
    existing[hysdsio] = list(set(existing[hysdsio]) - set(users))
    print()
    print()
    print("------------")
    sure = ""
    while sure == "":
        sure = eval(input("Are you sure you want to remove {} to allowed users for '{}' for a final user set of {}?" .format(
            users, hysdsio, json.dumps(existing[hysdsio]))))
        if not sure.startswith("y") and sure != "":
            print("User showed weakness, skipping")
            return
    doc = {
        "doc": {
            "allowed_accounts": existing[hysdsio]
        },
        "doc_as_upsert": True
    }
    try:
        r = requests.post('{}/{}/{}/{}/_update'.format(es_url,
                                                   "hysds_ios", "hysds_io", hysdsio), data=json.dumps(doc))
        r.raise_for_status()
    except Exception as e:
        print("[ERROR] Failed to update hysds-io. Resolve IMMEDIATELY. {}:{}\n{}".format(
            type(e), str(e), traceback.format_exc()), file=sys.stderr)
        sys.exit(-1)


if __name__ == "__main__":
    hysdsios, existing = get_matching_hysdsios()
    users = get_rm_users()
    for hysdsio in hysdsios:
        rm_to_ios(users, hysdsio, existing)
