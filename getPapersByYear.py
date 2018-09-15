
import time
import json
import datetime
import requests

from config import config

# create handle on main doc of Paper
db = config['conn']['dora']
paperCollection = db['Paper']


url = config['baseUrl'] + 'evaluate'


MAX_COUNT = 1000
PAUSE = 10 # in seconds
TIMEOUT = 60000 # in millis, 1 min

years = range(1926, 2018)


def getPapersByYear(year):

  offset = 0

  while True:

    params = {
      'expr': 'Y={}'.format(year),
      'count': MAX_COUNT, # max entities per page
      'offset': offset,
      'attributes': 'Id,Ti,L,Y,D,CC,ECC,AA.AuN,AA.AuId,AA.AfN,AA.AfId,AA.S,F.FN,F.FId,J.JN,J.JId,C.CN,C.CId,RId,W,E',
      'timeout': TIMEOUT
    }

    # send GET request
    req = requests.get(url, params = params, headers = config['headers'])
    res = req

    # make sure response gave 200, otherwise, raise exception
    res.raise_for_status()

    obj = res.json()

    for paper in obj['entities']:
      # if crappy paper that doesn't have Id, skip
      if not 'Id' in paper: continue

      key = str(paper['Id'])
      date = int(round(time.time() * 1000))

      # if doc already exists, update
      try:
        doc = paperCollection[key]
        doc['updateDate'] = date
      except:
        doc = paperCollection.createDocument()
        doc['_key'] = key
        doc['createDate'] = date


      for k, v in paper.items():
        if k == 'E':
          doc[k] = json.loads(v)
        else:
          doc[k] = v

      doc.save()

    print(obj['entities'][-1])
    print()

    
    # increase offset to next page
    offset += MAX_COUNT

    # sleep for the api
    time.sleep(PAUSE)

    # if we've reached end of paper list for the year
    if len(obj['entities']) < MAX_COUNT: break




# save() whatever updates where found
for year in years:
  getPapersByYear(year)
