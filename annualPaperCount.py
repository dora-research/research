
from time import sleep
import requests

from config import config

# create handle on main doc of AnnualPaperCount
db = config['conn']['dora']
annualPaperCountCollection = db['AnnualPaperCount']
doc = annualPaperCountCollection['main']


url = config['baseUrl'] + 'calchistogram'

years = range(1900, 2018)


def getAnnualPaperCount(year):
  params = {
    'expr': 'Y={}'.format(year),
    'timeout': 60000 # allow 1 min before timing out
  }

  # send GET request
  req = requests.get(url, params = params, headers = config['headers'])
  res = req

  # make sure response gave 200, otherwise, raise exception
  res.raise_for_status()

  obj = res.json()

  # try to access num_entities
  if not 'num_entities' in obj:
    raise ValueError('cannot find "num_entities" attribute in {}'.format(obj))

  print(obj)

  # put new value in doc
  doc[year] = obj['num_entities']

  


# save() whatever updates where found
try:
  for year in years:
    getAnnualPaperCount(year)
    # sleep for the api
    sleep(60)
finally:
  doc.save()
