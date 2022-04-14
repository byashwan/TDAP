try:
    from bs4 import BeautifulSoup
    import requests, lxml
    import json
    from datetime import date
    import re
    from elasticsearch import Elasticsearch
except Exception as e:
    print("Modules Missing {}".format(e))

proxies = {
   'http': 'http://proxy-dmz.intel.com:911/',
   'https': 'http://proxy-dmz.intel.com:912/',
}
headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.3"
}

client = Elasticsearch(
            "https://localhost:9200",
            ca_certs="http_ca.crt",
            basic_auth=("elastic", "MUca56YstFyUy673Tn8M")
        )

def Googleloader(word):
    elk_res={}
    today_date = date.today()
    html = requests.request('GET','https://www.google.com/search?q='+word, headers=headers)
    soup = BeautifulSoup(html.text, 'lxml')
    try:
        number_of_results = soup.select_one('#result-stats nobr').previous_sibling
        result = re.sub(r'[a-zA-Z ,]', '',number_of_results)
        elk_res['load_date']=today_date
        elk_res['dount']=int(result)
        elk_res['term']=word
    except Exception as e:
        print(e)
    try:
        if len(elk_res)==0:
            elk_res['load_date']=today_date
            elk_res['term']=word
            elk_res['dcount']='NA'
        #print(elk_res)
        response = client.index(index="google-trend",document=elk_res, id=word+str(today_date))
        print('working')
    except Exception as e:
        print(e)

with open('b.txt') as f:
    words = [line.rstrip('\n') for line in f]
    for i in words:
        print(i)
        a = Googleloader(i)



