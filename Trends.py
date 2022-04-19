try:
    from pytrends.request import TrendReq
    from datetime import date, timedelta
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
    import configparser
except Exception as e:
    print("Modules Missing {}".format(e))


class Loader:
    def __init__(self,term,country,index,days):
        self.term=term
        self.pytrends = TrendReq()
        self.country=country
        self.index=index
        self.days=days
        self.client = Elasticsearch(
            "https://localhost:9200",
            ca_certs="http_ca.crt",
            basic_auth=("elastic", "MUca56YstFyUy673Tn8M")
        )
       

    def generator(self,df2):
        for c,line in enumerate(df2):
            yield{
                '_index': 'tdap_google_trends_US_90days',
                '_id': line.get('id',''),
                'load_date':line.get("date",""),
                'term': line.get('term',''),
                'value': line.get('value',''),
                'related_topics': line.get('related_topics',''),
                'related_queries': line.get('related_queries','')
            }
        return

    
    def elastic_insert(self,data):
        if len(data)==0:
            print('nothing to insert')
            return
        try:
            res=helpers.bulk(self.client,self.generator(data))
            print("working")
        except Exception as e:
            print(e)

    def insert_data(self):
        #print(self.days)
        if self.days=='30':
            tf='today 1-m'
        elif self.days=='90':
            tf='today 3-m'
        elif self.days=='All':
            tf='all'
        #print(tf)
        
        self.pytrends.build_payload(kw_list=[self.term], timeframe=tf,geo=self.country)
        df = self.pytrends.interest_over_time()
        df_rt = self.pytrends.related_topics()
        df_rq = self.pytrends.related_queries()
       
        obt_val=list(df_rt.keys())[0]
        if len(df_rt[obt_val]['top'])==0:
            related_topics_csv = []
        else:
            related_topics_csv = list(df_rt[obt_val]['top']['topic_title'])
        
        if df_rq[obt_val]['top'] is None:
            related_queries_csv=[]
        else:
            related_queries_csv = list(df_rq[obt_val]['top']['query'])

        temp_dict=df.to_dict('index')
        fil=[]
        if len(temp_dict)==0:
            print('No results found')
            return 0
        else:
            for i in temp_dict.keys():
                fid={}
                a,b=str(i).split(' ')
                fid['date']=a
                fid['id']=str(a)+self.term
                fid['term']=self.term
                fid['value']=temp_dict[i][self.term]
                fid['related_topics']=related_topics_csv
                fid['related_queries']= related_queries_csv
                fil.append(fid)
        return fil

cfg = configparser.ConfigParser()
cfg.read('db.cfg')
for section in cfg.sections():
    country = cfg.get(section,'country').strip('"')
    index = cfg.get(section,'index').strip()
    days = cfg.get(section,'days').strip('"')
    with open('b.txt') as f:
        words = [line.rstrip('\n') for line in f]
        for i in words:
            a = Loader(i,country,index,days)
            result1=a.insert_data()
            if result1==0:
                pass
            else:
                a.elastic_insert(result1)





            


