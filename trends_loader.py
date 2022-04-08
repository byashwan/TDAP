try:
    from pytrends.request import TrendReq
    from datetime import date, timedelta
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
except Exception as e:
    print("Modules Missing {}".format(e))


class Loader:
    def __init__(self,term):
        self.term=term
        self.pytrends = TrendReq()
        self.client = Elasticsearch(
            "https://localhost:9200",
            ca_certs="http_ca.crt",
            basic_auth=("elastic", "MUca56YstFyUy673Tn8M")
        )
    
    def time_function(self):
        to_date = date.today()-timedelta(days=1)
        from_date = date.today()-timedelta(days=14)
        time_frame = str(from_date)+'T0'+' '+str(to_date)+'T0'
        return time_frame

    def rlast_date(self):
        li=[]
        for i in range(1,14):
            k=date.today()-timedelta(days=i)
            li.append(str(k))
        return li
        

    def generator(self,df2):
        for c,line in enumerate(df2):
            yield{
                '_index': 'trends_loader',
                '_id': line.get('id',''),
                'load_date':line.get("date",""),
                'term': line.get('term',''),
                'value': line.get('value',''),
                'related_topics': line.get('related_topics',''),
                'related_queries': line.get('related_queries','')
            }
        return

    def trend_count(self):
        q={
            "size": 14, 
            "_source": ["term","value","load_date"], 
            "query": {
                "match": {"term": self.term}
            },
            "sort": [
                {
                "load_date": {
                    "order": "desc"
                }
                }
            ]
        }
        
        res = self.client.search(index='trends_loader', body=q)
        res2=res['hits']['hits']
        li=[]
        for i in res2:
            li.append(i['_source']['load_date'])

        last_dates=self.rlast_date()
        
        not_present=[]
        for i in last_dates:
            if i not in li:
                present=1
                not_present.append(i)
        #print(not_present)
        if len(not_present)==0:
            return 'The data is present'
        
        self.pytrends.build_payload(kw_list=[self.term], timeframe=self.time_function())
        df = self.pytrends.interest_over_time()
        di=df.to_dict('index')
        fil=[]
        temp_dict={}
        for i in di.keys():
            res = str(i)
            a,b=res.split(' ')
            if a in not_present:
                if a in temp_dict:
                    temp_dict[a].append(di[i][self.term])
                else:
                    temp_dict[a]=[di[i][self.term]]
        for i in temp_dict:
            fid={}
            fid['date']=str(i)
            fid['id']=str(i)+self.term
            fid['term']=self.term
            fid['value']=max(temp_dict[i])
            #fid['related_topics']=self.related_topics()
            #fid['related_queries']= self.related_queries()
            fil.append(fid)
        return fil

    def check_word(self):
        if self.client.indices.exists(index='trends_loader'):
            res = self.client.search(index='trends_loader',query={"match": {"term": self.term}})
            #print(res['hits']['total']['value'])
            if res['hits']['total']['value']==0:
                return 0
            else:
                return 1
        else:
            return 0
        
        
    def related_topics(self): 
        df_rt = self.pytrends.related_topics()
        related_topics_csv = list(df_rt[self.term]['top']['topic_title'])
        return related_topics_csv

    def related_queries(self):
        df_rt = self.pytrends.related_queries()
        related_queries_csv = list(df_rt[self.term]['top']['query'])
        return related_queries_csv
    
    def elastic_insert(self,data):
        try:
            res=helpers.bulk(self.client,self.generator(data))
            print("working")
        except Exception as e:
            print(e)

    def insert_ten(self):
        self.pytrends.build_payload(kw_list=[self.term], timeframe='2011-01-01 2022-03-01')
        df = self.pytrends.interest_over_time()
        temp_dict=df.to_dict('index')
        fil=[]
        for i in temp_dict.keys():
            fid={}
            a,b=str(i).split(' ')
            fid['date']=a
            fid['id']=str(a)+self.term
            fid['term']=self.term
            fid['value']=temp_dict[i][self.term]
            fid['related_topics']=self.related_topics()
            fid['related_queries']= self.related_queries()
            fil.append(fid)
        return fil


with open('a.txt') as f:
    words = [line.rstrip('\n') for line in f]
    for i in words:
        a = Loader(i)
        print(i)
        if a.check_word()==0:
            print('no')
            result1=a.insert_ten()
            a.elastic_insert(result1)
            result2=a.trend_count()
            a.elastic_insert(result2)
            
        else:
            print('yes')
            result=a.trend_count()
            #print(result)
            if result == 'The data is present':
                print(result)
                pass
            else:
                a.elastic_insert(result)





            


