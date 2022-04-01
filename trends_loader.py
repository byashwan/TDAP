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
        self.pytrends.build_payload(kw_list=[term], timeframe=self.time_function())
    
    def time_function(self):
        to_date = date.today()-timedelta(days=1)
        from_date = date.today()-timedelta(days=7)
        time_frame = str(from_date)+'T0'+' '+str(to_date)+'T0'
        return time_frame

    def generator(self,df2):
        for c,line in enumerate(df2):
            yield{
                '_index': 'trends_loader',
                'load_date':line.get("date",""),
                'term': line.get('term',''),
                'value': line.get('value',''),
                'related_topics': line.get('related_topics',''),
                'related_queries': line.get('related_queries','')
            }
        return

    def trend_count(self):
        df = self.pytrends.interest_over_time()
        di=df.to_dict('index')
        fil=[]
        for i in di.keys():
            res = str(i)
            a,b=res.split(' ')
            if b=='00:00:00':
                fid={}
                fid['date']=str(i)
                fid['term']=self.term
                fid['value']=di[i][self.term]
                fid['related_topics']=self.related_topics()
                fid['related_queries']= self.related_queries()
                fil.append(fid)
        return fil



    def related_topics(self): 
        df_rt = self.pytrends.related_topics()
        related_topics_csv = list(df_rt[self.term]['top']['topic_title'])
        return related_topics_csv

    def related_queries(self):
        df_rt = self.pytrends.related_queries()
        related_queries_csv = list(df_rt[self.term]['top']['query'])
        return related_queries_csv
    
    def elastic_insert(self,data):
        client = Elasticsearch(
            "https://localhost:9200",
            ca_certs="http_ca.crt",
            basic_auth=("elastic", "MUca56YstFyUy673Tn8M")
        )
        try:
            res=helpers.bulk(client,self.generator(data))
            print("working")
        except Exception as e:
            print(e)



a = Loader('Spark')
result=a.trend_count()
a.elastic_insert(result)

        


