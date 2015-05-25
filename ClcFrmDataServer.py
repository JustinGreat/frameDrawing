#encoding=utf-8
from meinheld import server
from apscheduler.scheduler import Scheduler
import random
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import urlparse
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.utils import redirect
from werkzeug.contrib.sessions import SessionMiddleware
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.contrib.cache import SimpleCache
from jinja2 import Environment, FileSystemLoader
from pymongo import MongoClient
import cjson
import xmltodict
import copy
import hashlib
import re
import urllib
import urllib2
import logging
import struct;
import datetime
import json

def Connect2Mongo(ip,port,db,table,user="",pwd=""):
    client = MongoClient(ip,port)
    db = client[db]
    if user and pwd:
        db.authenticate(user,pwd)
    posts = db[table]
    return client,db,posts 
server_no = 0 
def InitLog(log_file):
    logger = logging.getLogger(log_file)  
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)  
    file_handler = logging.FileHandler(log_file)  
    file_handler.setFormatter(formatter)  
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    logger.info("Start process data ok!")
    return logger
class CrowdsourcingWeb(object):
    def __init__(self,configFile):
        self.url_map = Map([
                Rule("/get_fresh_rank",endpoint="get_fresh_rank"),
                ])
        self.logger1 = InitLog('./log/clc_info_%s.log'%server_no)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)

    def wsgi_app(self, environ, start_response):
        #self.m_sessionDict = environ['werkzeug.session']
        request =  Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def dispatch_request(self, request):
        adapter=self.url_map.bind_to_environ(request.environ);
        try:
            endpoint,values= adapter.match();
            return getattr(self, "on_" + endpoint)(request,**values);
        except HTTPException, e:
            return e

    def render_template(self,template_name,**context):
        retContent="Hello Word!"
        return Response(retContent, mimetype="text/html");

    def render_content(self,retContent,contentType="html"):
        if contentType=="html":
            return Response(retContent,mimetype="text/html");
        elif contentType=="xml":
            return Response(retContent,mimetype="text/xml");
        else:
            return Response(retContent,mimetype="application/json");

    def render_pic(self,retContent):
        return Response(retContent,mimetype="image/jpeg");

    
    def on_get_fresh_rank(self,request):
        flag=request.values.get("flag")
        print flag
        if flag=='city':
            return self.render_content('{"adcode":"010000","pos":[116.407395,39.904211],"poi_num":12794,"mileage":100000,"freshness":85,"rank":2,"prosp":79,"type":{"type1":146,"type2":246}}')
        if flag=='frame':
            client3,db3,posts3 = Connect2Mongo("localhost",27017,"drawFrame","raw_frame_test")
            data_back=[]
            for item in posts3.find():
                data_db={}
                data_db['id']=item['_id']
                data_db['dots']=[]
                for i in item['data'][2][0]:
                    data_db['dots'].append([i[0],i[1]])
                data_back.append(data_db)
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        if flag=='statistic':
            id_str=request.values.get("id")
            id_lst=id_str.split(',')
            num=len(id_lst)
            data_back={}
            data_back['id']=id_lst
            data_back['poi_num']=100*num
            data_back['mileage']=98*num
            data_back['freshness']=100-num
            data_back['importance']=40+num
            data_back['rank']=20+num*2+1
            data_back['prosp']=79
            data_back['type']={'type1':35*num,'type2':46*num,'type3':23*num}
            data_back['click']=78*num
            data_back['bus']=69*num
            data_back['search']=13*num
            data_back['drive']=64*num
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        if flag=='user':
            data_back={'id':[123],'click':{'chn':'点击量','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]},'bus':{'chn':'公交导航','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]},'drive':{'chn':'汽车导航','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]},'search':{'chn':'搜索量','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]}}
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        '''
        cls_num=[]
        total_num=[]
        fresh_level=[]
        city_cls_num=[]
        city_total_num=
        
        Q=0.0
        for i in range(type_num):
            Q+=cls_num[i]/total_num[i]/(city_cls_num[i]/city_total_num)
        zone_fresh=0.0
        for i in range(type_num):
            zone_fresh+=fresh_level[i]*cls_num[i]/total_num[i]/(city_cls_num[i]/city_total_num)/Q
            
        if nation_poi_num/rnk_num[i]>=10:
            rnk[i]=(1-(rnk_raw-1)/float(nation_poi_num))*100
        else:
            rnk[i]=(1-(rnk_raw-1)/float(nation_poi_num-rnk_num[i]))*100
        
        zone_rank=0.0
        for i in range(type_num):
            zone_rank+=rnk[i]*cls_num[i]/total_num[i]/(city_cls_num[i]/city_total_num)/Q
        '''
        return self.render_content("Hello Work!")
        '''
        f_in=open("region_level_1",'r')
        
        data_back='['
        lon=request.values.get("lon")
        cb=request.values.get("cb")
        lat=request.values.get("lat")
        dbflag=request.values.get("dbflag")
        self.logger5.info("get Suc")
        self.logger5.info("lon:%s   lat:%s    "%(lon,lat))
        #client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","raw_frame")
        client3,db3,posts3 = Connect2Mongo("localhost",27017,"drawFrame","raw_frame_test")
        client2,db2,posts2=Connect2Mongo('localhost',27017,'drawFrame','bj_frame_test')
        #frm_data=posts.find({"$and":[{"lon":{"$gte":float(lon)-0.025,"$lte":float(lon)+0.025}},{"lat":{"$gte":float(lat)-0.025,"$lte":float(lat)+0.025}}]})
        #for item in frm_data:
        if dbflag=='raw':
            #frm_data=posts3.find({"$and":[{"lon":{"$gte":float(lon)-0.025,"$lte":float(lon)+0.025}},{"lat":{"$gte":float(lat)-0.025,"$lte":float(lat)+0.025}}]})
            for item in posts3.find():
            #for item in frm_data:
                data_back+=json.dumps(item['data'],ensure_ascii=False).encode('utf-8','ignore')
                data_back+=','
        if dbflag=='bj':
            #frm_data=posts2.find({"$and":[{"lon":{"$gte":float(lon)-0.025,"$lte":float(lon)+0.025}},{"lat":{"$gte":float(lat)-0.025,"$lte":float(lat)+0.025}}]})
            for item in posts2.find():
            #for item in frm_data:
                data_back+=json.dumps(item['data'],ensure_ascii=False).encode('utf-8','ignore')
                data_back+=','
        data_back=data_back[:-1]+']'
        for line in f_in.readlines():
            item=json.loads(line)
            if item[1][0]>float(lon2):
                break
            #if item[1][0]>116.400285 and item[1][1] >39.903458 and item[1][0]<116.407796 and item[1][1] <39.907228:
            if item[1][0]>float(lon) and item[1][1] >float(lat) and item[1][0]<float(lon2) and item[1][1] <float(lat2):
                #data_back+=json.dumps(item,ensure_ascii=False).encode('utf-8','ignore')  
                data_back+=line                
        return self.render_content(cb +"&&"+cb+"("+ data_back+")")
        '''
                

    
def create_app(configFile):
    crowdsourcingWebApp=CrowdsourcingWeb(configFile);

    apschedulerObj = Scheduler()
    apschedulerObj.start()
    #apschedulerObj.add_cron_job(clean_expire_function,year="*", month="*", day="*", hour="*",minute="*",second="*",args=[crowdsourcingWebApp]);

    return crowdsourcingWebApp;

def application(environ, start_response):
    session = environ['werkzeug.session']
    session['visit_count'] = session.get('visit_count', 0) + 1

    start_response('200 OK', [('Content-Type', 'text/html')])
    return ['''
        <!doctype html>
        <title>Session Example</title>
        <h1>Session Example</h1>
        <p>You visited this page %d times.</p>
    ''' % session['visit_count']]


def make_app():
    return SessionMiddleware(application)

if __name__ == '__main__':
    if len(sys.argv)!= 4:
        print "python %s ip port server_no"%sys.argv[0];
        sys.exit(1);

    sIp=sys.argv[1];
    lPort=int(sys.argv[2]);
    #global server_no
    server_no = int(sys.argv[3]);

    server.listen((sIp,lPort));
    app=create_app("svr_conf.cnf");
    server.run(app);

