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
rank_val_total=1000000
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

def get_rgn_rank_val(poilst,rank_val_total,posts_rk,logger):
    rank_type_list={}
    for poi in poilst:
        key=GetMD5(poi[0])
        rslt=posts_rk.find_one({'_id':key})
        if rslt==None:
            continue
        if rslt.get('rank_type_val','')=='':
            continue
        if rslt['rank_type_val'] in rank_type_list:
            rank_type_list[rslt['rank_type_val']]['cnt']+=1
            rank_type_list[rslt['rank_type_val']]['rank']+=(1-float(rslt['city_rank_type']['inx']-1)/rslt['city_rank_type']['total'])*100
        else:
            rank_type_list[rslt['rank_type_val']]={}
            rank_type_list[rslt['rank_type_val']]['cnt']=1
            rank_type_list[rslt['rank_type_val']]['city_rank_total']=float(rslt['city_rank_type']['total'])
            rank_type_list[rslt['rank_type_val']]['rank']=(1-float(rslt['city_rank_type']['inx']-1)/rslt['city_rank_type']['total'])*100
    Q=0.0
    rank=0.0
    rgn_total=0.0
    for item_rank in rank_type_list:
        rank_type_list[item_rank]['rank']/=rank_type_list[item_rank]['cnt']
        rgn_total+=rank_type_list[item_rank]['cnt']
    for item_rank in rank_type_list:
        Q+=rank_type_list[item_rank]['cnt']/rgn_total/(rank_type_list[item_rank]['city_rank_total']/rank_val_total)
    for item_rank in rank_type_list:
        rank+=rank_type_list[item_rank]['rank']*rank_type_list[item_rank]['cnt']/rgn_total/(rank_type_list[item_rank]['city_rank_total']/rank_val_total)/Q
    return rank
    
class CrowdsourcingWeb(object):
    def __init__(self,configFile):
        self.url_map = Map([
                Rule("/get_fresh_rank",endpoint="get_fresh_rank"),
                ])
        self.logger1 = InitLog('./log/clc_info_%s.log'%server_no)
        self.CODE_CHN_TABLE={'01':'汽车服务','02':'汽车销售','03':'汽车维修','04':'摩托车服务','05':'餐饮服务','06':'购物服务','07':'生活服务','08':'体育休闲服务','09':'医疗保健服务','10':'住宿服务','11':'风景名胜','12':'商务住宅','13':'政府机构及社会团体','14':'科教文化服务','15':'交通设施服务','16':'金融保险服务','17':'公司企业','18':'道路附属设施','19':'地名地址信息','20':'公共设施','22':'事件活动','97':'室内设施','99':'通行设施'}

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
            return self.render_content('{"adcode":"110000","pos":[116.407395,39.904211],"poi_num":{"val":1159048,"proportion":3.2,"rank":1},"mileage":{"val":1000000,"proportion":3.8,"rank":1},"freshness":85,"importance":{"val":91,"rank":1},"rank":100,"drive":{"val":512098,"proportion":28,"rank":1},"bus":{"val":14128740,"proportion":19,"rank":1},"poi_info":{"val":324490,"proportion":15,"rank":1},"search":{"val":12033761,"proportion":24,"rank":1},"type":{"01":{"val":26306,"proportion":2.27,"rank":1,"chn":"公共设施"},"02":{"val":3523,"proportion":0.3,"rank":1,"chn":"通行设施"},"03":{"val":509,"proportion":0.04,"rank":1,"chn":"道路附属设施"},"04":{"val":12612,"proportion":1.09,"rank":1,"chn":"汽车服务"},"05":{"val":24042,"proportion":2.07,"rank":1,"chn":"医疗保健服务"},"06":{"val":1960,"proportion":0.17,"rank":1,"chn":"汽车销售"},"07":{"val":6382,"proportion":0.55,"rank":1,"chn":"室内设施"},"08":{"val":27978,"proportion":2.41,"rank":1,"chn":"体育休闲服务"},"09":{"val":131620,"proportion":11.36,"rank":1,"chn":"公司企业"},"10":{"val":10132,"proportion":0.87,"rank":1,"chn":"名胜风景"},"11":{"val":128597,"proportion":11.10,"rank":1,"chn":"餐饮服务"},"12":{"val":22341,"proportion":1.93,"rank":1,"chn":"金融保险服务"},"13":{"val":51551,"proportion":4.45,"rank":1,"chn":"商务住宅"},"14":{"val":4,"proportion":0,"rank":1,"chn":"事件活动"},"15":{"val":50563,"proportion":4.36,"rank":1,"chn":"科教文化服务"},"16":{"val":118429,"proportion":10.22,"rank":1,"chn":"生活服务"},"17":{"val":18369,"proportion":1.58,"rank":1,"chn":"住宿服务"},"18":{"val":227037,"proportion":19.59,"rank":1,"chn":"购物服务"},"19":{"val":5585,"proportion":0.48,"rank":1,"chn":"汽车维修"},"20":{"val":33042,"proportion":2.85,"rank":1,"chn":"政府机构及社会团体"},"2":{"val":529,"proportion":0.05,"rank":1,"chn":"摩托车服务"},"22":{"val":83349,"proportion":7.19,"rank":1,"chn":"交通设施服务"},"23":{"val":174588,"proportion":15.06,"rank":1,"chn":"地名地址服务"}}}')
        if flag=='frame':
            rangell=request.values.get("range")
            if rangell=='':
                return self.render_content('{status:failed,err_code:201,reason:there is no range in the request}')
            rangelst=rangell.split(',')
            lon1=float(rangelst[0])
            lat1=float(rangelst[1])
            lon2=float(rangelst[2])
            lat2=float(rangelst[3])
            print lon1,lat1,lon2,lat2
            if lon1>lon2 or lat1>lat2:
                return self.render_content('{status:failed,err_code:202,reason:range format error}')
            try:
                client_rgn,db_rgn,posts_rgn = Connect2Mongo("localhost", 27017,"collect_frame","region_info")
            except:
                client_rgn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            frm_data=posts_rgn.find({"$and":[{"lon":{"$gte":lon1,"$lte":lon2}},{"lat":{"$gte":lat1,"$lte":lat2}}]})
            data_back=[]
            count=0
            for frm in frm_data:
                if count==0:
                    self.logger1.info(frm)
                count+=1
                data_rgn={}
                data_rgn['id']=frm['_id']
                frm_str=frm['frm']
                frm_lst=frm_str.split('|')
                dots=[]
                for item in frm_lst:
                    dot_spl=item.split(',')
                    dots.append([float(dot_spl[0]),float(dot_spl[1])])
                data_rgn['dots']=dots
                data_back.append(data_rgn)
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        if flag=='statistic':
            id_str=request.values.get("id")
            if id_str=='':
                return self.render_content('{"status":"failed","err_code":301,"reason":"no id found"}')
            
            try:
                client_rgn,db_rgn,posts_rgn = Connect2Mongo("localhost", 27017,"collect_frame","region_info")
            except:
                client_rgn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            try:
                client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,"collect_frame","city_statistic")
            except:
                client_city.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            
            id_lst=id_str.split(',')
            id_num=len(id_lst)
            multirgn={}
            multirgn['id']=id_lst
            multirgn['poi_num']=0
            multirgn['mileage']=0
            multirgn['typelst']={}
            poilst=[]
            Q=0.0
            print id_lst
            for id in id_lst:
                rgn_item=posts_rgn.find_one({'_id':id})
                rank_val_total=0
                if rgn_item==None:
                    continue
                multirgn['poi_num']+=rgn_item['poi_num']
                multirgn['mileage']+=rgn_item['mileage']
                if rgn_item['info']['city_rank_val_total']!=None:
                    rank_val_total=rgn_item['info']['city_rank_val_total']
                else:
                    pass
                for type in rgn_item['info']['classify']:
                    try: 
                        a=rgn_item['info']['classify'][type]['total_num']
                    except:
                        continue
                    for poi in rgn_item['info']['classify'][type]['list']:
                        poilst.append(poi)
                    if not type in multirgn['typelst']:
                        multirgn['typelst'][type]={}
                        multirgn['typelst'][type]['city_type_total']=rgn_item['info']['classify'][type]['total_num']
                        multirgn['typelst'][type]['city_total']=rgn_item['info']['classify'][type]['city_total']
                        '''
                        rgn_fresh=rgn_item['info']['classify'][type]['freshness']*len(rgn_item['info']['classify'][type]['list'])
                        rgn_prosp=rgn_item['info']['classify'][type]['prosp']*len(rgn_item['info']['classify'][type]['list'])
                        multirgn['typelst'][type]['freshness']=rgn_fresh
                        multirgn['typelst'][type]['prosp']=rgn_prosp
                        '''
                        multirgn['typelst'][type]['num']=len(rgn_item['info']['classify'][type]['list'])
                        multirgn['typelst'][type]['bus']=rgn_item['info']['classify'][type]['usr_bhr']['m']['bus'][1][0]
                        multirgn['typelst'][type]['search']=rgn_item['info']['classify'][type]['usr_bhr']['m']['search'][1][0]
                        multirgn['typelst'][type]['drive']=rgn_item['info']['classify'][type]['usr_bhr']['m']['drive'][1][0]
                        multirgn['typelst'][type]['poi_info']=rgn_item['info']['classify'][type]['usr_bhr']['m']['poi_info'][1][0]
                    else:
                        '''
                        rgn_fresh=rgn_item['info']['classify'][type]['freshness']*len(rgn_item['info']['classify'][type]['list'])
                        rgn_prosp=rgn_item['info']['classify'][type]['prosp']*len(rgn_item['info']['classify'][type]['list'])
                        multirgn['typelst'][type]['freshness']+=rgn_fresh
                        multirgn['typelst'][type]['prosp']+=rgn_prosp
                        '''
                        multirgn['typelst'][type]['num']+=len(rgn_item['info']['classify'][type]['list'])
                        multirgn['typelst'][type]['bus']+=rgn_item['info']['classify'][type]['usr_bhr']['m']['bus'][1][0]
                        multirgn['typelst'][type]['search']+=rgn_item['info']['classify'][type]['usr_bhr']['m']['search'][1][0]
                        multirgn['typelst'][type]['drive']+=rgn_item['info']['classify'][type]['usr_bhr']['m']['drive'][1][0]
                        multirgn['typelst'][type]['poi_info']+=rgn_item['info']['classify'][type]['usr_bhr']['m']['poi_info'][1][0]
                    try:
                        Q+=len(rgn_item['info']['classify'][type]['list'])/float(rgn_item['poi_num'])/(float(rgn_item['info']['classify'][type]['total_num'])/float(rgn_item['info']['classify'][type]['city_total']))
                    except:
                        Q+=0.0
            '''
            multirgn['freshness']=0.0
            multirgn['prosp']=0.0
            
            for type in multirgn['typelst']:
                try:
                    tmp=multirgn['typelst'][type]['num']/float(multirgn['poi_num'])/(float(multirgn['typelst'][type]['city_type_total'])/multirgn['typelst'][type]['city_total'])/Q
                except:
                    tmp=0
                multirgn['freshness']+=tmp*multirgn['typelst'][type]['freshness']/multirgn['typelst'][type]['num']
                multirgn['prosp']+=tmp*multirgn['typelst'][type]['prosp']/multirgn['typelst'][type]['num']
            if rank_val_total==0:
                multirgn['rank']=0
            else:  
                if len(id_lst)==1:
                    multirgn['rank']=rgn_item['info']['rank']
                else:
                    multirgn['rank']='null'
            '''
            multirgn['poi_info']=0.0
            multirgn['bus']=0.0
            multirgn['search']=0.0
            multirgn['drive']=0.0
            for type in multirgn['typelst']:
                multirgn['bus']+=multirgn['typelst'][type]['bus']
                multirgn['drive']+=multirgn['typelst'][type]['drive']
                multirgn['search']+=multirgn['typelst'][type]['search']
                multirgn['poi_info']+=multirgn['typelst'][type]['poi_info']
            
            city_data=posts_city.find_one({'_id':'beijing'})
            if city_data==None:
                self.render_content('{"status":"failed","err_code":303,"reason":"no city found"}')
            city_poi_num=city_data['total_poi']
            city_mile_num=city_data['total_mile']
            city_poiinfo_num=city_data['total_poiinfo']
            city_search_num=city_data['total_search']
            city_bus_num=city_data['total_bus']
            city_drive_num=city_data['total_drive']
            
            data_back={}
            data_back['id']=multirgn['id']
            data_back['poi_num']={}
            data_back['poi_num']['val']=multirgn['poi_num']
            data_back['poi_num']['proportion']=round(float(multirgn['poi_num'])/city_poi_num*100)
            data_back['mileage']={}
            data_back['mileage']['val']=float(multirgn['mileage'])/1000.0
            data_back['mileage']['proportion']=round(float(multirgn['mileage'])/1000.0/city_mile_num*100,2)
            data_back['intence']={}
            data_back['intence']['val']=1000*float(multirgn['poi_num'])/multirgn['mileage']
            data_back['intence']['proportion']='null'
            data_back['importance']={}
            if rgn_item==None:
                data_back['importance']['val']='null'
            else:
                data_back['importance']['val']=rgn_item['info']['importance']
            data_back['drive']={}
            data_back['drive']['val']=multirgn['drive']
            data_back['drive']['proportion']=round(float(multirgn['drive'])/city_drive_num*100,2)
            data_back['search']={}
            data_back['search']['val']=multirgn['search']
            data_back['search']['proportion']=round(float(multirgn['search'])/city_search_num*100,2)
            data_back['bus']={}
            data_back['bus']['val']=multirgn['bus']
            data_back['bus']['proportion']=round(float(multirgn['bus'])/city_bus_num*100,2)
            data_back['poi_info']={}
            data_back['poi_info']['val']=multirgn['poi_info']
            data_back['poi_info']['proportion']=round(float(multirgn['poi_info'])/city_poiinfo_num*100,2)
            if id_num==1 and rgn_item!=None:
                data_back['rank']=rgn_item['info']['rank']
                data_back['freshness']=rgn_item['info']['freshness']
                data_back['poi_num']['rank']=rgn_item['poi_rank']
                data_back['mileage']['rank']=rgn_item['mile_rank']
                data_back['importance']['rank']=rgn_item['importance_rank']
                data_back['intence']['rank']=rgn_item['cap_rank']
                data_back['bus']['rank']=rgn_item['bus_rank']
                data_back['drive']['rank']=rgn_item['drive_rank']
                data_back['search']['rank']=rgn_item['search_rank']
                data_back['poi_info']['rank']=rgn_item['poiinfo_rank']
            else:
                data_back['freshness']=None
                data_back['rank']=None
                data_back['poi_num']['rank']=None
                data_back['mileage']['rank']=None
                data_back['importance']['rank']=None
                data_back['intence']['rank']=None
                data_back['bus']['rank']=None
                data_back['drive']['rank']=None
                data_back['search']['rank']=None
                data_back['poi_info']['rank']=None
            data_back['type']={}
            for type in multirgn['typelst']:
                if not type[:2] in data_back['type']:
                    data_back['type'][type[:2]]={}
                    data_back['type'][type[:2]]['val']=multirgn['typelst'][type]['num']
                    data_back['type'][type[:2]]['proportion']=float(multirgn['typelst'][type]['num'])/multirgn['poi_num']*100
                else:
                    data_back['type'][type[:2]]['val']+=multirgn['typelst'][type]['num']
                    data_back['type'][type[:2]]['proportion']+=float(multirgn['typelst'][type]['num'])/multirgn['poi_num']*100
            for type in data_back['type']:
                data_back['type'][type]['chn']=self.CODE_CHN_TABLE[type]
                data_back['type'][type]['proportion']=round(data_back['type'][type]['proportion'],2)
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        if flag=='user':
            id_str=request.values.get("id")
            period=request.values.get("period")
            try:
                client_rgn,db_rgn,posts_rgn = Connect2Mongo("localhost", 27017,"collect_frame","region_info")
            except:
                client_rgn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            id_lst=id_str.split(',')
            id_num=len(id_lst)
            userbh={}
            userbh['id']=id_lst
            busw=[0 for i in range(5)]
            drivew=[0 for i in range(5)]
            searchw=[0 for i in range(5)]
            poi_infow=[0 for i in range(5)]
            busm=[0 for i in range(5)]
            drivem=[0 for i in range(5)]
            searchm=[0 for i in range(5)]
            poi_infom=[0 for i in range(5)]
            for id in id_lst:
                user_item=posts_rgn.find_one({'_id':id})
                if user_item==None:
                    return self.render_content('{"status":"failed","err_code":402,"reason":"there is no user behavior info"}')
                for i in range(5):
                    for type in user_item['info']['classify']:
                        busw[i]+=user_item['info']['classify'][type]['usr_bhr']['w']['bus'][i][0]
                        drivew[i]+=user_item['info']['classify'][type]['usr_bhr']['w']['drive'][i][0]
                        searchw[i]+=user_item['info']['classify'][type]['usr_bhr']['w']['search'][i][0]
                        poi_infow[i]+=user_item['info']['classify'][type]['usr_bhr']['w']['poi_info'][i][0]
                        busm[i]+=user_item['info']['classify'][type]['usr_bhr']['m']['bus'][i][0]
                        drivem[i]+=user_item['info']['classify'][type]['usr_bhr']['m']['drive'][i][0]
                        searchm[i]+=user_item['info']['classify'][type]['usr_bhr']['m']['search'][i][0]
                        poi_infom[i]+=user_item['info']['classify'][type]['usr_bhr']['m']['poi_info'][i][0]
            userbh['bus']={}
            userbh['bus']['chn']='公交导航'
            userbh['bus']['info']={}
            userbh['bus']['info']['m']=[]
            userbh['bus']['info']['w']=[]
            userbh['drive']={}
            userbh['drive']['chn']='汽车导航'
            userbh['drive']['info']={}
            userbh['drive']['info']['m']=[]
            userbh['drive']['info']['w']=[]
            userbh['search']={}
            userbh['search']['chn']='搜索量'
            userbh['search']['info']={}
            userbh['search']['info']['m']=[]
            userbh['search']['info']['w']=[]
            userbh['poi_info']={}
            userbh['poi_info']['chn']='点击量'
            userbh['poi_info']['info']={}
            userbh['poi_info']['info']['w']=[]
            userbh['poi_info']['info']['m']=[]
            userbh['bus']['info']['w'].append({'text':'前四周','rank':0,'val':busw[4]})
            userbh['bus']['info']['w'].append({'text':'前三周','rank':0,'val':busw[3]})
            userbh['bus']['info']['w'].append({'text':'前两周','rank':0,'val':busw[2]})
            userbh['bus']['info']['w'].append({'text':'上周','rank':0,'val':busw[1]})
            userbh['bus']['info']['w'].append({'text':'本周','rank':0,'val':busw[0]})
            userbh['drive']['info']['w'].append({'text':'前四周','rank':0,'val':drivew[4]})
            userbh['drive']['info']['w'].append({'text':'前三周','rank':0,'val':drivew[3]})
            userbh['drive']['info']['w'].append({'text':'前两周','rank':0,'val':drivew[2]})
            userbh['drive']['info']['w'].append({'text':'上周','rank':0,'val':drivew[1]})
            userbh['drive']['info']['w'].append({'text':'本周','rank':0,'val':drivew[0]})
            userbh['search']['info']['w'].append({'text':'前四周','rank':0,'val':searchw[4]})
            userbh['search']['info']['w'].append({'text':'前三周','rank':0,'val':searchw[3]})
            userbh['search']['info']['w'].append({'text':'前两周','rank':0,'val':searchw[2]})
            userbh['search']['info']['w'].append({'text':'上周','rank':0,'val':searchw[1]})
            userbh['search']['info']['w'].append({'text':'本周','rank':0,'val':searchw[0]})
            userbh['poi_info']['info']['w'].append({'text':'前四周','rank':0,'val':poi_infow[4]})
            userbh['poi_info']['info']['w'].append({'text':'前三周','rank':0,'val':poi_infow[3]})
            userbh['poi_info']['info']['w'].append({'text':'前两周','rank':0,'val':poi_infow[2]})
            userbh['poi_info']['info']['w'].append({'text':'上周','rank':0,'val':poi_infow[1]})
            userbh['poi_info']['info']['w'].append({'text':'本周','rank':0,'val':poi_infow[0]})                
            userbh['bus']['info']['m'].append({'text':'前四月','rank':0,'val':busm[4]})
            userbh['bus']['info']['m'].append({'text':'前三月','rank':0,'val':busm[3]})
            userbh['bus']['info']['m'].append({'text':'前两月','rank':0,'val':busm[2]})
            userbh['bus']['info']['m'].append({'text':'上月','rank':0,'val':busm[1]})
            userbh['bus']['info']['m'].append({'text':'本月','rank':0,'val':busm[0]})
            userbh['drive']['info']['m'].append({'text':'前四月','rank':0,'val':drivem[4]})
            userbh['drive']['info']['m'].append({'text':'前三月','rank':0,'val':drivem[3]})
            userbh['drive']['info']['m'].append({'text':'前两月','rank':0,'val':drivem[2]})
            userbh['drive']['info']['m'].append({'text':'上月','rank':0,'val':drivem[1]})
            userbh['drive']['info']['m'].append({'text':'本月','rank':0,'val':drivem[0]})
            userbh['search']['info']['m'].append({'text':'前四月','rank':0,'val':searchm[4]})
            userbh['search']['info']['m'].append({'text':'前三月','rank':0,'val':searchm[3]})
            userbh['search']['info']['m'].append({'text':'前两月','rank':0,'val':searchm[2]})
            userbh['search']['info']['m'].append({'text':'上月','rank':0,'val':searchm[1]})
            userbh['search']['info']['m'].append({'text':'本月','rank':0,'val':searchm[0]})
            userbh['poi_info']['info']['m'].append({'text':'前四月','rank':0,'val':poi_infom[4]})
            userbh['poi_info']['info']['m'].append({'text':'前三月','rank':0,'val':poi_infom[3]})
            userbh['poi_info']['info']['m'].append({'text':'前两月','rank':0,'val':poi_infom[2]})
            userbh['poi_info']['info']['m'].append({'text':'上月','rank':0,'val':poi_infom[1]})
            userbh['poi_info']['info']['m'].append({'text':'本月','rank':0,'val':poi_infom[0]})                     
                
                
            #data_back={'id':[123],'click':{'chn':'点击量','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]},'bus':{'chn':'公交导航','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]},'drive':{'chn':'汽车导航','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]},'search':{'chn':'搜索量','info':[{'text':'3周','val':24,'rank':2},{'text':'4周','val':24,'rank':2},{'text':'5周','val':24,'rank':2},{'text':'6周','val':24,'rank':2},{'text':'本周','val':24,'rank':2}]}}
            data_back_json=json.dumps(userbh,ensure_ascii=False).encode('utf-8','ignore')
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

