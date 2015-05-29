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
import struct
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
                client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,"collect_frame","city_statistic")
            except:
                client_city.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            try:
                client_rgn,db_rgn,posts_rgn = Connect2Mongo("localhost", 27017,"collect_frame","region_info")
            except:
                client_rgn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            try:
                client_newrgn,db_newrgn,posts_newrgn = Connect2Mongo("localhost", 27017,"collect_frame","newregion_info")
            except:
                client_newrgn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            try:
                client_pdargn,db_pdargn,posts_pdargn = Connect2Mongo("localhost", 27017,"collect_frame","pdaregion_info")
            except:
                client_pdargn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            #frm_data=posts_pdargn.find({"$and":[{"lon":{"$gte":lon1,"$lte":lon2}},{"lat":{"$gte":lat1,"$lte":lat2}}]})
            frm_data=posts_newrgn.find({"$and":[{"lon":{"$gte":lon1,"$lte":lon2}},{"lat":{"$gte":lat1,"$lte":lat2}}]})
            data_back=[]
            count=0
            frm_num=posts_newrgn.count()
            city_data=posts_city.find_one({'_id':'beijing'})
            for frm in frm_data:
                data_rgn={}
                data_rgn['id']=frm['_id']
                frm_str=frm['frm']
                frm_lst=frm_str.split('|')
                dots=[]
                for item in frm_lst:
                    dot_spl=item.split(',')
                    dots.append([float(dot_spl[0]),float(dot_spl[1])])
                data_rgn['dots']=dots
                data_rgn['freshness']={}
                try:
                    data_rgn['freshness']['val']=frm['freshness']
                    data_rgn['freshness']['rank']=frm['fresh_rank']
                except:
                    data_rgn['freshness']['val']=0
                    data_rgn['freshness']['rank']=2188
                data_rgn['freshness']['hot']=(1-float(frm['fresh_rank']-1)/frm_num)*100
                data_rgn['lon']=frm['lon']
                data_rgn['lat']=frm['lat']
                data_rgn['poi_num']={}
                data_rgn['poi_num']['val']=frm['poi_num']
                data_rgn['poi_num']['proportion']=round(float(frm['poi_num'])/city_data['total_poi'],2)
                data_rgn['poi_num']['rank']=frm['poi_rank']
                data_rgn['poi_num']['hot']=(1-float(frm['poi_rank']-1)/frm_num)*100
                data_rgn['poi_cap']={}
                data_rgn['poi_cap']['val']=frm['poi_cap']
                data_rgn['poi_cap']['rank']=frm['cap_rank']
                data_rgn['poi_cap']['hot']=(1-float(frm['cap_rank']-1)/frm_num)*100
                data_rgn['mileage']={}
                data_rgn['mileage']['val']=frm['mileage']
                data_rgn['mileage']['proportion']=round(float(frm['mileage'])/city_data['total_mile'])
                data_rgn['mileage']['rank']=frm['mile_rank']
                data_rgn['mileage']['hot']=(1-float(frm['mile_rank']-1)/frm_num)*100
                data_rgn['prosp']={}
                try:
                    data_rgn['prosp']['val']=frm['prosp']
                    data_rgn['prosp']['rank']=frm['prosp_rank']
                except:
                    data_rgn['prosp']['val']=0
                    data_rgn['prosp']['rank']=2188
                data_rgn['prosp']['hot']=(1-float(frm['prosp_rank']-1)/frm_num)*100
                data_rgn['importance']={}
                data_rgn['importance']['val']=frm['importance']
                data_rgn['importance']['rank']=frm['importance_rank']
                data_rgn['importance']['hot']=(1-float(frm['importance_rank']-1)/frm_num)*100
                data_rgn['pos_hot']={}
                data_rgn['pos_hot']['val']=0
                data_rgn['pos_hot']['rank']=0
                data_rgn['pos_hot']['proportion']=0
                data_rgn['rgn_chg']={}
                data_rgn['rgn_chg']['val']=0
                data_rgn['rgn_chg']['rank']=0
                cont=['bus','drive','search','poi_info','share','error','collection','group']
                for c in cont:
                    data_rgn[c]={}
                    try:
                        data_rgn[c]['rank']=frm[c+'m1_rank']
                    except:
                        data_rgn[c]['rank']=0
                    data_rgn[c]['val']=int(frm[c+'_num']['m'][1])
                    data_rgn[c]['hot']=(1-float(frm[c+'m1_rank']-1)/frm_num)*100
                    try:
                        data_rgn[c]['proportion']=float(frm[c+'_num']['m'][1])/city_data['total_'+c]
                    except:
                        data_rgn[c]['proportion']=0
                data_back.append(data_rgn)
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        if flag=='statistic':
            id_str=request.values.get("id")
            if id_str=='':
                return self.render_content('{"status":"failed","err_code":301,"reason":"no id found"}')
            try:
                client_pdargn,db_pdargn,posts_pdargn = Connect2Mongo("localhost", 27017,"collect_frame","pdaregion_info")
            except:
                client_pdargn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            try:
                client_rgn,db_rgn,posts_rgn = Connect2Mongo("localhost", 27017,"collect_frame","region_info")
            except:
                client_rgn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            try:
                client_newrgn,db_newrgn,posts_newrgn = Connect2Mongo("localhost", 27017,"collect_frame","newregion_info")
            except:
                client_newrgn.disconnect()
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
                rgn_item=posts_newrgn.find_one({'_id':id})
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
                        multirgn['typelst'][type]['bus']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['bus'][1][0])
                        multirgn['typelst'][type]['search']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['search'][1][0])
                        multirgn['typelst'][type]['drive']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['drive'][1][0])
                        multirgn['typelst'][type]['poi_info']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['poi_info'][1][0])
                        multirgn['typelst'][type]['share']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['share'][1][0])
                        multirgn['typelst'][type]['collection']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['collection'][1][0])
                        multirgn['typelst'][type]['error']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['error'][1][0])
                        multirgn['typelst'][type]['group']=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['group'][1][0])
                    else:
                        '''
                        rgn_fresh=rgn_item['info']['classify'][type]['freshness']*len(rgn_item['info']['classify'][type]['list'])
                        rgn_prosp=rgn_item['info']['classify'][type]['prosp']*len(rgn_item['info']['classify'][type]['list'])
                        multirgn['typelst'][type]['freshness']+=rgn_fresh
                        multirgn['typelst'][type]['prosp']+=rgn_prosp
                        '''
                        multirgn['typelst'][type]['num']+=len(rgn_item['info']['classify'][type]['list'])
                        multirgn['typelst'][type]['bus']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['bus'][1][0])
                        multirgn['typelst'][type]['search']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['search'][1][0])
                        multirgn['typelst'][type]['drive']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['drive'][1][0])
                        multirgn['typelst'][type]['poi_info']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['poi_info'][1][0])
                        multirgn['typelst'][type]['share']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['share'][1][0])
                        multirgn['typelst'][type]['collection']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['collection'][1][0])
                        multirgn['typelst'][type]['error']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['error'][1][0])
                        multirgn['typelst'][type]['group']+=int(rgn_item['info']['classify'][type]['usr_bhr']['m']['group'][1][0])
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
            multirgn['share']=0.0
            multirgn['error']=0.0
            multirgn['collection']=0.0
            multirgn['group']=0.0
            for type in multirgn['typelst']:
                multirgn['bus']+=multirgn['typelst'][type]['bus']
                multirgn['drive']+=multirgn['typelst'][type]['drive']
                multirgn['search']+=multirgn['typelst'][type]['search']
                multirgn['poi_info']+=multirgn['typelst'][type]['poi_info']
                multirgn['error']+=multirgn['typelst'][type]['error']
                multirgn['share']+=multirgn['typelst'][type]['share']
                multirgn['collection']+=multirgn['typelst'][type]['collection']
                multirgn['group']+=multirgn['typelst'][type]['group']
            
            city_data=posts_city.find_one({'_id':'beijing'})
            if city_data==None:
                self.render_content('{"status":"failed","err_code":303,"reason":"no city found"}')
            city_poi_num=city_data['total_poi']
            city_mile_num=city_data['total_mile']
            city_poiinfo_num=city_data['total_poiinfo']
            city_search_num=city_data['total_search']
            city_bus_num=city_data['total_bus']
            city_drive_num=city_data['total_drive']
            city_group_num=city_data['total_group']
            city_error_num=city_data['total_error']
            city_collection_num=city_data['total_collection']
            city_share_num=city_data['total_share']
            
            data_back={}
            data_back['id']=multirgn['id']
            data_back['poi_num']={}
            data_back['poi_num']['val']=multirgn['poi_num']
            data_back['poi_num']['proportion']=round(float(multirgn['poi_num'])/city_poi_num*100)
            data_back['mileage']={}
            data_back['mileage']['val']=round(float(multirgn['mileage'])/1000.0,2)
            data_back['mileage']['proportion']=round(float(multirgn['mileage'])/1000.0/city_mile_num*100,2)
            data_back['intence']={}
            data_back['intence']['val']=round(1000*float(multirgn['poi_num'])/multirgn['mileage'],2)
            data_back['intence']['proportion']=None
            data_back['importance']={}
            if rgn_item==None:
                data_back['importance']['val']=None
            else:
                data_back['importance']['val']=rgn_item['info']['importance']
            data_back['drive']={}
            data_back['drive']['val']=int(multirgn['drive'])
            try:
                data_back['drive']['proportion']=round(float(multirgn['drive'])/city_drive_num*100,2)
            except:
                data_back['drive']['proportion']=0
            data_back['search']={}
            data_back['search']['val']=int(multirgn['search'])
            data_back['search']['proportion']=round(float(multirgn['search'])/city_search_num*100,2)
            data_back['bus']={}
            data_back['bus']['val']=int(multirgn['bus'])
            data_back['bus']['proportion']=round(float(multirgn['bus'])/city_bus_num*100,2)
            data_back['poi_info']={}
            data_back['poi_info']['val']=int(multirgn['poi_info'])
            data_back['poi_info']['proportion']=round(float(multirgn['poi_info'])/city_poiinfo_num*100,2)
            data_back['collection']={}
            data_back['collection']['val']=int(multirgn['collection'])
            try:
                data_back['collection']['proportion']=round(float(multirgn['collection'])/city_collection_num*100,2)
            except:
                data_back['collection']['proportion']=0
            data_back['share']={}
            data_back['share']['val']=multirgn['share']
            try:
                data_back['share']['proportion']=round(float(multirgn['share'])/city_share_num*100,2)
            except:
                data_back['share']['proportion']=0
            data_back['group']={}
            data_back['group']['val']=int(multirgn['group'])
            data_back['group']['proportion']=round(float(multirgn['group'])/city_group_num*100,2)
            data_back['error']={}
            data_back['error']['val']=int(multirgn['error'])
            try:
                data_back['error']['proportion']=round(float(multirgn['error'])/city_error_num*100,2)    
            except:
                data_back['error']['proportion']=0
            if id_num==1 and rgn_item!=None:
                data_back['rank']=rgn_item['info']['rank']
                data_back['freshness']=rgn_item['info']['freshness']
                data_back['poi_num']['rank']=rgn_item['poi_rank']
                data_back['mileage']['rank']=rgn_item['mile_rank']
                data_back['importance']['rank']=rgn_item['importance_rank']
                data_back['intence']['rank']=rgn_item['cap_rank']
                data_back['bus']['rank']=rgn_item['busm1_rank']
                data_back['drive']['rank']=rgn_item['drivem1_rank']
                data_back['search']['rank']=rgn_item['searchm1_rank']
                data_back['poi_info']['rank']=rgn_item['poi_infom1_rank']
                data_back['error']['rank']=rgn_item['errorm1_rank']
                data_back['share']['rank']=rgn_item['sharem1_rank']
                data_back['group']['rank']=rgn_item['groupm1_rank']
                data_back['collection']['rank']=rgn_item['collectionm1_rank']
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
                data_back['error']['rank']=None
                data_back['share']['rank']=None
                data_back['group']['rank']=None
                data_back['collection']['rank']=None
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
                client_pdargn,db_pdargn,posts_pdargn = Connect2Mongo("localhost", 27017,"collect_frame","pdaregion_info")
            except:
                client_pdargn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
            try:
                client_newrgn,db_newrgn,posts_newrgn = Connect2Mongo("localhost", 27017,"collect_frame","newregion_info")
            except:
                client_newrgn.disconnect()
                logger.error("connect 2 mongodb failed.")
                return
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
            busw=[[0,''] for i in range(5)]
            drivew=[[0,''] for i in range(5)]
            searchw=[[0,''] for i in range(5)]
            poi_infow=[[0,''] for i in range(5)]
            sharew=[[0,''] for i in range(5)]
            groupw=[[0,''] for i in range(5)]
            collectionw=[[0,''] for i in range(5)]
            errorw=[[0,''] for i in range(5)]
            busm=[[0,''] for i in range(5)]
            drivem=[[0,''] for i in range(5)]
            searchm=[[0,''] for i in range(5)]
            poi_infom=[[0,''] for i in range(5)]
            sharem=[[0,''] for i in range(5)]
            groupm=[[0,''] for i in range(5)]
            collectionm=[[0,''] for i in range(5)]
            errorm=[[0,''] for i in range(5)]
            for id in id_lst:
                user_item=posts_newrgn.find_one({'_id':id})
                if user_item==None:
                    return self.render_content('{"status":"failed","err_code":402,"reason":"there is no user behavior info"}')
                for i in range(5):
                    for type in user_item['info']['classify']:
                        busw[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['bus'][i][0]
                        busw[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['bus'][i][1]
                        drivew[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['drive'][i][0]
                        drivew[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['drive'][i][1]
                        searchw[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['search'][i][0]
                        searchw[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['search'][i][1]
                        poi_infow[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['poi_info'][i][0]
                        poi_infow[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['poi_info'][i][1]
                        sharew[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['share'][i][0]
                        sharew[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['share'][i][1]
                        groupw[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['group'][i][0]
                        groupw[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['group'][i][1]
                        collectionw[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['collection'][i][0]
                        collectionw[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['collection'][i][1]
                        errorw[i][0]+=user_item['info']['classify'][type]['usr_bhr']['w']['error'][i][0]
                        errorw[i][1]=user_item['info']['classify'][type]['usr_bhr']['w']['error'][i][1]
                        
                        busm[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['bus'][i][0]
                        busm[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['bus'][i][1]
                        drivem[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['drive'][i][0]
                        drivem[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['drive'][i][1]
                        searchm[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['search'][i][0]
                        searchm[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['search'][i][1]
                        poi_infom[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['poi_info'][i][0]
                        poi_infom[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['poi_info'][i][1]
                        sharem[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['share'][i][0]
                        sharem[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['share'][i][1]
                        groupm[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['group'][i][0]
                        groupm[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['group'][i][1]
                        collectionm[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['collection'][i][0]
                        collectionm[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['collection'][i][1]
                        errorm[i][0]+=user_item['info']['classify'][type]['usr_bhr']['m']['error'][i][0]
                        errorm[i][1]=user_item['info']['classify'][type]['usr_bhr']['m']['error'][i][1]
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
            userbh['share']={}
            userbh['share']['chn']='分享'
            userbh['share']['info']={}
            userbh['share']['info']['m']=[]
            userbh['share']['info']['w']=[]
            userbh['collection']={}
            userbh['collection']['chn']='收藏'
            userbh['collection']['info']={}
            userbh['collection']['info']['m']=[]
            userbh['collection']['info']['w']=[]
            userbh['group']={}
            userbh['group']['chn']='团购'
            userbh['group']['info']={}
            userbh['group']['info']['m']=[]
            userbh['group']['info']['w']=[]
            userbh['error']={}
            userbh['error']['chn']='报错'
            userbh['error']['info']={}
            userbh['error']['info']['w']=[]
            userbh['error']['info']['m']=[]


            userbh['bus']['info']['w'].append({'text':busw[4][1]+'周','rank':user_item['busw4_rank'],'val':int(busw[4][0])})
            userbh['bus']['info']['w'].append({'text':busw[3][1]+'周','rank':user_item['busw3_rank'],'val':int(busw[3][0])})
            userbh['bus']['info']['w'].append({'text':busw[2][1]+'周','rank':user_item['busw2_rank'],'val':int(busw[2][0])})
            userbh['bus']['info']['w'].append({'text':busw[1][1]+'周','rank':user_item['busw1_rank'],'val':int(busw[1][0])})
            userbh['bus']['info']['w'].append({'text':busw[0][1]+'周','rank':user_item['busw0_rank'],'val':int(busw[0][0])})
            userbh['drive']['info']['w'].append({'text':drivew[4][1]+'周','rank':user_item['drivew4_rank'],'val':int(drivew[4][0])})
            userbh['drive']['info']['w'].append({'text':drivew[3][1]+'周','rank':user_item['drivew3_rank'],'val':int(drivew[3][0])})
            userbh['drive']['info']['w'].append({'text':drivew[2][1]+'周','rank':user_item['drivew2_rank'],'val':int(drivew[2][0])})
            userbh['drive']['info']['w'].append({'text':drivew[1][1]+'周','rank':user_item['drivew1_rank'],'val':int(drivew[1][0])})
            userbh['drive']['info']['w'].append({'text':drivew[0][1]+'周','rank':user_item['drivew0_rank'],'val':int(drivew[0][0])})
            userbh['search']['info']['w'].append({'text':searchw[4][1]+'周','rank':user_item['searchw4_rank'],'val':int(searchw[4][0])})
            userbh['search']['info']['w'].append({'text':searchw[3][1]+'周','rank':user_item['searchw3_rank'],'val':int(searchw[3][0])})
            userbh['search']['info']['w'].append({'text':searchw[2][1]+'周','rank':user_item['searchw2_rank'],'val':int(searchw[2][0])})
            userbh['search']['info']['w'].append({'text':searchw[1][1]+'周','rank':user_item['searchw1_rank'],'val':int(searchw[1][0])})
            userbh['search']['info']['w'].append({'text':searchw[0][1]+'周','rank':user_item['searchw0_rank'],'val':int(searchw[0][0])})
            userbh['poi_info']['info']['w'].append({'text':poi_infow[4][1]+'周','rank':user_item['poi_infow4_rank'],'val':int(poi_infow[4][0])})
            userbh['poi_info']['info']['w'].append({'text':poi_infow[3][1]+'周','rank':user_item['poi_infow3_rank'],'val':int(poi_infow[3][0])})
            userbh['poi_info']['info']['w'].append({'text':poi_infow[2][1]+'周','rank':user_item['poi_infow2_rank'],'val':int(poi_infow[2][0])})
            userbh['poi_info']['info']['w'].append({'text':poi_infow[1][1]+'周','rank':user_item['poi_infow1_rank'],'val':int(poi_infow[1][0])})
            userbh['poi_info']['info']['w'].append({'text':poi_infow[0][1]+'周','rank':user_item['poi_infow0_rank'],'val':int(poi_infow[0][0])})
            userbh['collection']['info']['w'].append({'text':collectionw[4][1]+'周','rank':user_item['collectionw4_rank'],'val':int(collectionw[4][0])})
            userbh['collection']['info']['w'].append({'text':collectionw[3][1]+'周','rank':user_item['collectionw3_rank'],'val':int(collectionw[3][0])})
            userbh['collection']['info']['w'].append({'text':collectionw[2][1]+'周','rank':user_item['collectionw2_rank'],'val':int(collectionw[2][0])})
            userbh['collection']['info']['w'].append({'text':collectionw[1][1]+'周','rank':user_item['collectionw1_rank'],'val':int(collectionw[1][0])})
            userbh['collection']['info']['w'].append({'text':collectionw[0][1]+'周','rank':user_item['collectionw0_rank'],'val':int(collectionw[0][0])})
            userbh['share']['info']['w'].append({'text':sharew[4][1]+'周','rank':user_item['sharew4_rank'],'val':int(sharew[4][0])})
            userbh['share']['info']['w'].append({'text':sharew[3][1]+'周','rank':user_item['sharew3_rank'],'val':int(sharew[3][0])})
            userbh['share']['info']['w'].append({'text':sharew[2][1]+'周','rank':user_item['sharew2_rank'],'val':int(sharew[2][0])})
            userbh['share']['info']['w'].append({'text':sharew[1][1]+'周','rank':user_item['sharew1_rank'],'val':int(sharew[1][0])})
            userbh['share']['info']['w'].append({'text':sharew[0][1]+'周','rank':user_item['sharew0_rank'],'val':int(sharew[0][0])})
            userbh['group']['info']['w'].append({'text':groupw[4][1]+'周','rank':user_item['groupw4_rank'],'val':int(groupw[4][0])})
            userbh['group']['info']['w'].append({'text':groupw[3][1]+'周','rank':user_item['groupw3_rank'],'val':int(groupw[3][0])})
            userbh['group']['info']['w'].append({'text':groupw[2][1]+'周','rank':user_item['groupw2_rank'],'val':int(groupw[2][0])})
            userbh['group']['info']['w'].append({'text':groupw[1][1]+'周','rank':user_item['groupw1_rank'],'val':int(groupw[1][0])})
            userbh['group']['info']['w'].append({'text':groupw[0][1]+'周','rank':user_item['groupw0_rank'],'val':int(groupw[0][0])})
            userbh['error']['info']['w'].append({'text':errorw[4][1]+'周','rank':user_item['errorw4_rank'],'val':int(errorw[4][0])})
            userbh['error']['info']['w'].append({'text':errorw[3][1]+'周','rank':user_item['errorw3_rank'],'val':int(errorw[3][0])})
            userbh['error']['info']['w'].append({'text':errorw[2][1]+'周','rank':user_item['errorw2_rank'],'val':int(errorw[2][0])})
            userbh['error']['info']['w'].append({'text':errorw[1][1]+'周','rank':user_item['errorw1_rank'],'val':int(errorw[1][0])})
            userbh['error']['info']['w'].append({'text':errorw[0][1]+'周','rank':user_item['errorw0_rank'],'val':int(errorw[0][0])})   
            a=['01','02','03','04','05']          	
            userbh['bus']['info']['m'].append({'text':a[0]+'月','rank':user_item['busm4_rank'],'val':int(busm[4][0])})
            userbh['bus']['info']['m'].append({'text':a[1]+'月','rank':user_item['busm3_rank'],'val':int(busm[3][0])})
            userbh['bus']['info']['m'].append({'text':a[2]+'月','rank':user_item['busm2_rank'],'val':int(busm[2][0])})
            userbh['bus']['info']['m'].append({'text':a[3]+'月','rank':user_item['busm1_rank'],'val':int(busm[1][0])})
            userbh['bus']['info']['m'].append({'text':a[4]+'月','rank':user_item['busm0_rank'],'val':int(busm[0][0])})
            userbh['drive']['info']['m'].append({'text':a[0]+'月','rank':user_item['drivem4_rank'],'val':int(drivem[4][0])})
            userbh['drive']['info']['m'].append({'text':a[1]+'月','rank':user_item['drivem3_rank'],'val':int(drivem[3][0])})
            userbh['drive']['info']['m'].append({'text':a[2]+'月','rank':user_item['drivem2_rank'],'val':int(drivem[2][0])})
            userbh['drive']['info']['m'].append({'text':a[3]+'月','rank':user_item['drivem1_rank'],'val':int(drivem[1][0])})
            userbh['drive']['info']['m'].append({'text':a[4]+'月','rank':user_item['drivem0_rank'],'val':int(drivem[0][0])})
            userbh['search']['info']['m'].append({'text':a[0]+'月','rank':user_item['searchm4_rank'],'val':int(searchm[4][0])})
            userbh['search']['info']['m'].append({'text':a[1]+'月','rank':user_item['searchm3_rank'],'val':int(searchm[3][0])})
            userbh['search']['info']['m'].append({'text':a[2]+'月','rank':user_item['searchm2_rank'],'val':int(searchm[2][0])})
            userbh['search']['info']['m'].append({'text':a[3]+'月','rank':user_item['searchm1_rank'],'val':int(searchm[1][0])})
            userbh['search']['info']['m'].append({'text':a[4]+'月','rank':user_item['searchm0_rank'],'val':int(searchm[0][0])})
            userbh['poi_info']['info']['m'].append({'text':a[0]+'月','rank':user_item['poi_infom4_rank'],'val':int(poi_infom[4][0])})
            userbh['poi_info']['info']['m'].append({'text':a[1]+'月','rank':user_item['poi_infom3_rank'],'val':int(poi_infom[3][0])})
            userbh['poi_info']['info']['m'].append({'text':a[2]+'月','rank':user_item['poi_infom2_rank'],'val':int(poi_infom[2][0])})
            userbh['poi_info']['info']['m'].append({'text':a[3]+'月','rank':user_item['poi_infom1_rank'],'val':int(poi_infom[1][0])})
            userbh['poi_info']['info']['m'].append({'text':a[4]+'月','rank':user_item['poi_infom0_rank'],'val':int(poi_infom[0][0])})
            userbh['collection']['info']['m'].append({'text':a[0]+'月','rank':user_item['collectionm4_rank'],'val':int(collectionm[4][0])})
            userbh['collection']['info']['m'].append({'text':a[1]+'月','rank':user_item['collectionm3_rank'],'val':int(collectionm[3][0])})
            userbh['collection']['info']['m'].append({'text':a[2]+'月','rank':user_item['collectionm2_rank'],'val':int(collectionm[2][0])})
            userbh['collection']['info']['m'].append({'text':a[3]+'月','rank':user_item['collectionm1_rank'],'val':int(collectionm[1][0])})
            userbh['collection']['info']['m'].append({'text':a[4]+'月','rank':user_item['collectionm0_rank'],'val':int(collectionm[0][0])})
            userbh['share']['info']['m'].append({'text':a[0]+'月','rank':user_item['sharem4_rank'],'val':int(sharem[4][0])})
            userbh['share']['info']['m'].append({'text':a[1]+'月','rank':user_item['sharem3_rank'],'val':int(sharem[3][0])})
            userbh['share']['info']['m'].append({'text':a[2]+'月','rank':user_item['sharem2_rank'],'val':int(sharem[2][0])})
            userbh['share']['info']['m'].append({'text':a[3]+'月','rank':user_item['sharem1_rank'],'val':int(sharem[1][0])})
            userbh['share']['info']['m'].append({'text':a[4]+'月','rank':user_item['sharem0_rank'],'val':int(sharem[0][0])})
            userbh['group']['info']['m'].append({'text':a[0]+'月','rank':user_item['groupm4_rank'],'val':int(groupm[4][0])})
            userbh['group']['info']['m'].append({'text':a[1]+'月','rank':user_item['groupm3_rank'],'val':int(groupm[3][0])})
            userbh['group']['info']['m'].append({'text':a[2]+'月','rank':user_item['groupm2_rank'],'val':int(groupm[2][0])})
            userbh['group']['info']['m'].append({'text':a[3]+'月','rank':user_item['groupm1_rank'],'val':int(groupm[1][0])})
            userbh['group']['info']['m'].append({'text':a[4]+'月','rank':user_item['groupm0_rank'],'val':int(groupm[0][0])})
            userbh['error']['info']['m'].append({'text':a[0]+'月','rank':user_item['errorm4_rank'],'val':int(errorm[4][0])})
            userbh['error']['info']['m'].append({'text':a[1]+'月','rank':user_item['errorm3_rank'],'val':int(errorm[3][0])})
            userbh['error']['info']['m'].append({'text':a[2]+'月','rank':user_item['errorm2_rank'],'val':int(errorm[2][0])})
            userbh['error']['info']['m'].append({'text':a[3]+'月','rank':user_item['errorm1_rank'],'val':int(errorm[1][0])})
            userbh['error']['info']['m'].append({'text':a[4]+'月','rank':user_item['errorm0_rank'],'val':int(errorm[0][0])})   
            '''
            userbh['bus']['info']['m'].append({'text':busm[4][1]+'月','rank':user_item['busm4_rank'],'val':int(busm[4][0])})
            userbh['bus']['info']['m'].append({'text':busm[3][1]+'月','rank':user_item['busm3_rank'],'val':int(busm[3][0])})
            userbh['bus']['info']['m'].append({'text':busm[2][1]+'月','rank':user_item['busm2_rank'],'val':int(busm[2][0])})
            userbh['bus']['info']['m'].append({'text':busm[1][1]+'月','rank':user_item['busm1_rank'],'val':int(busm[1][0])})
            userbh['bus']['info']['m'].append({'text':busm[0][1]+'月','rank':user_item['busm0_rank'],'val':int(busm[0][0])})
            userbh['drive']['info']['m'].append({'text':drivem[4][1]+'月','rank':user_item['drivem4_rank'],'val':int(drivem[4][0])})
            userbh['drive']['info']['m'].append({'text':drivem[3][1]+'月','rank':user_item['drivem3_rank'],'val':int(drivem[3][0])})
            userbh['drive']['info']['m'].append({'text':drivem[2][1]+'月','rank':user_item['drivem2_rank'],'val':int(drivem[2][0])})
            userbh['drive']['info']['m'].append({'text':drivem[1][1]+'月','rank':user_item['drivem1_rank'],'val':int(drivem[1][0])})
            userbh['drive']['info']['m'].append({'text':drivem[0][1]+'月','rank':user_item['drivem0_rank'],'val':int(drivem[0][0])})
            userbh['search']['info']['m'].append({'text':searchm[4][1]+'月','rank':user_item['searchm4_rank'],'val':int(searchm[4][0])})
            userbh['search']['info']['m'].append({'text':searchm[3][1]+'月','rank':user_item['searchm3_rank'],'val':int(searchm[3][0])})
            userbh['search']['info']['m'].append({'text':searchm[2][1]+'月','rank':user_item['searchm2_rank'],'val':int(searchm[2][0])})
            userbh['search']['info']['m'].append({'text':searchm[1][1]+'月','rank':user_item['searchm1_rank'],'val':int(searchm[1][0])})
            userbh['search']['info']['m'].append({'text':searchm[0][1]+'月','rank':user_item['searchm0_rank'],'val':int(searchm[0][0])})
            userbh['poi_info']['info']['m'].append({'text':poi_infom[4][1]+'月','rank':user_item['poi_infom4_rank'],'val':int(poi_infom[4][0])})
            userbh['poi_info']['info']['m'].append({'text':poi_infom[3][1]+'月','rank':user_item['poi_infom3_rank'],'val':int(poi_infom[3][0])})
            userbh['poi_info']['info']['m'].append({'text':poi_infom[2][1]+'月','rank':user_item['poi_infom2_rank'],'val':int(poi_infom[2][0])})
            userbh['poi_info']['info']['m'].append({'text':poi_infom[1][1]+'月','rank':user_item['poi_infom1_rank'],'val':int(poi_infom[1][0])})
            userbh['poi_info']['info']['m'].append({'text':poi_infom[0][1]+'月','rank':user_item['poi_infom0_rank'],'val':int(poi_infom[0][0])})
            userbh['collection']['info']['m'].append({'text':collectionm[4][1]+'月','rank':user_item['collectionm4_rank'],'val':int(collectionm[4][0])})
            userbh['collection']['info']['m'].append({'text':collectionm[3][1]+'月','rank':user_item['collectionm3_rank'],'val':int(collectionm[3][0])})
            userbh['collection']['info']['m'].append({'text':collectionm[2][1]+'月','rank':user_item['collectionm2_rank'],'val':int(collectionm[2][0])})
            userbh['collection']['info']['m'].append({'text':collectionm[1][1]+'月','rank':user_item['collectionm1_rank'],'val':int(collectionm[1][0])})
            userbh['collection']['info']['m'].append({'text':collectionm[0][1]+'月','rank':user_item['collectionm0_rank'],'val':int(collectionm[0][0])})
            userbh['share']['info']['m'].append({'text':sharem[4][1]+'月','rank':user_item['sharem4_rank'],'val':int(sharem[4][0])})
            userbh['share']['info']['m'].append({'text':sharem[3][1]+'月','rank':user_item['sharem3_rank'],'val':int(sharem[3][0])})
            userbh['share']['info']['m'].append({'text':sharem[2][1]+'月','rank':user_item['sharem2_rank'],'val':int(sharem[2][0])})
            userbh['share']['info']['m'].append({'text':sharem[1][1]+'月','rank':user_item['sharem1_rank'],'val':int(sharem[1][0])})
            userbh['share']['info']['m'].append({'text':sharem[0][1]+'月','rank':user_item['sharem0_rank'],'val':int(sharem[0][0])})
            userbh['group']['info']['m'].append({'text':groupm[4][1]+'月','rank':user_item['groupm4_rank'],'val':int(groupm[4][0])})
            userbh['group']['info']['m'].append({'text':groupm[3][1]+'月','rank':user_item['groupm3_rank'],'val':int(groupm[3][0])})
            userbh['group']['info']['m'].append({'text':groupm[2][1]+'月','rank':user_item['groupm2_rank'],'val':int(groupm[2][0])})
            userbh['group']['info']['m'].append({'text':groupm[1][1]+'月','rank':user_item['groupm1_rank'],'val':int(groupm[1][0])})
            userbh['group']['info']['m'].append({'text':groupm[0][1]+'月','rank':user_item['groupm0_rank'],'val':int(groupm[0][0])})
            userbh['error']['info']['m'].append({'text':errorm[4][1]+'月','rank':user_item['errorm4_rank'],'val':int(errorm[4][0])})
            userbh['error']['info']['m'].append({'text':errorm[3][1]+'月','rank':user_item['errorm3_rank'],'val':int(errorm[3][0])})
            userbh['error']['info']['m'].append({'text':errorm[2][1]+'月','rank':user_item['errorm2_rank'],'val':int(errorm[2][0])})
            userbh['error']['info']['m'].append({'text':errorm[1][1]+'月','rank':user_item['errorm1_rank'],'val':int(errorm[1][0])})
            userbh['error']['info']['m'].append({'text':errorm[0][1]+'月','rank':user_item['errorm0_rank'],'val':int(errorm[0][0])})                    
            '''
                
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

