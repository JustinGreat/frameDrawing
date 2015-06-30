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
from werkzeug.datastructures import Headers
from jinja2 import Environment, FileSystemLoader
from pymongo import MongoClient
import cjson
import shapefile 
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
import gzip
NUM_MONTH=18
NUM_Q=6
NUM_HY=3
NUM_S_MONTH=13
NUM_S_Q=5
NUM_S_HY=3
rgnTypeList={'roadgrid':'PDA+路网'}
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
        self.CODE_CHN_TABLE={'01':'汽车服务','02':'汽车销售','03':'汽车维修','04':'摩托车服务','05':'餐饮服务','06':'购物服务','07':'生活服务','08':'体育休闲服务','09':'医疗保健服务','10':'住宿服务','11':'风景名胜','12':'商务住宅','13':'政府机构及社会团体','14':'科教文化服务','15':'交通设施服务','16':'金融保险服务','17':'公司企业','18':'道路附属设施','19':'地名地址信息','20':'公共设施','22':'事件活动','97':'室内设施','99':'通行设施'}
        self.date_map={}
        client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,'roadgrid',"city")
        city_data=posts_city.find_one({'_id':'110000'})
        for i in range(1,13):
            self.date_map['m'+str(i)]=city_data['month_list'][i]
        for i in range(1,5):
            self.date_map['q'+str(i)]=city_data['qter_list'][i]
        for i in range(1,3):
            self.date_map['hy'+str(i)]=city_data['hy_list'][i]
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

    def render_gzip_data(self,retContent):
        h=Response(retContent,mimetype="application/json")
        h.headers["Content-Encoding"]="gzip"
        return h
    def render_gzip(self,retContent,name):
        h=Response(retContent,mimetype="gzip")
        #h.headers["Content-Encoding"]="gzip"
        h.headers["Content-Disposition"]="attachment;filename="+name
        return h
    def on_get_fresh_rank(self,request):
        flag=request.values.get("flag")
        rgnType=request.values.get("rgnType")
        adcode=request.values.get("adcode")
        tm=request.values.get("timespan")
        sel_city=adcode
        if flag=='init':
            data={}
            data['city_list']=[]
            idata={}
            for rgnType in rgnTypeList:
                client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,rgnType,"city")
                for item in posts_city.find():
                    if idata.get(item['_id'],'')=='':
                        idata[item['_id']]={}
                        idata[item['_id']]['name']=item['name']
                        idata[item['_id']]['rgnType']=[{"rgnType":rgnType,"name":rgnTypeList[rgnType]}]
                    else:
                        idata[item['_id']]['rgnType']+=[{"rgnType":rgnType,"name":rgnTypeList[rgnType]}]
            for ad in idata:
                data['city_list'].append({'adcode':ad,'name':idata[ad]['name'],'rgnType_list':idata[ad]['rgnType']})
            data_back=json.dumps(data,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back)
        if flag=='city':
            if rgnType==None:
                rgnType='roadgrid'
            client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,rgnType,"city")
            city_data=posts_city.find_one({'_id':adcode})
            city_info={}
            city_info['adcode']=str(adcode)
            city_info['pos']=[city_data['lon'],city_data['lat']]
            cont=['mileage','poi_num','freshness','prosp','importance','drive','bus','poi_info','search','group','collection','error','share']
            cont_no_pro=['freshness','importance','prosp']
            for c in cont:
                city_info[c]={}
                city_info[c]['val']=0
                if not c in cont_no_pro:
                    city_info[c]['proportion']=0
                city_info[c]['rank']=0
            city_info['mileage']['val']=round(city_data['mileage']/1000,2)
            city_info['poi_num']['val']=city_data['poi_num']
            city_info['poi_info']['val']=city_data['poi_infom1']
            city_info['share']['val']=city_data['sharem1']
            city_info['search']['val']=city_data['searchm1']
            city_info['collection']['val']=city_data['collectionm1']
            city_info['error']['val']=city_data['errorm1']
            city_info['bus']['val']=city_data['busm1']
            city_info['drive']['val']=city_data['drivem1']
            city_info['group']['val']=city_data['groupm1']
            city_info['datelst']={}
            city_info['datelst']['m']=[]
            city_info['datelst']['q']=[]
            city_info['datelst']['hy']=[]
            for i in range(1,13):
                city_info['datelst']['m'].append({'val':'m'+str(i),'name':city_data['month_list'][i]})
                self.date_map['m'+str(i)]=city_data['month_list'][i]
            for i in range(1,5):
                city_info['datelst']['q'].append({'val':'q'+str(i),'name':city_data['qter_list'][i][:4]+'q'+city_data['qter_list'][i][5]})
                self.date_map['q'+str(i)]=city_data['qter_list'][i]
            for i in range(1,3):
                city_info['datelst']['hy'].append({'val':'hy'+str(i),'name':city_data['hy_list'][i][:4]+'hy'+city_data['hy_list'][i][5]})
                self.date_map['hy'+str(i)]=city_data['hy_list'][i]
            city_info['type']={}
            for type in city_data['classify']:
                if city_info['type'].get(type[:2],'')=='':
                    city_info['type'][type[:2]]={}
                    city_info['type'][type[:2]]['val']=city_data['classify'][type]['freshness_val']
                    city_info['type'][type[:2]]['chn']=self.CODE_CHN_TABLE[type[:2]]
                else:
                    city_info['type'][type[:2]]['val']+=city_data['classify'][type]['freshness_val']
            for type in city_info['type']:
                city_info['type'][type]['proportion']=round(float(city_info['type'][type]['val'])/city_info['poi_num']['val']*100,2)
                city_info['type'][type]['rank']=0

            city_json=json.dumps(city_info,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(city_json)
        if flag=='frame':
            client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,rgnType,"city")
            rangell=request.values.get("range")
            if rangell=='':
                return self.render_content('{"status":"failed","err_code":201,"reason":"there is no range in the request"}')
            rangelst=rangell.split(',')
            lon1=float(rangelst[0])
            lat1=float(rangelst[1])
            lon2=float(rangelst[2])
            lat2=float(rangelst[3])
            if lon1>lon2 or lat1>lat2:
                return self.render_content('{status:failed,err_code:202,reason:range format error}')
            client,db,posts = Connect2Mongo("localhost", 27017,rgnType,sel_city+tm)
            data_back=[]
            city_data=posts_city.find_one({'_id':sel_city})
            for frm in posts.find({"$and":[{"lon":{"$gte":lon1,"$lte":lon2}},{"lat":{"$gte":lat1,"$lte":lat2}}]}):
                frm['id']=frm['_id']
                frm['rgn_chg1']={}
                frm['rgn_chg1']['val']=0.1
                frm['rgn_chg1']['rank']=200
                frm['rgn_chg1']['hot']=50
                frm['rgn_chg2']={}
                frm['rgn_chg2']['val']=0.2
                frm['rgn_chg2']['rank']=100
                frm['rgn_chg2']['hot']=100
                data_back.append(frm)
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        if flag=='statistic':
            client,db,posts = Connect2Mongo("localhost", 27017,rgnType,sel_city)
            client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,rgnType,"city")
            id_str=request.values.get("id")
            if id_str=='':
                return self.render_content('{"status":"failed","err_code":301,"reason":"no id found"}')
            id_lst=id_str.split(',')
            id_num=len(id_lst)
            multirgn={}
            data_back={}
            data_back['type']={}
            multirgn['id']=id_lst
            multirgn['poi_num']=0
            multirgn['mileage']=0.0
            multirgn['poi_info']=0.0
            multirgn['bus']=0.0
            multirgn['search']=0.0
            multirgn['drive']=0.0
            multirgn['share']=0.0
            multirgn['error']=0.0
            multirgn['collection']=0.0
            multirgn['group']=0.0
            multirgn['rgn_chg1']=0.0
            multirgn['rgn_chg2']=0.0
            for id in id_lst:
                rgn_item=posts.find_one({'_id':id})
                if rgn_item==None:
                    return self.render_content('{"status":"failed","err_code":302,"reason":"Region Type does not fit"}')
                multirgn['poi_num']+=rgn_item['poi_num']
                multirgn['mileage']+=rgn_item['mileage']
                multirgn['poi_info']+=int(rgn_item['poi_info'+tm])
                multirgn['bus']+=int(rgn_item['bus'+tm])
                multirgn['search']+=int(rgn_item['search'+tm])
                multirgn['drive']+=int(rgn_item['drive'+tm])
                multirgn['share']+=int(rgn_item['share'+tm])
                multirgn['error']+=int(rgn_item['error'+tm])
                multirgn['collection']+=int(rgn_item['collection'+tm])
                multirgn['group']+=int(rgn_item['group'+tm])
                multirgn['rgn_chg1']+=round(rgn_item.get('rgn_chg1'+tm,0.0),2)
                multirgn['rgn_chg2']+=round(rgn_item.get('rgn_chg2'+tm,0.0),2)
                for type in rgn_item['classify']:
                    if not type[:2] in data_back['type']:
                        data_back['type'][type[:2]]={}
                        data_back['type'][type[:2]]['val']=len(rgn_item['classify'][type]['list'])
                    else:
                        data_back['type'][type[:2]]['val']+=len(rgn_item['classify'][type]['list'])
            for type in data_back['type']:
                data_back['type'][type]['chn']=self.CODE_CHN_TABLE[type]
                if multirgn['poi_num']<0.1:
                    data_back['type'][type[:2]]['proportion']=0
                else:
                    data_back['type'][type[:2]]['proportion']=float(data_back['type'][type[:2]]['val'])/multirgn['poi_num']*100
                data_back['type'][type]['proportion']=round(data_back['type'][type]['proportion'],2)
            city_data=posts_city.find_one({'_id':sel_city})
            if city_data==None:
                self.render_content('{"status":"failed","err_code":303,"reason":"no city found"}')
            city_poi_num=city_data['poi_num']
            city_mile_num=city_data['mileage']
            city_poiinfo_num=city_data['poi_info'+tm]
            city_search_num=city_data['search'+tm]
            city_bus_num=city_data['bus'+tm]
            city_drive_num=city_data['drive'+tm]
            city_group_num=city_data['group'+tm]
            city_error_num=city_data['error'+tm]
            city_collection_num=city_data['collection'+tm]
            city_share_num=city_data['share'+tm]
            
            
            data_back['id']=multirgn['id']
            data_back['poi_num']={}
            data_back['poi_num']['val']=multirgn['poi_num']
            try:
                data_back['poi_num']['proportion']=round(float(multirgn['poi_num'])/city_poi_num*100)
            except:
                data_back['poi_num']['proportion']=0
            data_back['mileage']={}
            data_back['mileage']['val']=round(float(multirgn['mileage'])/1000.0,2)
            try:
                data_back['mileage']['proportion']=round(float(multirgn['mileage'])/city_mile_num*100,2)
            except:
                data_back['mileage']['proportion']=0
            data_back['intence']={}
            try:
                data_back['intence']['val']=round(1000*float(multirgn['poi_num'])/multirgn['mileage'],2)
            except:
                data_back['intence']['val']=0
            data_back['intence']['proportion']=None
            data_back['importance']={}
            data_back['importance']['val']=rgn_item['importance']
            data_back['drive']={}
            data_back['drive']['val']=int(multirgn['drive'])
            try:
                data_back['drive']['proportion']=round(float(multirgn['drive'])/city_drive_num*100,2)
            except:
                data_back['drive']['proportion']=0
            data_back['search']={}
            data_back['search']['val']=int(multirgn['search'])
            try:
                data_back['search']['proportion']=round(float(multirgn['search'])/city_search_num*100,2)
            except:
                data_back['search']['proportion']=0
            data_back['bus']={}
            data_back['bus']['val']=int(multirgn['bus'])
            try:
                data_back['bus']['proportion']=round(float(multirgn['bus'])/city_bus_num*100,2)
            except:
                data_back['bus']['proportion']=0
            data_back['poi_info']={}
            data_back['poi_info']['val']=int(multirgn['poi_info'])
            try:
                data_back['poi_info']['proportion']=round(float(multirgn['poi_info'])/city_poiinfo_num*100,2)
            except:
                data_back['poi_info']['proportion']=0
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
            try:
                data_back['group']['proportion']=round(float(multirgn['group'])/city_group_num*100,2)
            except:
                data_back['group']['proportion']=0
            data_back['error']={}
            data_back['error']['val']=int(multirgn['error'])
            try:
                data_back['error']['proportion']=round(float(multirgn['error'])/city_error_num*100,2)    
            except:
                data_back['error']['proportion']=0
            data_back['rgn_chg1']={}
            data_back['rgn_chg1']['val']=int(multirgn['rgn_chg1'])
            data_back['rgn_chg2']={}
            data_back['rgn_chg2']['val']=int(multirgn['rgn_chg2'])
            if id_num==1 and rgn_item!=None:
                #data_back['rank']=rgn_item['info']['rank']
                data_back['freshness']=rgn_item['freshness']
                data_back['fresh_rank']=int(rgn_item['freshness_rank'])
                data_back['poi_num']['rank']=rgn_item['poi_num_rank']
                data_back['mileage']['rank']=rgn_item['mileage_rank']
                data_back['importance']['rank']=rgn_item['importance_rank']
                data_back['intence']['rank']=rgn_item['cap_rank']
                data_back['bus']['rank']=rgn_item['bus'+tm+'_rank']
                data_back['drive']['rank']=rgn_item['drive'+tm+'_rank']
                data_back['search']['rank']=rgn_item['search'+tm+'_rank']
                data_back['poi_info']['rank']=rgn_item['poi_info'+tm+'_rank']
                data_back['error']['rank']=rgn_item['error'+tm+'_rank']
                data_back['share']['rank']=rgn_item['share'+tm+'_rank']
                data_back['group']['rank']=rgn_item['group'+tm+'_rank']
                data_back['collection']['rank']=rgn_item['collection'+tm+'_rank']
                data_back['rgn_chg1']['rank']=rgn_item.get('rgn_chg1'+tm+'_rank',0)
                data_back['rgn_chg2']['rank']=rgn_item.get('rgn_chg2'+tm+'_rank',0)
            else:
                data_back['rgn_chg1']['val']=None
                data_back['rgn_chg2']['val']=None
                data_back['freshness']=None
                #data_back['rank']=None
                data_back['fresh_rank']=None
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
                data_back['rgn_chg1']['rank']=None
                data_back['rgn_chg2']['rank']=None
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            self.logger1.info(data_back_json)
            return self.render_content(data_back_json)
        if flag=='user':
            id_str=request.values.get("id")
            id_lst=id_str.split(',')
            id_num=len(id_lst)
            client,db,posts = Connect2Mongo("localhost", 27017,rgnType,sel_city)
            cont=['bus','drive','search','poi_info','share','error','collection','group','rgn_chg1','rgn_chg2']
            userbh={}
            userbh['id']=id_lst
            for c in cont:
                userbh[c]={}
                userbh[c]['info']={}
                userbh[c]['info']['m']=[{'val':0} for i in range(1,13)]
                userbh[c]['info']['q']=[{'val':0} for i in range(1,5)]
                userbh[c]['info']['hy']=[{'val':0} for i in range(1,3)]
            for id in id_lst:
                user_item=posts.find_one({'_id':id})
                if user_item==None:
                    return self.render_content('{"status":"failed","err_code":402,"reason":"there is no user behavior info"}')
                for c in cont:
                    for i in range(1,13):
                        userbh[c]['info']['m'][i-1]['val']+=user_item.get(c+'m'+str(13-i),0)
                        userbh[c]['info']['m'][i-1]['rank']=user_item.get(c+'m'+str(13-i)+'_rank',0)
                        userbh[c]['info']['m'][i-1]['text']=user_item['month_list'][13-i][-2:]
                    for i in range(1,5):
                        userbh[c]['info']['q'][i-1]['val']+=user_item.get(c+'q'+str(5-i),0)
                        userbh[c]['info']['q'][i-1]['rank']=user_item.get(c+'q'+str(5-i)+'_rank',0)
                        userbh[c]['info']['q'][i-1]['text']=user_item['qter_list'][5-i][-2:]
                    for i in range(1,3):
                        userbh[c]['info']['hy'][i-1]['val']+=user_item.get(c+'hy'+str(3-i),0)
                        userbh[c]['info']['hy'][i-1]['rank']=user_item.get(c+'hy'+str(3-i)+'_rank',0)
                        userbh[c]['info']['hy'][i-1]['text']=user_item['hy_list'][3-i][-2:]
            userbh['bus']['chn']='公交导航'
            userbh['drive']['chn']='汽车导航'
            userbh['search']['chn']='搜索量'
            userbh['poi_info']['chn']='点击量'
            userbh['share']['chn']='分享'
            userbh['collection']['chn']='收藏'
            userbh['group']['chn']='团购'
            userbh['error']['chn']='报错'
            userbh['rgn_chg1']['chn']='变化率1'
            userbh['rgn_chg2']['chn']='变化率2'
            '''
            if user_item.get('rgn_chgm12','')=='': 
                for i in range(12):
                    userbh['rgn_chg']['info']['m'].append({'text':str((i+5)%12+1),'rank':0,'val':int(100*0)}) 
            else:            
                for i in range(12):
                    userbh['rgn_chg']['info']['m'].append({'text':str((i+5)%12+1),'rank':user_item['rgn_chgm'+str(12-i)+'_rank'],'val':int(100*user_item['rgn_chgm'+str(12-i)])})  
            '''

            data_back_json=json.dumps(userbh,ensure_ascii=False).encode('utf-8','ignore')
            self.logger1.info(data_back_json)
            return self.render_content(data_back_json)
        if flag=='select':
            client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,rgnType,"city")
            client,db,posts = Connect2Mongo("localhost", 27017,rgnType,sel_city)
            cont=['mileage','poi_num','poi_cap','freshness','prosp','importance']
            u_cont=['bus','share','drive','collection','error','group','search','poi_info']
            cont_rank=[]
            for c in cont:
                if c=='poi_cap':
                    cont_rank.append('cap_rank')
                else:
                    cont_rank.append(c+'_rank')
            for u in u_cont:
                cont.append(u)
                cont_rank.append(u+tm+'_rank')
            city_data=posts_city.find_one({'_id':sel_city})
            sel_lst=[]
            rgn_num=posts.count()
            for c in cont:
                itm=request.values.get(c)
                if itm==None:
                    continue
                itm_lst=itm.split(',')
                if cont.index(c)>(len(cont)-len(u_cont)-1):
                    i=c+tm
                else:
                    i=c
                if itm_lst[0]=='' or itm_lst[1]=='':
                    pass
                else:
                    sel_itm={}
                    sel_itm[i]={}
                    sel_itm[i]["$gte"]=float(itm_lst[0])
                    sel_itm[i]["$lte"]=float(itm_lst[1])
                    sel_lst.append(sel_itm)
                    
                if itm_lst[2]=='' or itm_lst[3]=='':
                    pass
                else:
                    sel_itm={}

                    if cont.index(c)>(len(cont)-len(u_cont)-1) :#or cont.index(c)<2:
                        nm='acu_'+c+tm
                    else:
                        nm='acu_'+c
                    sel_itm[nm]={}
                    if city_data[c]==0:
                        pass
                    else:
                        sel_itm[nm]["$gte"]=float(itm_lst[2])*city_data[c]/100
                        sel_itm[nm]["$lte"]=float(itm_lst[3])*city_data[c]/100
                        sel_lst.append(sel_itm)
                    
                if itm_lst[4]=='' or itm_lst[5]=='':
                    pass
                else:
                    sel_itm={}
                    sel_itm[cont_rank[cont.index(c)]]={}
                    sel_itm[cont_rank[cont.index(c)]]["$gte"]=float(itm_lst[4])
                    sel_itm[cont_rank[cont.index(c)]]["$lte"]=float(itm_lst[5])
                    sel_lst.append(sel_itm)
                
                if len(itm_lst)>6:
                    if itm_lst[6]=='' or itm_lst[7]=='':
                        pass
                    else:
                        sel_itm={}
                        sel_itm[cont_rank[cont.index(c)]]={}
                        sel_itm[cont_rank[cont.index(c)]]["$gte"]=float(itm_lst[6])*rgn_num/100
                        sel_itm[cont_rank[cont.index(c)]]["$lte"]=float(itm_lst[7])*rgn_num/100
                        sel_lst.append(sel_itm)
            if sel_lst==[]:
                self.render_content('{"status":"null","err_code":"0404","reason":"no sel condition"}')
            frm_data=posts.find({"$and":sel_lst})
            #frm_data=posts_pdargn.find({"$and":[{"lon":{"$gte":lon1,"$lte":lon2}},{"lat":{"$gte":lat1,"$lte":lat2}}]})
            data_back=[]
            count=0
            frm_num=posts.count()
            city_data=posts_city.find_one({'_id':sel_city})
            for frm in frm_data:
                data_rgn={}
                data_rgn['id']=frm['_id']
                frm_str=frm['frm']
                frm_lst=frm_str.split(';')
                dots=[]
                for item in frm_lst:
                    dot_spl=item.split(',')
                    dots.append([float(dot_spl[0]),float(dot_spl[1])])
                data_rgn['dots']=dots
                data_rgn['freshness']={}
                data_rgn['freshness']['val']=frm['freshness']
                data_rgn['freshness']['rank']=frm['freshness_rank']
                data_rgn['freshness']['hot']=(1-float(frm['freshness_rank']-1)/frm_num)*100
                data_rgn['lon']=frm['lon']
                data_rgn['lat']=frm['lat']
                data_rgn['poi_num']={}
                data_rgn['poi_num']['val']=frm['poi_num']
                data_rgn['poi_num']['proportion']=round(float(frm['poi_num'])/city_data['poi_num'],2)
                data_rgn['poi_num']['rank']=frm['poi_num_rank']
                data_rgn['poi_num']['hot']=(1-float(frm['poi_num_rank']-1)/frm_num)*100
                data_rgn['poi_cap']={}
                data_rgn['poi_cap']['val']=frm['poi_cap']
                data_rgn['poi_cap']['rank']=frm['cap_rank']
                data_rgn['poi_cap']['hot']=(1-float(frm['cap_rank']-1)/frm_num)*100
                data_rgn['mileage']={}
                data_rgn['mileage']['val']=frm['mileage']
                data_rgn['mileage']['proportion']=round(float(frm['mileage'])/city_data['mileage'])
                data_rgn['mileage']['rank']=frm['mileage_rank']
                data_rgn['mileage']['hot']=(1-float(frm['mileage_rank']-1)/frm_num)*100
                data_rgn['prosp']={}
                data_rgn['prosp']['val']=frm['prosp']
                data_rgn['prosp']['rank']=frm['prosp_rank']
                data_rgn['prosp']['hot']=(1-float(frm['prosp_rank']-1)/frm_num)*100
                data_rgn['importance']={}
                data_rgn['importance']['val']=frm['importance']
                data_rgn['importance']['rank']=frm['importance_rank']
                data_rgn['importance']['hot']=(1-float(frm['importance_rank']-1)/frm_num)*100
                data_rgn['pos_hot']={}
                data_rgn['pos_hot']['val']=0
                data_rgn['pos_hot']['rank']=0
                data_rgn['pos_hot']['proportion']=0
                cont=['bus','drive','search','poi_info','share','error','collection','group','rgn_chg1','rgn_chg2']
                for c in cont:
                    data_rgn[c]={}
                    if frm.get(c+tm,'')=='':
                        data_rgn[c]['val']=0
                        data_rgn[c]['rank']=0
                        data_rgn[c]['hot']=0
                        continue
                    try:
                        data_rgn[c]['rank']=frm[c+tm+'_rank']
                    except:
                        data_rgn[c]['rank']=0
                    data_rgn[c]['val']=int(frm[c+tm])
                    data_rgn[c]['hot']=(1-float(frm[c+tm+'_rank']-1)/frm_num)*100
                    try:
                        data_rgn[c]['proportion']=float(frm[c+tm])/city_data[c+tm]
                    except:
                        data_rgn[c]['proportion']=0
                data_back.append(data_rgn)
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
            
        if flag=='find':
            client,db,posts = Connect2Mongo("localhost", 27017,rgnType,sel_city+tm)
            id=request.values.get('id')
            if id==None:
                return self.render_content('{"status":"city_find","err_code":"000","reason":"find city info"}')
            frm=posts.find_one({'_id':id})
            frm['id']=frm['_id']
            data_back=[]
            if frm==None:
                return self.render_content('{"status":"failed","err_code":"601","reason":"no matched frames"}')
            data_back.append(frm)
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            return self.render_content(data_back_json)
        if flag=='citydata':
            if True==os.path.exists('./frames/'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]):
                f_in=open('./frames/'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0],'r')
                ret_con=f_in.read()
                f_in.close()
                return self.render_content(ret_con)
            client,db,posts = Connect2Mongo("localhost", 27017,rgnType,sel_city+tm)
            data_back=[]
            for frm in posts.find():
                frm['id']=frm['_id']
                data_back.append(frm)
            data_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            data_back_json=json.dumps(data_back,ensure_ascii=False).encode('utf-8','ignore')
            f_out=open('./frames/'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0],'w')
            f_out.write(data_back_json)
            f_out.close()
            return self.render_content(data_back_json)
        if flag=='getfile':
            if rgnType==None:
                rgnType='roadgrid'
            client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,rgnType,"city")
            client,db,posts = Connect2Mongo("localhost", 27017,rgnType,sel_city+tm)
            scale=request.values.get("scale")
            if scale==None:
                return self.render_content('{"status":"failed","err_code":501,"reason":"range format error"}')
            city_info=posts_city.find_one({'_id':sel_city})
            if scale=='sel':
                id_str=request.values.get("id")
                if id_str==None:
                    return self.render_content('{"status":"failed","err_code":502,"reason":"There is no id"}')
                id_lst=id_str.split(',')
                w = shapefile.Writer()
                w.autoBalance = 1
                w = shapefile.Writer(shapefile.POLYGON)
                w.field('id','C','40')
                w.field('name','C','40')
                w.field('cj_date','D','10')
                w.field('cj_method','C','20')
                w.field('rgn_level','C','20')
                w.field('poi_pro','C','20')
                w.field('poi_rank','C','20')
                w.field('mile_pro','C','20')
                w.field('mile_rank','C','20')
                w.field('cap_rank','C','20')
                w.field('impt_rank','C','20')
                w.field('fresh_rank','C','20')
                w.field('prosp_rank','C','20')
                w.field('p_info_pro','C','20')
                w.field('p_info_rank','C','20')
                w.field('search_pro','C','20')
                w.field('search_rank','C','20')
                w.field('drive_pro','C','20')
                w.field('drive_rank','C','20')
                w.field('bus_pro','C','20')
                w.field('bus_rank','C','20')
                w.field('group_pro','C','20')
                w.field('group_rank','C','20')
                w.field('clc_pro','C','20')
                w.field('clc_rank','C','20')
                w.field('error_pro','C','20')
                w.field('error_rank','C','20')
                w.field('share_pro','C','20')
                w.field('share_rank','C','20')
                w.field('rgn1_rank','C','20')
                w.field('rgn2_rank','C','20')
                w.field('pos_hot_pro','C','20')
                w.field('pos_hot_rank','C','20')
                for id in id_lst:
                    polygon_dots=[]
                    item=posts.find_one({'_id':id})
                    if item==None:
                        return self.render_content('{"status":"failed","err_code":503,"reason":"Region Type does not fit"}')
                    data={}
                    data['id']=item['_id']
                    data['name']='null'
                    data['poi_pro']=str(item['poi_num']['proportion'])
                    data['poi_num_rank']=(item['poi_num']['rank'])
                    data['mile_pro']=str(item['mileage']['proportion'])
                    data['mileage_rank']=str(item['mileage']['rank'])
                    data['cap_rank']=str(item['poi_cap']['rank'])
                    data['importance_rank']=str(item['importance']['rank'])
                    data['freshness_rank']=str(item['freshness']['rank'])
                    data['prosp_rank']=str(item['prosp']['rank'])
                    data['poi_info_pro']=str(item['poi_info']['proportion'])
                    data['poi_info_rank']=str(item['poi_info']['rank'])
                    data['search_pro']=str(item['search']['proportion'])
                    data['search_rank']=str(item['search']['rank'])
                    data['drive_pro']=str(item['drive']['proportion'])
                    data['drive_rank']=str(item['drive']['rank'])
                    data['bus_pro']=str(item['bus']['proportion'])
                    data['bus_rank']=str(item['bus']['rank'])
                    data['group_pro']=str(item['group']['proportion'])
                    data['group_rank']=str(item['group']['rank'])
                    data['collection_pro']=str(item['collection']['proportion'])
                    data['collection_rank']=str(item['collection']['rank'])
                    data['error_pro']=str(item['error']['proportion'])
                    data['error_rank']=str(item['error']['rank'])
                    data['share_pro']=str(item['share']['proportion'])
                    data['share_rank']=str(item['share']['rank'])
                    data['rgn_chg1_rank']='0'
                    data['rgn_chg2_rank']='0'
                    data['pos_hot_pro']='0'
                    data['pos_hot_rank']='0'
                    polygon_dots=item['dots']
                    w.poly(parts=[polygon_dots])
                    w.record(data['id'],data['name'],'','','',data['poi_pro'],data['poi_num_rank'],data['mile_pro'],data['mileage_rank'],data['cap_rank'],data['importance_rank'],data['freshness_rank'],data['prosp_rank'],data['poi_info_pro'],data['poi_info_rank'],data['search_pro'],data['search_rank'],data['drive_pro'],data['drive_rank'],data['bus_pro'],data['bus_rank'],data['group_pro'],data['group_rank'],data['collection_pro'],data['collection_rank'],data['error_pro'],data['error_rank'],data['share_pro'],data['share_rank'],data['rgn_chg1_rank'],data['rgn_chg2_rank'],data['pos_hot_pro'],data['pos_hot_rank'])
                w.save('./shp_data/sel/shp_file.shp')
                os.system('tar cvf ./shp_files/selected_shp_pack.tar ./shp_data/sel')
                f_in=open('./shp_files/selected_shp_pack.tar','r')
                ret_con=f_in.read()
                f_in.close()
                #return self.render_content(ret_con)
                return self.render_gzip(ret_con,'sel_shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'.gz')
            if scale=='city':
                if True==os.path.exists('./shp_files/city_shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'.tar'):
                    f_in=open('./shp_files/city_shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'.tar','r')
                    ret_con=f_in.read()
                    f_in.close()
                    return self.render_gzip(ret_con,'city_shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'.gz')
                w = shapefile.Writer()
                w.autoBalance = 1
                w = shapefile.Writer(shapefile.POLYGON)
                w.field('id','C','40')
                w.field('name','C','40')
                w.field('cj_date','D','10')
                w.field('cj_method','C','20')
                w.field('rgn_level','C','20')
                w.field('poi_pro','C','20')
                w.field('poi_rank','C','20')
                w.field('mile_pro','C','20')
                w.field('mile_rank','C','20')
                w.field('cap_rank','C','20')
                w.field('impt_rank','C','20')
                w.field('fresh_rank','C','20')
                w.field('prosp_rank','C','20')
                w.field('p_info_pro','C','20')
                w.field('p_info_rank','C','20')
                w.field('search_pro','C','20')
                w.field('search_rank','C','20')
                w.field('drive_pro','C','20')
                w.field('drive_rank','C','20')
                w.field('bus_pro','C','20')
                w.field('bus_rank','C','20')
                w.field('group_pro','C','20')
                w.field('group_rank','C','20')
                w.field('clc_pro','C','20')
                w.field('clc_rank','C','20')
                w.field('error_pro','C','20')
                w.field('error_rank','C','20')
                w.field('share_pro','C','20')
                w.field('share_rank','C','20')
                w.field('rgn1_rank','C','20')
                w.field('rgn2_rank','C','20')
                w.field('pos_hot_pro','C','20')
                w.field('pos_hot_rank','C','20')
                for item in posts.find():
                    polygon_dots=[]
                    data={}
                    data['id']=item['_id']
                    data['name']='null'
                    data['poi_pro']=str(item['poi_num']['proportion'])
                    data['poi_num_rank']=(item['poi_num']['rank'])
                    data['mile_pro']=str(item['mileage']['proportion'])
                    data['mileage_rank']=str(item['mileage']['rank'])
                    data['cap_rank']=str(item['poi_cap']['rank'])
                    data['importance_rank']=str(item['importance']['rank'])
                    data['freshness_rank']=str(item['freshness']['rank'])
                    data['prosp_rank']=str(item['prosp']['rank'])
                    data['poi_info_pro']=str(item['poi_info']['proportion'])
                    data['poi_info_rank']=str(item['poi_info']['rank'])
                    data['search_pro']=str(item['search']['proportion'])
                    data['search_rank']=str(item['search']['rank'])
                    data['drive_pro']=str(item['drive']['proportion'])
                    data['drive_rank']=str(item['drive']['rank'])
                    data['bus_pro']=str(item['bus']['proportion'])
                    data['bus_rank']=str(item['bus']['rank'])
                    data['group_pro']=str(item['group']['proportion'])
                    data['group_rank']=str(item['group']['rank'])
                    data['collection_pro']=str(item['collection']['proportion'])
                    data['collection_rank']=str(item['collection']['rank'])
                    data['error_pro']=str(item['error']['proportion'])
                    data['error_rank']=str(item['error']['rank'])
                    data['share_pro']=str(item['share']['proportion'])
                    data['share_rank']=str(item['share']['rank'])
                    data['rgn_chg1_rank']='0'
                    data['rgn_chg2_rank']='0'
                    data['pos_hot_pro']='0'
                    data['pos_hot_rank']='0'
                    polygon_dots=item['dots']
                    w.poly(parts=[polygon_dots])
                    w.record(data['id'],data['name'],'','','',data['poi_pro'],data['poi_num_rank'],data['mile_pro'],data['mileage_rank'],data['cap_rank'],data['importance_rank'],data['freshness_rank'],data['prosp_rank'],data['poi_info_pro'],data['poi_info_rank'],data['search_pro'],data['search_rank'],data['drive_pro'],data['drive_rank'],data['bus_pro'],data['bus_rank'],data['group_pro'],data['group_rank'],data['collection_pro'],data['collection_rank'],data['error_pro'],data['error_rank'],data['share_pro'],data['share_rank'],data['rgn_chg1_rank'],data['rgn_chg2_rank'],data['pos_hot_pro'],data['pos_hot_rank'])
                os.system('rm -f ./shp_data/city/*')
                w.save('./shp_data/city/shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'_file.shp')
                os.system('tar cvf ./shp_files/city_shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'.tar ./shp_data/city')
                f_in=open('./shp_files/city_shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'.tar','r')
                ret_con=f_in.read()
                f_in.close()
                #return self.render_content(ret_con)
                return self.render_gzip(ret_con,'city_shp_'+adcode+'_'+rgnType+'_'+self.date_map[tm]+tm[0]+'.gz')
        if flag=='calc':
            filename=request.values.get("filename")
            if 0==os.system('cp /home/admin/rgn_resume/data/output/'+filename+' /home/admin/rgn_resume/data_server/rgn_server/src -f'):
                pass
            else:
                return self.render_content("Failed:no match file")
            check_running('rgnDataCalc.py','./src/'+filename)
            return self.render_content("Success")

def check_running(param,filename):
    lfile = "./nohup/rgnresumedataprocess_pid"
    os.system('ps -ef | grep %s > %s'%(param,lfile))
    lines = [line for line in file(lfile)]
    isexist=False
    for line in lines:
        if line.find('python %s %s %s'%(param,filename,server_no)) != -1:
            isexist=True
            break
    if isexist==False:
        res=os.system('nohup python %s %s %s > ./nohup/nohup_rgnresumedataprocess 2>&1 &'%(param,filename,server_no))
        if 0==res:
            return True
        else:
            return False
    else:
        return True
            
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

