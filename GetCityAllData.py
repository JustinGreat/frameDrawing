
#encoding=utf-8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import urllib
import urllib2
import logging
import time
import json
import base64
import hashlib
import traceback
from pymongo import MongoClient
import math
import struct
import datetime
import shapefile 
import gzip
from virtual_earth_projection import *
FRESHNESS_IP='10.16.41.16'
USER_BEHAVIOR_IP='10.16.41.16'
RANK_IP='10.16.41.16'
PROSP_IP='localhost'
FRM_IP='localhost'
MONGO_PORT=27017
DATA_BASE='cms_resume'
USER='jianchao.jjc'
PASSWD='jianchao.jjc.123'
def GetMD5(url):
    mo = hashlib.new("md5", url.encode())
    mv = mo.hexdigest()
    return mv.lower()
def Connect2Mongo(ip,port,db,table,user="",pwd=""):
    client = MongoClient(ip,port)
    db = client[db]
    if user and pwd:
        db.authenticate(user,pwd)
    posts = db[table]
    return client,db,posts 
def Start():
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
    db_slt=['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']
    client_ur=[0 for i in range(16)]
    db_ur=[0 for i in range(16)]
    posts_ur=[0 for i in range(16)]
    try:
        client_fr,db_fr,posts_fr = Connect2Mongo(FRESHNESS_IP,MONGO_PORT,DATA_BASE,"cms_poi_freshness",USER,PASSWD)
    except:
        client_fr.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    for i in range(16):
        try:
            TABLE="cms_poi_pv_level_"+db_slt[i]
            client_ur[i],db_ur[i],posts_ur[i] = Connect2Mongo(USER_BEHAVIOR_IP,MONGO_PORT,DATA_BASE,TABLE,USER,PASSWD)
        except:
            client_ur[i].disconnect()
            logger.error("connect 2 mongodb failed.")
            return
    try:
        client_rk,db_rk,posts_rk = Connect2Mongo(RANK_IP,MONGO_PORT,DATA_BASE,"cms_poi_rank",USER,PASSWD)
    except:
        client_rk.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_pr,db_pr,posts_pr = Connect2Mongo(PROSP_IP,MONGO_PORT,DATA_BASE,"cms_poi_prosp")
    except:
        client_pr.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_newfrm,db_newfrm,posts_newfrm = Connect2Mongo(FRM_IP,MONGO_PORT,'formal',"newfrm_table")
    except:
        client_newfrm.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_tmpfrm,db_tmpfrm,posts_tmpfrm = Connect2Mongo(FRM_IP,MONGO_PORT,'temp',"tmpfrm_table")
    except:
        client_tmpfrm.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_newrgn1,db_newrgn1,posts_newrgn1 = Connect2Mongo(FRM_IP,MONGO_PORT,"collect_frame","newregion1_info")
    except:
        client_newrgn1.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    cpr_city_data(posts_newrgn,posts_city,'beijing','bj_old')
    cpr_city_data(posts_newrgn1,posts_city,'beijing_new','bj_new')
    
def cpr_city_data(posts,posts_city,city,filename):
    data=[]
    city_data=posts_city.find_one({'_id':city})
    frm_num=posts.count()
    for frm in posts.find():
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
        data_rgn['freshness']['val']=frm['freshness']
        data_rgn['freshness']['rank']=frm['fresh_rank']
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
        data_rgn['rgn_chg']={}
        if frm.get('rgn_chg','')=='':
            data_rgn['rgn_chg']['val']=0
            data_rgn['rgn_chg']['rank']=0
        else:
            data_rgn['rgn_chg']['val']=frm['rgn_chg']
            data_rgn['rgn_chg']['rank']=frm['rgn_chg_rank']
            data_rgn['rgn_chg']['hot']=frm['rgn_chg_hot']
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
        data.append(data_rgn)
    data_json=json.dumps(data,ensure_ascii=False).encode('utf-8','ignore')
    f_out=open(filename,'w')
    f_out.write(data_json)
    '''
    g=gzip.GzipFile(filename, mode='wb',fileobj=open(filename+'.gz','wb'))
    g.write(data_json)
    g.close()
    '''

if __name__ == '__main__':
    Start()