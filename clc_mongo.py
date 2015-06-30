#encoding=utf-8
import json
#encoding=utf-8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import urllib
import urllib2
import logging
#import rsa
import time
import json
import base64
import hashlib
import traceback
from pymongo import MongoClient
import math
import struct;
import datetime
#cate_map = GetCateMap()
def Connect2Mongo(ip,port,db,table,user="",pwd=""):
    client = MongoClient(ip,port)
    db = client[db]
    if user and pwd:
        db.authenticate(user,pwd)
    posts = db[table]

    return client,db,posts 

def AddData2Mongo():
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","raw_frame")
    f_in=open("region_level_010",'r')

    for line in f_in.readlines():
        item=json.loads(line)
        json_data={}
        json_data['_id']=item[0][0]
        json_data['lon']=item[1][0]
        json_data['lat']=item[1][1]
        json_data['data']=[]
        json_data['data'].append(item[0])
        json_data['data'].append(item[1])
        json_data['data'].append([item[2]])
        posts.save(json_data)
    print posts.count()
    client.disconnect()
    
def AddDataPoi2Mongo():
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","raw_poi")
    f_in=open("region_level1_poi_relation",'r')
    for line in f_in.readlines():
        pos=line.find(":")
        json_data={}
        json_data['_id']=line[:pos]
        json_data['poi']=[]
        if pos==len(line)-2:
            posts.save(json_data)
            continue
        while pos<len(line)-1:
            json_data['poi'].append(line[pos+1:pos+11])
            pos+=11
        posts.save(json_data)
    print posts.count()
    client.disconnect()
def AddData2MongoTest():
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","raw_frame_test")
    f_in=open("region_level_1_test",'r')

    for line in f_in.readlines():
        item=json.loads(line)
        json_data={}
        json_data['_id']=item[0][0]
        json_data['lon']=item[1][0]
        json_data['lat']=item[1][1]
        json_data['data']=[]
        json_data['data'].append(item[0])
        json_data['data'].append(item[1])
        json_data['data'].append([item[2]])
        posts.save(json_data)
    print posts.count()
    client.disconnect()
    
def AddData2Mongo2(table):
    client,db,posts = Connect2Mongo("localhost",27017,"alio2o",table)
    lines = [line.strip("\n") for line in file("./data/ali_test/zzxx_data_2000")]
    #lines = [line.strip("\n") for line in file("./data/data_failed")]
    for line in lines:
        try:
            line = line.decode("utf-8","ignore")
            json_data = json.loads(line)
            #if str(json_data["cpid"]) != "9697293":
            #   continue
            #json_data = json.loads(line.decode("gb18030"))
            json_data["_id"] = json_data["cpid"] + json_data["prePareId"]
        except:
            traceback.print_exc()
            print "data err:\t" + line
            continue
        id = posts.save(json_data)
    print len(lines)
    posts = db[table]
    print posts.count()
    client.disconnect()

def GetDataFromMongo():
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","raw_frame_test")
    print posts.count()
    for item in posts.find():
        #del item["_id"]
        json_str = json.dumps(item,ensure_ascii=False)
        print json_str.encode("gb18030")
    client.disconnect()

def GetDataFromMongoCount(postname):
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame",postname)
    print posts.count()
    client.disconnect()

def ClearMongoDB(postname):
    try:
        client,db,posts = Connect2Mongo("localhost",27017,"drawFrame",postname)
        posts.drop()
        client.disconnect()
    except:
        print "111"

def TestMongo():
    client = MongoClient("localhost", 27017)
    db = client["alio2o"]
    #posts = db["metaq_fail"]
    posts = db["worklist_inconsist"]
    #post = json.loads("{"method":"add","poiid":"0000000002"}")
    #post_id = posts.insert(post)
    #print post_id
    #posts.remove({"poiid": "0000000002"})
    print posts.count()
    for item in posts.find():
        print item
        print item["poiid"]
    client.disconnect()

def RemoveData():
    client,db,posts = Connect2Mongo("localhost",27017,"alio2o","unprocess")
    posts.remove({"cpid":"67177004"})
    print posts.count()
    client.disconnect()
    
FRESHNESS_IP='10.16.41.16'
USER_BEHAVIOR_IP='10.16.41.16'
RANK_IP='10.16.41.16'
PROSP_IP='localhost'
FRM_IP='localhost'
MONGO_PORT=27017
DATA_BASE='cms_resume'
USER='jianchao.jjc'
PASSWD='jianchao.jjc.123'
db_slt=['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']
client_ur_local=[0 for i in range(16)]
db_ur_local=[0 for i in range(16)]
posts_ur_local=[0 for i in range(16)]
def GetMD5(url):
    mo = hashlib.new("md5", url.encode())
    mv = mo.hexdigest()
    return mv.lower()
def Test1(n):
    client,db,posts = Connect2Mongo("localhost",27017,'cms_resume','cms_poi_prosp')
    print 'fanhua:%d'%posts.count()
    client2,db2,posts2 = Connect2Mongo("localhost",27017,'cms_resume','frm_table')
    print 'raw frame:%d'%posts2.count()
    client3,db3,posts3 = Connect2Mongo("localhost",27017,"collect_frame","region_info")
    print 'region :%d'%posts3.count()
    '''
    count=0
    for i in posts3.find():
        if count==3:
            print i
        count+=1
    '''
    client_poi,db_poi,posts_poi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_poi")
    print 'region with poi:%d'%posts_poi.count()
    client_pdafrm,db_pdafrm,posts_pdafrm = Connect2Mongo("localhost",27017,'cms_resume',"pdafrm_table")
    print 'pda frame:%d'%posts_pdafrm.count()
    client_pdapoi,db_pdapoi,posts_pdapoi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_pdapoi")
    print 'pda regions with pois: %d'%posts_pdapoi.count()
    client_pdargn,db_pdargn,posts_pdargn = Connect2Mongo("localhost", 27017,"collect_frame","pdaregion_info")
    print 'pda regions:%d'%posts_pdargn.count()
    client_rk,db_rk,posts_rk = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,"cms_poi_rank")
    print 'local_rank:%d'%posts_rk.count()
    client_fr,db_fr,posts_fr = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,"cms_poi_freshness")
    print 'local_freshness:%d'%posts_fr.count()
    for i in range(16):
        try:
            TABLE="cms_poi_pv_level_"+db_slt[i]
            client_ur_local[i],db_ur_local[i],posts_ur_local[i] = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,TABLE)
            print posts_ur_local[i].count()
        except:
            client_ur_local[i].disconnect()
            logger.error("connect 2 mongodb failed.")
            return
    '''
    client_cityur,db_cityur,posts_cityur = Connect2Mongo(USER_BEHAVIOR_IP,MONGO_PORT,DATA_BASE,"pv_stat_city",USER,PASSWD)
    print 'city user behavior:%d'%posts_cityur.count()
    client_cityur_l,db_cityur_l,posts_cityur_l = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,"pv_stat_city")
    for item in posts_cityur.find():
        posts_cityur_l.save(item)
    '''
    client_cityur_l,db_cityur_l,posts_cityur_l = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,"pv_stat_city")
    print 'city user behavior:%d'%posts_cityur_l.count()
    client_prov,db_prov,posts_prov = Connect2Mongo('localhost',27017,'nation',"prov_data")
    print 'province :%d'%posts_prov.count()
    client_city,db_city,posts_city = Connect2Mongo('localhost',27017,'nation',"city_data")
    print 'city :%d'%posts_city.count()
    client_newfrm,db_newfrm,posts_newfrm = Connect2Mongo(FRM_IP,MONGO_PORT,'formal',"newfrm_table")
    print 'newfrm:%d'%posts_newfrm.count()
    client_newrgn,db_newrgn,posts_newrgn = Connect2Mongo("localhost", 27017,"collect_frame","newregion_info")
    print 'newrgn:%d'%posts_newrgn.count()
    client_newrgn1,db_newrgn1,posts_newrgn1 = Connect2Mongo(FRM_IP,MONGO_PORT,"collect_frame","newregion1_info")
    print 'newrgn1:%d'%posts_newrgn1.count()
    client_newrgn2,db_newrgn2,posts_newrgn2 = Connect2Mongo(FRM_IP,MONGO_PORT,"collect_frame","newregion2_info")
    print 'newrgn2:%d'%posts_newrgn2.count()
    client1,db1,posts1 = Connect2Mongo(FRM_IP,MONGO_PORT,'city_info','110000')
    print '110000:%d'%posts1.count()
    client2,db2,posts2 = Connect2Mongo(FRM_IP,MONGO_PORT,'city_info','120000')
    print '120000:%d'%posts2.count()
    rgnTypeList=['roadgrid']
    cityList=['110000','310000','610100']
    for rgnType in rgnTypeList:
        for city in cityList:
            client,db,posts = Connect2Mongo(FRM_IP,MONGO_PORT,rgnType,city)
            print rgnType+'('+city+'):%d'%posts.count()
            client_bak,db_bak,posts_bak = Connect2Mongo(FRM_IP,MONGO_PORT,rgnType+'_bak',city)
            print rgnType+'_bak('+city+'):%d'%posts_bak.count()
    count=0
    while True:
        clc='tmp_frm_'+str(count)
        client_tmpfrm,db_tmpfrm,posts_tmpfrm = Connect2Mongo('localhost',MONGO_PORT,'tmp',clc)
        if posts_tmpfrm.count()==0:
            break
        else:
            if n=='1':
                posts_tmpfrm.drop()
            print 'tmp_frm_%d:%d'%(count,posts_tmpfrm.count())
            count+=1
def Test1_t():
    AddData2MongoTest()
def Test2():
    try:
        client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,"collect_frame","city_statistic")
    except:
        client_city.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    '''
    city_data={}
    city_data['_id']='beijing'
    city_data['total_poi']=1200000
    city_data['total_mile']=40000000
    city_data['total_poiinfo']=12000000
    city_data['total_search']=20000000
    city_data['total_bus']=30000000
    city_data['total_drive']=10000000
    posts_city.save(city_data)
    '''
    print posts_city.count()
    for i in posts_city.find():
        print i

def Test3():
    #ClearMongoDB("worklist_unprocess")
    #GetDataFromMongoCount('raw_frame')
    #GetDataFromMongoCount('bj_frame')
    #GetDataFromMongoCount('raw_frame_test')
    #GetDataFromMongoCount('bj_frame_test')
    #GetDataFromMongoCount('raw_poi')
    GetDataFromMongo()
    
def dist((x1,y1),(x2,y2)):
    return ((x2-x1)**2+(y2-y1)**2)**0.5
def Test4():
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","bj_frame_test")
    print posts.count()
    max_cnt=0.0
    cnt=[0 for i in range(5)]
    for line in posts.find():
        line_json=line['data']
        circum=0.0
        for j in range(len(line_json[2])):
            for i in range(len(line_json[2][j])):
                circum+=dist((line_json[2][j][i%len(line_json[2][j])][0],line_json[2][j][i%len(line_json[2][j])][1]),(line_json[2][j][i%len(line_json[2][j])][2],line_json[2][j][i%len(line_json[2][j])][3]))
        if circum<0.03:
            cnt[0]+=1
        elif circum<0.08:
            cnt[1]+=1
        elif circum<0.10:
            cnt[2]+=1
        elif circum<0.13:
            cnt[3]+=1
        else:
            cnt[4]+=1
        if max_cnt < circum:
            max_cnt=circum
    print cnt
    print max_cnt
    client.disconnect()
def Test5():
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","raw_poi")
    posts.drop()
    client.disconnect()
    AddDataPoi2Mongo()
    
def Test6():
    client,db,posts = Connect2Mongo("localhost",27017,"drawFrame","raw_poi")
    item=posts.find_one({'_id':'375523459130842503'})
    print item

    
def TestClear():
    ClearMongoDB("raw_frame_test")
    ClearMongoDB("bj_frame_test")

def TestCell():
    ClearMongoDB()
    GetDataFromMongo("poi_cell")

def TestGet():
    client,db,posts = Connect2Mongo("localhost",27017,"alio2o","unprocess")
    item = posts.find({"cpid":"68127170"})
    json_str = json.dumps(item[0],ensure_ascii=False)
    print json_str.encode('gb18030','ignore')
    

if __name__ == "__main__":
    #TestCell()
    #TestClear()
    print sys.argv
    if len(sys.argv)==2:
        Test1(sys.argv[1])
    else:
        Test1('0')
    #TestMongo()         
    #Test2()
    #Test3()
    #Test4()
    #Test5()
    #Test6()
    #Test7()
    #Test8()
    #Test9()
    #TestGet()
    #Test10()
    #TestYuntu()
    #ClearMongoDB("cloudmap","cloudmap_deep")










