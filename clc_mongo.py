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
    
def Test1():
    client,db,posts = Connect2Mongo("localhost",27017,'cms_resume','cms_poi_prosp')
    print posts.count()
    count1=0
    count2=0
    client2,db2,posts2 = Connect2Mongo("localhost",27017,'cms_resume','frm_table')
    print posts2.count()
    client3,db3,posts3 = Connect2Mongo("localhost",27017,"collect_frame","region_info")
    print posts3.count()
    client_poi,db_poi,posts_poi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_poi")
    print posts_poi.count()
    client_pdafrm,db_pdafrm,posts_pdafrm = Connect2Mongo("localhost",27017,'cms_resume',"pdafrm_table")
    print posts_pdafrm.count()
    client_pdapoi,db_pdapoi,posts_pdapoi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_pdapoi")
    print posts_pdapoi.count()
    client_pdargn,db_pdargn,posts_pdargn = Connect2Mongo("localhost", 27017,"collect_frame","pdaregion_info")
    print posts_pdargn.count()
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
    Test1()
    #TestMongo()         
    Test2()
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










