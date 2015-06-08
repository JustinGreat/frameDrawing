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
from virtual_earth_projection import *

ERROR='0'
RIGHT='1'


FRESHNESS_IP='10.16.41.16'
USER_BEHAVIOR_IP='10.16.41.16'
RANK_IP='10.16.41.16'
PROSP_IP='localhost'
FRM_IP='localhost'
MONGO_PORT=27017
DATA_BASE='cms_resume'
USER='jianchao.jjc'
PASSWD='jianchao.jjc.123'


def InitLog(log_file):
    logger = logging.getLogger(log_file)  
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)  
    file_handler = logging.FileHandler(log_file)  
    file_handler.setFormatter(formatter)  
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    logger.info("Start process data ok!")
    return logger
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
    
MAX_RETRY=2
def GetUrlData(url,param = ""):
    code = ERROR
    content = ""
    count = 0
    while count < MAX_RETRY:
        try:
            if param != "":
                connect = urllib2.urlopen(url,param)
            else:
                connect = urllib2.urlopen(url)
            content = connect.read()
            connect.close()
            code = RIGHT
        except:
            #print url
            sys.stdout.flush()
            traceback.print_exc()
            code = ERROR
            
        count += 1
        if code == RIGHT:
            break
    return code,content
def Start():
    put_time_stamp()
    logger = InitLog('./log/clc_data_process.log')
    try:
        client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,"collect_frame","city_statistic")
    except:
        client_city.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    '''
    try:
        client_poi,db_poi,posts_poi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_poi")
    except:
        client_poi.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    '''
    '''
    try:
        client_pdapoi,db_pdapoi,posts_pdapoi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_pdapoi")
    except:
        client_pdapoi.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    '''
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
    '''
    try:
        client_pdargn,db_pdargn,posts_pdargn = Connect2Mongo("localhost", 27017,"collect_frame","pdaregion_info")
    except:
        client_pdargn.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
     '''
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
    '''
    try:
        client_frm,db_frm,posts_frm = Connect2Mongo(FRM_IP,MONGO_PORT,DATA_BASE,"frm_table")
    except:
        client_frm.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    
    try:
        client_pdafrm,db_pdafrm,posts_pdafrm = Connect2Mongo(FRM_IP,MONGO_PORT,DATA_BASE,"pdafrm_table")
    except:
        client_pdafrm.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    '''
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
    logger.info("connect 2 mongodb success")
    #print 'begin time'
    put_time_stamp()
    #put_pdapoi_2_db(posts_pdapoi,logger)
    #put_pdaframe_2_db(posts_pdafrm,posts_pdapoi,logger)
    #process_frame_2_poiids(posts_frm,logger)
    #put_frame_2_db(posts_frm,logger)
    #put_fanhua_2_db(posts_pr,logger)
    #put_frame_poi_2_db(posts_newfrm,logger)
    #get_region_frame(posts_city,posts_rgn,posts_pr,posts_fr,posts_rk,posts_ur,posts_newfrm,logger,logger_special)
    #get_region_frame(posts_poi,posts_city,posts_newrgn,posts_pr,posts_fr,posts_rk,posts_ur,posts_newfrm,logger,logger_special)
    #put_poi_2_db(posts_poi,logger)
    #test_rank(posts_frm,logger)
    #chg_frm(posts_newrgn,logger)
    #get_rank_value(posts_newrgn,logger)
    for i in range(110000,500000):
        city=str(i)
        filename='aoi/'+city+'_aoi.csv'
        if os.path.exists(filename)==True:
            cal_city(filename,posts_city,posts_pr,posts_fr,posts_rk,posts_ur,posts_tmpfrm,city,logger)
        else:
            continue
    put_time_stamp()
def cal_city(filename,posts_city,posts_pr,posts_fr,posts_rk,posts_ur,posts_tmpfrm,city,logger):
    try:
        client,db,posts = Connect2Mongo(FRM_IP,MONGO_PORT,'city_info',city)
    except:
        client.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    posts.drop()
    posts_tmpfrm.drop()
    put_frame_poi_2_db(filename,posts_tmpfrm,logger)
    #get_region_frame(posts_city,posts_rgn,posts_pr,posts_fr,posts_rk,posts_ur,posts_frm,city,logger)
    test_pois_2_db(filename,posts,logger)
    get_region_frame(posts_city,posts,posts_pr,posts_fr,posts_rk,posts_ur,posts_tmpfrm,city,logger)
    get_rank_value(posts,logger)
def put_time_stamp():
    t=time.time()
    tm=time.localtime(t)
    tm_str=time.strftime("%Y-%m-%d %H:%M:%S",tm)
    print tm_str
def put_fanhua_2_db(posts_pr,logger):
    f_in=open('./fanhua_2qi','r')
    for line in f_in:
        item=line.split('\t')
        data={}
        data['_id']=item[0]
        data['prosp']=float(item[-1].strip('\n'))
        posts_pr.save(data)
def chg_frm(posts_rgn,logger):
    f_in=open('combineresult_finalout.txt','r')
    for line in f_in:
        line_spl=line.split('\t')
        item=posts_rgn.find_one({'_id':line_spl[0]})
        if item==None:
            print line_spl[0]
            continue
        item['frm']=line_spl[1].strip('\n')
        posts_rgn.save(item)
def get_rank_value(posts_rgn,logger):
    s_rank=0
    posts_rgn.create_index([('mileage',-1)])
    acu_mileage=0.0
    for item in posts_rgn.find().sort([('mileage',-1)]):
        s_rank+=1
        acu_mileage+=item['mileage']
        item['acu_mileage']=acu_mileage
        item['mile_rank']=s_rank
        posts_rgn.save(item)
    posts_rgn.drop_indexes()
    s_rank=0
    posts_rgn.create_index([('poi_num',-1)])
    acu_poi_num=0
    for item in posts_rgn.find().sort([('poi_num',-1)]):
        s_rank+=1
        acu_poi_num+=item['poi_num']
        item['acu_poi_num']=acu_poi_num
        item['poi_rank']=s_rank
        posts_rgn.save(item)
    s_rank=0
    posts_rgn.create_index([('importance',-1)])
    for item in posts_rgn.find().sort([('importance',-1)]):
        try:
            if item['importance']>0.1:
                s_rank+=1
        except:
            pass
        item['importance_rank']=s_rank
        posts_rgn.save(item)
    s_rank=0
    posts_rgn.create_index([('prosp',-1)])
    for item in posts_rgn.find().sort([('prosp',-1)]):
        try:
            if item['prosp']>0.1:
                s_rank+=1
        except:
            pass
        item['prosp_rank']=s_rank
        posts_rgn.save(item)
    s_rank=0
    posts_rgn.create_index([('freshness',-1)])
    for item in posts_rgn.find().sort([('freshness',-1)]):
        if item.get('freshness','')=='':
            item['fresh_rank']=s_rank
            posts_rgn.save(item)
        else:
            if item['freshness']!=0:
                s_rank+=1
            item['fresh_rank']=s_rank
            posts_rgn.save(item)
    cont=['bus','drive','search','poi_info','share','error','collection','group']
    lst=[0 for i in range(8)]
    scope=['w','m']
    for c in cont:
        for s in scope:
            for i in range(5):
                '''
                for item in posts_rgn.find():
                    item[c+s+str(i)]=item[c+'_num'][s][i]
                    posts_rgn.save(item)
                '''
                acu_usr=0
                posts_rgn.create_index([(c+s+str(i),-1)])
                s_rank=0
                for item in posts_rgn.find().sort([(c+s+str(i),-1)]):
                    try:
                        if item[c+s+str(i)]!=0:
                            s_rank+=1
                        if s=='m' and i==1:
                            acu_usr+=item[c+s+str(i)]
                            item['acu_'+c]=acu_usr
                        item[c+s+str(i)+"_rank"]=s_rank
                        #del item[c+s+str(i)]
                    except:
                        if s=='m' and i==1:
                            acu_usr+=0
                            item['acu_'+c]=acu_usr
                        item[c+s+str(i)+"_rank"]=s_rank
                    posts_rgn.save(item)
                posts_rgn.drop_indexes()

def put_frame_2_db(posts_frm,logger):
    f_in=open('./combineresult.txt','r')
    frm_data={}
    for line in f_in.readlines():
        item=line.split('\t')
        data={}
        data['_id']=item[0]
        data['lon']=float(item[1])
        data['lat']=float(item[2])
        data['mileage']=float(item[3])
        data['poi_num']=int(item[4])
        data['poi_cap']=float(item[5])
        data['frm']=item[6].strip('\n')
        frm_data[item[0]]=data
    frm_sort=sorted(frm_data.iteritems(),key=lambda d:d[1]['poi_cap'],reverse=True)
    s_index=0
    for item_frm in frm_sort:
        s_index+=1
        data_ins=item_frm[1]
        data_ins['cap_rank']=s_index
        posts_frm.save(data_ins)
def put_frame_poi_2_db(filename,posts,logger):
    f_in=open(filename,'r')
    frm_data={}
    for line in f_in:
        item=line.split('\t')
        data={}
        data['_id']=item[0]
        data['lon']=float(item[2])
        data['lat']=float(item[1])
        data['mileage']=float(item[3])
        data['poi_num']=int(round(float(item[4])*float(item[3])/1000))
        data['poi_cap']=float(item[4])
        data['pois']=[]
        data['frm']=item[5].strip('\n')
        pois=item[6].strip('\n')
        pois_spl=pois.split(';')
        poilst=[]
        if data['poi_num']<1:
            pass
        else:
            for poi in pois_spl:
                try:
                    poi_item=poi.split(',')
                    poi_item[1]=poi_item[1].strip('\n')
                    poilst.append(poi_item)
                except:
                    pass
        data['pois']=poilst
        posts.save(data)
    s_rank=0
    posts.create_index([('poi_cap',-1)])
    for item in posts.find().sort([('poi_cap',-1)]):
        s_rank+=1
        item['cap_rank']=s_rank
        a=item['pois']
        posts.save(item)
def test_pois_2_db(filename,posts,logger):
    count=0
    for item in posts.find():
        count+=1
        if item.get('pois','')=='':
            print item
            print count
        
def put_pdaframe_2_db(posts_pdafrm,posts_pdapoi,logger):
    f_in=open('./PDArgn.csv','r')
    frm_data={}
    for line in f_in.readlines():
        item=line.split('\t')
        data={}
        data['_id']=GetMD5(item[1].strip('\n'))
        poi_item=posts_pdapoi.find_one({"_id":data['_id']})
        if poi_item==None:
            data['poi_num']=0
        else:
            data['poi_num']=poi_item['poi_num']
        data['frm']=item[0].strip('\n')
        dots_str=data['frm'].split('|')
        dots=[]
        for dot in dots_str:
            dots.append([float(dot.split(',')[0]),float(dot.split(',')[1])])
        length=len(dots)
        mileage=0.0
        for i in range(len(dots)):
            mileage+=calcDistanceLl(dots[i][0],dots[i][1],dots[(i+1)%length][0],dots[(i+1)%length][1])
        data['lon']=float(dots[0][0])
        data['lat']=float(dots[0][1])
        data['mileage']=mileage
        data['poi_cap']=float(data['poi_num'])/mileage
        frm_data[item[0]]=data
    frm_sort=sorted(frm_data.iteritems(),key=lambda d:d[1]['poi_cap'],reverse=True)
    s_index=0
    for item_frm in frm_sort:
        s_index+=1
        data_ins=item_frm[1]
        data_ins['cap_rank']=s_index
        posts_pdafrm.save(data_ins)
def put_poi_2_db(posts_poi,logger):
    f_in=open('rgn-pois.csv','r')
    for line in f_in:
        data={}
        poilst=[]
        item1=line.split(':')
        data['_id']=item1[0]
        item2=item1[1].split(';')
        for item2_inner in item2:
            item2_lst=item2_inner.split(',')
            poilst.append(item2_lst)
        data['poilst']=poilst
        posts_poi.save(data)
def put_pdapoi_2_db(posts_pdapoi,logger):
    f_in=open('PDArgn-pois.csv','r')
    for line in f_in:
        data={}
        poilst=[]
        item1=line.split(':')
        data['_id']=GetMD5(item1[0].strip('\n'))
        item2=item1[1].split(';')
        for item2_inner in item2:
            item2_lst=item2_inner.split(',')
            poilst.append(item2_lst)
        data['poilst']=poilst
        data['poi_num']=len(poilst)
        posts_pdapoi.save(data)
def process_frame_2_poiids(posts_frm,logger):
    f_in=open('./combineresult.txt','r')
    f_out=open('./comb.txt','w')
    frm_data={}
    for line in f_in.readlines():
        item=line.split('\t')
        f_out.write('北京')
        f_out.write('\t')
        f_out.write(item[0])
        f_out.write('\t')
        f_out.write(item[6].strip('\n'))
        f_out.write('\n')
def get_city_rank_val_total_2(posts_rk,logger):
    f_in=open('rank_type','r')
    rank_val_total=0
    rank_type_list=[]
    for line in f_in.readlines():
        item=line.strip('\n')
        rank_type_list.append(item)
    for smp in posts_rk.find():
        if smp['rank_type_val'] in rank_type_list:
            rank_val_total+=int(smp['city_rank_type']['total'])
            rank_type_list.remove(smp['rank_type_val'])
            if rank_type_list==[]:
                break
        else:
            continue
    return rank_val_total
def get_city_rank_val_total(posts_frm,posts_rk,logger):
    rank_val_total=0
    for item in posts_frm.find():
        id=item['_id']
        print 1
        smp=posts_rk.find_one(GetMD5(id))
        print 2
        if smp==None:
            #print smp
            continue
        rank_val_total=smp['city']['total']
        #print rank_val_total
        return rank_val_total
        rank_val_total+=int(smp['city_rank_type']['total'])
        print rank_val_total
    return rank_val_total
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
        
def get_region_frame(posts_city,posts_rgn,posts_pr,posts_fr,posts_rk,posts_ur,posts_frm,city,logger):
    city_total_poi_num=0
    city_total_mile_num=0
    city_total_poiinfo_num=0
    city_total_search_num=0
    city_total_bus_num=0
    city_total_drive_num=0
    city_total_group_num=0
    city_total_error_num=0
    city_total_share_num=0
    city_total_collection_num=0
    now=int(time.time())
    nowW=time.localtime(now)
    oneW=time.localtime(now-604800)
    twoW=time.localtime(now-604800*2)
    thrW=time.localtime(now-604800*3)
    forW=time.localtime(now-604800*4)
    nowyw=time.strftime("%Y%U",nowW)
    oneyw=time.strftime("%Y%U",oneW)
    twoyw=time.strftime("%Y%U",twoW)
    thryw=time.strftime("%Y%U",thrW)
    foryw=time.strftime("%Y%U",forW)
    nowym=time.strftime("%Y%m",nowW)
    if nowW[1]-1<1:
        oneym="%4d%02d"%(nowW[0]-1,nowW[1]+11)
    else:
        oneym="%4d%02d"%(nowW[0],nowW[1]-1)
    if nowW[1]-2<1:
        twoym="%4d%02d"%(nowW[0]-1,nowW[1]+10)
    else:
        twoym="%4d%02d"%(nowW[0],nowW[1]-2)
    if nowW[1]-3<1:
        thrym="%4d%02d"%(nowW[0]-1,nowW[1]+9)
    else:
        thrym="%4d%02d"%(nowW[0],nowW[1]-3)
    if nowW[1]-4<1:
        forym="%4d%02d"%(nowW[0]-1,nowW[1]+8)
    else:
        forym="%4d%02d"%(nowW[0],nowW[1]-4)
    scope=['w','m']                             
    cont=['bus','drive','search','poi_info','share','error','collection','group']
    wlst=[nowyw,oneyw,twoyw,thryw,foryw]
    mlst=[nowym,oneym,twoym,thrym,forym]
    print 'get_total'
    put_time_stamp()
    rank_val_total=get_city_rank_val_total(posts_frm,posts_rk,logger)
    put_time_stamp()
    print 'start to deal with rgn'
    put_time_stamp()
    frm_num=posts_frm.count()
    rgn_num=posts_frm.count()
    for item_frm in posts_frm.find().batch_size(2):
        '''
        frm_str=item_frm['frm']
        print frm_str
        url='http://10.16.29.116:2015/poi_correct_server/page/AutoRectangle.jsp?city=北京&flag=zone&poiflag=1'
        print 'get poiids'
        put_time_stamp()
        url_ct={}
        url_ct['zone']=frm_str
        code,result=GetUrlData(url,url_ct)
        if code == ERROR:
            logger.error('Visiting Poi Rgn server failed.')
            return ERROR
        put_time_stamp()
        rst_json=json.loads(result)
        rst_str=rst_json['result']['poiids']
        poiids=rst_str.split(',')
        '''
        print 'begin:'
        data={}
        data['_id']=item_frm['_id']
        data['lon']=float(item_frm['lon'])
        data['lat']=float(item_frm['lat'])
        data['poi_num']=item_frm['poi_num']
        data['poi_cap']=item_frm['poi_cap']
        data['cap_rank']=item_frm['cap_rank']
        data['mileage']=item_frm['mileage']
        data['frm']=item_frm['frm']
        poilst=item_frm['pois']
        #item_poi=posts_poi.find_one({'_id':item_frm['_id']})
        rank_val=get_rgn_rank_val(poilst,rank_val_total,posts_rk,logger)
        
        
        data['importance']=0.0
        data['prosp']=0.0
        data['freshness']=0.0
        data['poi_info_num']={}
        data['poi_info_num']['w']=[0,0,0,0,0]
        data['poi_info_num']['m']=[0,0,0,0,0]
        data['bus_num']={}
        data['bus_num']['w']=[0,0,0,0,0]
        data['bus_num']['m']=[0,0,0,0,0]
        data['drive_num']={}
        data['drive_num']['w']=[0,0,0,0,0]
        data['drive_num']['m']=[0,0,0,0,0]
        data['error_num']={}
        data['error_num']['w']=[0,0,0,0,0]
        data['error_num']['m']=[0,0,0,0,0]
        data['share_num']={}
        data['share_num']['w']=[0,0,0,0,0]
        data['share_num']['m']=[0,0,0,0,0]
        data['collection_num']={}
        data['collection_num']['w']=[0,0,0,0,0]
        data['collection_num']['m']=[0,0,0,0,0]
        data['group_num']={}
        data['group_num']['w']=[0,0,0,0,0]
        data['group_num']['m']=[0,0,0,0,0]
        data['search_num']={}
        data['search_num']['w']=[0,0,0,0,0]
        data['search_num']['m']=[0,0,0,0,0]
        city_total_mile_num+=data['mileage']
        if poilst==[]:
            cont=['bus','drive','search','poi_info','share','error','collection','group']
            scope=['w','m']
            for c in cont:
                for s in scope:
                    for i in range(5):
                        data[c+s+str(i)]=0
            data['info']={}
            data['info']['classify']={}
            data['info']['freshness']=0.0
            data['info']['prosp']=0.0
            data['info']['importance']=0.0
            data['info']['cap_rank']=(1-(float(item_frm['cap_rank'])-1)/rgn_num)*100
            data['city_rank_val_total']=0
            posts_rgn.save(data)
            continue
        rgn_lvli={}
        rgn_lvli['rank']=rank_val
        rgn_lvli['classify']={}
        rgn_lvli['cap_rank']=(1-(float(item_frm['cap_rank'])-1)/rgn_num)*100
        print 'Get poiinfos'
        put_time_stamp()
        fr_count={}
        for poi_item in poilst:
            poi_id=poi_item[0]
            try:
                poi_type=poi_item[1].split('|')[0]
            except:
                continue
            type=poi_type
            key=GetMD5(poi_id)
            item_fr=posts_fr.find_one({'_id':key})
            item_rk=posts_rk.find_one({'_id':key})
            if city_total_poi_num==0 and item_rk!=None:
                city_total_poi_num=item_rk['city']['total']
            db_slt=['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']
            item_ur=posts_ur[db_slt.index(key[0])].find_one({'_id':key})
            item_pr=posts_pr.find_one({'_id':poi_id})
            '''
            if not type in city_stc['typedic']:
                city_stc['typedic'][type]={}
                city_stc['typedic'][type]['city_type_total']=item_rk['city_type']['total']
                city_stc['typedic'][type]['all_type_total']=item_rk['type']['total']
            '''
            if rgn_lvli['classify'].get(type,'')=='':
                rgn_lvli['classify'][type]={}
                rgn_lvli['classify'][type]['list']=[]
                fr_count[type]=0
                if item_fr==None:
                    rgn_lvli['classify'][type]['freshness']=0.0
                    fr_count[type]+=1
                else:
                    rgn_lvli['classify'][type]['freshness']=float(item_fr['freshness'])
                rgn_lvli['classify'][type]['list'].append(poi_item)
                try:
                    rgn_lvli['classify'][type]['total_num']=item_rk['city_type']['total']
                    rgn_lvli['classify'][type]['city_total']=item_rk['city']['total']
                except:
                    pass
                #rgn_lvli['classify'][type]['rank']=(1-(item_rk['city_type']['inx']-1)/item_rk['city_type']['total'])
                if item_pr==None:
                    rgn_lvli['classify'][type]['prosp']=0.0
                else:
                    rgn_lvli['classify'][type]['prosp']=item_pr['prosp']
                rgn_lvli['classify'][type]['usr_bhr']={}
                rgn_lvli['classify'][type]['usr_bhr']['w']={}
                rgn_lvli['classify'][type]['usr_bhr']['m']={}
                for c in cont:
                    rgn_lvli['classify'][type]['usr_bhr']['w'][c]=[[0,''],[0,''],[0,''],[0,''],[0,'']]
                    for w in wlst:
                        if item_ur==None or item_ur['w'].get(c,'')=='' or item_ur['w'][c].get(w,'')=='':
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c][wlst.index(w)]=[0,w[-2:]]
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c][wlst.index(w)][0]=float(item_ur['w'][c][w]['pv'])
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c][wlst.index(w)][1]=w[-2:]
                    rgn_lvli['classify'][type]['usr_bhr']['m'][c]=[[0,''],[0,''],[0,''],[0,''],[0,'']]
                    for m in mlst:   
                        if item_ur==None or item_ur['m'].get(c,'')=='' or item_ur['m'][c].get(m,'')=='':
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)]=[0,m[-2:]]
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)][0]=float(item_ur['m'][c][m]['pv'])
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)][1]=m[-2:0]
            else:
                rgn_lvli['classify'][type]['list'].append(poi_item)
                try:
                    rgn_lvli['classify'][type]['total_num']=item_rk['city_type']['total']
                    rgn_lvli['classify'][type]['city_total']=item_rk['city']['total']
                except:
                    pass
                if item_fr==None:
                    rgn_lvli['classify'][type]['freshness']+=0.0
                    fr_count[type]+=1
                else:
                    rgn_lvli['classify'][type]['freshness']+=float(item_fr['freshness'])
                if item_pr==None:
                    pass
                else:
                    rgn_lvli['classify'][type]['prosp']+=float(item_pr['prosp'])
                if item_ur==None:
                    continue
                for c in cont:
                    for w in wlst:   
                        if item_ur['w'].get(c,'')=='' or item_ur['w'][c].get(w,'')=='' :
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c][wlst.index(w)][1]=w[-2:0]
                            continue
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c][wlst.index(w)][0]+=float(item_ur['w'][c][w]['pv'])
                    for m in mlst:
                        if item_ur['m'].get(c,'')=='' or item_ur['m'][c].get(m,'')=='' :
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)][1]=m[-2:0]
                            continue
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)][0]+=float(item_ur['m'][c][m]['pv'])
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)][1]=m[-2:0]
            if item_ur==None or item_ur['m'].get('poi_info','')=='' or item_ur['m']['poi_info'].get(oneym,'')=='':
                city_total_poiinfo_num+=0
            else:
                city_total_poiinfo_num+=int(item_ur['m']['poi_info'][oneym]['pv'])
            if item_ur==None or item_ur['m'].get('search','')=='' or item_ur['m']['search'].get(oneym,'')=='':
                city_total_search_num+=0
            else:
                city_total_search_num+=int(item_ur['m']['search'][oneym]['pv'])
            if item_ur==None or item_ur['m'].get('bus','')=='' or item_ur['m']['bus'].get(oneym,'')=='':
                city_total_bus_num+=0
            else:
                city_total_bus_num+=int(item_ur['m']['bus'][oneym]['pv'])
            if item_ur==None or item_ur['m'].get('drive','')=='' or item_ur['m']['drive'].get(oneym,'')=='':
                city_total_drive_num+=0
            else:
                city_total_drive_num+=int(item_ur['m']['drive'][oneym]['pv'])
            if item_ur==None or item_ur['m'].get('collection','')=='' or item_ur['m']['collection'].get(oneym,'')=='':
                city_total_collection_num+=0
            else:
                city_total_collection_num+=int(item_ur['m']['collection'][oneym]['pv'])
            if item_ur==None or item_ur['m'].get('group','')=='' or item_ur['m']['group'].get(oneym,'')=='':
                city_total_group_num+=0
            else:
                city_total_group_num+=int(item_ur['m']['group'][oneym]['pv'])
            if item_ur==None or item_ur['m'].get('share','')=='' or item_ur['m']['share'].get(oneym,'')=='':
                city_total_share_num+=0
            else:
                city_total_share_num+=int(item_ur['m']['share'][oneym]['pv'])
            if item_ur==None or item_ur['m'].get('error','')=='' or item_ur['m']['error'].get(oneym,'')=='':
                city_total_error_num+=0
            else:
                city_total_error_num+=int(item_ur['m']['error'][oneym]['pv'])
        Q=0.0
        for type in rgn_lvli['classify']:
            for m in mlst:
                data['poi_info_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['poi_info'][mlst.index(m)][0])
                data['bus_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['bus'][mlst.index(m)][0])
                data['drive_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['drive'][mlst.index(m)][0])
                data['search_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['search'][mlst.index(m)][0])
                data['group_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['group'][mlst.index(m)][0])
                data['share_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['share'][mlst.index(m)][0])
                data['error_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['error'][mlst.index(m)][0])
                data['collection_num']['m'][mlst.index(m)]+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['collection'][mlst.index(m)][0])
            for w in wlst:
                data['poi_info_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['poi_info'][wlst.index(w)][0])
                data['bus_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['bus'][wlst.index(w)][0])
                data['drive_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['drive'][wlst.index(w)][0])
                data['search_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['search'][wlst.index(w)][0])
                data['group_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['group'][wlst.index(w)][0])
                data['share_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['share'][wlst.index(w)][0])
                data['error_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['error'][wlst.index(w)][0])
                data['collection_num']['w'][wlst.index(w)]+=int(rgn_lvli['classify'][type]['usr_bhr']['w']['collection'][wlst.index(w)][0])
            if len(rgn_lvli['classify'][type]['list'])-fr_count[type]<0.1:
                rgn_lvli['classify'][type]['freshness']=0.0
            else:
                rgn_lvli['classify'][type]['freshness']/=(len(rgn_lvli['classify'][type]['list'])-fr_count[type])
            rgn_lvli['classify'][type]['prosp']/=len(rgn_lvli['classify'][type]['list'])
            for c in cont:
                for w in wlst:   
                    index=wlst.index(w)
                    rgn_lvli['classify'][type]['usr_bhr']['w'][c][index][0]/=len(rgn_lvli['classify'][type]['list'])
                for m in mlst:
                    index=mlst.index(m)
                    rgn_lvli['classify'][type]['usr_bhr']['m'][c][index][0]/=len(rgn_lvli['classify'][type]['list'])  
            try:
                Q+=len(rgn_lvli['classify'][type]['list'])/float(data['poi_num'])/(float(rgn_lvli['classify'][type]['total_num'])/float(rgn_lvli['classify'][type]['city_total']))
            except:
                Q+=0.0
        rgn_lvli['freshness']=0.0
        rgn_lvli['prosp']=0.0
        for type in rgn_lvli['classify']:
            try:
                tmp=(len(rgn_lvli['classify'][type]['list'])/float(data['poi_num'])/(float(rgn_lvli['classify'][type]['total_num'])/rgn_lvli['classify'][type]['city_total']))/Q
            except:
                tmp=0
            rgn_lvli['freshness']+=rgn_lvli['classify'][type]['freshness']*tmp
            rgn_lvli['prosp']+=rgn_lvli['classify'][type]['prosp']*tmp
        rgn_lvli['importance']=0.7*rgn_lvli['rank']+0.2*rgn_lvli['cap_rank']+0.1*rgn_lvli['prosp']
        #logger_special.info('id:%s   rank:%f    intense_rank:%f    prosp:%f    '%(data['_id'],rgn_lvli['rank'],rgn_lvli['cap_rank'],rgn_lvli['prosp']))
        rgn_lvli['city_rank_val_total']=rank_val_total
        data['info']=rgn_lvli
        data['importance']=rgn_lvli['importance']
        data['prosp']=rgn_lvli['prosp']
        data['freshness']=rgn_lvli['freshness']
        cont=['bus','drive','search','poi_info','share','error','collection','group']
        scope=['w','m']
        for c in cont:
            for s in scope:
                for i in range(5):
                    data[c+s+str(i)]=data[c+'_num'][s][i]
        print data
        posts_rgn.save(data)
        put_time_stamp()
    city_data={}
    city_data['_id']=city
    city_data['total_poi']=city_total_poi_num
    city_data['total_mile']=city_total_mile_num
    city_data['total_poiinfo']=city_total_poiinfo_num
    city_data['total_search']=city_total_search_num
    city_data['total_bus']=city_total_bus_num
    city_data['total_drive']=city_total_drive_num
    city_data['total_error']=city_total_error_num
    city_data['total_group']=city_total_group_num
    city_data['total_collection']=city_total_collection_num
    city_data['total_share']=city_total_share_num
    posts_city.save(city_data)
    logger.info('city:%s'%city)
    
    '''
    city_fresh=0.0
    city_poi_num=0
    city_mile=0
    for item in posts_rgn.find():
        rgn_fresh=0.0
        city_poi_num+=item['poi_num']
        city_mile+=item['mileage']
        for type in item['info']['classify']:
            tmp_fresh=item['info']['classify'][type]['freshness']
            tmp_fresh*=len(item['info']['classify'][type]['list'])
            rgn_fresh+=tmp_fresh
        city_fresh+=rgn_fresh
    city_stc['city_fresh']=city_fresh
    city_stc['city_mile']=city_mile
    city_stc['poi_num']=city_poi_num
    adcode='010000'
    city_stc['_id']='010000'
    posts_city.save(city_stc)
    '''
        
if __name__ == '__main__':
    Start()
    