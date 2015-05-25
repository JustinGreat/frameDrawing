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


FRESHNESS_IP='10.16.18.110'
USER_BEHAVIOR_IP='10.16.24.68'
RANK_IP='10.16.24.68'
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
            print url
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
    logger_special=InitLog('./log/clc_data_tmp.log')
    try:
        client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,"collect_frame","city_statistic")
    except:
        client_city.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_poi,db_poi,posts_poi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_poi")
    except:
        client_poi.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,"collect_frame","city_statistic")
    except:
        client_city.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_pdapoi,db_pdapoi,posts_pdapoi = Connect2Mongo("localhost", 27017,"collect_frame","rgn_pdapoi")
    except:
        client_pdapoi.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_pdargn,db_pdargn,posts_pdargn = Connect2Mongo("localhost", 27017,"collect_frame","pdaregion_info")
    except:
        client_pdargn.disconnect()
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
    logger.info("connect 2 mongodb success")
    print 'begin time'
    put_time_stamp()
    #put_pdapoi_2_db(posts_pdapoi,logger)
    #put_pdaframe_2_db(posts_pdafrm,posts_pdapoi,logger)
    #process_frame_2_poiids(posts_frm,logger)
    #put_frame_2_db(posts_frm,logger)
    #put_fanhua_2_db(posts_pr,logger)
    #get_region_frame(posts_poi,posts_city,posts_rgn,posts_pr,posts_fr,posts_rk,posts_ur,posts_frm,logger,logger_special)
    get_region_frame(posts_pdapoi,posts_city,posts_pdargn,posts_pr,posts_fr,posts_rk,posts_ur,posts_pdafrm,logger,logger_special)
    #put_poi_2_db(posts_poi,logger)
    #test_rank(posts_frm,logger)
    get_rank_value(posts_pdargn,logger)
    put_time_stamp()
    
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
def get_rank_value(posts_rgn,logger):
    s_rank=0
    posts_rgn.create_index([('poi_num',-1)])
    for item in posts_rgn.find().sort([('poi_num',-1)]):
        if item['poi_num']!=0:
            s_rank+=1
        item['poi_rank']=s_rank
        posts_rgn.save(item)
    s_rank=0
    posts_rgn.create_index([('mileage',-1)])
    for item in posts_rgn.find().sort([('mileage',-1)]):
        s_rank+=1
        item['mile_rank']=s_rank
        posts_rgn.save(item)
        if s_rank==3:
            print item
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
    posts_rgn.create_index([('poiinfo_num',-1)])
    for item in posts_rgn.find().sort([('poiinfo_num',-1)]):
        if item['poiinfo_num']!=0:
            s_rank+=1
        item['poiinfo_rank']=s_rank
        posts_rgn.save(item)
    s_rank=0
    posts_rgn.create_index([('bus_num',-1)])
    for item in posts_rgn.find().sort([('bus_num',-1)]):
        if item['bus_num']!=0:
            s_rank+=1
        item['bus_rank']=s_rank
        posts_rgn.save(item)
    s_rank=0
    posts_rgn.create_index([('drive_num',-1)])
    for item in posts_rgn.find().sort([('drive_num',-1)]):
        if item['drive_num']!=0:
            s_rank+=1
        item['drive_rank']=s_rank
        posts_rgn.save(item)
    s_rank=0
    posts_rgn.create_index([('search_num',-1)])
    for item in posts_rgn.find().sort([('search_num',-1)]):
        if item['search_num']!=0:
            s_rank+=1
        item['search_rank']=s_rank
        posts_rgn.save(item)

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
def get_city_rank_val_total(posts_rk,logger):
    f_in=open('rank_type','r')
    rank_val_total=0
    rank_type_list=[]
    for line in f_in.readlines():
        item=line.strip('\n')
        rank_type_list.append(item)
        smp=posts_rk.find_one({'rank_type_val':str(item)})
        if smp==None:
            continue
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
        
def get_region_frame(posts_poi,posts_city,posts_rgn,posts_pr,posts_fr,posts_rk,posts_ur,posts_frm,logger,logger_special):
    city_total_poi_num=0
    city_total_mile_num=0
    city_total_poiinfo_num=0
    city_total_search_num=0
    city_total_bus_num=0
    city_total_drive_num=0
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
    cont=['bus','drive','search','poi_info']
    wlst=[nowyw,oneyw,twoyw,thryw,foryw]
    mlst=[nowym,oneym,twoym,thrym,forym]
    print 'get_total'
    put_time_stamp()
    rank_val_total=get_city_rank_val_total(posts_rk,logger)
    put_time_stamp()
    print 'start to deal with rgn'
    put_time_stamp()
    frm_num=posts_frm.count()
    city_stc={}
    city_stc['typedic']={}
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
        poilst=[]
        item_poi=posts_poi.find_one({'_id':item_frm['_id']})
        if item_poi==None:
            poilst=[]
        else:
            poilst=item_poi['poilst']
        rank_val=get_rgn_rank_val(poilst,rank_val_total,posts_rk,logger)
        data={}
        data['_id']=item_frm['_id']
        data['lon']=float(item_frm['lon'])
        data['lat']=float(item_frm['lat'])
        data['poi_num']=item_frm['poi_num']
        data['poi_cap']=item_frm['poi_cap']
        data['mileage']=item_frm['mileage']
        data['cap_rank']=item_frm['cap_rank']
        data['frm']=item_frm['frm']
        data['importance']=0.0
        data['poiinfo_num']=0
        data['bus_num']=0
        data['drive_num']=0
        data['search_num']=0
        city_total_mile_num+=data['mileage']
        if poilst==[]:
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
                    rgn_lvli['classify'][type]['usr_bhr']['w'][c]=[]
                    for w in wlst:
                        if item_ur==None or item_ur['w'].get(c,'')=='' or item_ur['w'][c].get(w,'')=='' or item_ur['w'][c][w].get('pv_level','')=='':
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c].append([0,0])
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c].append([float(item_ur['w'][c][w]['pv']),float(item_ur['w'][c][w]['pv_level'][1])])
                    rgn_lvli['classify'][type]['usr_bhr']['m'][c]=[]
                    for m in mlst:   
                        if item_ur==None or item_ur['m'].get(c,'')=='' or item_ur['m'][c].get(m,'')==''or item_ur['m'][c][m].get('pv_level','')=='':
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c].append([0,0])
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c].append([float(item_ur['m'][c][m]['pv']),float(item_ur['m'][c][m]['pv_level'][1])])
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
                        if item_ur['w'].get(c,'')=='' or item_ur['w'][c].get(w,'')=='' or item_ur['w'][c][w].get('pv_level','')=='':
                            continue
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c][wlst.index(w)][0]+=float(item_ur['w'][c][w]['pv'])
                            rgn_lvli['classify'][type]['usr_bhr']['w'][c][wlst.index(w)][1]+=float(item_ur['w'][c][w]['pv_level'][1])
                    for m in mlst:
                        if item_ur['m'].get(c,'')=='' or item_ur['m'][c].get(m,'')=='' or item_ur['m'][c][m].get('pv_level','')=='':
                            continue
                        else:
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)][0]+=float(item_ur['m'][c][m]['pv'])
                            rgn_lvli['classify'][type]['usr_bhr']['m'][c][mlst.index(m)][1]+=float(item_ur['m'][c][m]['pv_level'][1])
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
        Q=0.0
        for type in rgn_lvli['classify']:
            data['poiinfo_num']+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['poi_info'][mlst.index(oneym)][0])
            data['bus_num']+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['bus'][mlst.index(oneym)][0])
            data['drive_num']+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['drive'][mlst.index(oneym)][0])
            data['search_num']+=int(rgn_lvli['classify'][type]['usr_bhr']['m']['search'][mlst.index(oneym)][0])
            if len(rgn_lvli['classify'][type]['list'])-fr_count[type]<0.1:
                rgn_lvli['classify'][type]['freshness']=0.0
            else:
                rgn_lvli['classify'][type]['freshness']/=(len(rgn_lvli['classify'][type]['list'])-fr_count[type])
            rgn_lvli['classify'][type]['prosp']/=len(rgn_lvli['classify'][type]['list'])
            for c in cont:
                for w in wlst:   
                    index=wlst.index(w)
                    rgn_lvli['classify'][type]['usr_bhr']['w'][c][index][1]/=len(rgn_lvli['classify'][type]['list'])
                for m in mlst:
                    index=mlst.index(m)
                    rgn_lvli['classify'][type]['usr_bhr']['m'][c][index][1]/=len(rgn_lvli['classify'][type]['list'])  
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
        logger_special.info('id:%s   rank:%f    intense_rank:%f    prosp:%f    '%(data['_id'],rgn_lvli['rank'],rgn_lvli['cap_rank'],rgn_lvli['prosp']))
        rgn_lvli['city_rank_val_total']=rank_val_total
        data['info']=rgn_lvli
        data['importance']=rgn_lvli['importance']
        data['prosp']=rgn_lvli['prosp']
        print data
        posts_rgn.save(data)
        put_time_stamp()
    city_data={}
    city_data['_id']='pda'
    city_data['total_poi']=city_total_poi_num
    city_data['total_mile']=city_total_mile_num
    city_data['total_poiinfo']=city_total_poiinfo_num
    city_data['total_search']=city_total_search_num
    city_data['total_bus']=city_total_bus_num
    city_data['total_drive']=city_total_drive_num
    posts_city.save(city_data)
    
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
    