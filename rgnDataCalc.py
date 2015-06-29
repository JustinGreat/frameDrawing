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
NUM_MONTH=18
NUM_Q=6
NUM_HY=3
NO_FRESH_TYPE=['19','18','22']
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
def Start(filename,num):
    logger = InitLog('./log/rgn_data_calc_%s.log'%num)
    db_slt=['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']
    client_ur=[0 for i in range(16)]
    db_ur=[0 for i in range(16)]
    posts_ur=[0 for i in range(16)]
    try:
        client_fr,db_fr,posts_fr = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,"cms_poi_freshness")
    except:
        logger.error("connect 2 mongodb failed.")
        return
    for i in range(16):
        try:
            TABLE="cms_poi_pv_level_"+db_slt[i]
            client_ur[i],db_ur[i],posts_ur[i] = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,TABLE)
        except:
            logger.error("connect 2 mongodb failed.")
            return
    try:
        client_rk,db_rk,posts_rk = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,"cms_poi_rank")
    except:
        logger.error("connect 2 mongodb failed.")
        return
    try:
        client_pr,db_pr,posts_pr = Connect2Mongo('localhost',MONGO_PORT,DATA_BASE,"cms_poi_prosp")
    except:
        logger.error("connect 2 mongodb failed.")
        return
    try:
        count=0
        while True:
            clc='tmp_frm_'+str(count)
            client_tmpfrm,db_tmpfrm,posts_tmpfrm = Connect2Mongo('localhost',MONGO_PORT,'tmp',clc)
            if posts_tmpfrm.count()==0:
                posts_tmpfrm.save({'_id':GetMD5('ocupied')})
                break
            else:
                count+=1
    except:
        client_tmpfrm.disconnect()
        logger.error("connect 2 mongodb failed.")
        return
        
    #try:
    if True:
        file_spl=filename.split('/')
        ab_filename=file_spl[-1]
        city=ab_filename[:6]
        pos1=ab_filename.find('_')
        pos2=ab_filename.find('_',pos1+1)
        if pos1==-1 or pos2==-1 or pos1>=pos2:
            logger.error('File name is not correct')
            return ERROR
        rgnType=ab_filename[pos1+1:pos2]
        client_rgn,db_rgn,posts_rgn = Connect2Mongo("localhost", 27017,rgnType,city)
        client_rgn_bak,db_rgn_bak,posts_rgn_bak = Connect2Mongo("localhost", 27017,rgnType+'_bak',city)
    #except:
     #   logger.error("connect 2 mongodb failed.")
     #   return
    try:
        client_city,db_city,posts_city = Connect2Mongo("localhost", 27017,rgnType,"city")
    except:
        logger.error("connect 2 mongodb failed.")
        return
    logger.info("connect 2 mongodb success")
 
    code=put_frame_poi_2_db(filename,posts_tmpfrm,logger)
    if code==ERROR:
        logger.error('Putting file data into frm_db met error')
        return ERROR
    else:
        logger.info("Putting file data into frm_db was OK")
    posts_rgn_bak.drop()
    code=get_region_attr(posts_city,posts_rgn_bak,posts_pr,posts_fr,posts_rk,posts_ur,posts_tmpfrm,city,logger)
    if code==ERROR:
        logger.error('Main Calculation met error')
        return ERROR
    else:
        logger.info("Main Calculation was OK")
    posts_tmpfrm.drop()
    code=get_rank_value(posts_rgn_bak,logger)
    if code==ERROR:
        logger.error('Calculating Rank met error')
        return ERROR
    else:
        logger.info("Calculating Rank was OK")
    code=copy_db(posts_rgn_bak,posts_rgn,logger)
    if code==ERROR:
        logger.error('Copying DB met error')
        return ERROR
    else:
        logger.info("Copying DB was OK")
    scale=[]
    for i in range(1,NUM_MONTH):
        scale.append('m'+str(i))
    for i in range(1,NUM_Q):
        scale.append('q'+str(i))
    for i in range(1,NUM_HY):
        scale.append('hy'+str(i))
    for tm in scale:
        client_show,db_show,posts_show = Connect2Mongo("localhost", 27017,rgnType,city+tm)
        posts_show.drop()
        code=org_db(posts_rgn,posts_show,posts_city,city,tm,logger)
        if code==ERROR:
            logger.error('Organizing DB met error')
            return ERROR
        else:
            logger.info("Organizing DB was OK")
    return RIGHT

def org_db(posts_s,posts_d,posts_city,city,tm,logger):
    city_data=posts_city.find_one({'_id':city})
    frm_num=posts_s.count()
    for frm in posts_s.find():
        data_rgn={}
        data_rgn['_id']=frm['_id']
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
        try:
            data_rgn['poi_num']['proportion']=round(float(frm['poi_num'])/city_data['poi_num'],2)
        except:
            data_rgn['poi_num']['proportion']=0
        data_rgn['poi_num']['rank']=frm['poi_num_rank']
        data_rgn['poi_num']['hot']=(1-float(frm['poi_num_rank']-1)/frm_num)*100
        data_rgn['poi_cap']={}
        data_rgn['poi_cap']['val']=frm['poi_cap']
        data_rgn['poi_cap']['rank']=frm['cap_rank']
        data_rgn['poi_cap']['hot']=(1-float(frm['cap_rank']-1)/frm_num)*100
        data_rgn['mileage']={}
        data_rgn['mileage']['val']=frm['mileage']
        try:
            data_rgn['mileage']['proportion']=round(float(frm['mileage'])/city_data['mileage'])
        except:
            data_rgn['mileage']['proportion']=0
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
        data_rgn['rgn_chg1']={}
        data_rgn['rgn_chg2']={}
        if frm.get('rgn_chg1','')=='':
            data_rgn['rgn_chg1']['val']=0
            data_rgn['rgn_chg1']['rank']=0
        else:
            data_rgn['rgn_chg1']['val']=frm['rgn_chg1'+tm]*100
            data_rgn['rgn_chg1']['rank']=frm['rgn_chg1'+tm+'_rank']
            data_rgn['rgn_chg1']['hot']=frm['rgn_chg1'+tm+'_hot']
        if frm.get('rgn_chg2','')=='':
            data_rgn['rgn_chg2']['val']=0
            data_rgn['rgn_chg2']['rank']=0
        else:
            data_rgn['rgn_chg2']['val']=frm['rgn_chg2'+tm]*100
            data_rgn['rgn_chg2']['rank']=frm['rgn_chg2'+tm+'_rank']
            data_rgn['rgn_chg2']['hot']=frm['rgn_chg2'+tm+'_hot']
        cont=['bus','drive','search','poi_info','share','error','collection','group']
        for c in cont:
            data_rgn[c]={}
            data_rgn[c]['rank']=frm[c+tm+'_rank']
            data_rgn[c]['val']=int(frm[c+tm])
            data_rgn[c]['hot']=(1-float(frm[c+tm+'_rank']-1)/frm_num)*100
            try:
                data_rgn[c]['proportion']=float(frm[c+tm])/city_data[c+tm]
            except:
                data_rgn[c]['proportion']=0
        posts_d.save(data_rgn)
        
def copy_db(posts_s,posts_d,logger):
    posts_d.drop()
    try:
        for item in posts_s.find():
            posts_d.save(item)
    except:
        logger.error('Copying error')
        return ERROR
    return RIGHT
    
def get_rank_value(posts,logger):
    cont=['mileage','poi_num','freshness','prosp','importance']
    u_cont=['bus','drive','search','poi_info','share','error','collection','group']
    for u in u_cont:
        for i in range(NUM_Q):
            cont.append(u+'q'+str(i))
    for u in u_cont:
        for i in range(NUM_MONTH):
            cont.append(u+'m'+str(i))
    for u in u_cont:
        for i in range(NUM_HY):
            cont.append(u+'hy'+str(i))
    no_acu=['freshness','prosp','importance']
    itemonth=[]
    lst=[[] for i in range(len(cont))]
    lst2=[[] for i in range(len(cont))]
    lst2_acu=[[] for i in range(len(cont))]
    for item in posts.find():
        itemonth.append(item)
        for c in cont:
            lst[cont.index(c)].append(item[c])
            lst2[cont.index(c)].append(item[c])
    for i in range(len(lst2)):
        lst2[i].sort(reverse=True)
    for i in range(len(lst2)):
        if cont[i] in no_acu:
            continue
        for j in range(len(lst2[i])):
            if j==0:
                lst2_acu[i].append(lst2[i][0])
            else:
                lst2_acu[i].append(lst2[i][j]+lst2_acu[i][-1])
    for i in range(len(itemonth)):
        for c in cont:
            itemonth[i][c+'_rank']=lst2[cont.index(c)].index(lst[cont.index(c)][i])+1
            if not c in no_acu:
                itemonth[i]['acu_'+c]=lst2_acu[cont.index(c)][lst2[cont.index(c)].index(lst[cont.index(c)][i])]
        posts.save(itemonth[i])
    logger.info('Ranking process was complete.')
    return RIGHT
        

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
def update_poi_num(filename,posts,logger):
    f_in=open(filename,'r')
    for line in f_in:
        item=line.split('\t')
        id=item[0]
        pois=item[7].strip('\n')
        pois_spl=pois.split(';')
        frm=posts.find_one({"_id":id})
        frm['poi_num']=len(pois_spl)
        posts.save(frm)
        
def put_frame_poi_2_db(filename,posts,logger):
    try:
        f_in=open(filename,'r')
        frm_data={}
        for line in f_in:
            item=line.split('\t')
            data={}
            data['_id']=item[0]
            data['lon']=float(item[2])
            data['lat']=float(item[1])
            data['mileage']=float(item[3])
            data['poi_num']=int(item[4])
            data['poi_cap']=float(item[5])
            data['pois']=[]
            data['frm']=item[6].strip('\n')
            pois=item[7].strip('\n')
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
            data['poi_num']=len(poilst)
            data['pois']=poilst
            posts.save(data)
        posts.delete_one({'_id':GetMD5('ocupied')})
        s_rank=0
        posts.create_index([('poi_cap',-1)])
        for item in posts.find().sort([('poi_cap',-1)]):
            s_rank+=1
            item['cap_rank']=s_rank
            posts.save(item)
    except Exception,e:
        logger.error(e)
        return ERROR
    return RIGHT

def get_city_rank_val_total(posts_frm,posts_rk,logger):
    rank_val_total=0
    for item in posts_frm.find():
        for i in range(len(item['pois'])):
            id=item['pois'][0][0]
            smp=posts_rk.find_one(GetMD5(id))
            if smp==None:
                continue
            rank_val_total=smp['city']['total']
            break
        if rank_val_total!=0:
            break
    type19=0
    no_list=NO_FRESH_TYPE
    for item in posts_frm.find():
        for i in range(len(item['pois'])):
            id=item['pois'][0][0]
            type=item['pois'][0][0]
            if type[:2] in no_list:  
                smp=posts_rk.find_one(GetMD5(id))
                if smp==None:
                    continue
                type19+=smp['city_type']['total']
                no_list.remove(type[:2])
                if len(no_list)==0:
                    break
        if len(no_list)==0:
            break
    logger.info('Getting rank value total Ok')
    return rank_val_total,type19
def get_rgn_rank_val(poilst,rank_val_total,posts_rk,dic,logger):
    rank_type_list={}
    keylst=[]
    for poi in poilst:
        key=GetMD5(poi[0])
        keylst.append(key)
    for rslt in posts_rk.find({"_id":{"$in":keylst}}):
        if rslt.get('rank_type_val','')=='':
            continue
        if rslt['rank_type_val'] in dic:
            dic[rslt['rank_type_val']]['cnt']+=1
            dic[rslt['rank_type_val']]['rank']+=(1-float(rslt['city_rank_type']['inx']-1)/rslt['city_rank_type']['total'])*100
        else:
            dic[rslt['rank_type_val']]={}
            dic[rslt['rank_type_val']]['cnt']=1
            dic[rslt['rank_type_val']]['city_rank_total']=float(rslt['city_rank_type']['total'])
            dic[rslt['rank_type_val']]['rank']=(1-float(rslt['city_rank_type']['inx']-1)/rslt['city_rank_type']['total'])*100
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
    if rgn_total<0.01:
        return 0
    for item_rank in rank_type_list:
        Q+=rank_type_list[item_rank]['cnt']/rgn_total/(rank_type_list[item_rank]['city_rank_total']/rank_val_total)
    for item_rank in rank_type_list:
        rank+=rank_type_list[item_rank]['rank']*rank_type_list[item_rank]['cnt']/rgn_total/(rank_type_list[item_rank]['city_rank_total']/rank_val_total)/Q
    return rank
def get_month_str_before(now_fmt,num):
    if now_fmt[1]-num<1:
        inter_year=1+int(num-now_fmt[1])/12
        return time.strftime("%4d%02d"%(now_fmt[0]-inter_year,now_fmt[1]-num+12*inter_year))
    else:
        return time.strftime("%4d%02d"%(now_fmt[0],now_fmt[1]-num))
def get_q_str_before(now_fmt,num):
    now_q=(now_fmt[1]-1)/3+1
    now_y=now_fmt[0]
    if now_q-num<1:
        inter_year=1+int(num-now_q)/4
        return time.strftime("%4d%02d"%(now_y-inter_year,now_q-num+4*inter_year))
    else:
        return time.strftime("%4d%02d"%(now_y,now_q-num))
def get_hy_str_before(now_fmt,num):
    now_y=now_fmt[0]
    if now_fmt[1]<=6:
        now_hy=1
    else:
        now_hy=2
    if now_hy-num<1:
        inter_year=1+int(num-now_hy)/2
        return time.strftime("%4d%02d"%(now_y-inter_year,now_hy-num+2*inter_year))
    else:
        return time.strftime("%4d%02d"%(now_y,now_hy-num))
def get_region_attr(posts_city,posts_rgn,posts_pr,posts_fr,posts_rk,posts_ur,posts_frm,city,logger):
    f_in=open('./src/adcity','r')
    for line in f_in:
        line_json=json.loads(line)
        adcode=line_json['code']
        if adcode==city:
            chn=line_json['chn']
            pos=line_json['pos']
            break
    try:
        print chn
    except:
        logger.error('City was not found in the list')
        return ERROR
    now=time.time()
    now_fmt=time.localtime(now)
    month=[get_month_str_before(now_fmt,i) for i in range(NUM_MONTH)]
    qter=[get_q_str_before(now_fmt,i) for i in range(NUM_Q)]
    halfy=[get_hy_str_before(now_fmt,i) for i in range(NUM_HY)]
    cont_cls_1=['importance','freshness','prosp']
    cont_cls_2=['poi_num','mileage']
    cont_cls_3=['bus','drive','search','poi_info','share','error','collection','group']
    rank_val_total,type19=get_city_rank_val_total(posts_frm,posts_rk,logger)
    rgn_num=posts_frm.count()
    city_data={}
    city_data['_id']=city
    city_data['lon']=pos[0]
    city_data['lat']=pos[1]
    city_data['name']=chn
    city_data['poi_num']=0
    city_data['mileage']=0
    city_data['poi_cap']=0
    city_data['month_list']=month
    city_data['qter_list']=qter
    city_data['hy_list']=halfy
    city_data['classify']={}
    city_data['classify_rank']={}
    for c in cont_cls_3:
        for i in range(NUM_MONTH):
            city_data[c+'m'+str(i)]=0
        for i in range(NUM_Q):
            city_data[c+'q'+str(i)]=0
        for i in range(NUM_HY):
            city_data[c+'hy'+str(i)]=0
    logger.info('Begin calculating...')
    for item_frm in posts_frm.find().batch_size(1):
        data={}
        data['_id']=item_frm['_id']
        data['lon']=float(item_frm['lon'])
        data['lat']=float(item_frm['lat'])
        data['month_list']=month
        data['qter_list']=qter
        data['hy_list']=halfy
        data['poi_num']=item_frm['poi_num']
        data['poi_cap']=item_frm['poi_cap']
        data['cap_rank']=item_frm['cap_rank']
        data['mileage']=item_frm['mileage']
        data['frm']=item_frm['frm']
        data['cap_rank']=item_frm['cap_rank']
        poilst=item_frm['pois']
        data['poi_num']=len(poilst)
        city_data['poi_num']+=data['poi_num']
        city_data['mileage']+=data['mileage']
        rank_val=get_rgn_rank_val(poilst,rank_val_total,posts_rk,city_data['classify_rank'],logger)
        data['importance']=0.0
        data['prosp']=0.0
        data['freshness']=0.0
        data['rank']=rank_val
        data['classify']={}
        data['city_rank_val_total']=0
        for c in cont_cls_3:
            for i in range(NUM_MONTH):
                data[c+'m'+str(i)]=0
            for i in range(NUM_Q):
                data[c+'q'+str(i)]=0
            for i in range(NUM_HY):
                data[c+'hy'+str(i)]=0
        if poilst==[]:
            posts_rgn.save(data)
            continue
        fr_count={}
        poi_list=[]
        key_list=[]
        type_list=[]
        for poi_item in poilst:
            try:
                poi_type=poi_item[1].split('|')[0]
            except:
                continue
            poi_list.append(poi_item[0])
            type_list.append(poi_type)
            key=GetMD5(poi_item[0])
            key_list.append(key)

        for type in type_list:
            if city_data['classify'].get(type,'')=='':
                city_data['classify'][type]={}
                city_data['classify'][type]['freshness_val']=0
                city_data['classify'][type]['freshness']=0.0
                city_data['classify'][type]['prosp']=0.0
                city_data['classify'][type]['prosp_val']=0
        for item_rk in posts_rk.find({"_id":{"$in":key_list}}):
            type=type_list[poi_list.index(item_rk['poiid'])]
            if data['classify'].get(type,'')=='':
                data['classify'][type]={}
                data['classify'][type]['list']=[]
                data['classify'][type]['list'].append(poilst[poi_list.index(item_rk['poiid'])])
                try:
                    data['classify'][type]['total_num']=item_rk['city_type3']['total']
                    data['classify'][type]['city_total']=item_rk['city']['total']
                except:
                    pass
            else:
                data['classify'][type]['list'].append(poilst[poi_list.index(item_rk['poiid'])])
                try:
                    data['classify'][type]['total_num']=item_rk['city_type3']['total']
                    data['classify'][type]['city_total']=item_rk['city']['total']
                except:
                    pass

        for item_fr in posts_fr.find({"_id":{"$in":key_list}}):
            type=type_list[poi_list.index(item_fr['poiid'])]
            city_data['classify'][type]['freshness_val']+=1
            city_data['classify'][type]['freshness']+=float(item_fr['freshness'])
            if data['classify'].get(type,'')=='':
                data['classify'][type]={}
                data['classify'][type]['list']=[]
                data['classify'][type]['freshness']=float(item_fr['freshness'])
                data['classify'][type]['list'].append(poilst[poi_list.index(item_fr['poiid'])])
            else:
                if not poilst[poi_list.index(item_fr['poiid'])] in data['classify'][type]['list']:
                    data['classify'][type]['list'].append(poilst[poi_list.index(item_fr['poiid'])])
                if data['classify'][type].get('freshness','')=='':
                    data['classify'][type]['freshness']=float(item_fr['freshness'])
                else:
                    data['classify'][type]['freshness']+=float(item_fr['freshness'])
            
        for item_pr in posts_pr.find({"_id":{"$in":poi_list}}):
            type=type_list[poi_list.index(item_pr['_id'])]
            city_data['classify'][type]['prosp_val']+=1
            city_data['classify'][type]['prosp']+=float(item_pr['prosp'])
            if data['classify'].get(type,'')=='':
                data['classify'][type]={}
                data['classify'][type]['list']=[]
                data['classify'][type]['prosp']=float(item_pr['prosp'])
                if not poilst[poi_list.index(item_pr['_id'])] in data['classify'][type]['list']:
                    data['classify'][type]['list'].append(poilst[poi_list.index(item_pr['_id'])])
            else:
                if not poilst[poi_list.index(item_pr['_id'])] in data['classify'][type]['list']:
                    data['classify'][type]['list'].append(poilst[poi_list.index(item_pr['_id'])])
                if data['classify'][type].get('prosp','')=='':
                    data['classify'][type]['prosp']=float(item_pr['prosp'])
                else:
                    data['classify'][type]['prosp']+=float(item_pr['prosp'])
        db_slt=['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']
        for i in range(16):
            s_keylst=[key  for key in key_list if key[0]==db_slt[i]]
            for item_ur in posts_ur[i].find({"_id":{"$in":s_keylst}}):
                type=type_list[poi_list.index(item_ur['poiid'])]
                if data['classify'].get(type,'')=='':
                    data['classify'][type]={}
                    data['classify'][type]['list']=[]
                    data['classify'][type]['usr_bhr']={}
                    data['classify'][type]['usr_bhr']['q']={}
                    data['classify'][type]['usr_bhr']['m']={}
                    data['classify'][type]['usr_bhr']['hy']={}
                    if not poilst[poi_list.index(item_ur['poiid'])] in data['classify'][type]['list']:
                        data['classify'][type]['list'].append(poilst[poi_list.index(item_ur['poiid'])])
                    for c in cont_cls_3:
                        data['classify'][type]['usr_bhr']['m'][c]=[0 for i in range(NUM_MONTH)]
                        for m in month:
                            if item_ur==None or item_ur['m'].get(c,'')=='' or item_ur['m'][c].get(m,'')=='':
                                continue
                            else:
                                data['classify'][type]['usr_bhr']['m'][c][month.index(m)]=float(item_ur['m'][c][m]['pv'])
                else:
                    if not poilst[poi_list.index(item_ur['poiid'])] in data['classify'][type]['list']:
                        data['classify'][type]['list'].append(poilst[poi_list.index(item_ur['poiid'])])
                    if data['classify'][type].get('usr_bhr','')=='':
                        data['classify'][type]['usr_bhr']={}
                        data['classify'][type]['usr_bhr']['m']={}
                        data['classify'][type]['usr_bhr']['q']={}
                        data['classify'][type]['usr_bhr']['hy']={}
                        for c in cont_cls_3:
                            data['classify'][type]['usr_bhr']['m'][c]=[0 for i in range(NUM_MONTH)]
                            for m in month:
                                if item_ur['m'].get(c,'')=='' or item_ur['m'][c].get(m,'')=='' :
                                    continue
                                else:
                                    data['classify'][type]['usr_bhr']['m'][c][month.index(m)]=float(item_ur['m'][c][m]['pv'])
                    else:
                        for c in cont_cls_3:
                            for m in month:
                                if item_ur['m'].get(c,'')=='' or item_ur['m'][c].get(m,'')=='' :
                                    continue
                                else:
                                    data['classify'][type]['usr_bhr']['m'][c][month.index(m)]+=float(item_ur['m'][c][m]['pv'])
                                    
        for type in data['classify']:
            if data['classify'][type].get('usr_bhr','')=='':
                continue
            for c in cont_cls_3:
                data['classify'][type]['usr_bhr']['q'][c]=[0 for i in range(NUM_Q)]
                data['classify'][type]['usr_bhr']['hy'][c]=[0 for i in range(NUM_HY)]
                nowm=month[0][-2:]
                if nowm[0]=='0':
                    nowm=nowm[1]
                nowm_int=int(nowm)
                inx=nowm_int%3
                if inx==0:
                    inx=3
                data['classify'][type]['usr_bhr']['q'][c][0]=sum(data['classify'][type]['usr_bhr']['m'][c][:inx])
                for i in range(1,NUM_Q):
                    data['classify'][type]['usr_bhr']['q'][c][i]=sum(data['classify'][type]['usr_bhr']['m'][c][inx+3*(i-1):inx+3*i])
                if (nowm_int<=6 and nowm_int>3) or (nowm_int<=12 and nowm_int>9):
                    for i in range(NUM_HY):
                        if 2*i+1<len(data['classify'][type]['usr_bhr']['q'][c]):
                            data['classify'][type]['usr_bhr']['hy'][c][i]=data['classify'][type]['usr_bhr']['q'][c][2*i]+data['classify'][type]['usr_bhr']['q'][c][2*i+1]
                else:
                    data['classify'][type]['usr_bhr']['hy'][c][0]=data['classify'][type]['usr_bhr']['q'][c][0]
                    for i in range(1,NUM_HY):
                        if 2*i<len(data['classify'][type]['usr_bhr']['q'][c]):
                            data['classify'][type]['usr_bhr']['hy'][c][i]=data['classify'][type]['usr_bhr']['q'][c][2*i-1]+data['classify'][type]['usr_bhr']['q'][c][2*i]
                    
        Q=0.0
        Qf=0.0
        rgn_type19=0
        for type in data['classify']:
            if type[:2] in NO_FRESH_TYPE:
                rgn_type19+=len(data['classify'][type]['list'])
            if data['classify'][type].get('freshness','')=='':
                data['classify'][type]['freshness']=0.0
            if data['classify'][type].get('prosp','')=='':
                data['classify'][type]['prosp']=0.0
            if data['classify'][type].get('usr_bhr','')=='':
                data['classify'][type]['usr_bhr']={}
                data['classify'][type]['usr_bhr']['list']=[]
                data['classify'][type]['usr_bhr']['m']={}
                data['classify'][type]['usr_bhr']['q']={}
                data['classify'][type]['usr_bhr']['hy']={}
                for c in cont_cls_3:
                    data['classify'][type]['usr_bhr']['q'][c]=[0 for i in range(NUM_Q)]
                    data['classify'][type]['usr_bhr']['m'][c]=[0 for i in range(NUM_MONTH)]
                    data['classify'][type]['usr_bhr']['hy'][c]=[0 for i in range(NUM_HY)]
                    
            for m in month:
                for c in cont_cls_3:
                    data[c+'m'+str(month.index(m))]+=int(data['classify'][type]['usr_bhr']['m'][c][month.index(m)])
            for i in range(NUM_Q):
                for c in cont_cls_3:
                    data[c+'q'+str(i)]+=int(data['classify'][type]['usr_bhr']['q'][c][i])
            for i in range(NUM_HY):
                for c in cont_cls_3:
                    data[c+'hy'+str(i)]+=int(data['classify'][type]['usr_bhr']['hy'][c][i])
            if len(data['classify'][type]['list'])==0:
                data['classify'][type]['freshness']=0.0
            else:
                data['classify'][type]['freshness']/=len(data['classify'][type]['list'])
            if len(data['classify'][type]['list'])==0:
                data['classify'][type]['prosp']=0.0
            else:
                data['classify'][type]['prosp']/=len(data['classify'][type]['list'])
                
                
        for type in data['classify']:
            try:
                Q+=len(data['classify'][type]['list'])/float(data['poi_num'])/(float(data['classify'][type]['total_num'])/float(data['classify'][type]['city_total']))
            except:
                Q+=0
            try:
                if not type[:2] in NO_FRESH_TYPE:
                    Qf+=len(data['classify'][type]['list'])/(float(data['poi_num'])-rgn_type19)/(float(data['classify'][type]['total_num'])/(float(data['classify'][type]['city_total'])-type19))
            except:
                Qf+=0
        data['freshness']=0.0
        data['prosp']=0.0
        for type in data['classify']:
            try:
                tmp=(len(data['classify'][type]['list'])/float(data['poi_num'])/(float(data['classify'][type]['total_num'])/data['classify'][type]['city_total']))/Q
            except:
                tmp=0
            try:
                if not type[:2] in NO_FRESH_TYPE:
                    tmp2=len(data['classify'][type]['list'])/(float(data['poi_num'])-rgn_type19)/(float(data['classify'][type]['total_num'])/(float(data['classify'][type]['city_total'])-type19))/Qf
                else:
                    tmp2=0.0
            except:
                tmp2=0.0
            data['freshness']+=data['classify'][type]['freshness']*tmp2
            data['prosp']+=data['classify'][type]['prosp']*tmp
        data['importance']=0.7*data['rank']+0.2*(1-float(data['cap_rank']-1)/rgn_num)*100+0.1*data['prosp']
        data['city_rank_val_total']=rank_val_total
        
        #print data
        posts_rgn.save(data)
    try:
        client_cityur,db_cityur,posts_cityur = Connect2Mongo(USER_BEHAVIOR_IP,MONGO_PORT,DATA_BASE,"pv_stat_city",USER,PASSWD)
    except:
        logger.error('Remote DB does not connect.')
        return ERROR
    key=GetMD5(city)
    city_ur=posts_cityur.find_one({'_id':key})
    for c in cont_cls_3:
        for m in month:
            if city_ur['m'].get(c,'')=='' or city_ur['m'][c].get(m,'')=='':
                continue
            else:
                city_data[c+'m'+str(month.index(m))]=float(city_ur['m'][c][m]['pv'])
    for c in cont_cls_3:
        nowm=month[0][-2:]
        if nowm[0]=='0':
            nowm=nowm[1]
        nowm_int=int(nowm)
        inx=nowm_int%3
        if inx==0:
            inx=3
        if inx==1:
            city_data[c+'q0']=city_data[c+'m0']
        elif inx==2:
            city_data[c+'q0']=city_data[c+'m0']+city_data[c+'m1']
        else:
            city_data[c+'q0']=city_data[c+'m0']+city_data[c+'m1']+city_data[c+'m2']
        for i in range(1,NUM_Q):
            city_data[c+'q'+str(i)]=city_data[c+'m'+str(inx+3*(i-1))]+city_data[c+'m'+str(inx+3*(i-1)+1)]+city_data[c+'m'+str(inx+3*(i-1)+2)]
        if (nowm_int<=6 and nowm_int>3) or (nowm_int<=12 and nowm_int>9):
            for i in range(NUM_HY):
                city_data[c+'hy'+str(i)]=city_data[c+'q'+str(2*i)]+city_data[c+'q'+str(2*i+1)]
        else:
            city_data[c+'hy'+'0']=city_data[c+'q0']
            for i in range(1,NUM_HY):
                city_data[c+'hy'+str(i)]=city_data[c+'q'+str(2*i-1)]+city_data[c+'q'+str(2*i)]
    posts_city.save(city_data)
    logger.info('City_%s is ok!'%city)
    logger.info(json.dumps(city_data,ensure_ascii=False).encode('utf-8','ignore'))
    return RIGHT

        
if __name__ == '__main__':
    if len(sys.argv)==3:
        filename=sys.argv[1]
        num=sys.argv[2]
        if os.path.exists(filename) and num.isdigit():
            code=Start(filename,num)
            if code==RIGHT:
                print 'The Process is ok'
            else:
                print 'The Process failed'
        else:
            print 'Arguments error. Please check.'
            sys.exit(1)
    else:
        print 'Number of the arguments error. Please check.'
    
    