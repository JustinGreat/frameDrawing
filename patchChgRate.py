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
def get_rank_value(posts):
    cont=[]
    u_cont=['rgn_chg1','rgn_chg2']
    for u in u_cont:
        for i in range(5):
            cont.append(u+'q'+str(i))
    for u in u_cont:
        for i in range(18):
            cont.append(u+'m'+str(i))
    for u in u_cont:
        for i in range(3):
            cont.append(u+'hy'+str(i))
    itemlst=[]
    lst=[[] for i in range(len(cont))]
    lst2=[[] for i in range(len(cont))]
    for item in posts.find():
        itemlst.append(item)
        for c in cont:
            lst[cont.index(c)].append(item[c])
            lst2[cont.index(c)].append(item[c])
    for i in range(len(lst2)):
        lst2[i].sort(reverse=True)
    for i in range(len(itemlst)):
        for c in cont:
            itemlst[i][c+'_rank']=lst2[cont.index(c)].index(lst[cont.index(c)][i])+1
        posts.save(itemlst[i])
def Start(filename):
    client,db,posts = Connect2Mongo(FRM_IP,MONGO_PORT,"collect_frame","newregion2_info")
    patchChgRate(posts,filename)
    return RIGHT
def patchChgRate(posts,filename):
    f_in=open(filename,'r')
    work_zone=False
    dic1={}
    dic2={}
    cont=[]
    u_cont=['rgn_chg1','rgn_chg2']
    for u in u_cont:
        for i in range(18):
            cont.append(u+'m'+str(i))
    for line in f_in:
        if line[:5]=='[005]':
            work_zone=True
        if work_zone==False:
            continue
        line_spl=line.split('\t')
        try:
            int(line_spl[0])
        except:
            continue
        id=line_spl[2]
        mdate=int(line_spl[1][:4]+line_spl[1][-2:])
        add_num2=int(line_spl[5])+int(line_spl[6])#int(line_spl[3])
        del_num2=int(line_spl[22])+int(line_spl[23])#int(line_spl[20])
        net_num2=add_num2-del_num2
        add_num1=int(line_spl[5])
        del_num1=int(line_spl[22])
        net_num1=add_num1-del_num1
        if dic1.get(id,'')=='':
            dic1[id]=[]
            dic1[id].append([mdate,net_num1,add_num1,del_num1])
        else:
            dic1[id].append([mdate,dic1[id][-1][1]+net_num1,add_num1,del_num1])
        if dic2.get(id,'')=='':
            dic2[id]=[]
            dic2[id].append([mdate,net_num2,add_num2,del_num2])
        else:
            dic2[id].append([mdate,dic2[id][-1][1]+net_num2,add_num2,del_num2])
    now=time.time()
    now_fmt=time.localtime(now)
    now_int=int('%04d%02d'%(now_fmt[0],now_fmt[1]))
    start_before=18
    if now_fmt[1]-start_before<1:
        inter_year=1+int(start_before-now_fmt[1])/12
        start_str=time.strftime("%4d%02d"%(now_fmt[0]-inter_year,now_fmt[1]-start_before+12*inter_year))
    else:
        start_str=time.strftime("%4d%02d"%(now_fmt[0],now_fmt[1]-num))
    start_date=int(start_str)

    for item in posts.find().batch_size(10):
        for i in range(18):
            item['rgn_chg1m'+str(i)]=0.0
            item['rgn_chg2m'+str(i)]=0.0
        if item['_id'] in dic1:
            for item_lst in dic1[item['_id']]:
                if item_lst[0]>=start_date:
                    if item_lst[1]-item_lst[2]+item_lst[3]==0:
                        continue
                    if now_int-start_date<50:
                        item['rgn_chg1m'+str(now_int-start_date)]=float(item_lst[2]+item_lst[3])/(item_lst[1]-item_lst[2]+item_lst[3])
                    elif now_int-start_date<150:
                        item['rgn_chg1m'+str(now_int-start_date-88)]=float(item_lst[2]+item_lst[3])/(item_lst[1]-item_lst[2]+item_lst[3])
                    else:
                        item['rgn_chg1m'+str(now_int-start_date-88*2)]=float(item_lst[2]+item_lst[3])/(item_lst[1]-item_lst[2]+item_lst[3])
        if item['_id'] in dic2:
            for item_lst in dic2[item['_id']]:
                if item_lst[0]>=start_date:
                    if item_lst[1]-item_lst[2]+item_lst[3]==0:
                        continue
                    if now_int-start_date<50:
                        item['rgn_chg2m'+str(now_int-start_date)]=float(item_lst[2]+item_lst[3])/(item_lst[1]-item_lst[2]+item_lst[3])
                    elif now_int-start_date<150:
                        item['rgn_chg2m'+str(now_int-start_date-88)]=float(item_lst[2]+item_lst[3])/(item_lst[1]-item_lst[2]+item_lst[3])
                    else:
                        item['rgn_chg2m'+str(now_int-start_date-88*2)]=float(item_lst[2]+item_lst[3])/(item_lst[1]-item_lst[2]+item_lst[3])
        
        for c in cont:
            nowm=now_fmt[1]
            nowm_int=int(nowm)
            inx=nowm_int%3
            if inx==0:
                inx=3
            if inx==1:
                item[c+'q0']=item[c+'m0']
            elif inx==2:
                item[c+'q0']=item[c+'m0']+item[c+'m1']
            else:
                item[c+'q0']=item[c+'m0']+item[c+'m1']+item[c+'m2']
            for i in range(1,5):
                item[c+'q'+str(i)]=item[c+'m'+str(inx+3*(i-1))]+item[c+'m'+str(inx+3*(i-1)+1)]+item[c+'m'+str(inx+3*(i-1)+2)]
            if (nowm_int<=6 and nowm_int>3) or (nowm_int<=12 and nowm_int>9):
                for i in range(3):
                    item[c+'hy'+str(i)]=item[c+'q'+str(2*i)]+item[c+'q'+str(2*i+1)]
            else:
                item[c+'hy'+'0']=item[c+'q0']
                for i in range(1,3):
                    item[c+'hy'+str(i)]=item[c+'q'+str(2*i-1)]+item[c+'q'+str(2*i)]
        posts.save(item)
    get_rank_value(posts)

if __name__ == '__main__':
    if len(sys.argv)==2:
        filename=sys.argv[1]
        if os.path.exists(filename):
            code=Start(filename)
            if code==RIGHT:
                print 'The Patching CHG Process is ok'
            else:
                print 'The Patching CHG Process failed'
        else:
            print 'Arguments error. Please check.'
            sys.exit(1)
    else:
        print 'Number of the arguments error. Please check.'