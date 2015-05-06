#!/usr/bin/python

import pickle
import json
from util import *

ERROR='0'
RIGHT='1'

opt_circum_min=0.08
opt_circum_max=0.1
opt_circum_min_2=0.03
opt_circum_max_2=0.13
opt_poi_max=320
opt_poi_max_2=400
'''
class Roadmap:
    def __init__(self):
        self.dic={}
        self.client_raw,self.db_raw,self.posts_raw=Connect2Mongo("localhost",27017,"drawFrame","raw_frame_test")
        self.client_poi,self.db_poi,self.posts_poi=Connect2Mongo("localhost",27017,"drawFrame","raw_poi")
        self.client_res,self.db_res,self.posts_res=Connect2Mongo('localhost',27017,'drawFrame','bj_frame_test')
    def dist(self,(x1,y1),(x2,y2)):
        return ((x2-x1)**2+(y2-y1)**2)**0.5
    def getDataFromDB(self):
        for line in self.posts_raw.find():
            line_json=line['data']
            faceID=line_json[0][0]
            self.dic[faceID]={}
            poi_json=self.posts_poi.find_one({"_id":line_json[0][0]})
            self.dic[faceID]['poi']=[]
            self.dic[faceID]['poi']+=poi_json['poi']
            self.dic[faceID]['poi_num']=len(self.dic[line_json[0][0]]['poi'])
            self.dic[faceID]['link']=[]
            self.dic[faceID]['circum']=0.0
            self.dic[faceID]['pos']=[line_json[1]]
            self.dic[faceID]['frame']=[]
            self.dic[faceID]['frame'].append(line_json[2][0])
            self.dic[faceID]['corner']=[]
    def getNearByRlt(self):
        for item in self.dic:
            for other in self.dic:
                if abs(self.dic[other]['pos'][0][0]-self.dic[item]['pos'][0][0])>0.02 or abs(self.dic[other]['pos'][0][1]-self.dic[item]['pos'][0][1])>0.02:
                    continue
                if self.dic[other]['pos'][0]==self.dic[item]['pos'][0]:
                    continue
                if other in self.dic[item]['link']:
                    continue
                minDis=self.dist((self.dic[other]['frame'][0][0][0],self.dic[other]['frame'][0][0][1]),(self.dic[item]['frame'][0][0][0],self.dic[item]['frame'][0][0][1]));
                for j in range(len(self.dic[item]['frame'][0])):
                    for k in range(len(self.dic[other]['frame'][0])):
                        minDis=min(self.dist((self.dic[other]['frame'][0][k][0],self.dic[other]['frame'][0][k][1]),(self.dic[item]['frame'][0][j][0],self.dic[item]['frame'][0][j][1])),minDis)
                        if minDis < 0.001:
                            break
                if minDis < 0.001:
                    self.dic[item]['link'].append(other)
                    self.dic[other]['link'].append(item)
                else:
                    continue
        
    def mergeZones(self,item,key):
        self.dic[item]['circum']+=self.dic[key]['circum']
        self.dic[item]['link']+=self.dic[key]['link']
        self.dic[item]['link']=list(set(self.dic[item]['link']))
        try:
            self.dic[item]['link'].remove(item)
        except:
            pass
        try:
            self.dic[item]['link'].remove(key)
        except:
            pass
        for d_key in self.dic[key]['link']:
            try:
                self.dic[d_key]['link'].remove(key)
            except:
                pass
        self.dic[item]['pos']+=self.dic[key]['pos']
        self.dic[item]['frame']+=self.dic[key]['frame']
        self.dic[item]['corner']+=self.dic[key]['corner']
        del self.dic[key]
    def stResult(self):
        self.posts_res.drop()
        for frm in self.dic:
            json_dic={}
            json_dic['_id']=frm
            json_dic['lon']=self.dic[frm]['pos'][0][0]
            json_dic['lat']=self.dic[frm]['pos'][0][1]
            json_dic['data']=[]
            json_dic['data'].append([frm])
            json_dic['data'].append(self.dic[frm]['pos'][0])
            json_dic['data'].append(self.dic[frm]['frame'])
            self.posts_res.save(json_dic)
        self.client_poi.disconnect()
        self.client_raw.disconnect()
        self.client_res.disconnect()
        
        
def Start2():
    r=Roadmap()

    r.getDataFromDB()
    r.getNearByRlt()

    for item in r.dic.keys():
      #  print "ITEM:%s"%item
        if not (item in r.dic):
            continue
        if r.dic[item]['circum']>=opt_circum_min:
            continue
        if r.dic[item]['link']==[]:
            continue
        for key in r.dic[item]['link']:
#            print "item:%s,key:%s"%(item,key)
            if r.dic[item]['link']==[]:
                break
            if key == item:
                continue
            if not (key in r.dic) or not (item in r.dic):
                continue
            if r.dic[item]['circum']+r.dic[key]['circum']>opt_circum_max_2:
                continue
            if r.dic[item]['circum']+r.dic[key]['circum']>opt_circum_max:
                continue
            if r.dic[item]['circum']+r.dic[key]['circum']<opt_circum_max:
                r.mergeZones(item,key)
                
    for item in r.dic.keys():
        if r.dic.get(item,'')=='':
            continue
        if r.dic[item]['circum']<opt_circum_min_2:
            for key2 in r.dic[item]['link']:
                if r.dic.get(key2,'')=='':
                    continue
                if r.dic[item]['link']==[]:
                    break
                if key2 == item:
                    continue
            for key in r.dic[item]['link']:
                if r.dic.get(key,'')=='':
                    continue
                if r.dic[item]['link']==[]:
                    break
                if key == item:
                    continue
                if r.dic[item]['circum']+r.dic[key]['circum']>opt_circum_max_2:
                    continue
                if r.dic[item]['circum']+r.dic[key]['circum']<opt_circum_max_2:
                    r.mergeZones(item,key)
    r.stResult()
'''    
def dist((x1,y1),(x2,y2)):
    return ((x2-x1)**2+(y2-y1)**2)**0.5
def Start():
    dic={}
    client,db,posts=Connect2Mongo("localhost",27017,"drawFrame","raw_frame_test")
    client4,db4,posts4=Connect2Mongo("localhost",27017,"drawFrame","raw_poi")
    # Begin to get data
    for line in posts.find():
        line_json=line['data']
        dic[line_json[0][0]]={}
        poi_json=posts4.find_one({"_id":line_json[0][0]})
        dic[line_json[0][0]]['poi']=[]
        dic[line_json[0][0]]['poi']+=poi_json['poi']
        dic[line_json[0][0]]['poi_num']=len(dic[line_json[0][0]]['poi'])
        dic[line_json[0][0]]['link']=[]
        dic[line_json[0][0]]['circum']=0
        dic[line_json[0][0]]['pos']=[line_json[1]]
        dic[line_json[0][0]]['frame']=[]
        dic[line_json[0][0]]['frame'].append(line_json[2][0])
        dic[line_json[0][0]]['corner']=[]
        for i in range(len(line_json[2][0])):
            dic[line_json[0][0]]['circum']+=dist((line_json[2][0][i%len(line_json[2][0])][0],line_json[2][0][i%len(line_json[2][0])][1]),(line_json[2][0][i%len(line_json[2][0])][2],line_json[2][0][i%len(line_json[2][0])][3]))
            ai=line_json[2][0][i%len(line_json[2][0])][2]-line_json[2][0][i%len(line_json[2][0])][0]
            bi=line_json[2][0][i%len(line_json[2][0])][3]-line_json[2][0][i%len(line_json[2][0])][1]
            aj=line_json[2][0][(i+1)%len(line_json[2][0])][2]-line_json[2][0][(i+1)%len(line_json[2][0])][0]
            bj=line_json[2][0][(i+1)%len(line_json[2][0])][3]-line_json[2][0][(i+1)%len(line_json[2][0])][1]
            ai*=1000000
            bi*=1000000
            aj*=1000000
            bj*=1000000
            try:
                if abs((ai*bj-aj*bi)/(((ai**2+bi**2)**0.5)*((aj**2+bj**2)**0.5)))<0.707:
                    continue
            except:
                continue
            dic[line_json[0][0]]['corner'].append((line_json[2][i%len(line_json[2])][2],line_json[2][i%len(line_json[2])][3]))
    #Begin to find the zones nearby
    for item in dic:
        for other in dic:
            if abs(dic[other]['pos'][0][0]-dic[item]['pos'][0][0])>0.02 or abs(dic[other]['pos'][0][1]-dic[item]['pos'][0][1])>0.02:
                continue
            if dic[other]['pos'][0]==dic[item]['pos'][0]:
                continue
            if other in dic[item]['link']:
                continue
            minDis=dist((dic[other]['frame'][0][0][0],dic[other]['frame'][0][0][1]),(dic[item]['frame'][0][0][0],dic[item]['frame'][0][0][1]));
            for j in range(len(dic[item]['frame'][0])):
                for k in range(len(dic[other]['frame'][0])):
                    minDis=min(dist((dic[other]['frame'][0][k][0],dic[other]['frame'][0][k][1]),(dic[item]['frame'][0][j][0],dic[item]['frame'][0][j][1])),minDis)
                    if minDis < 0.001:
                        break
            if minDis < 0.001:
                dic[item]['link'].append(other)
                dic[other]['link'].append(item)
            else:
                continue
    # Begin to merge the nearby zones          
    for item in dic.keys():
      #  print "ITEM:%s"%item
        if not (item in dic):
            continue
        if dic[item]['circum']>=opt_circum_min:
            continue
        if dic[item]['link']==[]:
            continue
        for key in dic[item]['link']:
#            print "item:%s,key:%s"%(item,key)
            if dic[item]['link']==[]:
                break
            if key == item:
                continue
            if not (key in dic) or not (item in dic):
                continue
            
            if dic[item]['circum']+dic[key]['circum']>opt_circum_max_2:
                continue
            if dic[item]['circum']+dic[key]['circum']>opt_circum_max:
                continue
            if dic[item]['circum']+dic[key]['circum']<opt_circum_max:
                dic[item]['circum']+=dic[key]['circum']
#                print "mergeBefore"
 #               print dic[item]['link']
  #              print dic[key]['link']
                dic[item]['link']+=dic[key]['link']
                dic[item]['link']=list(set(dic[item]['link']))
   #             print "merge"
    #            print dic[item]['link']
                try:
                    dic[item]['link'].remove(item)
                except:
                    pass
                try:
                    dic[item]['link'].remove(key)
                except:
                    pass
     #           print "adjust"
      #          print dic[item]['link']
                for d_key in dic[key]['link']:
                    try:
                        dic[d_key]['link'].remove(key)
                    except:
                        pass
                dic[item]['pos']+=dic[key]['pos']
                dic[item]['frame']+=dic[key]['frame']
                dic[item]['corner']+=dic[key]['corner']
                del dic[key]
                '''
                for u in range(len(dic[item]['frame'])-1):
                    for v in range(len(dic[item]['frame'][u])):
                        minDis=1.0
                        for c in range(len(dic[item]['frame'][-1])):
                            if c>=len(dic[item]['frame'][-1]) or v>=len(dic[item]['frame'][u]):
                                continue
                            minDis=min(dist((dic[item]['frame'][u][v][0],dic[item]['frame'][u][v][1]),(dic[item]['frame'][-1][c][0],dic[item]['frame'][-1][c][1])),minDis)
                            if minDis>0.001:
                 '''     
    for item in dic.keys():
        if dic.get(item,'')=='':
            continue

        if dic[item]['circum']<opt_circum_min_2:
            for key2 in dic[item]['link']:
                if dic.get(key2,'')=='':
                    continue
                if dic[item]['link']==[]:
                    break
                if key2 == item:
                    continue
            for key in dic[item]['link']:
                if dic.get(key,'')=='':
                    continue
                if dic[item]['link']==[]:
                    break
                if key == item:
                    continue
                if dic[item]['circum']+dic[key]['circum']>opt_circum_max_2:
                    continue
                if dic[item]['circum']+dic[key]['circum']<opt_circum_max_2:
                    dic[item]['circum']+=dic[key]['circum']
                    dic[item]['link']+=dic[key]['link']
                    dic[item]['link']=list(set(dic[item]['link']))
                    try:
                        dic[item]['link'].remove(item)
                    except:
                        pass
                    try:
                        dic[item]['link'].remove(key)
                    except:
                        pass
                    for d_key in dic[key]['link']:
                        try:
                            dic[d_key]['link'].remove(key)
                        except:
                            pass
                    dic[item]['pos']+=dic[key]['pos']
                    dic[item]['frame']+=dic[key]['frame']
                    dic[item]['corner']+=dic[key]['corner']
                    del dic[key]
                    '''
                    for u in range(len(dic[item]['frame'])-1):
                        for v in range(len(dic[item]['frame'][u])):
                            minDis=1.0
                            for c in range(len(dic[item]['frame'][-1])):
                                if c>=len(dic[item]['frame'][-1]) or v>=len(dic[item]['frame'][u]):
                                    continue
                                minDis=min(dist((dic[item]['frame'][u][v][0],dic[item]['frame'][u][v][1]),(dic[item]['frame'][-1][c][0],dic[item]['frame'][-1][c][1])),minDis)
                                if minDis<0.001:
                                    try:
                                        dic[item]['frame'][-1].remove(dic[item]['frame'][-1][c])
                                    except:
                                        pass
                                    try:
                                        dic[item]['frame'][u].remove(dic[item]['frame'][u][v])
                                    except:
                                        pass
                '''
                '''
                minDis=1
                for p in range(len(dic[item]['frame'])):
                    minDis=1
                    for q in range(len(dic[key]['frame'])):
                        minDis=min(dist((dic[key]['frame'][q][0],dic[key]['frame'][q][1]),(dic[item]['frame'][p][0],dic[item]['frame'][p][1])),minDis)

                        if minDis < 0.001 and (dic[key]['frame'][q][0],dic[key]['frame'][q][0]) in dic[key]['corner']:
                            if not (dic[key]['frame'][q][0],dic[key]['frame'][q][1]) in qcorner:
                                qcorner.append((dic[key]['frame'][q][0],dic[key]['frame']))
                        if minDis < 0.001 and (dic[item]['frame'][p][0],dic[item]['frame'][p][1]) in dic[item]['corner']:
                            if not (dic[item]['frame'][p][0],dic[item]['frame'][p][1]) in pcorner:
                                pcorner.append((dic[item]['frame'][p][0],dic[item]['frame'][p][1]))    

                    if minDis > 0.001:
                        pstart=p
                pcur=pstart
                for p in range(len(dic[item]['frame'])):
                    minDis=1.0
                    pcur=(p+pstart)%len(dic[item]['frame'])
                    if (dic[item]['frame'][pcur][0],dic[item]['frame'][pcur][1]) in pcorner :
                        qpos=0xff
                        for q in range(len(qcorner)):
                            pdist=dist((dic[item]['frame'][pcur][0],(dic[item]['frame'][pcur][1]),(q[0],q[1])))
                            if pdist<minDis:
                                minDis=pdist
                                qpos=q
                        #if qpos!=0xff:                     
                            #dic[item]['frame'].append([dic[item]['frame'][pcur][0],dic[item]['frame'][pcur][1],qcorner[qpos][0],qcorner[qpos][1]]) 
                '''
    
        #        print dic[item]['link']
       #         print "item:%s,circum:%f"%(item,dic[item]['circum'])
    # Begin to store data
    client2,db2,posts2=Connect2Mongo('localhost',27017,'drawFrame','bj_frame_test')
    posts2.drop()
    for frm in dic:
        json_dic={}
        json_dic['_id']=frm
        json_dic['lon']=dic[frm]['pos'][0][0]
        json_dic['lat']=dic[frm]['pos'][0][1]
        json_dic['data']=[]
        json_dic['data'].append([frm])
        json_dic['data'].append(dic[frm]['pos'][0])
        json_dic['data'].append(dic[frm]['frame'])
        posts2.save(json_dic)
    client.disconnect()
    client2.disconnect()


if __name__ == "__main__":
    Start()
