#!/usr/bin/python

import pickle
import json
from util import *
import math

ERROR='0'
RIGHT='1'

opt_circum_min=0.08
opt_circum_max=0.1
opt_circum_min_2=0.03
opt_circum_max_2=0.13
opt_poi_max=320
opt_poi_max_2=400
blk_size=0.002
line_num=100
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
    min_pos_x=116.0
    min_pos_y=40.0
    max_pos_x=116.0
    max_pos_y=40.0
    for line in posts.find():
        line_json=line['data']
        dic[line_json[0][0]]={}
        poi_json=posts4.find_one({"_id":line_json[0][0]})
        dic[line_json[0][0]]['poi']=[]
        dic[line_json[0][0]]['poi']+=poi_json['poi']
        dic[line_json[0][0]]['poi_num']=len(dic[line_json[0][0]]['poi'])
        dic[line_json[0][0]]['link']=[]
        dic[line_json[0][0]]['circum']=0.0
        dic[line_json[0][0]]['pos']=[line_json[1]]
        dic[line_json[0][0]]['frame']=[]
        dic[line_json[0][0]]['frame'].append(line_json[2][0])
        dic[line_json[0][0]]['index']=[]
        if line_json[1][0]<min_pos_x:
            min_pos_x=line_json[1][0]
        if line_json[1][0]>max_pos_x:
            max_pos_x=line_json[1][0]
        if line_json[1][1]<min_pos_y:
            min_pos_y=line_json[1][1]
        if line_json[1][1]>max_pos_y:
            max_pos_y=line_json[1][1]
    map_index=[[[] for i in range(int(math.ceil((max_pos_x-min_pos_x)/blk_size+100)))] for j in range(int(math.ceil(max_pos_y-min_pos_y)/blk_size+100))]
    for item in dic.keys():
        for j in range(len(dic[item]['frame'][0])):
            xbeg=dic[item]['frame'][0][j][0]
            ybeg=dic[item]['frame'][0][j][1]
            xend=dic[item]['frame'][0][j][2]
            yend=dic[item]['frame'][0][j][3]
            dic[item]['circum']+=dist((xbeg,ybeg),(xend,yend))
       # if dic[item]['circum']<0.005:
           # del dic[item]
            
    for item in dic:
        for j in range(len(dic[item]['frame'][0])):
            xbeg=dic[item]['frame'][0][j][0]
            ybeg=dic[item]['frame'][0][j][1]
            xend=dic[item]['frame'][0][j][2]
            yend=dic[item]['frame'][0][j][3]
            x=int(math.floor((xbeg-min_pos_x)/blk_size))
            y=int(math.floor((ybeg-min_pos_y)/blk_size))
            if item in map_index[x][y]:
                continue
            else:
                for key in map_index[x][y]:
                    if not key in dic[item]['link']:
                        dic[item]['link'].append(key)
                    if not item in dic[key]['link']:
                        dic[key]['link'].append(item)
                map_index[x][y].append(item)
                dic[item]['index'].append((x,y))
            
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
            if dic.get(key,'')=='':
                continue
            if dic.get(item,'')=='':
                break
            if dic[item]['link']==[]:
                break
            if key == item:
                continue
            if dic[item]['frame'][0]==[]:
                continue
            if not (key in dic) or not (item in dic):
                continue
            
            if dic[item]['circum']+dic[key]['circum']>opt_circum_max_2:
                continue
            if dic[item]['circum']+dic[key]['circum']>opt_circum_max:
                continue
            if dic[item]['circum']+dic[key]['circum']<opt_circum_max:
                frame=[]
                
                minItemPos=[]
                minKeyPos=[]
                minPos=[]
                minDist=1.0
                for seg_line in dic[item]['frame'][0]:
                    ikdist=dist((seg_line[0],seg_line[1]),(dic[key]['pos'][0][0],dic[key]['pos'][0][1]))
                    if ikdist<minDist:
                        minItemPos=[seg_line[0],seg_line[1]]
                        minDist=ikdist
                minDist=1.0
                for seg_line in dic[key]['frame'][0]:
                    ikdist=dist((seg_line[0],seg_line[1]),(dic[item]['pos'][0][0],dic[item]['pos'][0][1]))
                    if ikdist<minDist:
                        minKeyPos=[seg_line[0],seg_line[1]]
                        minDist=ikdist
                #minPos=[(minItemPos[0]+minKeyPos[0])/2,(minItemPos[1]+minKeyPos[1])/2]
                minPos=minItemPos
                angle=[[] for i in range(line_num)]
                '''
                for i in range(line_num):
                    agl=(2*math.pi/line_num)*i
                    if abs(agl-math.pi/2)<0.0001 or abs(agl-math.pi*3/2)<0.0001:
                        continue
                    for seg_line in dic[item]['frame'][0]:
                        ai=(seg_line[0]-minPos[0])*100000
                        bi=(seg_line[1]-minPos[1])*100000
                        aj=(seg_line[2]-minPos[0])*100000
                        bj=(seg_line[3]-minPos[1])*100000
                        if (bi-ai*math.tan(agl))*(bj-aj*math.tan(agl))<0 and (seg_line[0]+seg_line[1]*math.tan(agl))>0:
                            if angle[i]==[] or dist((minPos[0],minPos[1]),(angle[i][0],angle[i][1])) < dist((minPos[0],minPos[1]),(seg_line[0],seg_line[1])):
                                angle[i]=[seg_line[0],seg_line[1]]
                    for seg_line in dic[key]['frame'][0]:
                        ai=(seg_line[0]-minPos[0])*100000
                        bi=(seg_line[1]-minPos[1])*100000
                        aj=(seg_line[2]-minPos[0])*100000
                        bj=(seg_line[3]-minPos[1])*100000
                        if (bi-ai*math.tan(agl))*(bj-aj*math.tan(agl))<0 and (seg_line[0]+seg_line[1]*math.tan(agl))>0:
                            if angle[i]==[] or dist((minPos[0],minPos[1]),(angle[i][0],angle[i][1])) < dist((minPos[0],minPos[1]),(seg_line[0],seg_line[1])):
                                angle[i]=[seg_line[0],seg_line[1]]
                angle=[e for e in angle if e!=[]]
                for i in range(len(angle)):
                    frame.append(angle[i%len(angle)]+angle[(i+1)%len(angle)])
 
                '''
                for i in range(line_num):
                    if i==line_num/4 or i==line_num/4*3:
                        continue
                    print 'item'
                    for seg_line in dic[item]['frame'][0]:
                        ai=(seg_line[0]-minPos[0])*1000000
                        bi=(seg_line[1]-minPos[1])*1000000
                        aj=(seg_line[2]-minPos[0])*1000000
                        bj=(seg_line[3]-minPos[1])*1000000
                        print (bi-math.tan(i*2*math.pi/line_num)*ai)*(bj-math.tan(i*2*math.pi/line_num)*aj)
                        if (i<=line_num/2 and bi>0) or (i>line_num/2 and bi <0):     
                            if (bi-math.tan(i*2*math.pi/line_num)*ai)*(bj-math.tan(i*2*math.pi/line_num)*aj)<0:
                                if angle[i]==[] or dist((minPos[0],minPos[1]),(angle[i][0],angle[i][1])) < dist((minPos[0],minPos[1]),(seg_line[0],seg_line[1])):
                                    angle[i]=[seg_line[0],seg_line[1]]
                                    print angle[i]
                    print 'key'
                    for seg_line in dic[key]['frame'][0]:
                        ai=(seg_line[0]-minPos[0])*1000000
                        bi=(seg_line[1]-minPos[1])*1000000
                        aj=(seg_line[2]-minPos[0])*1000000
                        bj=(seg_line[3]-minPos[1])*1000000
                        print (bi-math.tan(i*2*math.pi/line_num)*ai)*(bj-math.tan(i*2*math.pi/line_num)*aj)
                        if (i<=line_num/2 and bi>0) or (i>line_num/2 and bi <0):     
                            if (bi-math.tan(i*2*math.pi/line_num)*ai)*(bj-math.tan(i*2*math.pi/line_num)*aj)<0:
                                if angle[i]==[] or dist((minPos[0],minPos[1]),(angle[i][0],angle[i][1])) < dist((minPos[0],minPos[1]),(seg_line[0],seg_line[1])):
                                    angle[i]=[seg_line[0],seg_line[1]]
                                    print angle[i]
                angle=[e for e in angle if e!=[]]
                for i in range(len(angle)):
                    frame.append(angle[i%len(angle)]+angle[(i+1)%len(angle)])
                
                dic[item]['circum']+=dic[key]['circum']
                dic[item]['link']+=dic[key]['link']
                dic[item]['link']=list(set(dic[item]['link']))
                dic[item]['index']+=dic[key]['index']
                dic[item]['frame'][0]=frame
                #dic[item]['frame']+=dic[key]['frame']
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
                del dic[key]
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

        #        print dic[item]['link']
       #         print "item:%s,circum:%f"%(item,dic[item]['circum'])



if __name__ == "__main__":
    Start()
