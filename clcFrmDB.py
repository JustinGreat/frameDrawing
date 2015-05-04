#!/usr/bin/python

import pickle
import json
from util import *
'''
    count=0
    lon=116.413332-0.2
    lat=39.902043-0.1
    lon2=116.413332+0.2
    lat2=39.902043+0.2
    for line in f_in.readlines():
        item=json.loads(line)
        if item[1][0]>lon and item[1][1] >lat and item[1][0]<lon2 and item[1][1] <lat2:
            data_back=json.dumps(item,ensure_ascii=False).encode('utf-8','ignore')
            f_out.write(data_back+'\n')
'''
'''
    dic={}
    for line in f_in.readlines():
        item=line.split('],[')
        item[0]=item[0][3:-1]
        item[2]=item[2][1:]
        item[-1]=item[-1][:-4]
        dic[item[0]]={}
        dic[item[0]]['pos']=(float(item[1].split(',')[0]),float(item[1].split(',')[0]))
        dic[item[0]]['frame']=[]
        dic[item[0]]['frame'].append({'loS':float(item[2].split(',')[0]),'laS':float(item[2].split(',')[1]),'LoE':float(item[2].split(',')[2]),'LaE':float(item[2].split(',')[3])})
    
    data_json=json.dumps(dic,ensure_ascii=False).encode('utf-8','ignore')
    f_out.write(data_json)
'''
opt_circum_min=0.08
opt_circum_max=0.1
opt_circum_min_2=0.03
opt_circum_max_2=0.13
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
        print line_json[0][0]
        poi_json=posts4.find_one({"_id":line_json[0][0]})
        print poi_json
        dic[line_json[0][0]]['poi']=[]
        dic[line_json[0][0]]['poi']+=poi_json['poi']
        dic[line_json[0][0]]['link']=[]
        dic[line_json[0][0]]['circum']=0
        dic[line_json[0][0]]['pos']=[line_json[1]]
        dic[line_json[0][0]]['frame']=[]
        dic[line_json[0][0]]['frame'].append(line_json[2][0])
        dic[line_json[0][0]]['corner']=[]
    #Find corners and judge which zone the poi is in.
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
    
    for item in dic.keys():
        if dic.get(item,'')=='':
            continue

        if dic[item]['circum']<0.03:
            print "item"
            print dic[item]['circum']
            print 'link'
            for key2 in dic[item]['link']:
                if dic.get(key2,'')=='':
                    continue
                if dic[item]['link']==[]:
                    break
                if key2 == item:
                    continue
                print dic[key2]['circum']
            print ''
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
                minDis=1
                pcorner=[]
                qcorner=[]
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
