#!/usr/bin/python

import pickle
import json

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
    f_in=open("region_level_1_test",'r')
    f_out=open("format_reg.txt",'w')
    dic={}
    for line in f_in.readlines():
        line_json=json.loads(line)
        dic[line_json[0][0]]={}
        dic[line_json[0][0]]['link']=[]
        dic[line_json[0][0]]['circum']=0
        dic[line_json[0][0]]['pos']=[line_json[1]]
        dic[line_json[0][0]]['frame']=line_json[2]
        dic[line_json[0][0]]['corner']=[]
        for i in range(len(line_json[2])):
            dic[line_json[0][0]]['circum']+=dist((line_json[2][i%len(line_json[2])][0],line_json[2][i%len(line_json[2])][1]),(line_json[2][i%len(line_json[2])][2],line_json[2][i%len(line_json[2])][3]))
            ai=line_json[2][i%len(line_json[2])][2]-line_json[2][i%len(line_json[2])][0]
            bi=line_json[2][i%len(line_json[2])][3]-line_json[2][i%len(line_json[2])][1]
            aj=line_json[2][(i+1)%len(line_json[2])][2]-line_json[2][(i+1)%len(line_json[2])][0]
            bj=line_json[2][(i+1)%len(line_json[2])][3]-line_json[2][(i+1)%len(line_json[2])][1]
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

    for item in dic:
        for other in dic:
            if abs(dic[other]['pos'][0][0]-dic[item]['pos'][0][0])>0.1 and abs(dic[other]['pos'][0][1]-dic[item]['pos'][0][1])>0.1:
                continue
            if dic[other]['pos'][0]==dic[item]['pos'][0]:
                continue
            if other in dic[item]['link']:
                continue
            minDis=dist((dic[other]['frame'][0][0],dic[other]['frame'][0][1]),(dic[item]['frame'][0][0],dic[item]['frame'][0][1]));
            for j in range(len(dic[item]['frame'])):
                for k in range(len(dic[other]['frame'])):
                    minDis=min(dist((dic[other]['frame'][k][0],dic[other]['frame'][k][1]),(dic[item]['frame'][j][0],dic[item]['frame'][j][1])),minDis)
                    if minDis < 0.001:
                        break
            if minDis < 0.001:
                dic[item]['link'].append(other)
                dic[other]['link'].append(item)
            else:
                continue 
    for item in dic.keys():
        print "ITEM:%s"%item
        if not (item in dic):
            continue
        if dic[item]['circum']>=opt_circum_min:
            continue
        if dic[item]['link']==[]:
            continue
        for key in dic[item]['link']:
            print "item:%s,key:%s"%(item,key)
            if dic[item]['link']==[]:
                break
            if key == item:
                continue
            if not (key in dic) or not (item in dic):
                continue
            if dic[item]['circum']+dic[key]['circum']>opt_circum_max_2:
                break
            if dic[item]['circum']+dic[key]['circum']>opt_circum_max:
                break
            if dic[item]['circum']+dic[key]['circum']<opt_circum_min:
                dic[item]['circum']+=dic[key]['circum']
                print "mergeBefore"
                print dic[item]['link']
                print dic[key]['link']
                dic[item]['link']+=dic[key]['link']
                dic[item]['link']=list(set(dic[item]['link']))
                print "merge"
                print dic[item]['link']
                try:
                    dic[item]['link'].remove(item)
                except:
                    pass
                try:
                    dic[item]['link'].remove(key)
                except:
                    pass
                print "adjust"
                print dic[item]['link']
                for d_key in dic[key]['link']:
                    try:
                        dic[d_key]['link'].remove(key)
                    except:
                        pass
                dic[item]['pos']+=dic[key]['pos']
                dic[item]['corner'].append(dic[key]['corner'])
                dic[item]['frame'].append(dic[key]['frame'])
                del dic[key]
                print dic[item]['link']
                print "item:%s,circum:%f"%(item,dic[item]['circum'])
            
    data_json=json.dumps(dic,ensure_ascii=False).encode('utf-8','ignore')
    f_out.write(data_json)

if __name__ == "__main__":
    Start()
