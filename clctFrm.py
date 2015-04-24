#!/usr/bin/python

import pickle
import json
def Start():
    f_in=open("region_level_1",'r')
    f_out=open("format_reg.txt",'w')
    count=0
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
if __name__ == "__main__":
    Start()
