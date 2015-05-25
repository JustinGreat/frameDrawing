import os
import sys
CLC_FRM_DATA_SERVER_IP="10.16.18.96"

def Start(server_num):
    for i in range(1,server_num+1):
        os.system('nohup python ClcFrmDataServer.py %s 2443%s %s > ./nohup/nohup_clcfrmdataserver_%s 2>&1 &'%(CLC_FRM_DATA_SERVER_IP,i,i,i))

def GetPid(line):
    line_split = line.split(' ')
    for pid in line_split[1:]:
        if pid != '':
            return pid

def Stop(param):
    lfile = "./nohup/clcfrmdataserver_pid"
    os.system('ps -ef | grep %s > %s'%(param,lfile))
    lines = [line for line in file(lfile)]

    for line in lines:
        if line.find('python %s'%param) != -1:
            pid = GetPid(line)
            os.system('kill -9 %s'%pid)

if __name__ == '__main__':
    oper = sys.argv[1]
    if oper == 'start':
        server_num = int(sys.argv[2])
        Start(server_num)
    elif oper == "stop":
        Stop('ClcFrmDataServer.py')
    else:
        print 'unknown oper'
