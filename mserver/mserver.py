#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# 说明: 作为节点服务端，接受来自客户端的请求，执行相应操作，然后返回即可
# 
# 

import time
import os, sys, atexit
from signal import SIGTERM 
from rpyc import Service
from rpyc.utils.server import ThreadedServer
import logging
from libs.libraries import *
from config import *
from elasticsearch import Elasticsearch

# 定义服务器端模块存放路径
sysdir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.sep.join((sysdir, 'modules/' + AUTO_PLATFORM)))

    
class Daemon():
    #def __init__(self, pidfile, stdin=stdin, stdout=stdout, stderr=stderr):
    def __init__(self, pidfile, stdin, stdout, stderr, logger):
        self.pidfile= pidfile 
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.logger = logger

    #创建守护子进程
    def _daemonize(self):
        try:
            pid = os.fork()
            # 退出主进程
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.wirte('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
            logger.info('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)
        os.chdir("/")
        os.umask(0)
        os.setsid()
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.wirte('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            logger.info('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)
        #进程已经是守护进程，重定向标准文件描述符
        for f in sys.stdout, sys.stderr: f.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout,'a+')
        se = file(self.stderr,'a+',0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
           
        #创建processid文件
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'aw+').write('%s\n' % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def _run(self):
        message = 'Starting m_server daemon...' 
        logger.info("%s" % message)
        # 开启监听服务
        while True:
            try:
                s = ThreadedServer(ManagerService, port=PORT, auto_register=False)  
                # 启动rpyc服务监听、接受、响应请求
                s.start()
            except Exception, e:
                message = 'Starting service error!'
                logger.error(message)
                sys.exit()
                
   
    def start(self):
        #检查pid文件是否存在以探测是否存在进程
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        
        if pid:
            message = 'pidfile %s already exist. Daemon already running?' % self.pidfile
            logger.warning("%s" % message)
            sys.stderr.write(message+'\n')
            sys.exit(1)
        else:
            message = 'Starting m_server daemon...' 
            sys.stdout.write(message+'\n')

        #启动监控
        self._daemonize()
        self._run()

    def status(self):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        
        if pid:
            message = 'mserver is [\033[0;32;1mrunning\033[0m]...'
        else:
            message = "mserver is [\033[1;31;1mstopped\033[0m]."
        sys.stderr.write(message+'\n')

    def stop(self):
        #读取pid
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid:
            message = 'pidfile %s does not exist. Daemon not running?' % (self.pidfile)
            logger.warning("%s" % message)
            sys.stderr.write(message+'\n')
            return #重启不报错
        #杀进程
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
                message = "Stop listening services...."
                # print "[%s] Stop listening all services...." % end_time
                sys.stdout.write(message + '\n')
                logger.info("%s" % message)
        except OSError, err:
            err = str(err)
            if err.find('No such process') > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                logger.info("%s" % str(err))
                sys.exit(1)
    
    def restart(self):
        self.stop()
        self.start()

# 服务器管理类
class ManagerService(Service):
    # 定义login认证方法，对外开放调用的方法，对于服务端来说， 只有以"exposed_"打头的方法才能被客户端调用，所以要提供给客户端的方法都得加"exposed_" ，客户端使用login()即可
    def exposed_login(self, user, passwd):
    # 认证结果标记变量，值为“True”，则为通过认证，反之为认证失败。
	if user == USER and passwd == PASSWD:
            self.Checkout_pass = True
        else:
            self.Checkout_pass = False
        return self.Checkout_pass
    
    def exposed_Runcommands(self, get_string):
        # 权限验证
        try:	        
            if self.Checkout_pass != True:
                return m_encode(r"用户验证失败!", SECRET_KEY)
        except:
	    return m_encode(r"非法登录!", SECRET_KEY)
        # get_string 从客户端发送过来的请求，格式要求：模块id##serverip#domain##platname##game_name##action,以##为分割，在web页面构造请求
        # 获取rpyc client的请求串get_string, 通过m_decode方法解密再分割
        # 重要部分，解析需要处理的机器
        self.get_string_array = m_decode(get_string, SECRET_KEY).split('&') 
        # ['action=search', 'loglevel=warn', 'logtime=1s']
        #print self.get_string_array
        # 解析需要处理的机器，生成list
        self.need_to_handle = []
        for i in range(len(self.get_string_array)): 
            self.get_detail = self.get_string_array[i].split('=')
            if 'action' in self.get_detail[0]:
                action = self.get_detail[1]
            elif 'loglevel' in self.get_detail[0]:
                loglevel = self.get_detail[1].upper()
            elif 'logtime' in self.get_detail[0]:
                logtime = self.get_detail[1]
            else:
                print "wrong params"
                returnString = "No paltform"
        mid = "action"
        importsting = "from modules." + mid + " import Modulehandle"
        try:
            # 导入模块
            exec importsting
        except Exception,e :
            print e
            return m_encode(u"module\"" + mid + u"\" does not exist, please add it", SECRET_KEY)
            # return "errror"
        # 调用模块相关方法，下发任务
        self.need_to_handle = {'server':ESEVER, 'eport':EPORT, 'action':action,'loglevel':loglevel,'logtime':logtime}
        Runobj = Modulehandle(self.need_to_handle)
        #returnString = el_search(ESEVER, EPORT, action,loglevel,logtime)
        #Runmessages = 'kkj'
        Runmessages = Runobj.run()
        #Runmessages = {'abc':'sdd'}
        # time.sleep(10)
        # 对返回给rpyc client的数据串加密
        return m_encode(str(Runmessages), SECRET_KEY)


if __name__ == "__main__":
    # 启用日志系统记录
    log_path = sys.path[0] + '/logs/sys.log'
    logging.basicConfig(level = logging.INFO,
                format = '[%(asctime)s] [%(levelname)-4s] %(message)s',
                filename = '%s' % log_path,
                filemode = 'a')
    logger = logging.getLogger("logging")


    # 检查配置文件
    # path_pre = os.getcwd()
    config= sys.path[0] + '/' + 'config.py'
    if not os.path.exists(config):
        logger.error('configure file %s does not exists, no such file.' % (config))
        print "Open %s failed, no such file." % (config)
        sys.exit()

    if len(sys.argv) == 2:
        daemon = Daemon('/tmp/mserver_process.pid', '/dev/null', '%s' % log_path, '%s' % log_path, logger)
        if sys.argv[1] == 'start':
            daemon.start()
        elif sys.argv[1] == 'stop':
            daemon.stop()
        elif  sys.argv[1] == 'restart':
            daemon.restart()
        elif sys.argv[1] == 'status':
            daemon.status()            
        else:
            print 'Usage: %s (start|stop|status|restart)' % sys.argv[0]
            sys.exit(0)
        sys.exit(0)
    else:
        print 'Usage: %s (start|stop|status|restart)' % sys.argv[0]
        sys.exit(2)
