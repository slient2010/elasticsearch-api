#!/usr/bin/python
# -*- coding:utf-8 -*-
# 
# 说明： 客户端，集成到web或者独立使用。
# 
# 

import sys
import rpyc
from libs.libraries import *
from config import *

# import logging
# 常量定义，集成到web时候，可以不要config.py
# SECRET_KEY="asdfklasjdfkdsjkfjjiozji"
# SERVER='127.0.0.1'
# PORT=9911
# USER="admin"
# PASSWD="abcdefghijklmnopqrstuvwxyz"

try:
    # paramters host, port
    conn =rpyc.connect(SERVER, PORT)
    # 是服务端的那个以"exposed_"开头的方法
    cResult =conn.root.login("%s" % USER, "%s" % PASSWD)    
except Exception, e:
    cResult = False
    print 'Connect to rpyc server error:' + str(e)
    sys.exit()

# 对请求数据串使用m_encode方法加密
# params
# 'action', action type
# 'loglevel', 'info, warn, error' are available
# 'logtime', recent time log.
put_string="action=search&loglevel=INFO&logtime=30s"

put_string = m_encode(put_string, SECRET_KEY)
if cResult:
    # 调用rpyc Server的Runcommands方法实现功能模块的任务下发，返回结果使用m_decode进行解密
    try:
        cResult =eval(m_decode(conn.root.Runcommands(put_string), SECRET_KEY))
        if cResult:
            print eval(cResult)
        else:
            print "no data"
    except Exception,e:
        print "秘钥异常，或是%s" % e
    conn.close()
else:
    print "用户验证失败，请重试！"
