#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os
#sys.path.insert(0, os.path.join(sys.path[0], 'libs/public_libs/swall'))

#from libs.public_libs.swall.swall.parser import Swall
from libs.libraries import *
from elasticsearch import Elasticsearch

class Modulehandle():
    def __init__(self, sys_param_row): # 初始化方法
        self.Runresult = ""
        self.sys_param_row = sys_param_row

    def el_search(self, eserver, eport, action, logtype, logtime):
        es = Elasticsearch('%s:%d'%(eserver, int(eport)))
        page = es.search(
            index = 'logstash-*',
            doc_type = 'fluentd',
            scroll = '2m',
            search_type = 'scan',
            size = 10000,
            body={
                'query':{
                    'filtered': {
                'query':{
                     'match':{
                         'level':'%s' % logtype
                     }
                },
                        'filter':{
                             'range': {
                                 '@timestamp':{'gt':'now-%s' % logtime}
                             }
                        }
                    }
                 }
        
             }
        )
        
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        self.data = []
        while(scroll_size > 0):
            #print "Scrolling..."
            page = es.scroll(scroll_id = sid, scroll = '2m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            if scroll_size == 0:
                #print "scroll size: " + str(scroll_size)
                break
            else:
                self.data.append(page)
        return self.data

    def run(self):
        try:
            if len(self.sys_param_row) < 1:
                return self.Runresult
            self.eserver = self.sys_param_row['server']
            self.eport = self.sys_param_row['eport']
            self.action= self.sys_param_row['action']
            self.loglevel = self.sys_param_row['loglevel']
            self.logtime = self.sys_param_row['logtime']
            
            #self.Runresult = self.el_search(self.eserver, self.eport, self.action, self.loglevel, self.logtime)
#            el_search(server='10.96.33.38', port='9200', action='search',log_type='INFO', log_time='1m'):
    
            #self.Runresult = self.el_search(self.eserver, self.eport, self.action , self.loglevel , self.logtime)
#            print type(self.el_search(self.eserver, self.eport, self.action , self.loglevel , self.logtime))
            self.Runresult= self.el_search(self.eserver, self.eport, self.action , self.loglevel , self.logtime)
#            self.bbbb = self.el_search(self.eserver, self.eport, self.action , self.loglevel , self.logtime)
#            print type(self.bbbb)
            #self.Runresult = self.el_search()
            if len(self.Runresult) == 0:
                return "No data found!"
        except Exception,e:
             return str(e)
        # 返回执行结果
        return self.Runresult
