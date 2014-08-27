#!/usr/bin/env python

import xml.etree.cElementTree as ET
import csv
import sys
import os
import smtplib
import shutil
import MySQLdb
import re
import itertools
import xml_gen as XML
import random

from optparse import OptionParser
from subprocess import Popen, PIPE
from shlex import split


JVM='java'
SGCBJAR='SGCB_nTier.jar'
PJDUMP='pj_dump'
RSCRIPT='Rscript'


vm_map_to_service={'0'     :'lb_http',
                   '1'     :'lb_app',
                   '2'     :'lb_db',
                   '3'     :'http',
                   '4'     :'http',
                   '5'     :'app',
                   '6'     :'app',
                   '7'     :'db',
               '8'     :'db'
              }



def getArgs(string):
    return string.split(',');


def replaceWord(src, dst, word1, word2):
    inFile=open(src, "r");
    outFile=open(dst, "w");
    for line in inFile.readlines():
        newLine=line.replace(word1,word2)
    outFile.write(newLine); 

def insertExp(expName, traceFile, xmlConfFile):
    try:
        sql = "INSERT INTO exp (exp_name, exp_id_cfg, trace_file, xml_file) VALUES (%s, %s, %s, %s)"
        cur.execute(sql,(expName, 1, traceFile, xmlConfFile))
        exp_id=cur.lastrowid
        db.commit()   
    except:
        db.rollback()
    return exp_id

def updateVm(vm_name, tier_id, vm_cfg_id, exp_id):            
    try:
        sql="INSERT INTO vm (vm_name, tier_id, vm_cfg_id, exp_id ) VALUES (%s, %s, %s, %s)"
        cur.execute(sql, (vm_name, tier_id, vm_cfg_id, exp_id))
        vm_id=cur.lastrowid    
        db.commit()   
    except:
        db.rollback()
    return vm_id

def insertTrace(vm_id, start, final, duration, state, url):
    try:
        sql = "INSERT INTO trace (vm_id, ts_start, ts_final, duration, state, url) VALUES (%s, %s, %s, %s, %s, %s)"        
        cur.execute(sql,(vm_id, start, final, duration, state, url))
        trace_id=cur.lastrowid
        db.commit()   
    except:
        db.rollback()
    return trace_id

def insertStatistic(rtt_global, rtt_lb_http, rtt_http, rtt_lb_app, rtt_app, rtt_lb_db, rtt_db, throughput, cost, exp_id):
    try:
        sql = "INSERT INTO statistic (rtt_global, rtt_lb_http, rtt_http, rtt_lb_app, rtt_app, rtt_lb_db, rtt_db, throughput, cost, exp_id)       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"        
        cur.execute(sql,(rtt_global, rtt_lb_http, rtt_http, rtt_lb_app, rtt_app, rtt_lb_db, rtt_db, throughput, cost, exp_id))
        stat_id=cur.lastrowid
        db.commit()   
    except:
        db.rollback()
    return stat_id

def getId(id_col, table, col, value):
    cur.execute("SELECT "+id_col+" FROM "+table+" WHERE "+col+" = %s",value)
    return cur.fetchone()[0]
    

if __name__ == "__main__":

    usage = "usage: %prog [options] [args] "
    parser = OptionParser(usage=usage)

    parser.add_option("-f", dest="confFile", help="experiments' configuration file name", default="conf.xml")
    parser.add_option("--cb", dest="comb", help="current combination")

    (options, args) = parser.parse_args()

    if not (options.confFile):
        parser.error("You must provide the configuration file name of the experiments !")


    CONF=options.confFile
    name=options.comb

    if  not os.path.exists("log") :   
        os.mkdir("/home/Work/sgcbntier/paasage_demo/log",755);
    if  not os.path.exists("csv") :
        os.mkdir("/home/Work/sgcbntier/paasage_demo/csv",755);
    
    
    
    traceFile="ntier_"+name
    confFile="sgcb_ntier_"+name+".conf"
    xmlConfFile="test.xml"


    shutil.copyfile("/home/Work/sgcbntier/paasage_demo/sgcb_ntier.conf", "/home/Work/sgcbntier/paasage_demo/"+confFile+".tmp")

    replaceWord("/home/Work/sgcbntier/paasage_demo/"+confFile+".tmp", "/home/Work/sgcbntier/paasage_demo/"+confFile,"TRACE_FILE",traceFile);
    replaceWord("/home/Work/sgcbntier/paasage_demo/"+confFile, "/home/Work/sgcbntier/paasage_demo/"+confFile+".tmp","XML_FILE",xmlConfFile);

    shutil.move("/home/Work/sgcbntier/paasage_demo/"+confFile+".tmp", "/home/Work/sgcbntier/paasage_demo/"+confFile)

             
    os.system(JVM+" -jar /home/Work/sgcbntier/paasage_demo/"+SGCBJAR+" 100 /home/Work/sgcbntier/paasage_demo/"+confFile+" >  /home/Work/sgcbntier/paasage_demo/log/"+traceFile+".log")
    os.system(PJDUMP+" /home/Work/sgcbntier/paasage_demo/"+traceFile+".trace | grep REQTASK > /home/Work/sgcbntier/paasage_demo/csv/REQTASK_"+traceFile+".csv") 

    os.remove("/home/Work/sgcbntier/paasage_demo/"+traceFile+".trace")
    os.remove("/home/Work/sgcbntier/paasage_demo/"+confFile)
    os.remove("/home/Work/sgcbntier/paasage_demo/"+xmlConfFile)
    os.remove("/home/Work/sgcbntier/paasage_demo/"+traceFile+".plist")
        

                   

