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

db = MySQLdb.connect(host="localhost", 
		     user="root", 
		     passwd="sergiu1806", 
		     db="testAws", 
		     unix_socket="/opt/lampp/var/mysql/mysql.sock"
		    ) 
cur = db.cursor() 


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

	(options, args) = parser.parse_args()

	if not (options.confFile):
		parser.error("You must provide the configuration file name of the experiments !")


	CONF=getArgs(options.confFile)

	if  not os.path.exists("log") :   
		os.mkdir("log",755);
	if  not os.path.exists("csv") :
		os.mkdir("csv",755);
	

	traceFile="ntier_"+name
	confFile="sgcb_ntier_"+name+".conf"
	xmlConfFile="test.xml"


	shutil.copyfile("sgcb_ntier.conf", confFile+".tmp")

	replaceWord(confFile+".tmp", confFile,"TRACE_FILE",traceFile);
	replaceWord(confFile, confFile+".tmp","XML_FILE",xmlConfFile);

	shutil.move(confFile+".tmp", confFile)

		     
	os.system(JVM+" -jar "+SGCBJAR+" 100 "+confFile+" > log/"+traceFile+".log")
	os.system(PJDUMP+" "+traceFile+".trace | grep REQTASK > csv/REQTASK_"+traceFile+".csv") 

	os.remove(traceFile+".trace")
	os.remove(confFile)
	os.remove(xmlConfFile)
	os.remove(traceFile+".plist")
	    

cur.close()
db.close()
               	


