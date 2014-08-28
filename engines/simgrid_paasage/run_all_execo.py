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
import xml_gen_execo as XML
import random

from optparse import OptionParser
from subprocess import Popen, PIPE
from shlex import split


JVM='java'
SGCBJAR='SGCB_nTier.jar'
PJDUMP='pj_dump'
RSCRIPT='Rscript'


def replaceWord(src, dst, word1, word2):
	inFile=open(src, "r")
	outFile=open(dst, "w")
	for line in inFile.readlines():
		newLine=line.replace(word1,word2)
		outFile.write(newLine) 
  

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

	if  not os.path.exists("/home/Work/sgcbntier/paasage_demo/log") :   
		os.mkdir("/home/Work/sgcbntier/paasage_demo/log",755);
	if  not os.path.exists("/home/Work/sgcbntier/paasage_demo/csv") :
		os.mkdir("/home/Work/sgcbntier/paasage_demo/csv",755);



	traceFile="ntier_"+name
	confFile="sgcb_ntier_"+name+".conf"
	xmlConfFile="exp_"+name


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
		

                   

