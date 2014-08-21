import xml.etree.cElementTree as ET
import xml.dom.minidom as DOM
import shutil
import lxml.etree as le
import re
import itertools
	

def initXML():
	
	root = ET.Element("nTierApplication")
	root.set("version", "1")

	ami = ET.SubElement(root, "AMI")

	field1 = ET.SubElement(ami, "webService")
	field1.set("size", "10000000000.0")
	field2 = ET.SubElement(ami, "appsService")
	field2.set("size", "10000000000.0")
	field3 = ET.SubElement(ami, "dbService")
	field3.set("size", "10000000000.0")
	field4 = ET.SubElement(ami, "webProxy")
	field4.set("size", "10000000000.0")
	field5 = ET.SubElement(ami, "appsProxy")
	field5.set("size", "10000000000.0")
	field6 = ET.SubElement(ami, "dbProxy")
	field6.set("size", "10000000000.0")

	proxy = ET.SubElement(root, "proxy")

	field7 = ET.SubElement(proxy, "webProxy")
	field7.set("region", "eu_1")
	field7.set("instanceType","m1.small")

	field8 = ET.SubElement(proxy, "appsProxy")
	field8.set("region", "eu_1")
	field8.set("instanceType","m1.small")

	field9 = ET.SubElement(proxy, "dbProxy")
	field9.set("region", "eu_1")
	field9.set("instanceType","m1.small")


	return root

def createService(parent, name):
	tmp = ET.SubElement(parent, name)
	return tmp

def createRegion(parent, name):
	tmp = ET.SubElement(parent, "region")
	tmp.set("name", name )
	return tmp

def createInstance(parent, ty, qt):
	tmp = ET.SubElement(parent, "instance")
	tmp.set("quantity", qt )
	tmp.set("type", ty )
	return tmp	


def generateExp(lis, rootSrc):
	root=initXML()
	servParent=ET.SubElement(root, "services")
	servWeb=createService(servParent,"webService")
	servApp=createService(servParent,"appsService")
	servDb=createService(servParent,"dbService")

	i=0

	web=rootSrc.find("webService")
	if (web==None):
		print "webService tag not found!"
		exit(1)
	for child1 in web.iter("region"):
		regionTmp=createRegion(servWeb, child1.get("name"))
		for child2 in child1.iter("instance"):	
			if (lis[i] != '0'):
				createInstance(regionTmp, child2.get("type"), lis[i])
				i+=1
			else:
				i+=1
				continue
		if not regionTmp.getchildren():
			servWeb.remove(regionTmp)
	if(not servWeb.getchildren()):
		print "ERROR: Web service does not has any vm instance associated for first experiment"
		exit(2)		
		
	app=rootSrc.find("appsService")
	if (app==None):
		print "ERROR: appsService tag not found!"
		exit(1)
	for child1 in app.iter("region"):
		regionTmp=createRegion(servApp, child1.get("name"))
		for child2 in child1.iter("instance"):		
			if (lis[i] != '0'):
				createInstance(regionTmp, child2.get("type"), lis[i])
				i+=1
			else:
				i+=1
				continue
		if not regionTmp.getchildren():
			servApp.remove(regionTmp)
	if(not servApp.getchildren()):
		print "ERROR: Apps Service does not has any vm instance associated for first experiment"
		exit(2)
		
			

	db=rootSrc.find("dbService")
	if (db==None):
		print "ERROR: dbService tag not found!"
		exit(1)
	for child1 in db.iter("region"):
		regionTmp=createRegion(servDb, child1.get("name"))
		for child2 in child1.iter("instance"):	
			if (lis[i] != '0'):
				createInstance(regionTmp, child2.get("type"), lis[i])
				i+=1
			else:
				i+=1
				continue
		if not regionTmp.getchildren():	
   			servDb.remove(regionTmp)
	if(not servDb.getchildren()):
		print "ERROR: Db service does not has any vm instance associated for first experiment"
		exit(2)		


	xml_string=ET.tostring(root, encoding='utf8', method='xml')
	xml = DOM.parseString(xml_string) 
	pretty_xml_as_string = xml.toprettyxml()
	outFile=open("test.xml", "w")
	outFile.write(pretty_xml_as_string)





if __name__ == "__main__":

	
	tree = ET.parse("conf.xml")
	rootSrc = tree.getroot()


	usage = "usage: %prog [options] [args] "
	parser = OptionParser(usage=usage)

	parser.add_option("-cb", dest="comb", help="current combination")

	(options, args) = parser.parse_args()

	if not (options.comb):
		parser.error("You must provide parameters for the experiment !")


	comb=getArgs(options.comb)

	comb_list=comb.split("-")

	generateExp(comb_list, rootSrc) 
	



