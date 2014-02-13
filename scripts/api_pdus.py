#!/usr/bin/env python
from pprint import pprint
from execo import logger
from execo.log import style
from execo_g5k import get_g5k_sites, get_resource_attributes

sites = sorted(get_g5k_sites())
#sites = ['reims']

for site in sites:
    log = 'Site '+ style.emph(site)
    try:
        pdus = get_resource_attributes('/sites/'+site+'/pdus/')['items']        
        for pdu in get_resource_attributes('/sites/'+site+'/pdus/')['items']:            
            try:
                log += '\n'+style.host(pdu['uid'])
                
                if pdu.has_key('sensors'):
#                    if site == 'rennes':
#                        pprint(pdu['sensors'])
                    for sensor in pdu['sensors']:
                        if sensor.has_key('power'):
                            if not isinstance(sensor['power'], dict):
                                log += ' sensor[\'power\'] has '+type(sensor['power']).__name__+' instead of dict'
                                exit()
                            else:
                                if not sensor['power']['per_outlets']:
                                    log += ' global'
                                    if sensor['power']['snmp']['available']:
                                        if sensor['power']['snmp'].has_key('total_oids'):                                        
                                            log += ' '+sensor['power']['snmp']['total_oids'][0]
                                        else:
                                            log += ' no total_oids'
                                    else:
                                        log += ' not available'
                                else:
                                    log += ' per_outlets'
                                    if sensor['power']['snmp']['available']:
                                        if sensor['power']['snmp'].has_key('outlet_prefix_oid'):                                            
                                            log += ' '+sensor['power']['snmp']['outlet_prefix_oid']
                                            if not isinstance(sensor['power']['snmp']['total_oids'], list):
                                                log += ' [\'total_oids\'] has type '+ type(sensor['power']['snmp']['total_oids']).__name__
                                                exit()
                                            else:    
                                                log += ' ('+', '.join( [ oid for oid in sensor['power']['snmp']['total_oids'] ])+')' 
#                                                log += ' ('+','.join( [ oid[-1] for oid in sensor['power']['snmp']['total_oids'] ])+')'

                                        else:
                                            log += ' no outlet_prefix_oid'
                                    else:
                                        log += ' not available'
                        else:
                            log +=' No power in sensor ?'
                
            except:
                log += style.report_error(' Wrong description')
            
    except:
        log +='\nNo PDU URL'
    
    logger.info(log)
