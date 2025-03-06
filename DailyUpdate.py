# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 11:29:37 2021

@author: ywang1
"""

import sys
import datetime
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger

sys.path.append('C:\\AnalystCode\\YWang\\class') 
from FlowMA10V11 import LNG_MA10
from FlowMA30V3 import LNG_MA30
from Freight import Freight
from MA10ChartV6 import FlowMA10Chart
from MA30Chart import FlowMA30Chart
from diff_tableV5 import DiffTable
from SuezRegV2 import SuezRegression
from cargo_trackerV4 import Cargo_traker
from IHSCapa import IHS_capa
from KplerHistV1 import Kpler_hist_data
from JKCdemandV4 import JKC_Demand
from weatherV10 import Weather
from KplerGroupAct import KplerGroupData
from CEinventory import country_Inventory
from JPMETIFCSTV1 import Japan_METI_FCST
from nukoutageV9 import Nuk_Outage
from waterfallV1 import WaterFall
from JKTC_weatherV1 import JKTC_Temp
from ce_sendout import EU_CE_regas
from brazilV1 import Brazil_hydro
from JapanPowerPrice import Japan_Power_price
from USfeedgasV1 import US_Feed
from AUSfeedgas import Aus_Feed
from JKCIMA30chart import JKCI
from SupplyDeskYoY import modelYoY
from EUsendoutV1 import EU_sendout
from LNGFcstV3 import Fundamental_research
from TempCompare import model_compare
from AlgeriaFlow import Algeria_Flow
from LNG_trackingV1 import EU_LNG_Tracker
#from copy import copy


pd.set_option('display.max_columns',10)

def send_email(sender, receivers, cc, subject, body):
    # Import smtplib for the actual sending function
    import smtplib
    
    # Import the email modules we'll need
    from email.message import EmailMessage
    
    # Create a text/plain message
    msg = EmailMessage()
    msg.set_content(body, subtype='html')
    
    # me == the sender's email address
    # you == the recipient's email address
    msg['Subject'] = subject #'SMTP e-mail test 2'
    msg['From'] = sender #
    msg['To'] = receivers #'svaiyani@freepoint.com'
    msg['Cc'] = cc
    
    # Send the message via our own SMTP server.
    s = smtplib.SMTP('smtp.freepoint.local')
    s.send_message(msg)
    s.quit()


def daily_update():
    
    try:
        LNGMA10 = LNG_MA10
        update_LNGMA10 = LNGMA10.dftodb()
        print('\033[0;31;46m LNGMA10 has updated at:\033[0m' , datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'LNGMA10 '+str(e))
        pass
    
    
    try:
        LNGMA30 = LNG_MA30
        update_LNGMA30 = LNGMA30.dftodb()
        print('\033[0;31;46m LNGMA30 has updated at:\033[0m' , datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'LNGMA30'+str(e))
        pass
        
    try:
        Kpler_hist = Kpler_hist_data
        update_Kpler_hist = Kpler_hist.full_data()
        print('\033[0;31;46m Kpler Hist has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'Kpler Hist'+str(e))
        pass
        
    try:
        Kplergroup = KplerGroupData
        update_Kpler_group = Kplergroup.dftodb()
        print('\033[0;31;46m Kpler Group Data has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'Kpler Group Data'+str(e))
        pass
        
    try:
        Freightdaily = Freight
        update_Freightdaily=Freightdaily.get_freight()
        print('\033[0;31;46m Freight has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'Freight'+str(e))
        pass
        
    try:
        MA10Chart = FlowMA10Chart
        update_MA10Chart = MA10Chart.flow_daily()
        print('\033[0;31;46m MA10 Chart has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'MA10 Chart'+str(e))
        pass
    
    try:
        MA30Chart = FlowMA30Chart
        update_MA30Chart = MA30Chart.flow_daily()
        print('\033[0;31;46m MA30 Chart has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'MA30 Chart'+str(e))
        pass
        
    try:
        diff=DiffTable
        update_diff_table = diff.update()
        print('\033[0;31;46m Diff table has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'Diff table'+str(e))
        pass
        
    try:
        Suez_Regression=SuezRegression
        update_SuezReg = SuezRegression.update()
        print('\033[0;31;46m SuezRegression has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'SuezRegression'+str(e))
        pass
        
    try:
        CargoTracker = Cargo_traker
        update_CargoTracker = CargoTracker.update()
        print('\033[0;31;46m Cargo Tracker has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'Cargo Tracker'+str(e))
        pass
    
    try:
        IHS_Capa = IHS_capa
        update_regas_capa=IHS_Capa.update_capa()
        print('\033[0;31;46m IHS Capa has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'IHS Capa'+str(e))
        pass
    ''' 
    try:
        JKC_demand = JKC_Demand
        update_JKC_demand=JKC_demand.update()
        print('\033[0;31;46m JKC demand has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'JKC demand'+str(e))
        pass
    '''   
    try:
        CE = country_Inventory
        CE_inventory = CE.inventory_update()
        print('\033[0;31;46m CE inventory has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'CE inventory'+str(e))
        pass
    
    try:
        JPMETI = Japan_METI_FCST
        JPMETI_FCST = JPMETI.update()
        print('\033[0;31;46m JP METI inventory has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'JP METI inventory'+str(e))
        pass
    
    try:
        NukOutage = Nuk_Outage
        NukOutageUpdate = NukOutage.update()
        print('\033[0;31;46m JP KR NUk Outage has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'JP KR NUk Outage'+str(e))
        pass
    
    try:
        Waterfall = WaterFall
        WaterFallUpdate = Waterfall.update()
        print('\033[0;31;46m WaterFall has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'WaterFall'+str(e))
        pass
        
    try:
        cesendout = EU_CE_regas
        cesendoutdata = cesendout.update()
        print('\033[0;31;46m CE sendout data has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'CE sendout data'+str(e))
        pass
        
    try:
        weather = Weather 
        update_weather = weather.update()
        print('\033[0;31;46m temp, wind and wave has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com', '', 'Daily update error', 'temp, wind and wave'+str(e))
        pass
    '''
    try:
        jktc_weather = JKTC_Temp 
        update_jktc_weather = jktc_weather.update()
        print('\033[0;31;46m JKTC temp has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'JKTC temp '+str(e))
        pass
    '''    
    try:
        brazil_hydro = Brazil_hydro 
        update_brazil_hydro = brazil_hydro.update()
        print('\033[0;31;46m Brazil hydro has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com','', 'Daily update error', 'Brazil hydro'+str(e))
        pass
        
    try:
        updatejppowerprice = Japan_Power_price.update() 
        print('\033[0;31;46m japan power price has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'japan power price'+str(e))
        pass
        
    
    try:
        updateUSfeed = US_Feed.update() 
        print('\033[0;31;46m us feedgas has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'us feedgas'+str(e))
        pass
        
    try:
        updateAusfeed = Aus_Feed.update() 
        print('\033[0;31;46m AUS feedgas has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e)   
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'AUS feedgas'+str(e))
        pass
        
    try:
        updateJKCIMA30 = JKCI.update() 
        print('\033[0;31;46m JKCI has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'JKCI'+str(e))
        pass
        
    try:
        updatesupplydeskyoy = modelYoY.update() 
        print('\033[0;31;46m supply model yoy has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'supply model yoy'+str(e))
        pass
        
    try:
        updateEU_sendout = EU_sendout.update() 
        print('\033[0;31;46m EU_sendout has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'EU_sendout'+str(e))
        pass
    
    try:
        TRfcst = Fundamental_research.update() 
        print('\033[0;31;46m TR has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'TR '+str(e))
        pass


    try:
        tempmodelcompare =  model_compare.update() 
        print('\033[0;31;46m temp models compare has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'temp models compare '+str(e))
        pass
    
    
    try:
        algeria =  Algeria_Flow.update() 
        print('\033[0;31;46m algeria has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'temp models compare '+str(e))
        pass
    
    try:
        EUtracker = EU_LNG_Tracker.update() 
        print('\033[0;31;46m EU tracker has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'temp models compare '+str(e))
        pass
    '''
    try:
        copyfile = copy.update() 
        print('\033[0;31;46m copy has updated at:\033[0m ', datetime.datetime.now())
    except Exception as e:
        print('KeyError: ', e) 
        send_email('ywang1@freepoint.com', 'svaiyani@freepoint.com, swasti@freepoint.com, ywang1@freepoint.com', '', 'Daily update error', 'temp models compare '+str(e))
        pass

    '''

#daily_update() 
scheduler = BackgroundScheduler(job_defaults={'misfire_grace_time': 15*60})
#trigger = OrTrigger([CronTrigger(day_of_week='0-6', hour='00, 07,08, 12, 16', minute='22')])
trigger = OrTrigger([CronTrigger(day_of_week='0-6', hour='07, 09, 11, 13, 15, 17, 18, 19, 20', minute='00')])
scheduler.add_job(func=daily_update,trigger=trigger,id='daily_update')
scheduler.start()
runtime = datetime.datetime.now()
print (scheduler.get_jobs(), runtime)
#scheduler.remove_job('daily_update') 
