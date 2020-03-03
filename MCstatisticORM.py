# -*- coding: utf-8 -*-
"""
Created on Fri May 18 09:52:00 2018
Version 2.2
Releise Date: Sept 27.

@author: Dejan
"""
import pyodbc
import time
import getpass
import logging
from logging.handlers import RotatingFileHandler
from queue import Queue
from threading import Thread, RLock
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select, bindparam
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


thr_num = 5
every_hours = 2
st_delta = 5 #sekundi; 
#logging.basicConfig(filename='statistic2.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')
path = "Log.log"
logger = logging.getLogger("main")
logger.setLevel(logging.WARNING)
handler = RotatingFileHandler(path, maxBytes=20000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

mcdb_pass = getpass.getpass('sql pass za MCDB:')
db2_pass = getpass.getpass('sql pass za statistic:')
mc_conn_string = 'DRIVER={SQL Server};SERVER=mcdb.zenit.org;DATABASE=mc_pdb;UID=sa;PWD='+mcdb_pass
engine = create_engine("mssql+pyodbc://sa:{}@upp.zenit.org/mc_client?driver=SQL+Server+Native+Client+11.0".format(db2_pass), echo=False)

cond_1 = "msisdn != 'NULL' and CallingAddress != 'NULL' and CalledAddress != 'NULL' and IMEI != 'NULL'"
querry_1 = "SELECT Duration, CallingAddress, CalledAddress, MSISDN, IMEI, IMSI, StartTime, IID FROM {} where IID > {} and " + cond_1
querry_2 = "SELECT Duration, CallingAddress, CalledAddress, MSISDN, IMEI, IMSI, StartTime, IID FROM {} where " + cond_1
mc_get_akcije = 'SELECT FID, Name FROM Folder where ParentFID = 2 and FID>3' #fid 1 i 3 pripadaju unallocated folderima
mc_get_rest = 'SELECT FID, Name, ParentFID FROM Folder where ParentFID != 2 and FID>3' #folderi koji nisu akcije (mete i lica)


def MCDBConnect():
    mc_conn = False
    while not mc_conn:
        print('establishing MCDB connection')
        logger.info('establishing MCDB connection')
        try:
            mc_conn = pyodbc.connect(mc_conn_string)
        except Exception as e:
            mc_conn = False
            print(repr(e))
            logger.warning(repr(e))
        time.sleep(10)
    print('MCDB connection established')
    logger.info('MCDB connection established')
    return mc_conn


def statConnect():
    print('establishing connection to stat db')
    logger.info('establishing connection to stat db')
    conn_db2 = False
    while not conn_db2:
        try:
            conn_db2 = engine.connect()
        except Exception as e:
            print(repr(e))
            logger.warning(repr(e))
            print('stat conn failed, waiting for 10sec')
            logger.info('stat conn failed, waiting for 10sec')
            time.sleep(10)
    print('connection to stat established')
    logger.info('connection to stat established')
    return conn_db2


#mc_conn = pyodbc.connect(mc_conn_string)
mc_conn = MCDBConnect()

Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

metadata = MetaData()

StatTbl = Table('mc_stat', metadata,
    Column('kljuc',String(50), primary_key=True, nullable=False),
    Column('msisdn', String(20), nullable=False),
    Column('imei', String(20), nullable=False),
    Column('imsi',String(20), default="null"),
    Column('num_b',String(30), nullable=False),
    Column('direction',Integer, nullable=False),
    Column('tip',Integer, nullable=False),
    Column('cnt',Integer, nullable=False)
)

LastIidTbl = Table('last_iid', metadata,
                   Column('fid',String(20), primary_key=True, nullable=False),
                   Column('last_iid',Integer, nullable=False)
                   )

KeyFidTbl = Table('key_fid', metadata,
                  Column('kljuc',String(50), primary_key=True, nullable=False),
                  Column('fid',String(20), primary_key=True, nullable=False),
                  Column('iid',Integer, primary_key=True, nullable=False),
                  )

FolderTreeTbl = Table('folder_tree', metadata,
                 Column('fid', Integer, primary_key=True, nullable=False),
                 Column('meta', String(50), nullable=False),
                 Column('lice_id', Integer, nullable=False),
                 Column('lice', String(50), nullable=False),
                 Column('akcija_id', Integer, nullable=False),
                 Column('akcija', String(50), nullable=False)
)

q_fid = Queue(-1)
new_key_fid = Queue(-1)
q_keys = Queue(-1)
lck = RLock()
param_list = StatTbl.c.keys()


def clear_db():
    print('INFO: clearing dbs')
    logger.info('clearing dbs')
    #conn_db2 = engine.connect()
    conn_db2 = statConnect()
    with conn_db2.begin():
        conn_db2.execute(StatTbl.delete())
        conn_db2.execute(LastIidTbl.delete())
        conn_db2.execute(KeyFidTbl.delete())
        conn_db2.execute(FolderTreeTbl.delete())
        
def printer():
    while new_key_fid.qsize():
        with lck:
            print('key_fid size: '+str(new_key_fid.qsize()))
        time.sleep(20)


def is_num(num):
    #print('num',num)
    try:
        int(num)
        return len(num)>=8 #za servisne brojeve vrati False
    except ValueError:
        return False
    except TypeError:
        return False
    
    
def non_duplicat(unique_times, key, Stime):  # da li je komunikacija duplikat? Samo za pozive.

    with lck:
        times = unique_times[key]

    st_cond = any([abs(st-Stime).total_seconds() < st_delta for st in times])
    
    if st_cond:
        return False  #Duplikat
    else:
        with lck:
            unique_times[key].append(Stime)
        return True


def insert2statistic(new_data, old_cnt):
    
    print('INFO: upis u mc_stat')
    logger.info('upis u mc_stat')
    
    #conn_db2 = engine.connect()
    conn_db2 = statConnect()
    
    with conn_db2.begin():
        while not q_keys.empty():
            key = q_keys.get()
            if key in old_cnt:  # Ako je ključ već u bazi (smanjuje broj upita na bazu)
                new_data[key][-1] += old_cnt[key]    # [key, msisdn, imei, imsi, Num_B, direction, tip, broj_komunikacija]
                s = StatTbl.update().where(StatTbl.c.k==key).values(cnt=new_data[key][-1])
            else:
                s = StatTbl.insert().values(dict(zip(param_list, new_data[key])))
            
            try:
                conn_db2.execute(s)
            except Exception as e:
                print('ERROR: '+repr(e))
                print(dict(zip(param_list, new_data[key])))
                logger.error(repr(e))
                q_keys.put(key)
    
    print('INFO: upis u mc_stat završen')
    logger.info('upis u mc_stat završen')


#def insert2fk():
#    
#    print('INFO: upis u folder_key')
#    logger.info('upis u folder_key')
#    conn_db2 = engine.connect()
#    for_insert = []
#    while not new_key_fid.empty():
#        data = new_key_fid.get()
#        for_insert.append(data)
#    if len(for_insert):
#        try:
#            with conn_db2.begin():
#                conn_db2.execute(KeyFidTbl.insert(), for_insert)
#        except Exception as err:
#            print('ERROR: '+repr(err))
#            logger.error(repr(err))
#        
#    print('INFO: upis u folder key zavrsen')
#    logger.info(('upis u folder key zavrsen'))


def insert2fk():

    print('INFO: upis u folder_key')
    logger.info('upis u folder_key')
    #conn_db2 = engine.connect()
    conn_db2 = statConnect()
    with conn_db2.begin():
        while not new_key_fid.empty():
            data = [new_key_fid.get()]
            try:
                conn_db2.execute(KeyFidTbl.insert(), data)
            except Exception as err:
                print('ERROR: '+repr(err))
                logger.error(repr(err))
        
    print('INFO: upis u folder key zavrsen')
    logger.info(('upis u folder key zavrsen'))

      

def UpdateHierarchy(mete, lica, akcije, inserted_fids):
    print('INFO: zanavljanje tabele hijerarhija')
    logger.info('zanavljanje tabele hijerarhija')
    folderTreeRows = []
    for fid in mete.keys():
        if fid not in inserted_fids:
            meta = mete[fid][0]
            lice_id = mete[fid][1]
            try:
                lice = lica[lice_id][0]
                akcija_id = lica[lice_id][1]
                akcija = akcije[akcija_id]
                folderTreeRows.append({'fid':fid, 'meta':meta, 'lice_id': lice_id, 'lice': lice, 'akcija_id': akcija_id, 'akcija': akcija })
            except KeyError:      # kada je hijerarhija dublja od tri sloja doći će ovde do greške
                print('WARNING: više od tri sloja; preskačem fid = {}, pfid = {}, name={}'.format(fid, lice_id, meta))
                logger.warning('vise od tri sloja; preskacem fid = {}, pfid = {}, name={}'.format(fid, lice_id, meta))

    if len(folderTreeRows):
        conn_db2 = statConnect()
        with conn_db2.begin():
            conn_db2.execute(FolderTreeTbl.insert(), folderTreeRows)


    print('INFO: tabela hijerarhija je obnovljena')
    logger.info('tabela hijerarhija je obnovljena')


def get_data_from_db(fid, fid_iid, cursor, conn_db2):  # pass fid as string; c - connection to mcdb; c2 - connection to sqlite

    table = "Intercept_" + fid
    cursor.execute("select max(iid) from " + table)
    last_iid = cursor.fetchone()

    if last_iid[0]:
        last_iid = last_iid[0]
    else:  # fid je none
        last_iid = 0

    if fid in fid_iid:  # test da li je fid već obrađen
        if last_iid > fid_iid[fid]:  # test da li ima novih unosa u intercept tabelu. Zadnji IID se čuva u lokalnoj bazi.
            cursor.execute(querry_1.format(table, fid_iid[fid]))
            #conn_db2 = statConnect()
            try:
                with conn_db2.begin():
                    conn_db2.execute(LastIidTbl.update().where(LastIidTbl.c.fid==fid).values(last_iid=last_iid))
            except Exception as err:
                print(repr(err))
                logger.error(repr(err))

        elif last_iid < fid_iid[fid]:
            print('WARNING: neko je nešto brisao! Fid=' + fid)
    else:  # ako fid nije obrađivan prije...
        cursor.execute(querry_2.format(table))
        #conn_db2 = statConnect()
        try:
            with conn_db2.begin():
                conn_db2.execute(LastIidTbl.insert().values(fid=fid, last_iid=last_iid))
        except Exception as err:
            print('ERROR: '+ repr(err))
            logger.error(repr(err))

    data = cursor.fetchall()

    return data


def format_data(row):
    msisdn = row[3]
    if len(msisdn) < 11:
        if msisdn[0] == '0':
            msisdn = '387' + msisdn[1:]
        else:
            msisdn = '387' + msisdn

    imei = row[4]
    imsi = row[5]

    if row[1][-8:] == msisdn[-8:]:  # Target is calling
        direction = 0  # Out
        Num_B = row[2]  # Ced number
    else:
        direction = 1  # In
        Num_B = row[1]
    if row[0] is not None:
        tip = 0  # call
    else:
        tip = 1  # sms

    return [msisdn, imei, imsi, Num_B[:30], direction, tip, 1]


# %%
def mainFn(fid_iid, old_cnt, new_data, unique_times, key_fid):
    #mc_conn = pyodbc.connect(mc_conn_string)
    mc_conn = MCDBConnect()
    cursor = mc_conn.cursor()
    conn_db2=statConnect()
    with mc_conn:
        while not q_fid.empty():
            fid = q_fid.get()
            data = get_data_from_db(fid, fid_iid, cursor, conn_db2) #vraća samo nove podatke od zadnjeg pokretanja ili praznu listu
            for row in data:
                if row[2] is not None and is_num(row[1]) and is_num(row[2]) and row[3] is not None and row[4] is not None: #valid communication
                    Stime = row[6]
                    iid = row[7]
                    row = format_data(row) #  [msisdn, imei, imsi, Num_B, direction, tip, broj_komunikacija]
                    key = row[0][-8:]+row[1]+row[3][-8:]+str(row[4])+str(row[5])
                    row.insert(0, key)  # [key, msisdn, imei, imsi, Num_B, direction, tip, broj_komunikacija]

                    with lck:
                        #print(key)
                        if key in key_fid:                            
                            if fid not in key_fid[key]['fids']:
                                key_fid[key]['fids'].append(fid)
                                key_fid[key]['iids'].append(iid)
                                new_key_fid.put({'kljuc': key, 'fid': fid, 'iid': iid})
                                #print({'kljuc': key, 'fid': fid, 'iid': iid})
                            elif iid not in key_fid[key]['iids']:
                                key_fid[key]['iids'].append(iid)
                                new_key_fid.put({'kljuc': key, 'fid': fid, 'iid': iid})
                                #print({'kljuc': key, 'fid': fid, 'iid': iid})
                        else: #Implicitno key nije ni u old_cnt
                            key_fid[key] = {'fids': [fid], 'iids': [iid]}
                            new_key_fid.put({'kljuc': key, 'fid': fid, 'iid': iid})


                            if key in new_data:
                                if non_duplicat(unique_times, key, Stime): #is it duplicat; non_duplicat() appends Stime in unique_times
                                    new_data[key][-1] += 1
                            else:
                                new_data[key] = row
                                q_keys.put(key)
                                unique_times[key] = [Stime]

    t1 = Thread(target=insert2statistic, args=(new_data, old_cnt,), daemon=True)
    t1.start()
    t2 = Thread(target=insert2fk, args=(), daemon=True)
    t2.start()
    t3 = Thread(target=printer, args=(), daemon=True)
    t3.start()

    t1.join()
    t2.join()
    t3.join()
            

# %%
def Starter(pocetak):

    if pocetak:
        clear_db()

    c = mc_conn.cursor()
    conn_db2 = statConnect()

    while True:
        lica = {}  # {fid: [lice, pfid]}
        mete = {}  # {fid: [meta, pfid]}

        # %% meta lice akcija %%
        with c:
            c.execute(mc_get_akcije)
            akcije_data = c.fetchall()  # [fid, name]
            c.execute(mc_get_rest)
            rest_data = c.fetchall()

        akcije = {row[0]: row[1] for row in akcije_data}

        for row in rest_data:  # row = [FID, Name, ParentFID]
            if row[2] in akcije:
                lica[row[0]] = [row[1], row[2]]
            else:
                mete[row[0]] = [row[1].replace('IMEI', '').replace(' ', '').replace('.', ''), row[2]]

        rez = conn_db2.execute(select([FolderTreeTbl.c.fid]))
        inserted_fids = [fid[0] for fid in rez]

        [q_fid.put(str(fid)) for fid in mete.keys()]
 
        Thread(target=UpdateHierarchy, args=(mete, lica, akcije, inserted_fids,), daemon=True).start()
        # %%

        new_data = {}  # dict = {key: row}
        unique_times = {}  # dict = {key: [t1, t2,...,tn]}
        key_fid = {}  # key_fid = {key: {'fids':[fid1, fid2, ...], 'iids': [iid1, iid2, ...]}} - key-fid dict
        with conn_db2.begin():
            fid_iid = {row[0]: row[1] for row in conn_db2.execute(select([LastIidTbl]))}
            old_cnt = {row[0]: row[1] for row in conn_db2.execute(select([StatTbl.c.kljuc, StatTbl.c.cnt]))}
            for row in conn_db2.execute(select([KeyFidTbl])):
                if row[0] in key_fid:    # row[0]=key
                    if row[1] not in key_fid[row[0]]['fids']:
                        key_fid[row[0]]['fids'].append(row[1])  #row[1]=fid
                        key_fid[row[0]]['iids'].append(row[2])
                    elif row[2] not in key_fid[row[0]]['iids']:
                        key_fid[row[0]]['iids'].append(row[2])
                else:
                    key_fid[row[0]] = {'fids': [row[1]], 'iids': [row[2]]}  # row[2] = iid
                
        #logger.debug(repr(key_fid))
        thr_list = []
        for _ in range(thr_num):
            t = Thread(target=mainFn, args=(fid_iid, old_cnt, new_data, unique_times, key_fid,), daemon=True)
            t.start()
            thr_list.append(t)

        for t in thr_list:
            t.join()

        print("INFO: round finished, waiting for {0} hrs".format(every_hours))
        logger.info("round finished, waiting for {0} hrs".format(every_hours))
        time.sleep(every_hours * 60 * 60)  # Pokreni sledeći update posle "every_hours"


try:
    metadata.create_all(engine)
    Starter(pocetak=False)
except Exception as e:
    print(repr(e))
    logging.error(repr(e))
    handler.close()
    logger.removeHandler(handler)