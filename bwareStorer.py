"""

on going 'crime scraper'
data from 'b aware' site.

1) pull down and store the feeds, 1/2009 - present
2) first load to DB
3) geocache
4) map frontend
5) entities w/ GeoAPI

3/6/2010

got up to p 430
500 error on 431

crimey.db:

CREATE TABLE incident (
id  primary key,
incident_type varchar(100) not null,
district varchar(100) not null,
incident_date datetime,
address varchar(120),
summary text);

3/12/2010
Some mega-file reppin

3/20/2010
Happy spring
TO DO:
geocodes - add handling for 'failed_geocoding' (for example - multiple placemarkers are a big problem)
add date tagging to error logging. Duh! - done

grabbin the latest (daily!)

4/10/2010

Been away. Wrap all pieces (inc DB) into one piece for updates no matter when it is run....
http://bloomington.b-aware.us/2010/page/1

4/30/2010

again been off goofing. tweak for target date, 'fixed_incident_date' yeah man.

9/17/2010 Holy shit, this is still going on. Anyway. Fixed the going back to get stuff we haven't got yet. once you find a url that's
already there, that's it, stop.


"""
import geopy
import sys
import datetime
import urllib2
import time
import random
import os
import re
import sqlite3
from BeautifulSoup import BeautifulSoup
import logging

import dateinator

LOG_FILENAME = 'scrapy.log'
#logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
logger = logging.getLogger(sys.argv[0])
logger.setLevel(logging.INFO)
file_log = logging.FileHandler(LOG_FILENAME,'a')
formatter = logging.Formatter("%(asctime)s - %(name)s - %(lineno)s - %(levelname)s - %(message)s")
file_log.setFormatter(formatter)
logger.addHandler(file_log)
ROOT_URL = "http://bloomington.b-aware.us/"
STORE_DIR = "./bewareDump2/"
URL_PATTERN = "%s%s/page/%s"
FILE_PATTERN = "%sbware%spage%s.html"
year = "2010"
WAIT_INTERVAL = 2 # seconds btw fetches
STRIPRE = re.compile('^(\s*)(.*?)(\s*)$')
# date formats used in reports
DATE_PATTERN = "%m/%d/%y"
DATE_TIME_PATTERN = DATE_PATTERN + "    %I:%M:S %p"
DATABASE_FILE = "./crimey.db"
DATERE =  re.compile(r'\/(\d+)\/(\d+)\/(\d+)')
GMAPS_API_KEY = "ABQIAAAAsN3dUjp7Qu90aAfa3r-eHxT2yXp_ZAY8_ufC3CFXhHIE1NvwkxRyosXyGpE39F8_XJV0vYxDyNHEJw"

URL_SKIP_PATTERNS = ['sex-offender']

def date_from_url(url):
    retVal = None
    mtch = DATERE.search(url)
    if mtch:
        retVal = datetime.datetime(int(mtch.group(1)), int(mtch.group(2)), int(mtch.group(3)))
    return retVal

def strip(str):
    m = STRIPRE.match(str)
    if m is not None:
        return m.group(2)
    else:
        return str

class BailOut(Exception):
    pass

class GoofDB:
    def __init__(self,databaseFile = DATABASE_FILE):
        self.conn = conn = sqlite3.connect(databaseFile)
        self.records = {}

    def urlExists(self, url):
        try:
          return self.conn.cursor().execute("select * from incident where url = ?",(url,)).fetchone()
        except Exception, val:
            logger.error("fuck up w/ url search: %s, %s" %(Exception, val))
        return False #???

    def storeCoords(self, lat, long, place, url):
        try:
            self.conn.execute("update incident set update_time = datetime('now'), geocode_attempt = 1, failed_geocoding = 0; lat=?, long=?, geocoded_place=? where url=?",(lat,long, place, url))
            self.conn.commit()
        except Exception, val:
            logger.error("fuck up in store coords: %s, %s" % (Exception, val)) 
    # incident be saved
    
    def saveContents(self, contents):
        success = True
        try:
            self.conn.execute("""insert into incident (url, incident_type, district, incident_date, 
                              address, summary, update_time, fixed_incident_date) values (?,?,?,?,?,?,?,?)""" , (contents['url'], contents['incident type'], contents['district'],
                                                                           contents['when'], contents['address'], contents['summary'],
                                                                           datetime.datetime.now(), dateinator.fix_date(contents['when'])))
            self.conn.commit()
        except Exception, val:
            logger.error("fuck up in save contents: %s: %s (%s)" % (Exception, val, contents))
            success = False
        return success

    def getUnattemptedAddressURLs(self):
        urls = []
        try:
            for row in self.conn.execute("""
                        select url,address from incident where geocode_attempt=0
                        or failed_geocoding is null
                        """):
                urls.append(row)
        except Exception, val:
            logger.error("fuck up in get no address vals: %s, %s" % (Exception, val))
        return urls

    def dumpDataToDB(self):
        print "dadump"
        crsr = self.conn.cursor()
        for key in self.records.keys():
            print key
            if self.records[key]:
                for address in self.records[key]:
                    crsr.execute("insert into wikiAddress (url, address) values (?,?)" , (key, address))
            else:
                crsr.execute("insert into wikiAddress (url) values (?)", (key,))
        self.conn.commit()
        crsr.close()

    def clearDB(self):
        self.conn.execute("truncate table incident")
        self.conn.commit()
# below filters down nice, however, still we get bpedia, curr events, blah
def clean_address(address):
    address = address.lower()
    print "%s\n" % address
    address = address.replace('/',' and ')
    address = address.replace('blk','') + " Bloomington, IN"
    return address

"""
 now returning the raw val
 
 change up, goin for year only to work backward
"""

def fetch_page(year, month=None, page=1):

    raw = ''
    try:
        date_str = str(year)
        if month:
            date_str = date_str + '/' +  str(month)
        
        url = URL_PATTERN %( ROOT_URL, date_str, page)
        print "goin for: %s" % (url)
        raw = urllib2.urlopen(url).read()
        f = open(FILE_PATTERN %(STORE_DIR, year, page), "w")
        f.write(raw)
        f.close()
        success = True
    except Exception, val:
        logger.error("ooh, FAIL: %s - %s" % (Exception, val))
    return raw

def fetch_raw_pages(year, month, page=1):
    success = True
    while success:
        success = fetch_page(year, month, page)
        time.sleep(random.randrange(WAIT_INTERVAL))
        page = page + 1

def grab_pages():
    page = 1
    if len(sys.argv) >= 2:
        year = sys.argv[1]
    if len(sys.argv) == 3:
        month = sys.argv[2]
    if len(sys.argv) == 4:
        page = sys.argv[3]
        
    fetch_raw_pages(year, month,int(page))   

def scrape_file(db,file):
    succeed = True
    soup = BeautifulSoup(open(STORE_DIR + file).read())
    nodes = soup.findAll("div",{"class":"postin"})
    for node in nodes:
        incident = {}
        url = node.find("a",{"class":"thetip"})["href"]
        print url
        for pattern in URL_SKIP_PATTERNS:
            if url.find(pattern) < 0:
                logger.info("skipping because of a skip pattern in the url:%s" % pattern)
                continue
        print "URL:%s" % (url)
        incident["url"] = url
        pees = node.find("div",{"class":"entry"}).findAll("p")

        for pee in pees: # breaking down here
            strs = pee.findAll(text=True)
            if incident.has_key("summary"):
                incident["summary"] = incident["summary"] + "\n" + "\n".join(strs)
            elif len(strs) == 2:
                key =  strip(strs[0].split(":")[0].lower())
                incident[key] = strip(strs[1])
            elif len(strs) == 1 and strs[0].find('ummary') != -1:
                incident["summary"] = "" #triggers summation
        incident["summary"] = strip(incident["summary"])
        print str(incident)
        if not db.saveContents(incident):
            print "already saved this URL, amigo"
            succeed = False
    return succeed
        
"""        

 gettin info from raw text.
 note, need to catch 'key violation', return fail val if so

"""
def scrape_page(db,raw, target = None):
    succeed = True
    soup = BeautifulSoup(raw)
    nodes = soup.findAll("div",{"class":"postin"})
    for node in nodes:
        try: 
            incident = {}
            url = node.find("a",{"class":"thetip"})["href"]
            # if we went backward past target, bail.
            if target and date_from_url(url) < target:
                return False
            # if we've already got this one, bail.
            if db.urlExists(url):
                return False
            print "URL:%s" % (url)
            for pattern in URL_SKIP_PATTERNS:
                if url.find(pattern) > -1:
                    logger.info("skipping because of a skip pattern in the url:%s" % pattern)
                    raise BailOut # go to next node
            incident["url"] = url
            pees = node.find("div",{"class":"entry"}).findAll("p")
    
            for pee in pees: # breaking down here
                strs = pee.findAll(text=True)
                if incident.has_key("summary"):
                    incident["summary"] = incident["summary"] + "\n" + "\n".join(strs)
                elif len(strs) == 2:
                    key =  strip(strs[0].split(":")[0].lower())
                    incident[key] = strip(strs[1])
                elif len(strs) == 1 and strs[0].find('ummary') != -1:
                    incident["summary"] = "" #triggers summation
            if incident.has_key("summary"):
                incident["summary"] = strip(incident["summary"])
            print str(incident)
            if db.saveContents(incident) != True:
                print "Error, amigo %s\n" % url
                #succeed = False #pass # for now ignoreing
                
        except BailOut:
            pass
    return succeed


"""

to scrape files you dumped in a directory

"""
def scrape_to_db(goofy):
    for file in os.listdir(STORE_DIR):
      try:
        scrape_file(goofy,file)
      except Exception, val:
        logger.error("%s: %s, %s"%(file, Exception, val))
        
# modified 9/18/2010, now return a set to try.
def clean_addresses(address):
    print "raw address: %s\n" % address
    addresses = []
    address = address.lower()
    address = address.replace('blk','')
    if address.find('/') > 0:
        addresses.append(address.replace('/',' and ') + " Bloomington, IN")
        addresses.append(address.split('/')[0] + " Bloomington, IN")
    else:
        address = address + " Bloomington, IN"
        addresses = [address,]
    return addresses
         
# get the coords...
def geo_main(goofDB, geo):
    addRows = goofDB.getUnattemptedAddressURLs()
    count = 0
    for row in addRows:
        lat = 0
        lng = 0

        # address attempts - / -> &. or drop stuff after the and
        addresses = clean_addresses(row[1])
        print "%s\n" % addresses
        for addr in addresses:
            print "go for: %s\n" % addr
            try:
                place, (lat, lng) = geo.geocode(addr)
                goofDB.storeCoords( lat, lng, place, row[0])
                print "%s: %s,%s %s\n" %(place, lat, lng, addr) 
                count += 1
                print "succeed on: %s" % addr
                break
            except Exception, val:
                logger.error("Fail on %s, %s, %s" %(row[0], Exception, val))

        if count == 1500:
            break
        
    print "processed %i records." % count
      
       
       
"""

Temp approach b4 date clean up.
just go backward (ignore key violations, won't be stored anyway) until 'target date' is hit.

"""
       
if __name__ == "__main__":
    logger.info("Here we go now!")
    target = None
    #target = datetime.datetime(2010,3,1)
    if len(sys.argv) == 2:
        target = datetime.datetime.strptime(sys.argv[1],'%m/%d/%Y')
    year = datetime.datetime.now().year
    page = 1
    succeed = True
    while succeed:
        goofy = GoofDB()
        raw = fetch_page(year,None, page)
        #print raw
        succeed = scrape_page(goofy, raw, target)
        page = page + 1
               
    geo = geopy.geocoders.Google(GMAPS_API_KEY)
    geo_main(goofy, geo)

