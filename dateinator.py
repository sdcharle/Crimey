# dateinator


import sqlite3
from datetime import datetime
import re

date1 = re.compile(r'\d+/\d+/\d{4}')
date2 = re.compile(r'\d+/\d+/\d{2}')
datelong = re.compile(r'\d{4}-\d+-\d+ \d+:\d+:\d+ [A|P]M')


date1parse = '%m/%d/%Y'
date2parse = '%m/%d/%y'
datelongparse = '%Y-%m-%d %H:%M:%S %p'

parseOMatic = (
    (date1, date1parse),
    (date2, date2parse),
    (datelong, datelongparse)
)

def fix_date(inDate):
    datey = None
    try:
        for option in parseOMatic:
            match = option[0].search(inDate)
            if match:
                datey = datetime.strptime(match.group(), option[1])
                break
    except Exception, val:
        print "oh shit I crapped: %s %s" %(inDate)
    return datey

if __name__ == '__main__':

    conn = sqlite3.connect('crimey.db')
    upConn = sqlite3.connect('crimey.db')
    for row in conn.execute("select url, incident_date from incident"):
        datey = None
        print "%s %s" % row
        try:
            if row[1]:
                for option in parseOMatic:
                    match = option[0].search(row[1])
                    if match:
                        datey = datetime.strptime(match.group(), option[1])
                        break
                print "%s\t%s" % (row[1], datey)
                print "go for da update son!!!"
                conn.execute("update incident set fixed_incident_date=? where url=?",(datey, row[0]))
                #conn.commit()

        except Exception, val:
            print "Yo Exception: %s with val: %s" % (Exception, val)
    conn.commit()
               