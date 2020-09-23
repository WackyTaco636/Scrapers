import ssl
import re
import sqlite3
import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# define database and variables (timeout for writing block)
conn = sqlite3.connect('/home/chris/Documents/01_Projects/01_Grundstückvaluierung/01_Databases/GV_IN_DB_DE.sqlite', timeout=3)
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Ads
(offer type UNIQUE PRIMARY KEY, area REAL, zip TEXT, objectcat TEXT, fed TEXT, city TEXT, obcat TEXT, title TEXT, marketingtype TEXT, price REAL, areaeffective REAL, sqmprice REAL)''')

url = 'https://www.immonet.de'
src1 = 'https://www.immonet.de/immobiliensuche/sel.do?&sortby=0&suchart=1&objecttype=1&marketingtype=1&parentcat=3&toprice=999999999&fromarea=0&federalstate='

fed = range(1,17)
for i in fed :
    src = src1 + str(i)
    # print(src)

    ###### AD EXTRACTION FROM SEARCH RESULTS ###################

    #perform the search using urlib and parse the webpage using beautifulsoup
    #do this 10 times for 10 pages
    srcrng = range(1,31)
    for i in srcrng :
        rng = '&page=' + str(i)
        srcrng = src + rng
        print(srcrng)

        # print(src+rng)
        html = urllib.request.urlopen(srcrng).read().decode()
        soup = BeautifulSoup(html, "html.parser")
        tags = soup('a')
        for tag in tags :
            try :
                href = tag.get('href', None)
                if href.startswith('/angebot/') :
                    ad = url+href
                    print(ad)
                    cur.execute('INSERT OR REPLACE INTO Ads (offer) VALUES ( ? )', (ad,) ) #INSERT OR IGNORE
                    conn.commit()
            except Exception as e :
                print(e)
                continue

    ##### FEATURE EXTRACTION FROM ADS ###################

cur.execute("SELECT * FROM Ads WHERE zip is NULL")
results = cur.fetchall()
print(results)

# for all the urls in the Ads table:
feats = list()
int = 0
for sel in results :
    selad = sel[0]
    #print(selad)

    # 2) open the ad url, decode in utf-8 (for umlauts), make it readible in regex (string), and extract vehicle features (feat) using regex
    opennewad = urllib.request.urlopen(selad).read()
    soup = BeautifulSoup(opennewad, "html.parser")
    script = soup.find_all('script', type='text/javascript')#.text
    strpar = str(script)
    tarpam = str(re.findall('{(.*?)}',strpar))
    tarpam.replace('[\\','')
    clnpam = tarpam[2:-2].split(',')

    for i in clnpam :
        keyval = i.split(':')
        try :
            key = keyval[0].replace('"','')
            val = keyval[1]#.replace('"','')
            offer = '"' + sel[0] + '"'

            print('offer:',offer)
            print('key:',key)
            print('val:',val)
            print('SQL Command:','''UPDATE Ads SET {}={} WHERE offer={}'''.format(key,val,offer))

            cur.execute('''UPDATE Ads SET {}={} WHERE offer={}'''.format(key,val,offer))
            cur.execute('''UPDATE Ads SET sqmprice=price/areaeffective''')
            conn.commit()

        except Exception as e :
            print(e)
            continue

conn.commit()
