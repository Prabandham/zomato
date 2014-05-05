#!/usr/bin/python
import urllib2
from bs4 import BeautifulSoup
import MySQLdb
import itertools

#Define the URL's to be crawled for data.
# This acts as the first crawller.
def get_urls():
    url ="http://zomato.com"
    print "Started Crawling http://zomato.com"
    urls = []
    #Open Zomato First
    page = urllib2.urlopen(url).read()
    #Crawl and prettify the page to get the html
    soup = BeautifulSoup(page)
    #Get list of all bangalore based url lists
    lists = soup.select("li.grid_4.grid-m-4.column.alpha > a")
    #loop throught the lists and get the links defined by href
    for list in lists:
        urls.append(list['href'])
    #Add the ?page=1 to make it the default url
    urls = [s + "?page=" for s in urls]
    return urls

#This guy here will get the maximum number of pages in the currently scraping url
def get_max_page_number_of(url):
    #first get the page
    page = urllib2.urlopen(url).read()
    soup = BeautifulSoup(page)
    page_numbers = soup.select("div.pagination-meta")
    #The plus one is there cause of the python for loop, else it would end up loosing the last page
    max_pages = int(page_numbers[0].string[-2:])+1
    print "Total pages for "+url+" is "+str(max_pages)
    return max_pages

#Snippet to insert into the DB.
#This is essentially the model.
def insert_in_db(name,url,cuisines,locality,costfor2):
    db = MySQLdb.connect("localhost","root","root","restaurants_new")
    cursor = db.cursor()
    try:
        sql = "insert into bangalore_restaurants(name,url,cuisines,locality,costfor2) values(%s,%s,%s,%s,%s);"
        cursor.execute(sql, (name,url,cuisines,locality,costfor2))
        db.commit()
    except MySQLdb.Error as e:
        db.rollback()
        print "Found Duplicate Entry !! Rolling back !!"
        pass
    finally:
        db.close()

#Snippet that parses and removes \n.
def parse_and_split(name):
    name_strip = [elem.strip().split(';') for elem in name]
    name_concat = list(itertools.chain.from_iterable(name_strip))
    final = [str(item.encode('ascii', 'ignore')) for item in name_concat]
    return final

#Snippet to build the array of raw scraped data
def to_array(name):
    #strip = [elem.strip() for elem in name]
    raw_array =  list(itertools.chain.from_iterable(name))
    strip = [elem.strip() for elem in raw_array]
    return strip

#This is used to ignore the non acsii contents that they have in the names of the restaurants.
def parse_name(name):
    name = [item.string.encode('ascii', 'ignore') for item in name]
    return name

def parse_url(name):
    url = [item['href'] for item in name]
    return url

def make_urls(url,no_of_pages):
    sub_urls = []
    for i in range(1,no_of_pages):
        sub_urls.append(url+str(i))
    return sub_urls
#The main part of the code which does the actual scraping of data

#First get the base url
urls = get_urls();
#loop through each of the urls
for url in urls:
    #get max page for the url
    max_page = get_max_page_number_of(url)
    #make a list of urls untill the end of max count for this url
    urls_in_url = make_urls(url,max_page)
    #For each of the above url pages get the records and save them in the db.
    for url in urls_in_url:
        print "                    Started Scraping Data for "+url
        page= urllib2.urlopen(url).read()
        soup = BeautifulSoup(page)
        soup.prettify()
        hotels_name_and_url = soup.select("h3.top-res-box-name > a")
        hotel_names = parse_name(hotels_name_and_url)
        hotel_urls = parse_url(hotels_name_and_url)
        hotel_cuisines = parse_name(soup.select("div.res-snippet-small-cuisine"))
        hotel_localities = to_array(soup.select("div.ln24 > a.cblack"))
        hotel_costfor2 = parse_and_split([node.next.next for node in soup.select("div.ln24 > span.upc")])
        for name,url,cuisines,locality,costfor2 in map(None,hotel_names,hotel_urls,hotel_cuisines,hotel_localities,hotel_costfor2):
            print name,url,cuisines,locality,costfor2
            insert_in_db(name,url,cuisines,locality,costfor2)

