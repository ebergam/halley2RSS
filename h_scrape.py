import csv, requests, time, sys
from bs4 import BeautifulSoup as bs
from lxml import etree
import xml.etree.ElementTree as ET
import urllib2
from lxml.html.clean import clean_html
from yattag import Doc
import datetime

#the input csv file is a list of xml source sites and their relative attributes
if len(sys.argv) > 1:
	elenco_albi = sys.argv[1]
else:
	elenco_albi = "elenco_albi_trial.csv"

#the output xmls
if len(sys.argv) > 2:
	output_dir = sys.argv[2]
else:
	output_dir = "./"

with open (elenco_albi, "rb") as csvfile:
	reader = csv.reader(csvfile)
	reader.next() # Skip the header row
	for line in reader:
		albo = line[0]
		url = line[1]
		title_xpath = line[2]
		pubDate_xpath = line[3]
		href_xpath = line[4]
		partial_url = line[5]
		pubEnd_xpath = line[6]
		uid_xpath = line[7]
		type_xpath = line[8]
		#META NEW SPECS
		RSS_title = line[9]
		RSS_link = line[10]
		RSS_description = line[11]
		channel_category_type = line[12]
		channel_category_municipality = line[13]
		channel_category_province = line[14]
		channel_category_region = line[15]
		channel_category_latitude = line[16]
		channel_category_longitude = line[17]
		channel_category_country = line[18]
		channel_category_name = line[19]
		channel_category_uid = line[20]
		time_format = line[21]
		channel_category_webmaster = line[22]

		def clean_date(x):
			try:
				d = datetime.datetime.strptime(x.text, time_format)
				clean_d = d.strftime("%a, %d %b %Y %H:%M:%S %z +0200")
				return clean_d
			except Exception as e:
				#print e
				clean_d = datetime.date.today().strftime("%a, %d %b %Y %H:%M:%S %z +0200") #METODO DA MIGLIORARE
				return clean_d

		title_list = []
		href_list = []
		pubDate_list = []
		pubEnd_list = []
		uid_list = []
		type_list = []
		guid_list = []
		
		raw_datalist = []

		def open_page():
			global page
			response = urllib2.urlopen(url)
			resp_data = response.read()
			page = etree.XML(resp_data)
			response.close()

		def scrape_data():
			global raw_datalist
			year = datetime.date.today().year
			#get and clean title
			title_tags = page.findall(title_xpath)
			for item in title_tags:
				rawname = bs(item.text.encode('utf-8'), "xml")
				clean_item = rawname.find('a').text
				title_list.append(clean_item.encode('utf-8'))

			#get hrefs
			href_tags = page.findall(href_xpath)
			for item in href_tags:
				guid_list.append(item.get('id'))
				href_clean = partial_url + item.get('id').encode('utf-8')
				href_list.append(href_clean)

			#get pubdates
			pubDate_tags = page.findall(pubDate_xpath)
			for item in pubDate_tags:
				pubDate_list.append(clean_date(item))

			#get pubends
			pubEnd_tags = page.findall(pubEnd_xpath)
			for item in pubEnd_tags:
				pubEnd_list.append(clean_date(item))

			#get uids
			uid_tags = page.findall(uid_xpath)
			for item in uid_tags:
				uid_list.append(str(year)+"/"+item.text)

			#get types
			tyep_tags = page.findall(type_xpath)
			for item in tyep_tags:
				type_list.append(item.text.encode('utf-8'))

			raw_datalist = zip(title_list, href_list, pubDate_list, pubEnd_list, uid_list, type_list, guid_list)
			#print raw_datalist

		def generate_csv():
			with open(albo +'_data.csv', 'wb') as f:	
				writer = csv.writer(f)
				for row in raw_datalist:
		   			writer.writerow(row)
		
		def generate_feed():
			doc, tag, text, line = Doc().ttl()
			doc.asis('<?xml version="1.0" encoding="UTF-8"?>')
			with tag('rss',
				('xmlns:atom', 'http://www.w3.org/2005/Atom'),
				('version', '2.0')
				):
					with tag('channel'):
						line('title', RSS_title)
						line('link', RSS_link) #TODO
						line('description', RSS_description)
						line('language', 'it')
						line('docs', 'http://albopop.it/comune/') #TODO
						line('category', channel_category_type, domain="http://albopop.it/specs#channel-category-type")
						line('category', channel_category_municipality, domain="http://albopop.it/specs#channel-category-municipality")
						line('category', channel_category_province, domain="http://albopop.it/specs#channel-category-province")
						line('category', channel_category_region, domain="http://albopop.it/specs#channel-category-region")
						line('category', channel_category_latitude, domain="http://albopop.it/specs#channel-category-latitude")
						line('category', channel_category_longitude, domain="http://albopop.it/specs#channel-category-longitude")
						line('category', channel_category_country, domain="http://albopop.it/specs#channel-category-country")
						line('category', channel_category_name, domain="http://albopop.it/specs#channel-category-name")
						line('category', channel_category_uid, domain="http://albopop.it/specs#channel-category-uid")
						line('webMaster', channel_category_webmaster)
						for row in raw_datalist:
			   				with tag('item'):
			   					line('title', row[0])
			   					line('link', row[1])
			   					line('description', row[0])
			   					line('pubDate', row[2])
			   					line('guid', row[6], isPermaLink="true")
			   					line('category', row[2], domain="http://albopop.it/specs#item-category-pubStart")
			   					line('category', row[3], domain="http://albopop.it/specs#item-category-pubEnd")
			   					line('category', row[4], domain="http://albopop.it/specs#item-category-uid")
			   					line('category', row[5], domain="http://albopop.it/specs#item-category-type")
		 	#print(doc.getvalue())
		 	with open(output_dir+'/'+albo+'_feed.xml','wf') as f:
		 		f.write(doc.getvalue())
		open_page()
		scrape_data()
		#generate_csv()	#solo per osservare, ma puo' servire da backup
		generate_feed()
