import bs4
import csv
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
#url start point for the loop
baseUrl = 'https://www.newegg.com/Desktop-Graphics-Cards/SubCategory/ID-48?Tid=7709&PageSize=96'
count = 1
index = baseUrl.find('?')
dataset = []

def Contains():
    containers = page_soup.findAll("a",{"class":"item-brand"})
    # number of classes on this page
    iterations = len(containers)
    for i in range(iterations):
        container = containers[i]
        nameSearch = page_soup.findAll("a",{'class':"item-title"})
        namer = nameSearch[i]
        contained = container
        #contained = contained.replace(branding, '')
        #strip_container = str(contained)[1:].replace(',', '')
        brand = str(contained.img['title'])
        name = str(str(namer.text).replace(',',''))
        if brand in name:
            name = str(str(namer.text).replace(',','').replace(brand,''))[1:]
        else:
            name = name = str(str(namer.text).replace(',',''))
        
        
        pricer = page_soup.findAll("li",{"class":"price-current"})
        priced = str(pricer[i].strong).strip("</strong>").replace(',','')
        
        shipper = page_soup.findAll("li",{"class":"price-ship"})
        shipped = str(shipper[i])[23:29].replace('$','').replace('S','')
        shipping = ''
        if shipped == 'Free ':
            shipping = '0'
        elif shipped == 'pecia':
            shipping = '?'
        else:
            shipping = shipped
            
        thewriter.writerow({'Brand':brand, 'Product Name': name, 'Price in $': priced, 'Shipping in $': shipping})
        

if __name__ == "__main__":
#opening up connection and grabbing the page
    with open('Scrapey.csv', 'w', newline='') as csvfile:
        fieldnames = ['Brand', 'Product Name', 'Price in $', 'Shipping in $']
        thewriter = csv.DictWriter(csvfile, fieldnames = fieldnames)
        thewriter.writeheader()
        uClient = uReq(baseUrl)
        page_html = uClient.read()
        #html parsing
        page_soup = soup(page_html, "html.parser")
        page_number = page_soup.findAll("span",{"class":"list-tool-pagination-text"})
        ender = int(str(page_number)[80:82]) + 1
        for t in range(1,ender): 
            if t == 1:
                Contains()
                count += 1
                print(count)
                
            else:
                uClient = uReq(baseUrl[:index] + '/Page-'+ str(count) + baseUrl[index:])
                page_html = uClient.read()
                #html parsing
                page_soup = soup(page_html, "html.parser")
                Contains()
                count += 1
                print(count)
        print('all pages printed') 
        uClient.close
                
            
        
       
