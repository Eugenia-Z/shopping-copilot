import csv
import time
import random
import requests
import traceback
from time import sleep
from lxml import etree

def get_one_page(url):
    n = 3
    while True:
        try:
            sleep(random.uniform(1,2))
            headers = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36",
                'Referer': 'http://yz.yuzhuprice.com:8003/findProductPriceByCondition.jspx?page.curPage=1&categoryName=%E7%BA%A2%E6%9C%A8%E7%B1%BB&market=0&prName=&t1=2019-01-01&t2=2020-05-08'}
            response = requests.get(url, headers=headers, timeout=10)
            return response.text
        except(TimeoutError, Exception):
            n -= 1
            if n == 0:
                print("The request failed 3 times. Abandoning this URL request. Check the request conditions.")
                return
            else:
                print("The request failed. Please try again.")
                continue

def parse_one_page(html):
    try:
        parse = etree.HTML(html)
        items = parse.xpath('//*[@id="173200"]')
        for item in items:
            item = {
                'name':''.join(item.xpath('./td[1]/text()')).strip(),
                'price':''.join(item.xpath('./td[2]/text()')).strip(),
                'unit':''.join(item.xpath('./td[3]/text()')).strip(),
                'supermarket':''.join(item.xpath('./td[4]/text()')).strip(),
                'time':''.join(item.xpath('./td[5]/text()')).strip(),
            }
            print(item)
        try:
            with open ('./wood.csv','a', encoding = 'utf_8_sig', newline = '') as file:
                fieldnames = ['name','price','unit','supermarket','time']
                writer = csv.DictWriter(file, fieldnames)
                writer.writerow(item)
        except Exception:
            print(traceback.print_exc())
    except Exception:
        print(traceback.print_exc())
        

def main(x):
    url = 'http://yz.yuzhuprice.com:8003/findPriceByName.jspx?page.curPage={}&priceName=%E7%BA%A2%E6%9C%A8%E7%B1%BB'.format(x)
    html = get_one_page(url)
    parse_one_page(html)
    

if __name__ == '__main__':
    for i in range(1,5):
        main(x=i)
        time.sleep(random.uniform(1,2))
        print(str(i) + "th page parsing completed.")