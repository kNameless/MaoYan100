import requests
from requests.exceptions import RequestException
from multiprocessing import Pool
import re
import json
from DBcm import UseDatabase


dbconfig = {
    'host': '127.0.0.1',
    'user': 'pachong',
    'password': 'l2204551',
    'database': 'pachongdb',
}

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"}


def get_one_page(url):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None


def parse_one_page(html):
    pattern = re.compile('<dd>.*?board-index.*?>(\d+)</i>.*?data-src="(.*?)".*?name"><a'
                         +'.*?>(.*?)</a>.*?star">(.*?)</p>.*?releasetime">(.*?)</p>'
                         +'.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?</dd>', re.S)

    items = re.findall(pattern, html)
    for item in items:
        yield {
            'indexs': item[0],
            'image': item[1],
            'title': item[2],
            'actor': item[3].strip()[3:],
            'time': item[4].strip()[5:],
            'score': item[5]+item[6],
        }



def write_to_flie(content):
    with open('result.txt', 'a', encoding='utf-8') as f:
        f.write(json.dumps(content, ensure_ascii=False) + '\n')
        f.close()


def write_to_sql(item):
    indexs = item['indexs']
    image = item['image']
    title = item['title']
    actor = item['actor']
    time = item['time']
    score = item['score']

    with UseDatabase(dbconfig) as cursor:
        _SQL = """insert into maoyan(title, indexs )values (%s, %s)
        """
        cursor.execute(_SQL, ('霸王别姬', '霸王别姬'))

def main(offset):
    url = 'http://maoyan.com/board/4?offset=' + str(offset)
    html = get_one_page(url)

    for item in parse_one_page(html):
        write_to_sql(item)



if __name__ == '__main__':
      for i in range(10):
        main(i*10)
     # pool = Pool(maxtasksperchild=1)
     # pool.map(main, [i*10 for i in range(10)])
