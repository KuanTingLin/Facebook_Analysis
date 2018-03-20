import requests
from bs4 import BeautifulSoup
import re
import json
from concurrent.futures import ThreadPoolExecutor, wait
from pymongo import MongoClient
from Stop_Watch import StopWatch

def get_url_links(page, post_urls):
    StopWatch.start()
    URL = "https://www.wetalk.tw/forum.php?mod=forumdisplay&fid=2&page={}".format(page)
    resp = requests.get(URL)
    StopWatch.split('')

    soup = BeautifulSoup(resp.text, "html5lib")
    tmp_urls = [a.get('href') for a in soup.select('tbody > tr > th > a.xst')]
    post_urls.extend(tmp_urls[2:])

def save_posts_info(url, news_list, count):
    news = {}
    resp_c = requests.get(url)
    soup_c = BeautifulSoup(resp_c.text, 'html5lib')

    news["title"] = soup_c.select("h1.ts")[0].text.replace('\n', '')
    news["datetime"] = soup_c.select('span.date-show-info')[0].text.replace('發表', '').strip()
    news["reporter"] = soup_c.select('a[itemprop="author"]')[0].text
    news["media"] = "wetalk"
    news["category"] = "Forum"
    news["hash"] = hash(resp_c.text)
    news["url"] = url
    news["content"] = soup_c.select("td.t_f > article")[0].text.replace('\n', '').replace('發表', '').strip()
    news["comments"] = []

    for i in range(len(soup_c.select("td.info-post-td > a"))):
        news["comments"].append({})
        news["comments"][i]["comment_content"] = "".join(
            [p.text for p in soup_c.select('[id^="postmessage_"] > div[align="left"]')])
        news["comments"][i]["datetime"] = soup_c.select("span.date-show-info")[0].text.replace('\n', '').replace('發表', '').strip()
        news["comments"][i]["user_id"] = soup_c.select("td.info-post-td > a")[0].text
    news_list.append(news)
    count.append('SUCCESS')  # 這裏我使用list做為計數器，因為list傳的是記憶體位址，各個thread執行完去append都不會出現問題

    try:
        if 0 == len(count) % 100:  # 每100個post輸出一次，先輸出為json檔做本地備份，同時利用insert.many() load進共享的mongodb
            temp_list = news_list[:100]
            del news_list[:100]
            output_as_json(len(count), 'wetalk', temp_list)
            load_into_mongodb('testDatabase', 'testCollection', temp_list)
            print('輸出一個檔案囉～')

    except Exception as e:
        with open("./error_log.txt", 'a', encoding='UTF-8') as f:
            f.write(str(datetime.now()) + '  ' + 'error : <' + str(e) + ' >' + '\n')
            # 開一個叫error_log的檔案，寫入Error message和產生時間，方便後人查找
            # 如此讓程式不要斷掉，且能在之後檢測error發生的時間點及狀況

def output_as_json(index, media, x):
    with open("./News_Crawler/{}/{}_{}.json".format(media, media, index), 'w', encoding='UTF-8') as f:
        jd = json.dumps(x, ensure_ascii=False, indent=4)
        f.write(jd)

def load_into_mongodb(db_name, collection_name, doc):
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(doc)

def show_db_data(db_name, collection_name):
    db = client[db_name]
    # utilize for-loop to iterate through curser-object
    for doc in db.collection_name.find():
        print(doc)

def main(THREAD_NUM):
    threads = ThreadPoolExecutor(THREAD_NUM)

    post_urls = []
    futures = [threads.submit(get_url_links, page, post_urls) for page in range(1, 2)]
    wait(futures)

    news_list = []
    count = []
    futures = [threads.submit(save_posts_info, post_url, news_list, count) for post_url in post_urls]
    wait(futures)

    # show_db_data('testDatabase', 'testCollection')

if __name__ == "__main__":
    StopWatch.start()
    client = MongoClient('localhost', 27017, maxPoolSize=None)  # 這裡需要指名你要連線的MongoDB所在的ip和port
    main(10)
    StopWatch.split()
