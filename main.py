import calendar
import smtplib
import ssl
from time import strptime
import requests
from bs4 import BeautifulSoup
import time as timed
import mysql.connector
from datetime import datetime

mydb = mysql.connector.connect(
    host="feederdb.c6v9iediak6n.ap-south-1.rds.amazonaws.com",
    port="3306",
    database='feederdb',
    user="admin",
    password="kesar302"
)

mycursor = mydb.cursor()

sender_email = "developmenthai95@gmail.com"
password = "Tonystark302@"
reciever_emailis = "arpanrocks95@gmail.com"
def send_mail(SUBJECT, TEXT, reciever_email=reciever_emailis):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, reciever_email, message)

money_control_dict = {
    "MoneyControl":
        {
            "latest_news": 'https://www.moneycontrol.com/rss/latestnews.xml',
            "business": 'https://www.moneycontrol.com/rss/business.xml',
            "brokers_news": 'https://www.moneycontrol.com/rss/brokeragerecos.xml',
            "buzzing_stocks": 'https://www.moneycontrol.com/rss/buzzingstocks.xml',
            "economy": 'https://www.moneycontrol.com/rss/economy.xml',
            "market_reports": 'https://www.moneycontrol.com/rss/marketreports.xml',
            "international_markets": 'https://www.moneycontrol.com/rss/internationalmarkets.xml',
            "market_edge": 'https://www.moneycontrol.com/rss/marketedge.xml',
            "market_outlook": 'https://www.moneycontrol.com/rss/marketoutlook.xml',
            "technicals": 'https://www.moneycontrol.com/rss/technicals.xml',
            "ipo_news": 'https://www.moneycontrol.com/rss/iponews.xml',
            "_mutual_funds_news": 'https://www.moneycontrol.com/rss/mfnews.xml',
            "nri": 'https://www.moneycontrol.com/rss/nrinews.xml',
            "results": 'https://www.moneycontrol.com/rss/results.xml',
            "technology": 'https://www.moneycontrol.com/rss/technology.xml',
            "entertainment": 'https://www.moneycontrol.com/rss/entertainment.xml',
            "world_news": 'https://www.moneycontrol.com/rss/worldnews.xml',
            "sports": 'https://www.moneycontrol.com/rss/sports.xml',
            "current_affairs": 'https://www.moneycontrol.com/rss/currentaffairs.xml',
            "currency": 'https://www.moneycontrol.com/rss/currency.xml',
            'top_videos': 'https://www.moneycontrol.com/rss/topvideos.xml',
        },
}

cnbc_tv_18 = {
    "CNBCTV18": {
        "entire_news_cnbc": 'https://www.cnbctv18.com/rss/',
    },
}


def monthToNum(shortMonth):
    return {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr': 4,
        'may': 5,
        'jun': 6,
        'jul': 7,
        'aug': 8,
        'sep': 9,
        'oct': 10,
        'nov': 11,
        'dec': 12
    }[shortMonth]


# scraping function
def scrapemoneycontrol():
    print("Scraping at : ", datetime.now(), "For MoneyControl")
    for website_name in money_control_dict.keys():
        for category in money_control_dict[website_name].keys():
            try:
                print("Inside try: ", category)
                r = requests.get(money_control_dict[website_name][category])
                # print(website_name, category,urls_dict[website_name][category] )
                # exit()
                soup = BeautifulSoup(r.content, features='xml')

                articles = soup.findAll('item')

                for a in articles:
                    title = a.find('title').text
                    link = a.find('link').text
                    published = a.find('pubDate').text

                    datedata = published.replace(',', '').split(' ')

                    date_time = datetime(year=int(datedata[3]), month=monthToNum(str(datedata[2]).lower()),
                                         day=int(datedata[1]),
                                         hour=int(datedata[4].split(":")[0]), minute=int(datedata[4].split(":")[1]),
                                         second=int(datedata[4].split(":")[2]))

                    sql_check = "SELECT *, COUNT(*) FROM feeds_feeds WHERE title = %s and link = %s and published = " \
                                "%s and website = %s and category = %s "
                    val = (title, link, date_time, website_name, category)
                    mycursor.execute(sql_check, val)

                    count = mycursor.fetchone()[4]
                    print(count)
                    if not count:
                        print(title, link, published, website_name, category)

                        sql = "INSERT INTO feeds_feeds (title, link, published, website, category) VALUES (%s, %s, %s, %s, %s)"
                        mycursor.execute(sql, val)
                        mydb.commit()

                        print(mycursor.rowcount, "record inserted.")


            except Exception as e:
                print('The scraping job for MONEYCONTROL failed. See exception:')
                print(e)
                print("Exception accured ")


def scrapecnbctv18():
    print("Scraping at : ", datetime.now(), "For CNBC TV 18")
    for website_name in cnbc_tv_18.keys():
        for category in cnbc_tv_18[website_name].keys():
            try:
                print("Inside try: ", category)
                r = requests.get(cnbc_tv_18[website_name][category])

                soup = BeautifulSoup(r.content, features='xml')
                channel = soup.find('channel')
                articles = channel.findAll('item')
                for a in articles:
                    title = str(a.find('title').text)
                    link = a.find('link').text
                    published = a.find('pubDate').text

                    datedata = (published.replace('-', ' ')).replace('T', ' ').split(' ')
                    timedata_first = datedata[3]
                    timedata_second = timedata_first.split(':')

                    article_year = datedata[0]
                    article_month = datedata[1]
                    article_date = datedata[2]
                    article_hour = timedata_second[0]
                    article_minute = timedata_second[1]
                    article_second = timedata_second[2].split('+')[0]

                    # exit()
                    date_time = datetime(year=int(article_year), month=int(article_month),
                                         day=int(article_date),
                                         hour=int(article_hour), minute=int(article_minute),
                                         second=int(article_second))

                    sql_check = "SELECT *, COUNT(*) FROM feeds_feeds WHERE title = %s and link = %s and published = %s and website = %s and category = %s"
                    val = (title, link, date_time, website_name, category)
                    mycursor.execute(sql_check, val)

                    count = mycursor.fetchone()[4]

                    print(count)
                    if not count:
                        print(title, link, published, website_name, category)

                        sql = "INSERT INTO feeds_feeds (title, link, published, website, category) VALUES (%s, %s, %s, %s, %s)"
                        mycursor.execute(sql, val)
                        mydb.commit()

                        print(mycursor.rowcount, "record inserted.")



            except Exception as e:
                print('The scraping job for CNBCTV18 failed. See exception:')
                print(e)
                print("Exception accured ")

economics_times = {
    "ET-MARKETS": {
        "stocks": 'https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms',
        "IPOs/FPOs": "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
        "Market Moguls": "https://economictimes.indiatimes.com/markets/market-moguls/rssfeeds/54953131.cms",
        "Expert Views": "https://economictimes.indiatimes.com/markets/expert-view/rssfeeds/50649960.cms",
        "Commodities": "https://economictimes.indiatimes.com/markets/commodities/rssfeeds/1808152121.cms",
        "Forex": "https://economictimes.indiatimes.com/markets/forex/rssfeeds/1150221130.cms",
        "Bonds": "https://economictimes.indiatimes.com/markets/bonds/rssfeeds/2146846.cms",
        "Webinars": "https://economictimes.indiatimes.com/markets/webinars/rssfeeds/53555278.cms",
        "Gold": "https://economictimes.indiatimes.com/rsssymbolfeeds/commodityname-Gold.cms",
        "Podcast": "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/53613060.cms",
    },
    "ET-News": {
        "India": "https://economictimes.indiatimes.com/news/india/rssfeeds/81582957.cms",
        "Podcast": "https://economictimes.indiatimes.com/news/podcasts/rssfeeds/66647137.cms",
        "Morning Brief Podcast": "https://economictimes.indiatimes.com/news/morning-brief-podcast/rssfeeds/79263773.cms",
        "Newsblogs": "https://economictimes.indiatimes.com/news/newsblogs/rssfeeds/65098458.cms",
        "Economy": "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms",
        "Politics": "https://economictimes.indiatimes.com/news/politics-and-nation/rssfeeds/1052732854.cms",
        "Company": "https://economictimes.indiatimes.com/news/company/rssfeeds/2143429.cms",
        "Defence": "https://economictimes.indiatimes.com/news/defence/rssfeeds/46687796.cms",
        "International": "https://economictimes.indiatimes.com/news/international/rssfeeds/858478126.cms",
        "ET Evoke": "https://economictimes.indiatimes.com/news/et-evoke/rssfeeds/79339235.cms",
        "Elections": "https://economictimes.indiatimes.com/news/elections/rssfeeds/65869819.cms",
        "ET Explains": "https://economictimes.indiatimes.com/news/et-explains/rssfeeds/64552206.cms",
        "Sports": "https://economictimes.indiatimes.com/news/sports/rssfeeds/26407562.cms",
        "Science": "https://economictimes.indiatimes.com/news/science/rssfeeds/39872847.cms",
        "India Unlimited": "https://economictimes.indiatimes.com/news/india-unlimited/rssfeeds/45228216.cms",
        "Environment": "https://economictimes.indiatimes.com/news/environment/rssfeeds/2647163.cms",
        "ET TV": "https://economictimes.indiatimes.com/news/et-tv/rssfeedsvideo/48897386.cms",
        "Latest News": "https://economictimes.indiatimes.com/news/latest-news/rssfeeds/20989204.cms",
    },
    "Economics Times": {
        "ET Home": "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
        "Top Stories": "https://economictimes.indiatimes.com/rssfeedstopstories.cms",
        "Budget 2021": "https://economictimes.indiatimes.com/budget-2021/rssfeeds/79755474.cms",
        "ETPrime": "https://economictimes.indiatimes.com/prime/rssfeeds/69891145.cms",
        "Tech":"https://economictimes.indiatimes.com/tech/rssfeeds/13357270.cms",
        "Wealth": "https://economictimes.indiatimes.com/wealth/rssfeeds/837555174.cms",
    },
}


def scrape_economics_times():
    print("Scraping at : ", datetime.now())
    for website_name in economics_times.keys():
        for category in economics_times[website_name].keys():
            try:
                print("Inside try: ", category)
                r = requests.get(economics_times[website_name][category])

                soup = BeautifulSoup(r.content, features='xml')
                channel = soup.find('channel')
                articles = channel.findAll('item')
                print(articles)
                # exit()
                for a in articles:
                    title = str(a.find('title').text)
                    link = a.find('link').text
                    published = a.find('pubDate').text
                    print(title, link, published)

                    datedata = (published.replace('-', ' ')).replace('T', ' ').split(' ')
                    timedata_first = datedata[3]
                    timedata_second = timedata_first.split(':')

                    article_year = datedata[0]
                    article_month = datedata[1]
                    article_date = datedata[2]
                    article_hour = timedata_second[0]
                    article_minute = timedata_second[1]
                    article_second = timedata_second[2].split('+')[0]

                    # exit()
                    date_time = datetime(year=int(article_year), month=int(article_month),
                                         day=int(article_date),
                                         hour=int(article_hour), minute=int(article_minute),
                                         second=int(article_second))

                    sql_check = "SELECT *, COUNT(*) FROM feeds_feeds WHERE title = %s and link = %s and published = %s and website = %s and category = %s"
                    val = (title, link, date_time, website_name, category)
                    mycursor.execute(sql_check, val)

                    count = mycursor.fetchone()[4]

                    print(count)
                    if not count:
                        print(title, link, published, website_name, category)

                        sql = "INSERT INTO feeds_feeds (title, link, published, website, category) VALUES (%s, %s, %s, %s, %s)"
                        mycursor.execute(sql, val)
                        mydb.commit()

                        print(mycursor.rowcount, "record inserted.")



            except Exception as e:
                print('The scraping job for CNBCTV18 failed. See exception:')
                print(e)
                print("Exception accured ")


def scrape_everything():
    scrapemoneycontrol()
    scrapecnbctv18()
    scrape_economics_times()
    timed.sleep(60)
    scrape_everything()




if __name__ == '__main__':
    try:
        scrape_everything()
    except Exception as e:
        print(e)
    finally:
        send_mail(SUBJECT="An error has accured in scrapy",
                  TEXT="Please check the error \n".encode('utf-8') + str(e).encode('utf-8'))
        scrape_everything()
