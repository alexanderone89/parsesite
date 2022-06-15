import aiohttp
import asyncio
import requests
from bs4 import BeautifulSoup
import re
import time
import json
import pandas as pd


result_list = []
start_time = time.time()
sem = asyncio.Semaphore(100)

def get_count():
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;"
                  " q = 0.9, image / avif, image / webp, image / apng,"
                  " * / *;q = 0.8,application/signed-exchange; v = b3; q = 0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67"
                      " Safari/537.36"
    }

    url = "http://air.intim-city.nl/persons.php?"\
          "type=0&style=0&new=0&video=N&retouch=0&index=0"

    response = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")
    # count = int(soup.find_all("a", class_="index")[-2].text)
    # print(count)
    #
    # carts = soup.find_all("table", class_="psnCrd1")
    # for i, cart in enumerate(carts):
    #     m = re.findall(r'\d+', cart['id'])[0]
        # print(f"{i}     {m}")

    page_count = int(soup.find_all("a", class_="index")[-2].text)
    print(f"_____________________________________{page_count}")


    for page in range(0 ,page_count - 1):
        url_page = f"http://air.intim-city.nl/persons.php?" \
                f"type=0&style=0&new=0&video=N&retouch=0&index={page}"
        response_p = requests.get(url=url_page, headers=headers)
        soup = BeautifulSoup(response_p.text, "lxml")
        carts = soup.find_all("table", class_="psnCrd1")

        for card in carts:
            card_id = re.findall(r'\d+', card['id'])[0]
            print(card_id)
            url_card = f"http://air.intim-city.nl/persons.php?id={card_id}"
            response_c = requests.get(url=url_card, headers=headers)
            soup = BeautifulSoup(response_c.text, "lxml")

            try:
                tel = soup.find("a", class_="b").text
                print(f"-----{tel}")
            except:
                print("Телефон не найден")
            try:
                id_cart = soup.find("tr", class_="noprint").find_all("td")[-1].text
                print(f"-----{id_cart}")
            except:
                print("ID не найден")
            try:
                date_update = soup.find("td", text=re.compile(r'(0?[1-9]|[12][0-9]|3[01])([\.\\\/-])(0?[1-9]|1[012])\2(((19|20)\d\d)|(\d\d))')).text
                print(f"-----{date_update}")
            except:
                print("Дата обновления не найдена")
            try:
                metro_count = soup.find_all("a", class_="menu", title=re.compile("Проститутки"))[1:]
                metl = []
                for i in metro_count:
                    metl.append(i.text)
                metro = ",".join(metl)
                print(f"-----{metro}")
            except:
                print("Метро не найдено")
            try:
                region = soup.find_all("a", class_="menu", title=re.compile("Проститутки"))[0].text
                print(f"-----{region}")
            except:
                print("Район не найден")

            result_list.append(
                {
                    "id": id_cart,
                    "tel": tel,
                    "date_update": date_update,
                    "region": region,
                    "metro": metro
                }
            )

            print(f"[INFO] Обработал страницу {card_id}")
    with open('jsfile.txt', 'w') as file:
        json.dump(result_list, file)
    data = json.loads(str(result_list))
    df = pd.DataFrame(data)
    df.to_csv("result.csv", index=False)

    data = json.loads(str(result_list))
    df = pd.DataFrame(data)
    df.to_excel("result.xls", index=False)

async def get_card_data(session, id):
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;"
                  " q = 0.9, image / avif, image / webp, image / apng,"
                  " * / *;q = 0.8,application/signed-exchange; v = b3; q = 0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67"
                      " Safari/537.36"
    }

    url = f"http://air.intim-city.nl/persons.php?id={id}"

    async with session.get(url=url, headers=headers) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, "lxml")

        try:
            tel = ""
            tel = soup.find("a", class_="b").text
        except:
            print("Телефон не найден")
        try:
            id_cart = ""
            id_cart = soup.find("tr", class_="noprint").find_all("td")[-1].text
        except:
            print("ID не найден")
        try:
            date_update = ""
            date_update = soup.find("td", text=re.compile(
                r'(0?[1-9]|[12][0-9]|3[01])([\.\\\/-])(0?[1-9]|1[012])\2(((19|20)\d\d)|(\d\d))')).text
        except:
            print("Дата обновления не найдена")
        try:
            metro_count = ""
            metro_count = soup.find_all("a", class_="menu", title=re.compile("Проститутки"))[1:]
            metl = []
            for i in metro_count:
                metl.append(i.text)
            metro = ",".join(metl)
        except:
            print("Метро не найдено")
        try:
            region = ""
            region = soup.find_all("a", class_="menu", title=re.compile("Проститутки"))[0]
        except:
            print("Район не найден")

        # *Номер анкеты
        # *Номер телефона
        # *Дата  обновления
        # *Номер анкеты
        # *Город
        # *Метро
        # *Район
        result_list.append(
            {
                "id": id_cart,
                "tel": tel,
                "date_update": date_update,
                "region": region,
                "metro": metro
            }
        )
    print(f"[INFO] Обработал анкету {id_cart}    {len(result_list)}")
    await asyncio.sleep(2)

async def get_page_data(session, page):
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;"
                  " q = 0.9, image / avif, image / webp, image / apng,"
                  " * / *;q = 0.8,application/signed-exchange; v = b3; q = 0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67"
                      " Safari/537.36"
    }

    url = f"http://air.intim-city.nl/persons.php?"\
          f"type=0&style=0&new=0&video=N&retouch=0&index={page}"

    async with session.get(url=url, headers=headers) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, "lxml")
        carts = soup.find_all("table", class_="psnCrd1")

        for cart in carts:
            cart_id = re.findall(r'\d+', cart['id'])[0]
            await get_card_data(session, cart_id)
            await asyncio.sleep(1)


async def ffff():
    print("erttrter")

async def get_data():
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;"
                  " q = 0.9, image / avif, image / webp, image / apng,"
                  " * / *;q = 0.8,application/signed-exchange; v = b3; q = 0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67"
                      " Safari/537.36"
    }

    url = "http://air.intim-city.nl/persons.php?type=0&style=0&new=0&video=N&retouch=0&index=0"

    connector = aiohttp.TCPConnector(limit=50)

    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        response = await session.get(url=url, headers=headers)

        soup = BeautifulSoup(await response.text(), "lxml")

        page_count = int(soup.find_all("a", class_="index")[-2].text)
        print(f"_____________________________________{page_count}")
        tasks = []

        for page in range(0, page_count):
            task = asyncio.create_task(get_page_data(session, page))
            # task = asyncio.create_task(ffff())
            tasks.append(task)
        await asyncio.gather(*tasks)


def main():
    get_count()
    # asyncio.run(get_data())
    finish_time = time.time() - start_time
    print(f"Затраченное время {finish_time}")


if __name__ == '__main__':
    main()
    # get_count()