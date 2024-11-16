import aiohttp
import asyncio
import json

class GoldAppleParser:
    """`
    Асинхронный парсер для сбора информации о товарах с сайта 'Золотое Яблоко'.
    """
    BASE_URL = "https://goldapple.ru/front/api/catalog/products"

    def __init__(self, category_ids_file, output_file, proxies_file, city_id="0c5b2444-70a0-4932-980c-b4dc0d3f02b5"):
        """
        Инициализация парсера.

        :param category_ids_file: Файл с идентификаторами категорий.
        :param output_file: Имя выходного файла формата jsonL.
        :param proxies_file: Файл с прокси.
        :param city_id: Идентификатор города (по умолчанию - Москва).
        """
        self.category_ids_file = category_ids_file
        self.output_file = output_file
        self.city_id = city_id
        self.proxies_file = proxies_file

    def load_proxies(self):
        """
        Загрузка прокси из файла.

        :return: Список прокси.
        """
        with open(self.proxies_file, "r") as file:
            return file.read().splitlines()

    async def fetch_page(self, session, category_id, page_number, proxy):
        """
        Получение страницы товаров из API.

        :param session: Экземпляр aiohttp.ClientSession.
        :param category_id: Идентификатор категории.
        :param page_number: Номер страницы.
        :param proxy: Прокси для этой категории.
        :return: Ответ в формате JSON или None.
        """
        params = {
            "categoryId": category_id,
            "cityId": self.city_id,
            "pageNumber": page_number,
            "z": "16-46"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
        }
        try:
            async with session.get(self.BASE_URL, params=params, headers=headers, proxy=proxy) as response:
                if response.status == 200:
                    return await response.json()
        except Exception as e:
            print(f"Ошибка при запросе с прокси {proxy}: {e}")
        return None

    async def parse_category(self, session, category_id, proxy):
        """
        Парсинг всех страниц категории.

        :param session: Экземпляр aiohttp.ClientSession.
        :param category_id: Идентификатор категории.
        :param proxy: Прокси для этой категории.
        """
        page_number = 1
        results = []
        while True:
            print(f"Парсинг категории {category_id}, страница {page_number} с прокси {proxy}")
            data = await self.fetch_page(session, category_id, page_number, proxy)
            if not data or not data["data"]["products"]:
                break
            for product in data["data"]["products"]:
                results.append({
                    "id": product["itemId"],
                    "name": product["name"],
                    "brand": product.get("brand", "Не указан"),
                    "type": product.get("productType", "Не указан"),
                    "photos": [img["url"].replace("${screen}.${format}", "fullhd.jpg") for img in product["imageUrls"]],
                    "in_stock": product["inStock"],
                    "price": product["price"]["actual"]["amount"] if product["price"]["actual"] else None
                })
            page_number += 1
        return results

    async def run(self):
        """
        Запуск парсинга.
        """
        async with aiohttp.ClientSession() as session:
            with open(self.category_ids_file, "r") as file:
                category_ids = file.read().splitlines()

            proxies = self.load_proxies()  # Загрузка прокси

            # Проверка, что количество прокси и категорий совпадает
            if len(proxies) < len(category_ids):
                print("Ошибка: недостаточно прокси для всех категорий.")
                return

            # Асинхронно запускаем парсинг всех категорий с использованием разных прокси
            tasks = []
            for category_id, proxy in zip(category_ids, proxies):
                tasks.append(self.parse_category(session, category_id, proxy))

            all_results = await asyncio.gather(*tasks)

            with open(self.output_file, "w", encoding="utf-8") as file:
                for category_results in all_results:
                    for item in category_results:
                        file.write(json.dumps(item, ensure_ascii=False) + "\n")

            print(f"Парсинг завершен. Сохранено товаров в {self.output_file}.")

if __name__ == "__main__":
    category_ids_file = "categoryID.txt"  # Файл с категориями
    output_file = "products.jsonl"  # Файл для сохранения результатов
    proxies_file = "proxy.txt"  # Файл с прокси

    parser = GoldAppleParser(category_ids_file, output_file, proxies_file)
    asyncio.run(parser.run())