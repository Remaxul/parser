import json
import logging
import time
import threading
import signal
import sys
import random
import asyncio
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from telegram import Bot
import schedule
import os
from threading import Semaphore

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
MANGALIB_URL = "https://mangalib.me/ru/collections"
SLASHLIB_URL = "https://v2.shlib.life/ru/collections"
CURRENT_COLLECTIONS_FILE_MANGALIB = "current_collections_mangalib.json"
COLLECTION_DATA_FILE_MANGALIB = "collection_data_mangalib.json"
CURRENT_COLLECTIONS_FILE_SLASHLIB = "current_collections_slashlib.json"
COLLECTION_DATA_FILE_SLASHLIB = "collection_data_slashlib.json"
TELEGRAM_TOKEN = "7552508743:AAEmGQw499vk_94gzzbHh4drkZdsd45Zz9Q"
CHAT_ID = "-1002589466518"
bot = Bot(token=TELEGRAM_TOKEN)

# Флаг для остановки
running = True
# Флаг для отслеживания выполнения полного парсинга
full_parse_running = False
# Флаг для отслеживания выполнения минутных проверок
check_running = False
# Семафор для ограничения одновременных потоков с selenium
selenium_semaphore = Semaphore(1)
# Очередь для отправки сообщений в Telegram
message_queue = asyncio.Queue()
loop = None

# Загрузка текущего состояния коллекций
def load_current_collections(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

# Сохранение текущего состояния коллекций
def save_current_collections(collection_ids, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(list(collection_ids), f, ensure_ascii=False, indent=4)
    logger.info(f"Состояние коллекций сохранено в {filename}")

# Сохранение данных о коллекциях
def save_collection_data(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info(f"Данные коллекций сохранены в {filename}")

def fetch_page(url, scroll=False, max_retries=3, max_scroll_time=360):
    """Получает HTML-код страницы с помощью Selenium с повторными попытками."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--ignore-certificate-errors")

    for attempt in range(max_retries):
        driver = None
        try:
            with selenium_semaphore:
                driver = webdriver.Chrome(options=chrome_options)
                logger.info(f"Попытка {attempt + 1}/{max_retries}: Запуск Selenium для загрузки страницы: {url} (поток: {threading.current_thread().name})")
                driver.set_page_load_timeout(30)
                driver.get(url)

                # Ожидаем появления контейнера с коллекциями
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.aj_ak.es_ca"))
                )
                logger.info("Контейнер с коллекциями (div.aj_ak.es_ca) успешно загружен.")

                if scroll and running:
                    # Прокрутка страницы до конца для подгрузки всех коллекций
                    initial_fade_count = len(driver.find_elements(By.CLASS_NAME, "fade"))
                    logger.info(f"Начальное количество элементов с классом fade: {initial_fade_count}")
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    start_time = time.time()
                    while running:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(random.uniform(3, 5))
                        new_height = driver.execute_script("return document.body.scrollHeight")
                        current_fade_count = len(driver.find_elements(By.CLASS_NAME, "fade"))
                        elapsed_time = time.time() - start_time
                        if new_height == last_height and current_fade_count == initial_fade_count:
                            logger.info("Высота и количество элементов не изменились, прокрутка завершена.")
                            break
                        if elapsed_time > max_scroll_time:
                            logger.warning(f"Превышено максимальное время прокрутки ({max_scroll_time} сек). Останавливаем прокрутку.")
                            break
                        last_height = new_height
                        initial_fade_count = current_fade_count
                        logger.info(f"Прокрутка страницы: подгружаются новые коллекции... (текущее количество fade: {current_fade_count})")

                    # Ожидаем, пока появится хотя бы один элемент с классом fade
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "fade"))
                    )
                    logger.info("Все коллекции подгружены.")

                html_content = driver.page_source
                logger.info(f"Успешно получена отрендеренная страница: {url}")
                # Логируем первые 500 символов HTML для отладки
                html_snippet = html_content[:500]
                logger.info(f"Первые 500 символов HTML: {html_snippet}")
                driver.quit()
                return html_content
        except Exception as e:
            logger.error(f"Ошибка при загрузке страницы {url} (попытка {attempt + 1}/{max_retries}): {e}")
            if driver:
                driver.quit()
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))
                logger.info("Ожидание перед повторной попыткой...")
            else:
                logger.error("Все попытки исчерпаны.")
                return None

def parse_collections(html_content, site="mangalib"):
    """Парсит все коллекции из HTML-кода страницы."""
    if not html_content:
        logger.error("HTML-контент пустой.")
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    logger.info("HTML успешно распарсен с помощью BeautifulSoup.")

    # Находим контейнер с коллекциями (div с классами aj_ak es_ca)
    collections_container = soup.find("div", class_="aj_ak es_ca")
    if not collections_container:
        logger.error("Не удалось найти контейнер с коллекциями на странице (div.aj_ak.es_ca).")
        # Логируем все div с классом aj_ak для отладки
        all_aj_ak_divs = soup.find_all("div", class_="aj_ak")
        logger.info(f"Найдено {len(all_aj_ak_divs)} div с классом aj_ak:")
        for i, div in enumerate(all_aj_ak_divs, 1):
            logger.info(f"div {i}: {str(div)[:100]}...")
        return None
    logger.info(f"Найден контейнер с коллекциями: {str(collections_container)[:100]}...")

    # Определяем классы для коллекций в зависимости от сайта
    if site == "mangalib":
        collection_classes = ["fade ox_n", "fade ox_n ox_oy"]
    else:  # slashlib
        collection_classes = ["fade ox_n ox_os", "fade ox_n ox_oy ox_os"]

    # Находим все коллекции
    collection_links = collections_container.find_all("a", class_=collection_classes)
    if not collection_links:
        logger.error(f"Не удалось найти теги <a> с классами {collection_classes}.")
        # Логируем все теги <a> с классом fade для отладки
        all_fade_links = collections_container.find_all("a", class_="fade")
        logger.info(f"Найдено {len(all_fade_links)} тегов <a> с классом fade:")
        for i, link in enumerate(all_fade_links, 1):
            logger.info(f"link {i}: {str(link)[:100]}...")
        return None
    logger.info(f"Найдено {len(collection_links)} коллекций.")

    # Список для хранения данных о коллекциях
    collections_data = []

    # Обрабатываем каждую коллекцию
    for idx, collection_link in enumerate(collection_links, 1):
        if not running:
            break

        logger.info(f"Обработка коллекции {idx}...")

        # Извлекаем ссылку из тега <a>
        link = collection_link.get("href")
        base_url = "https://mangalib.me" if site == "mangalib" else "https://v2.shlib.life"
        if link and not link.startswith(base_url):
            link = base_url + link
        collection_id = link.split("/collections/")[-1] if "/collections/" in link else link
        logger.info(f"Извлечённая ссылка: {link}, ID: {collection_id}")

        # Находим блок с названием внутри тега <a> (div.ox_e2)
        collection_block = collection_link.find("div", class_="ox_e2")
        if not collection_block:
            logger.warning(f"Не удалось найти блок коллекции (div.ox_e2) для ссылки {link}. Пропускаем.")
            continue
        logger.info(f"Найден блок коллекции: {str(collection_block)[:100]}...")

        # Извлекаем название (div с классом ox_at внутри ox_e2)
        title_elem = collection_block.find("div", class_="ox_at")
        title = title_elem.get_text(strip=True) if title_elem else "Без названия"
        logger.info(f"Извлечённое название: {title}")

        # Добавляем данные о коллекции в список
        collections_data.append({
            "title": title,
            "link": link
        })

    logger.info(f"Всего извлечено {len(collections_data)} коллекций.")
    return collections_data

async def telegram_message_worker():
    """Асинхронный обработчик очереди сообщений для отправки в Telegram."""
    while running:
        try:
            message = await message_queue.get()
            if message is None:
                break
            await bot.send_message(chat_id=CHAT_ID, text=message)
            logger.info("Сообщение успешно отправлено в Telegram.")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
        finally:
            message_queue.task_done()

def queue_telegram_message(message):
    """Добавляет сообщение в очередь для отправки в Telegram."""
    try:
        asyncio.run_coroutine_threadsafe(message_queue.put(message), loop)
    except Exception as e:
        logger.error(f"Ошибка при добавлении сообщения в очередь: {e}")

def check_new_collections_mangalib():
    """Проверяет наличие новых коллекций на Mangalib и отправляет уведомления в Telegram."""
    global full_parse_running, check_running
    if full_parse_running:
        logger.info("Полный парсинг выполняется, пропускаем ежеминутную проверку (Mangalib).")
        return
    if check_running:
        logger.info("Другая минутная проверка выполняется, пропускаем проверку (Mangalib).")
        return

    check_running = True
    try:
        if not running:
            return

        logger.info("Запуск проверки новых коллекций (Mangalib)... (поток: %s)", threading.current_thread().name)
        html_content = fetch_page(MANGALIB_URL, scroll=False)
        if not html_content:
            return

        current_collections = parse_collections(html_content, site="mangalib")
        if not current_collections:
            return

        # Извлекаем ID коллекций
        current_ids = {col["link"].split("/collections/")[-1] for col in current_collections if "/collections/" in col["link"]}
        previous_ids = load_current_collections(CURRENT_COLLECTIONS_FILE_MANGALIB)

        # Проверяем новые коллекции
        new_ids = current_ids - previous_ids
        if new_ids:
            logger.info(f"Найдено {len(new_ids)} новых коллекций (Mangalib): {new_ids}")
            new_collections = [col for col in current_collections if col["link"].split("/collections/")[-1] in new_ids]
            for col in new_collections:
                message = f"Mangalib: Новая коллекция:\nНазвание: {col['title']}\nСсылка: {col['link']}"
                queue_telegram_message(message)
                logger.info(f"Отправлено уведомление в Telegram (Mangalib): {col['title']}")
        else:
            logger.info("Новых коллекций не найдено (Mangalib).")

        # Проверяем удалённые коллекции
        removed_ids = previous_ids - current_ids
        if removed_ids:
            logger.info(f"Удалено {len(removed_ids)} коллекций (Mangalib): {removed_ids}")

        # Обновляем состояние
        save_current_collections(current_ids, CURRENT_COLLECTIONS_FILE_MANGALIB)
    finally:
        check_running = False

def check_new_collections_slashlib():
    """Проверяет наличие новых коллекций на Slashlib и отправляет уведомления в Telegram."""
    global full_parse_running, check_running
    if full_parse_running:
        logger.info("Полный парсинг выполняется, пропускаем ежеминутную проверку (Slashlib).")
        return
    if check_running:
        logger.info("Другая минутная проверка выполняется, пропускаем проверку (Slashlib).")
        return

    check_running = True
    try:
        if not running:
            return

        logger.info("Запуск проверки новых коллекций (Slashlib)... (поток: %s)", threading.current_thread().name)
        html_content = fetch_page(SLASHLIB_URL, scroll=False)
        if not html_content:
            return

        current_collections = parse_collections(html_content, site="slashlib")
        if not current_collections:
            return

        # Извлекаем ID коллекций
        current_ids = {col["link"].split("/collections/")[-1] for col in current_collections if "/collections/" in col["link"]}
        previous_ids = load_current_collections(CURRENT_COLLECTIONS_FILE_SLASHLIB)

        # Проверяем новые коллекции
        new_ids = current_ids - previous_ids
        if new_ids:
            logger.info(f"Найдено {len(new_ids)} новых коллекций (Slashlib): {new_ids}")
            new_collections = [col for col in current_collections if col["link"].split("/collections/")[-1] in new_ids]
            for col in new_collections:
                message = f"Slashlib: Новая коллекция:\nНазвание: {col['title']}\nСсылка: {col['link']}"
                queue_telegram_message(message)
                logger.info(f"Отправлено уведомление в Telegram (Slashlib): {col['title']}")
        else:
            logger.info("Новых коллекций не найдено (Slashlib).")

        # Проверяем удалённые коллекции
        removed_ids = previous_ids - current_ids
        if removed_ids:
            logger.info(f"Удалено {len(removed_ids)} коллекций (Slashlib): {removed_ids}")

        # Обновляем состояние
        save_current_collections(current_ids, CURRENT_COLLECTIONS_FILE_SLASHLIB)
    finally:
        check_running = False

def full_parse_mangalib():
    """Полный парсинг всех коллекций с прокруткой (Mangalib)."""
    global full_parse_running
    full_parse_running = True
    try:
        if not running:
            return

        logger.info("Запуск полного парсинга (Mangalib)... (поток: %s)", threading.current_thread().name)
        html_content = fetch_page(MANGALIB_URL, scroll=True)
        if not html_content:
            return

        collections_data = parse_collections(html_content, site="mangalib")
        if not collections_data:
            return

        save_collection_data(collections_data, COLLECTION_DATA_FILE_MANGALIB)
        current_ids = {col["link"].split("/collections/")[-1] for col in collections_data if "/collections/" in col["link"]}
        save_current_collections(current_ids, CURRENT_COLLECTIONS_FILE_MANGALIB)
    finally:
        full_parse_running = False

def full_parse_slashlib():
    """Полный парсинг всех коллекций с прокруткой (Slashlib)."""
    global full_parse_running
    full_parse_running = True
    try:
        if not running:
            return

        logger.info("Запуск полного парсинга (Slashlib)... (поток: %s)", threading.current_thread().name)
        html_content = fetch_page(SLASHLIB_URL, scroll=True)
        if not html_content:
            return

        collections_data = parse_collections(html_content, site="slashlib")
        if not collections_data:
            return

        save_collection_data(collections_data, COLLECTION_DATA_FILE_SLASHLIB)
        current_ids = {col["link"].split("/collections/")[-1] for col in collections_data if "/collections/" in col["link"]}
        save_current_collections(current_ids, CURRENT_COLLECTIONS_FILE_SLASHLIB)
    finally:
        full_parse_running = False

def run_scheduled_tasks():
    """Запуск задач в отдельных потоках."""
    # Минутные проверки (смещены во времени)
    schedule.every(1).minutes.do(lambda: threading.Thread(target=check_new_collections_mangalib, name="CheckThreadMangalib").start())
    schedule.every(1).minutes.at(":30").do(lambda: threading.Thread(target=check_new_collections_slashlib, name="CheckThreadSlashlib").start())

    # Полный парсинг (смещён во времени)
    schedule.every(60).minutes.do(lambda: threading.Thread(target=full_parse_mangalib, name="FullParseThreadMangalib").start())
    schedule.every(60).minutes.at(":05").do(lambda: threading.Thread(target=full_parse_slashlib, name="FullParseThreadSlashlib").start())

    # Начальный запуск
    threading.Thread(target=check_new_collections_mangalib, name="InitialCheckMangalib").start()
    threading.Thread(target=full_parse_mangalib, name="InitialFullParseMangalib").start()
    threading.Thread(target=check_new_collections_slashlib, name="InitialCheckSlashlib").start()
    threading.Thread(target=full_parse_slashlib, name="InitialFullParseSlashlib").start()

    # Бесконечный цикл для выполнения расписания
    while running:
        schedule.run_pending()
        time.sleep(1)

def signal_handler(sig, frame):
    """Обработчик сигналов для корректного завершения."""
    global running
    logger.info("Получен сигнал завершения. Останавливаем работу...")
    running = False
    asyncio.run_coroutine_threadsafe(message_queue.put(None), loop)
    sys.exit(0)

async def main_async():
    """Запуск асинхронного цикла для обработки сообщений."""
    await telegram_message_worker()

if __name__ == "__main__":
    # Создаём событийный цикл в главном потоке
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Запускаем обработчик сообщений в отдельном потоке
    threading.Thread(target=lambda: loop.run_until_complete(main_async()), daemon=True).start()

    # Устанавливаем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill

    # Запуск в отдельном потоке
    threading.Thread(target=run_scheduled_tasks, name="Scheduler").start()

    # Бесконечный цикл в главном потоке для ожидания сигналов
    while True:
        time.sleep(1)
