import discord
from discord.ext import commands
import random
import sqlite3
import os
from datetime import datetime, timedelta, timezone
import asyncio
import shutil
import glob
import json
import logging
from logging.handlers import RotatingFileHandler
import traceback
import sys
import io

# На Windows консоль по умолчанию в cp1251, и print с эмодзи роняет бота при запуске
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
# ============================================
# ========== НАСТРОЙКИ ЛОГИРОВАНИЯ ==========
# ============================================

LOG_FOLDER = "./data"
LOG_FILE = os.path.join(LOG_FOLDER, "bot.log")

# Создаём папку для логов если нет
if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER)

# Настройка логгера
logger = logging.getLogger('fat_bot')
logger.setLevel(logging.DEBUG)

# Формат логов
formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Ротация файлов (10 МБ, хранить 5 файлов)
file_handler = RotatingFileHandler(
    LOG_FILE, 
    maxBytes=10*1024*1024,
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Консольный вывод (только INFO и выше)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Функции для удобного логирования
def log_command(ctx, command_name, **kwargs):
    user = ctx.author
    guild = ctx.guild
    channel = ctx.channel
    log_msg = f"КОМАНДА | {command_name} | Пользователь: {user.name} (ID:{user.id}) | Сервер: {guild.name if guild else 'ЛС'} (ID:{guild.id if guild else 'None'}) | Канал: {channel.name if hasattr(channel, 'name') else 'ЛС'} | Аргументы: {kwargs}"
    logger.info(log_msg)

def log_error(error, context=""):
    error_msg = f"ОШИБКА | {context} | {str(error)}\n{traceback.format_exc()}"
    logger.error(error_msg)

def log_user_action(user_id, user_name, action, details=""):
    log_msg = f"ДЕЙСТВИЕ | Пользователь: {user_name} (ID:{user_id}) | {action} | {details}"
    logger.info(log_msg)

def log_background(task_name, details=""):
    log_msg = f"ФОН | {task_name} | {details}"
    logger.info(log_msg)

def log_db_change(user_id, user_name, table, changes):
    log_msg = f"БД | Пользователь: {user_name} (ID:{user_id}) | Таблица: {table} | Изменения: {changes}"
    logger.debug(log_msg)
# ============================================
# ========== НАСТРОЙКИ БОТА ==========
# ============================================

TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
PREFIX = "!"
DB_FOLDER = "./data/guild_databases"
COOLDOWN_HOURS = 1
CASE_COOLDOWN_HOURS = 24

TESTER_ROLE_NAME = "тестер"
HIGH_TESTER_ROLE_NAME = "Высший тестер"

BASE_MINUS_CHANCE = 0.2
MAX_MINUS_CHANCE = 0.6
PITY_INCREMENT = 0.1
CONSECUTIVE_MINUS_BOOST = 0.2
MAX_CONSECUTIVE_MINUS_BOOST = 0.8

BASE_JACKPOT_CHANCE = 0.001
JACKPOT_PITY_INCREMENT = 0.001
MAX_JACKPOT_CHANCE = 0.05
JACKPOT_MIN = 500
JACKPOT_MAX = 1000

AUTO_FAT_INTERVALS = {1: 12, 2: 6, 3: 3, 4: 1, 5: 0.75, 6: 0.5, 7: 0.25, 8: 0.20, 9: 0.15, 10: 0.1}
AUTO_FAT_BASE_COST = 500
AUTO_FAT_COST_INCREMENT = 1500
AUTO_FAT_MAX_LEVEL = 10

PRESTIGE_BONUS_PER_LEVEL = 0.10
PRESTIGE_LUCK_PER_LEVEL = 0.01
PRESTIGE_XP_BONUS_PER_LEVEL = 0.5
PRESTIGE_BASE_COST = 2000
PRESTIGE_COST_INCREMENT = 1000

INCOME_BONUS_PER_LEVEL = 0.05
INCOME_BASE_COST = 250
INCOME_COST_INCREMENT = 100

LUCK_CASE_BONUS_PER_LEVEL = 0.25
LUCK_UPGRADE_BONUS_PER_LEVEL = 0.5
LUCK_BASE_COST = 1000
LUCK_COST_INCREMENT = 500

FAT_CD_REDUCTION_PER_LEVEL = 59 / 12
FAT_CD_BASE_COST = 150
FAT_CD_COST_INCREMENT = 100
FAT_CD_MAX_LEVEL = 12

CASE_CD_REDUCTION_PER_LEVEL = 1380 / 24
CASE_CD_BASE_COST = 200
CASE_CD_COST_INCREMENT = 150
CASE_CD_MAX_LEVEL = 24

AUTO_OPEN_CASES_PRICE = 20000
AUTO_OPEN_CASES_REQUIRED_PRESTIGE = 5

JACKPOT_CHANCE_BONUS_PER_LEVEL = 0.0005
JACKPOT_BASE_COST = 1000
JACKPOT_COST_INCREMENT = 500
JACKPOT_MAX_LEVEL = 1000

FAT_PLUS_BONUS_PER_LEVEL = 0.02
FAT_PLUS_BASE_COST = 500
FAT_PLUS_COST_INCREMENT = 200
FAT_PLUS_MAX_LEVEL = 1000

CASE_PLUS_BONUS_PER_LEVEL = 0.01
CASE_PLUS_BASE_COST = 500
CASE_PLUS_COST_INCREMENT = 200
CASE_PLUS_MAX_LEVEL = 1000

XP_MULTIPLIER_PER_LEVEL = 0.01
XP_MULTIPLIER_BASE_COST = 300
XP_MULTIPLIER_COST_INCREMENT = 100
XP_MULTIPLIER_MAX_LEVEL = 1000

SHOP_SLOTS = 10
SHOP_UPDATE_HOURS = 12

XP_PER_FAT = 30
XP_PER_UPGRADE = 50
XP_PER_UPGRADE_KG = 40
XP_PER_CASE = 100
XP_PER_DUEL_WIN = 100
XP_PER_SHOP_BUY = 20
LEVEL_UP_REWARD_PER_LEVEL = 15

# ============================================
# ========== ПРИЗЫ КЕЙСОВ ==========
# ============================================

CASE_PRIZES = [
    {"value": 0, "chance": 21.0, "emoji": "🔄", "name": "Ничего"},
    {"value": 10, "chance": 20.0, "emoji": "📈", "name": "+10 кг"},
    {"value": 20, "chance": 20.0, "emoji": "⬆️", "name": "+20 кг"},
    {"value": 50, "chance": 20.0, "emoji": "🚀", "name": "+50 кг"},
    {"value": 100, "chance": 10.0, "emoji": "🚀", "name": "+100 кг"},
    {"value": 200, "chance": 5.0, "emoji": "🚀", "name": "+200 кг"},
    {"value": 300, "chance": 5.0, "emoji": "💫", "name": "+300 кг"},
    {"value": 400, "chance": 5.0, "emoji": "💫", "name": "+400 кг"},
    {"value": 500, "chance": 5.0, "emoji": "💫", "name": "+500 кг"},
    {"value": 1000, "chance": 2.0, "emoji": "⭐", "name": "+1000 кг"},
    {"value": 1500, "chance": 2.0, "emoji": "⭐", "name": "+1500 кг"},
    {"value": 2500, "chance": 1.0, "emoji": "💥", "name": "+2500 кг"},
    {"value": 5000, "chance": 1.0, "emoji": "💥", "name": "+5000 кг"},
    {"value": 10000, "chance": 0.5, "emoji": "💥", "name": "+10000 кг"},
]

total_chance = sum(p["chance"] for p in CASE_PRIZES)
for p in CASE_PRIZES:
    p["normalized_chance"] = (p["chance"] / total_chance) * 100

CASES = {
    "daily": {"name": "Жиркейс", "emoji": "📦", "tradable": False, "daily": True, "prizes": CASE_PRIZES},
    "chicken": {"name": "Коробка от чикенбургера", "emoji": "🍗", "tradable": True, "daily": False, "shop_chance": 0.3, "min_shop": 1, "max_shop": 3, "price": 10, "prizes": [{"value": -10, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 10, "chance": 20, "emoji": "📈"}, {"value": 15, "chance": 10, "emoji": "📈"}, {"value": 20, "chance": 10, "emoji": "⬆️"}, {"value": 25, "chance": 10, "emoji": "⬆️"}]},
    "bigmac": {"name": "Коробка от Биг Мака", "emoji": "🍔", "tradable": True, "daily": False, "shop_chance": 0.25, "min_shop": 1, "max_shop": 3, "price": 15, "prizes": [{"value": -15, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 15, "chance": 20, "emoji": "📈"}, {"value": 20, "chance": 10, "emoji": "⬆️"}, {"value": 25, "chance": 10, "emoji": "⬆️"}, {"value": 30, "chance": 10, "emoji": "🚀"}]},
    "whopper": {"name": "Коробка от Воппера", "emoji": "🔥", "tradable": True, "daily": False, "shop_chance": 0.23, "min_shop": 1, "max_shop": 3, "price": 25, "prizes": [{"value": -25, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 25, "chance": 20, "emoji": "📈"}, {"value": 30, "chance": 10, "emoji": "🚀"}, {"value": 40, "chance": 9, "emoji": "🚀"}, {"value": 50, "chance": 1, "emoji": "💫"}]},
    "green_whopper": {"name": "Коробка от Зеленого Воппера", "emoji": "💚", "tradable": True, "daily": False, "shop_chance": 0.17, "min_shop": 1, "max_shop": 2, "price": 50, "prizes": [{"value": -25, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 10, "emoji": "🔄"}, {"value": 10, "chance": 20, "emoji": "📈"}, {"value": 30, "chance": 10, "emoji": "🚀"}, {"value": 50, "chance": 10, "emoji": "💫"}, {"value": 100, "chance": 9, "emoji": "⭐"}, {"value": 250, "chance": 1, "emoji": "💥"}]},
    "burger_pizza": {"name": "Коробка от Бургер пиццы", "emoji": "🍕", "tradable": True, "daily": False, "shop_chance": 0.15, "min_shop": 1, "max_shop": 2, "price": 100, "prizes": [{"value": -10, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 10, "emoji": "🔄"}, {"value": 30, "chance": 20, "emoji": "🚀"}, {"value": 50, "chance": 30, "emoji": "💫"}, {"value": 100, "chance": 5, "emoji": "⭐"}, {"value": 250, "chance": 5, "emoji": "⭐"}, {"value": 500, "chance": 4, "emoji": "💥"}, {"value": 1000, "chance": 1, "emoji": "💥"}]},
    "mcguffin": {"name": "Коробка от МакГаффина", "emoji": "🎁", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 1, "price": 200, "prizes": [{"value": 100, "chance": 80, "emoji": "⭐"}, {"value": 200, "chance": 5, "emoji": "💥"}, {"value": 250, "chance": 5, "emoji": "💥"}, {"value": 500, "chance": 5, "emoji": "💥"}, {"value": 750, "chance": 1, "emoji": "✨"}, {"value": 1000, "chance": 1, "emoji": "✨"}, {"value": 1200, "chance": 1, "emoji": "✨"}, {"value": 1500, "chance": 1, "emoji": "✨"}]},
    "rotten_pack": {"name": "Упаковка Гнилой Ножки KFC", "emoji": "💀📦", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 10, "price": 100, "prizes": [{"value": 0, "chance": 90, "emoji": "🔄"}, {"value": "rotten_leg", "chance": 10, "emoji": "💀"}]},
    "water_pack": {"name": "Упаковка Стакана Воды", "emoji": "💧📦", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 10, "price": 100, "prizes": [{"value": 0, "chance": 90, "emoji": "🔄"}, {"value": "water", "chance": 10, "emoji": "💧"}]}
}

SHOP_ITEMS = [
    {"name": "Горелый бекон", "chance": 1.0, "min_amount": 3, "max_amount": 20, "price": 20, "gain_per_24h": 1, "description": "🏭 Даёт +1 кг каждые 24 часа"},
    {"name": "Горелый бутерброд", "chance": 0.4, "min_amount": 1, "max_amount": 5, "price": 70, "gain_per_24h": 3, "description": "🥪 Даёт +3 кг каждые 24 часа"},
    {"name": "Горелый додстер", "chance": 0.4, "min_amount": 1, "max_amount": 3, "price": 100, "gain_per_24h": 5, "description": "🌯 Даёт +5 кг каждые 24 часа"},
    {"name": "Тарелка макарон", "chance": 0.3, "min_amount": 1, "max_amount": 2, "price": 200, "gain_per_24h": 10, "description": "🍝 Даёт +10 кг каждые 24 часа"},
    {"name": "Тарелка хинкалей", "chance": 0.2, "min_amount": 1, "max_amount": 2, "price": 300, "gain_per_24h": 15, "description": "🥟 Даёт +15 кг каждые 24 часа"},
    {"name": "Бургер", "chance": 0.15, "min_amount": 1, "max_amount": 2, "price": 400, "gain_per_24h": 20, "description": "🍔 Даёт +20 кг каждые 24 часа"},
    {"name": "Пицца", "chance": 0.1, "min_amount": 1, "max_amount": 2, "price": 500, "gain_per_24h": 30, "description": "🍕 Даёт +30 кг каждые 24 часа"},
    {"name": "Ведро KFC", "chance": 0.08, "min_amount": 1, "max_amount": 2, "price": 800, "gain_per_24h": 50, "description": "🍗 Даёт +50 кг каждые 24 часа"},
    {"name": "Комбо за 1000!", "chance": 0.06, "min_amount": 1, "max_amount": 2, "price": 1000, "gain_per_24h": 100, "description": "🍱 Даёт +100 кг каждые 24 часа"},
    {"name": "Бездонное ведро KFC", "chance": 0.04, "min_amount": 1, "max_amount": 1, "price": 1500, "gain_per_24h": 150, "description": "🪣 Даёт +150 кг каждые 24 часа"},
    {"name": "Бездонная пачка чипсов", "chance": 0.03, "min_amount": 1, "max_amount": 1, "price": 3000, "gain_per_24h": 250, "description": "🥨 Даёт +250 кг каждые 24 часа"},
    {"name": "Пожизненный запас чикенбургеров", "chance": 0.02, "min_amount": 1, "max_amount": 1, "price": 5000, "gain_per_24h": 500, "description": "🍔🍔🍔 Даёт +500 кг каждые 24 часа"},
    {"name": "Автоматическая система подачи холестерина", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 7000, "gain_per_24h": 1000, "description": "⚙️💉 Даёт +1000 кг каждые 24 часа"},
    {"name": "Святой сэндвич", "chance": 0.005, "min_amount": 1, "max_amount": 1, "price": 10000, "gain_per_24h": 0, "description": "✨ **ЛЕГЕНДАРНО** ✨"},
    {"name": "Гнилая ножка KFC", "chance": 0.005, "min_amount": 1, "max_amount": 5, "price": 1, "gain_per_24h": 0, "description": "💀 **ПРОКЛЯТО** 💀"},
    {"name": "Стакан воды", "chance": 0.005, "min_amount": 1, "max_amount": 5, "price": 1, "gain_per_24h": 0, "description": "💧 **ОЧИЩЕНИЕ** 💧"},
    {"name": "Автохолестерол", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 10кг в час"},
    {"name": "Холестеринимус", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 500, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 5кг в час"},
    {"name": "Яблоко", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 500, "gain_per_24h": 0, "description": "🍎 Уменьшает кулдаун !жир на 5% за штуку"},
    {"name": "Апельсин", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 750, "gain_per_24h": 0, "description": "🍊 Уменьшает кулдаун !жиркейс на 5% за штуку"},
    {"name": "Золотое Яблоко", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🍎✨ Уменьшает кулдаун !жир на 10% за штуку"},
    {"name": "Золотой Апельсин", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🍊✨ Уменьшает кулдаун !жиркейс на 10% за штуку"},
    {"name": "Драгонфрукт", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🐉🍈 Повышает шанс джекпота на 1% за штуку"},
    {"name": "Золотой Драгонфрукт", "chance": 0.005, "min_amount": 1, "max_amount": 1, "price": 3000, "gain_per_24h": 0, "description": "🐉🍈✨ Повышает шанс джекпота на 5% за штуку"},
    {"name": "Снатчер", "chance": 0.001, "min_amount": 1, "max_amount": 1, "price": 2000, "gain_per_24h": 0, "description": "👾 **СНАТЧЕР** 👾"},
]

ITEM_EMOJIS = {item["name"]: "📦" for item in SHOP_ITEMS}
ITEM_EMOJIS.update({"Снатчер": "👾", "Святой сэндвич": "✨", "Гнилая ножка KFC": "💀", "Стакан воды": "💧", "Автохолестерол": "💊", "Холестеринимус": "💊", "Яблоко": "🍎", "Апельсин": "🍊", "Золотое Яблоко": "🍎✨", "Золотой Апельсин": "🍊✨", "Драгонфрукт": "🐉🍈", "Золотой Драгонфрукт": "🐉🍈✨"})

CASES["shop_case"] = {"name": "Магазинный кейс", "emoji": "🏪", "tradable": True, "daily": False, "shop_chance": 0.2, "min_shop": 1, "max_shop": 5, "price": 100, "prizes": []}
shop_case_prizes = [{"value": item["name"], "chance": item["chance"] * 100, "emoji": ITEM_EMOJIS.get(item["name"], "🎁"), "name": item["name"]} for item in SHOP_ITEMS]
total = sum(p["chance"] for p in shop_case_prizes)
if total < 100:
    shop_case_prizes.append({"value": 0, "chance": 100 - total, "emoji": "🔄", "name": "Ничего"})
else:
    for prize in shop_case_prizes:
        prize["chance"] = (prize["chance"] / total) * 100
CASES["shop_case"]["prizes"] = shop_case_prizes

LEGENDARY_UPGRADE_PRICES = {
    "Святой сэндвич": 20000, "Гнилая ножка KFC": 5000, "Стакан воды": 3000,
    "Автохолестерол": 5000, "Холестеринимус": 2500, "Яблоко": 1500,
    "Золотое Яблоко": 3000, "Апельсин": 2000, "Золотой Апельсин": 4000,
    "Драгонфрукт": 4000, "Золотой Драгонфрукт": 8000, "Снатчер": 20000
}

RANKS = [
    {"name": "Задолженность по кг", "min": -999, "max": -51, "emoji": "👻"},
    {"name": "Невесомый", "min": -50, "max": -21, "emoji": "🍃"},
    {"name": "Бедыч", "min": -20, "max": -1, "emoji": "🎈"},
    {"name": "Абсолютный ноль", "min": 0, "max": 0, "emoji": "⚖️"},
    {"name": "Микро жирик", "min": 1, "max": 29, "emoji": "🏃"},
    {"name": "Мини жирик", "min": 30, "max": 69, "emoji": "🍔"},
    {"name": "Вес имеет", "min": 70, "max": 119, "emoji": "🐘"},
    {"name": "Толстый", "min": 120, "max": 199, "emoji": "🏋️"},
    {"name": "Бронзовая лига Бургер Кинга", "min": 200, "max": 599, "emoji": "🟤"},
    {"name": "Серебрянная лига Бургер Кинга", "min": 600, "max": 1199, "emoji": "🔘"},
    {"name": "Золотая лига Бургер Кинга", "min": 1200, "max": 1799, "emoji": "🟡"},
    {"name": "Платиновая лига Бургер Кинга", "min": 1800, "max": 2399, "emoji": "💠"},
    {"name": "Алмазная лига Бургер Кинга", "min": 2400, "max": 2999, "emoji": "💎"},
    {"name": "Ониксовая лига Бургер Кинга", "min": 3000, "max": 3599, "emoji": "◆︎"},
    {"name": "Жирмезис", "min": 3600, "max": 5000, "emoji": "⚜️"},
    {"name": "Арчжирмезис", "min": 5001, "max": 10000, "emoji": "♛"},
    {"name": "ЖИРНАЯ ТОЛСТАЯ ОГРОМНАЯ СВИНЬЯ", "min": 10001, "max": 99999999, "emoji": "🐖"},
]

print("="*60)
print("🍔 ЖИРНЫЙ БОТ - ЗАПУСК")
print("="*60)

if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найден токен!")
    exit(1)

# ============================================
# ========== ФУНКЦИИ ==========
# ============================================

def get_user_items(item_counts_str):
    try:
        return json.loads(item_counts_str) if item_counts_str and item_counts_str != '{}' else {}
    except:
        return {}

def save_user_items(items_dict):
    # Убираем предметы с нулевым количеством, кириллицу храним как есть
    return json.dumps({k: v for k, v in items_dict.items() if v > 0}, ensure_ascii=False)

def to_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None

def get_fat_cooldown_hours(data, items_dict):
    cd_reduction = get_fat_cd_reduction(data.get('fat_cd_upgrade', 0) or 0)
    cooldown = max(1, COOLDOWN_HOURS * 60 - cd_reduction) / 60
    reduction = items_dict.get("Яблоко", 0) * 0.05 + items_dict.get("Золотое Яблоко", 0) * 0.10
    return cooldown * (1 - min(reduction, 0.9))

def get_case_cooldown_minutes(case_cd_upgrade, items_dict):
    cooldown = max(10, CASE_COOLDOWN_HOURS * 60 - get_case_cd_reduction(case_cd_upgrade or 0))
    reduction = items_dict.get("Апельсин", 0) * 0.05 + items_dict.get("Золотой Апельсин", 0) * 0.10
    return max(10, cooldown * (1 - min(reduction, 0.9)))

DUEL_RESET = dict(duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
UPGRADE_RESET = dict(upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
UPGRADE_TIMEOUT_SECONDS = 300

def get_xp_for_next_level(level):
    return (50 * (level + 1)) + ((level + 1) * 5)

def get_level_and_xp(total_xp):
    level = 0
    rem = total_xp
    while True:
        need = get_xp_for_next_level(level)
        if rem < need:
            break
        rem -= need
        level += 1
    return level, rem

def get_jackpot_bonus(level):
    return level * JACKPOT_CHANCE_BONUS_PER_LEVEL

def get_fat_plus_bonus(level):
    return 1 + (level * FAT_PLUS_BONUS_PER_LEVEL)

def get_case_plus_bonus(level):
    return 1 + (level * CASE_PLUS_BONUS_PER_LEVEL)

def get_xp_multiplier_bonus(level):
    return 1 + (level * XP_MULTIPLIER_PER_LEVEL)

def add_xp(guild_id, user_id, xp_amount):
    data = get_user_data(guild_id, user_id)
    old_level = data.get('user_level', 0)
    prestige = data.get('prestige', 0)
    xp_multiplier = get_xp_multiplier_bonus(data.get('xp_multiplier_upgrade', 0))
    xp_bonus = 1 + (prestige * PRESTIGE_XP_BONUS_PER_LEVEL)
    xp_amount = int(xp_amount * xp_bonus * xp_multiplier)
    new_total = data.get('user_xp', 0) + xp_amount
    new_level, current_xp = get_level_and_xp(new_total)
    total_kg = 0
    for lvl in range(old_level + 1, new_level + 1):
        total_kg += LEVEL_UP_REWARD_PER_LEVEL * lvl
    new_weight = data['current_number'] + total_kg
    update_user_data(guild_id, user_id, user_xp=new_total, user_level=new_level, number=new_weight)
    if total_kg > 0:
        asyncio.create_task(update_user_nick(guild_id, user_id, data.get('user_name')))
    return new_level - old_level, total_kg, new_level

def format_nick_with_prestige(prestige, weight, user_name):
    if prestige > 0:
        return f"{prestige}🌟 {weight}kg {user_name}"
    return f"{weight}kg {user_name}"

def get_prestige_bonus(p):
    return 1 + (p * PRESTIGE_BONUS_PER_LEVEL)

def get_prestige_luck(p):
    return p * PRESTIGE_LUCK_PER_LEVEL

def get_income_bonus(up):
    return 1 + (up * INCOME_BONUS_PER_LEVEL)

def get_fat_cd_reduction(up):
    return up * FAT_CD_REDUCTION_PER_LEVEL

def get_case_cd_reduction(up):
    return up * CASE_CD_REDUCTION_PER_LEVEL

def get_auto_fat_interval(lvl):
    if lvl <= 0:
        return None
    return AUTO_FAT_INTERVALS.get(lvl, 1)

def get_upgrade_cost(typ, lvl):
    if typ == "fat_cd":
        return FAT_CD_BASE_COST + lvl * FAT_CD_COST_INCREMENT
    elif typ == "case_cd":
        return CASE_CD_BASE_COST + lvl * CASE_CD_COST_INCREMENT
    elif typ == "luck":
        return LUCK_BASE_COST + lvl * LUCK_COST_INCREMENT
    elif typ == "income":
        return INCOME_BASE_COST + lvl * INCOME_COST_INCREMENT
    elif typ == "prestige":
        return PRESTIGE_BASE_COST + lvl * PRESTIGE_COST_INCREMENT
    elif typ == "auto_fat":
        return AUTO_FAT_BASE_COST + lvl * AUTO_FAT_COST_INCREMENT
    elif typ == "jackpot":
        return JACKPOT_BASE_COST + lvl * JACKPOT_COST_INCREMENT
    elif typ == "fat_plus":
        return FAT_PLUS_BASE_COST + lvl * FAT_PLUS_COST_INCREMENT
    elif typ == "case_plus":
        return CASE_PLUS_BASE_COST + lvl * CASE_PLUS_COST_INCREMENT
    elif typ == "xp_multiplier":
        return XP_MULTIPLIER_BASE_COST + lvl * XP_MULTIPLIER_COST_INCREMENT
    return 0

def repair_database(db_path):
    if not os.path.exists(db_path):
        return False
    backup = db_path + f".corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(db_path, backup)
    os.remove(db_path)
    return True

BACKUP_FOLDER = "/tmp/guild_databases_backup"
BACKUP_MIN_INTERVAL = 60  # не чаще раза в минуту на сервер
_last_backup_time = {}

def backup_and_restore_db():
    main = DB_FOLDER
    backup = BACKUP_FOLDER
    os.makedirs(main, exist_ok=True)
    os.makedirs(backup, exist_ok=True)
    # Восстанавливаем из бэкапа только если основной базы нет или бэкап новее
    for db in glob.glob(os.path.join(backup, "*.db")):
        target = os.path.join(main, os.path.basename(db))
        if not os.path.exists(target) or os.path.getmtime(db) > os.path.getmtime(target):
            shutil.copy2(db, target)
    for db in glob.glob(os.path.join(main, "*.db")):
        target = os.path.join(backup, os.path.basename(db))
        if not os.path.exists(target) or os.path.getmtime(db) > os.path.getmtime(target):
            shutil.copy2(db, target)

def backup_guild_db(db_path, force=False):
    now = datetime.now().timestamp()
    if not force and now - _last_backup_time.get(db_path, 0) < BACKUP_MIN_INTERVAL:
        return
    try:
        os.makedirs(BACKUP_FOLDER, exist_ok=True)
        shutil.copy2(db_path, os.path.join(BACKUP_FOLDER, os.path.basename(db_path)))
        _last_backup_time[db_path] = now
    except Exception:
        pass

backup_and_restore_db()

def add_missing_columns(db_path, existing_columns):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    required = {
        'item_counts': "TEXT DEFAULT '{}'",
        'last_command': "TEXT",
        'last_command_target': "TEXT",
        'last_command_use_time': "TIMESTAMP",
        'fat_cooldown_time': "TIMESTAMP",
        'active_case_message_id': "TEXT",
        'active_case_channel_id': "TEXT",
        'daily_case_last_time': "TIMESTAMP",
        'snatcher_last_time': "TIMESTAMP",
        'duel_active': "INTEGER DEFAULT 0",
        'duel_opponent': "TEXT",
        'duel_amount': "INTEGER DEFAULT 0",
        'duel_message_id': "TEXT",
        'duel_channel_id': "TEXT",
        'duel_initiator': "INTEGER DEFAULT 0",
        'last_case_type': "TEXT",
        'last_case_prize': "TEXT",
        'upgrade_active': "INTEGER DEFAULT 0",
        'upgrade_data': "TEXT",
        'duel_start_time': "TIMESTAMP",
        'shadow_upgrade_chance': "INTEGER DEFAULT 0",
        'user_xp': "INTEGER DEFAULT 0",
        'user_level': "INTEGER DEFAULT 0",
        'fat_cd_upgrade': "INTEGER DEFAULT 0",
        'case_cd_upgrade': "INTEGER DEFAULT 0",
        'luck_upgrade': "INTEGER DEFAULT 0",
        'income_upgrade': "INTEGER DEFAULT 0",
        'prestige': "INTEGER DEFAULT 0",
        'auto_fat_level': "INTEGER DEFAULT 0",
        'next_auto_fat_time': "TIMESTAMP",
        'animations_enabled': "INTEGER DEFAULT 1",
        'last_passive_income': "TIMESTAMP",
        'last_hourly_income': "TIMESTAMP",
        'daily_last_added': "TIMESTAMP",
        'daily_case_count': "INTEGER DEFAULT 0",
        'auto_open_cases': "INTEGER DEFAULT 0",
        'jackpot_upgrade': "INTEGER DEFAULT 0",
        'fat_plus_upgrade': "INTEGER DEFAULT 0",
        'case_plus_upgrade': "INTEGER DEFAULT 0",
        'xp_multiplier_upgrade': "INTEGER DEFAULT 0",
    }
    for col, typ in required.items():
        if col not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col} {typ}")
            except:
                pass
    for case_id in CASES:
        if case_id != "daily":
            col = f"case_{case_id}_count"
            if col not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col} INTEGER DEFAULT 0")
                except:
                    pass
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shop'")
    if not cursor.fetchone():
        cursor.execute('''CREATE TABLE shop (guild_id TEXT PRIMARY KEY, slots TEXT, last_update TIMESTAMP, next_update TIMESTAMP)''')
    conn.commit()
    conn.close()

_initialized_dbs = set()

def safe_init_guild_database(guild_id, guild_name="Unknown"):
    # Проверка структуры базы — один раз за запуск, а не на каждый запрос
    db_path = get_db_path(guild_id)
    if db_path in _initialized_dbs and os.path.exists(db_path):
        return True
    result = _safe_init_guild_database(db_path, guild_id, guild_name)
    if result:
        _initialized_dbs.add(db_path)
    return result

def _safe_init_guild_database(db_path, guild_id, guild_name):
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_fat'")
            if not cursor.fetchone():
                conn.close()
                return create_new_database(db_path, guild_id, guild_name)
            cursor.execute("PRAGMA table_info(user_fat)")
            cols = [c[1] for c in cursor.fetchall()]
            conn.close()
            add_missing_columns(db_path, cols)
            return True
        except sqlite3.DatabaseError as e:
            log_error(e, f"Повреждена база {db_path}, создаю новую")
            repair_database(db_path)
            return create_new_database(db_path, guild_id, guild_name)
    else:
        return create_new_database(db_path, guild_id, guild_name)

def create_new_database(db_path, guild_id, guild_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE user_fat (
        user_id TEXT PRIMARY KEY, user_name TEXT, current_number INTEGER DEFAULT 0, last_command_time TIMESTAMP,
        consecutive_plus INTEGER DEFAULT 0, consecutive_minus INTEGER DEFAULT 0, jackpot_pity INTEGER DEFAULT 0,
        last_case_time TIMESTAMP, item_counts TEXT DEFAULT '{}', last_command TEXT, last_command_target TEXT,
        last_command_use_time TIMESTAMP, fat_cooldown_time TIMESTAMP, active_case_message_id TEXT,
        active_case_channel_id TEXT, daily_case_last_time TIMESTAMP, snatcher_last_time TIMESTAMP,
        duel_active INTEGER DEFAULT 0, duel_opponent TEXT, duel_amount INTEGER DEFAULT 0, duel_message_id TEXT,
        duel_channel_id TEXT, duel_initiator INTEGER DEFAULT 0, last_case_type TEXT, last_case_prize TEXT,
        upgrade_active INTEGER DEFAULT 0, upgrade_data TEXT, duel_start_time TIMESTAMP,
        shadow_upgrade_chance INTEGER DEFAULT 0, user_xp INTEGER DEFAULT 0, user_level INTEGER DEFAULT 0,
        fat_cd_upgrade INTEGER DEFAULT 0, case_cd_upgrade INTEGER DEFAULT 0, luck_upgrade INTEGER DEFAULT 0,
        income_upgrade INTEGER DEFAULT 0, prestige INTEGER DEFAULT 0, auto_fat_level INTEGER DEFAULT 0,
        next_auto_fat_time TIMESTAMP, animations_enabled INTEGER DEFAULT 1,
        last_passive_income TIMESTAMP, last_hourly_income TIMESTAMP, daily_last_added TIMESTAMP,
        daily_case_count INTEGER DEFAULT 0, auto_open_cases INTEGER DEFAULT 0,
        jackpot_upgrade INTEGER DEFAULT 0, fat_plus_upgrade INTEGER DEFAULT 0,
        case_plus_upgrade INTEGER DEFAULT 0, xp_multiplier_upgrade INTEGER DEFAULT 0)''')
    for case_id in CASES:
        if case_id != "daily":
            try:
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN case_{case_id}_count INTEGER DEFAULT 0")
            except:
                pass
    cursor.execute('''CREATE TABLE IF NOT EXISTS shop (guild_id TEXT PRIMARY KEY, slots TEXT, last_update TIMESTAMP, next_update TIMESTAMP)''')
    conn.commit()
    conn.close()
    return True

def get_db_path(guild_id):
    return os.path.join(DB_FOLDER, f"guild_{guild_id}.db")

def get_user_data(guild_id, user_id, user_name=None):
    safe_init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    all_cols = [c[1] for c in cursor.fetchall()]
    select_cols = [c for c in ['user_id','user_name','current_number','last_command_time','consecutive_plus','consecutive_minus','jackpot_pity','last_case_time','item_counts','last_command','last_command_target','last_command_use_time','fat_cooldown_time','active_case_message_id','active_case_channel_id','daily_case_last_time','snatcher_last_time','duel_active','duel_opponent','duel_amount','duel_message_id','duel_channel_id','duel_initiator','last_case_type','last_case_prize','upgrade_active','upgrade_data','duel_start_time','shadow_upgrade_chance','user_xp','user_level','fat_cd_upgrade','case_cd_upgrade','luck_upgrade','income_upgrade','prestige','auto_fat_level','next_auto_fat_time','animations_enabled','daily_case_count','auto_open_cases','jackpot_upgrade','fat_plus_upgrade','case_plus_upgrade','xp_multiplier_upgrade'] if c in all_cols]
    case_cols = [f"case_{cid}_count" for cid in CASES if cid != "daily" and f"case_{cid}_count" in all_cols]
    query = f"SELECT {', '.join(select_cols + case_cols)} FROM user_fat WHERE user_id = ?"
    cursor.execute(query, (str(user_id),))
    row = cursor.fetchone()
    if row:
        data = list(row)
        user_data = {}
        for i, col in enumerate(select_cols):
            user_data[col] = data[i]
        cases = {}
        for i, col in enumerate(case_cols):
            cid = col.replace("case_","").replace("_count","")
            cases[cid] = data[len(select_cols)+i] or 0
        if 'shop' in cases:
            cases['shop_case'] = cases.get('shop_case',0) + cases['shop']
            del cases['shop']
        user_data['cases_dict'] = cases
        # Обновляем имя, если пользователь его сменил (нужно для таблицы лидеров)
        if user_name and user_data.get('user_name') != user_name:
            cursor.execute("UPDATE user_fat SET user_name = ? WHERE user_id = ?", (user_name, str(user_id)))
            conn.commit()
            user_data['user_name'] = user_name
        conn.close()
        return user_data
    else:
        user_data = {
            'user_id': str(user_id), 'user_name': user_name or "Unknown", 'current_number': 0, 'last_command_time': None,
            'consecutive_plus': 0, 'consecutive_minus': 0, 'jackpot_pity': 0, 'last_case_time': None, 'item_counts': '{}',
            'last_command': None, 'last_command_target': None, 'last_command_use_time': None, 'fat_cooldown_time': None,
            'active_case_message_id': None, 'active_case_channel_id': None, 'daily_case_last_time': None, 'snatcher_last_time': None,
            'duel_active': 0, 'duel_opponent': None, 'duel_amount': 0, 'duel_message_id': None, 'duel_channel_id': None,
            'duel_initiator': 0, 'last_case_type': None, 'last_case_prize': None, 'upgrade_active': 0, 'upgrade_data': None,
            'duel_start_time': None, 'shadow_upgrade_chance': 0, 'user_xp': 0, 'user_level': 0, 'fat_cd_upgrade': 0,
            'case_cd_upgrade': 0, 'luck_upgrade': 0, 'income_upgrade': 0, 'prestige': 0, 'auto_fat_level': 0,
            'next_auto_fat_time': None, 'animations_enabled': 1, 'daily_case_count': 0, 'auto_open_cases': 0,
            'jackpot_upgrade': 0, 'fat_plus_upgrade': 0, 'case_plus_upgrade': 0, 'xp_multiplier_upgrade': 0,
            'cases_dict': {}
        }
        for cid in CASES:
            if cid != "daily":
                user_data['cases_dict'][cid] = 0
        create_new_user(cursor, user_data, all_cols)
        conn.commit()
        conn.close()
        return user_data

def create_new_user(cursor, user_data, all_columns):
    cols = []
    vals = []
    base = ['user_id','user_name','current_number','last_command_time','consecutive_plus','consecutive_minus','jackpot_pity','last_case_time','item_counts','last_command','last_command_target','last_command_use_time','fat_cooldown_time','active_case_message_id','active_case_channel_id','daily_case_last_time','snatcher_last_time','duel_active','duel_opponent','duel_amount','duel_message_id','duel_channel_id','duel_initiator','last_case_type','last_case_prize','upgrade_active','upgrade_data','duel_start_time','shadow_upgrade_chance','user_xp','user_level','fat_cd_upgrade','case_cd_upgrade','luck_upgrade','income_upgrade','prestige','auto_fat_level','next_auto_fat_time','animations_enabled','daily_case_count','auto_open_cases','jackpot_upgrade','fat_plus_upgrade','case_plus_upgrade','xp_multiplier_upgrade']
    for f in base:
        if f in all_columns:
            cols.append(f)
            vals.append(user_data.get(f))
    for cid, cnt in user_data['cases_dict'].items():
        col = f"case_{cid}_count"
        if col in all_columns:
            cols.append(col)
            vals.append(cnt)
    q = f"INSERT INTO user_fat ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})"
    cursor.execute(q, vals)

def update_user_data(guild_id, user_id, **kwargs):
    safe_init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    cols = [c[1] for c in cursor.fetchall()]
    updates = []
    vals = []
    for k, v in kwargs.items():
        if isinstance(v, datetime):
            v = v.isoformat()
        if k == 'number':
            if 'current_number' in cols:
                updates.append("current_number = ?")
                vals.append(v)
        elif k == 'cases_dict' and isinstance(v, dict):
            for cid, cnt in v.items():
                col = f"case_{cid}_count"
                if col in cols:
                    updates.append(f"{col} = ?")
                    vals.append(cnt)
        elif k in cols:
            updates.append(f"{k} = ?")
            vals.append(v)
    if not updates:
        conn.close()
        return
    vals.append(str(user_id))
    q = f"UPDATE user_fat SET {', '.join(updates)} WHERE user_id = ?"
    try:
        cursor.execute(q, vals)
    except Exception as e:
        log_error(e, f"update_user_data {user_id}: {list(kwargs.keys())}")
        conn.close()
        return
    conn.commit()
    conn.close()
    backup_guild_db(db_path)

def add_user_weight(guild_id, user_id, delta, **extra):
    # Атомарное прибавление веса (без гонок с другими командами/фоновыми задачами)
    safe_init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    sets = ["current_number = current_number + ?"]
    vals = [int(delta)]
    for k, v in extra.items():
        sets.append(f"{k} = ?")
        vals.append(v.isoformat() if isinstance(v, datetime) else v)
    vals.append(str(user_id))
    conn.execute(f"UPDATE user_fat SET {', '.join(sets)} WHERE user_id = ?", vals)
    conn.commit()
    conn.close()
    backup_guild_db(db_path)

def get_rank(weight):
    for r in RANKS:
        if r["min"] <= weight <= r["max"]:
            return r["name"], r["emoji"]
    if weight > 99999999:
        return "🌀 Бесконечность", "🌀"
    if weight < -999:
        return "Черная дыра", "💀"
    return "❓ Неопределённый", "❓"

# ============================================
# ========== БОТ ==========
# ============================================

if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# ============================================
# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
# ============================================

def get_user_cases(guild_id, user_id):
    return get_user_data(guild_id, user_id).get('cases_dict', {})

def get_all_users_sorted(guild_id):
    safe_init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    cols = [c[1] for c in cursor.fetchall()]
    sel = ['user_name','current_number','last_command_time','consecutive_plus','consecutive_minus','jackpot_pity']
    if 'prestige' in cols:
        sel.append('prestige')
    cursor.execute(f"SELECT {', '.join(sel)} FROM user_fat ORDER BY current_number DESC")
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_guild_stats(guild_id):
    users = get_all_users_sorted(guild_id)
    total_users = len(users)
    total_weight = sum(u[1] for u in users)
    avg = total_weight / total_users if total_users else 0
    pos = sum(1 for u in users if u[1] > 0)
    neg = sum(1 for u in users if u[1] < 0)
    zero = sum(1 for u in users if u[1] == 0)
    return {'total_users': total_users, 'total_weight': total_weight, 'avg_weight': avg, 'positive': pos, 'negative': neg, 'zero': zero}

def check_cooldown(last_time, hours):
    if not last_time:
        return True, 0
    if isinstance(last_time, str):
        last_time = datetime.fromisoformat(last_time)
    diff = (datetime.now() - last_time).total_seconds()
    cd = hours * 3600
    if diff >= cd:
        return True, 0
    return False, cd - diff

def format_time(sec):
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = int(sec % 60)
    if h > 0:
        return f"{h} ч {m} мин"
    if m > 0:
        return f"{m} мин {s} сек"
    return f"{s} сек"

def has_tester_role(m):
    if not m:
        return False
    return any(r.name.lower() == TESTER_ROLE_NAME.lower() for r in m.roles)

def has_high_tester_role(m):
    if not m:
        return False
    return any(r.name.lower() == HIGH_TESTER_ROLE_NAME.lower() for r in m.roles)

def are_animations_enabled(user_data):
    return user_data.get('animations_enabled', 1) == 1

def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, luck_upgrade=0, prestige_bonus=1.0, fat_plus_bonus=1.0, jackpot_bonus=0, items_dict=None, current_weight=None):
    if items_dict is None:
        items_dict = {}
    has_rotten_leg = items_dict.get("Гнилая ножка KFC", 0) > 0
    has_holy_sandwich = items_dict.get("Святой сэндвич", 0) > 0
    has_water = items_dict.get("Стакан воды", 0) > 0
    minus_boost = min(consecutive_minus * CONSECUTIVE_MINUS_BOOST, MAX_CONSECUTIVE_MINUS_BOOST)
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - minus_boost
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))
    # Пити ограничено MAX_JACKPOT_CHANCE, прокачка и драгонфрукты добавляются сверху
    jackpot_chance = min(BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT), MAX_JACKPOT_CHANCE)
    jackpot_chance += jackpot_bonus
    jackpot_chance += items_dict.get("Драгонфрукт", 0) * 0.01 + items_dict.get("Золотой Драгонфрукт", 0) * 0.05
    if has_holy_sandwich:
        sandwich_bonus = 0.3 * items_dict.get("Святой сэндвич", 0)
        jackpot_chance = max(jackpot_chance, sandwich_bonus)
    jackpot_chance = min(jackpot_chance, 0.9)

    # Бонусы (престиж, усиление !жир) применяются только к прибавке, не к минусу
    def boost(value):
        return int(value * prestige_bonus * fat_plus_bonus)

    if has_water:
        # Стакан воды очищает: минусов нет, но прибавка в 3 раза меньше
        if random.random() < jackpot_chance:
            change = boost(random.randint(JACKPOT_MIN, JACKPOT_MAX) // 3)
            return change, False, consecutive_plus+1, 0, 0, True
        change = boost(random.randint(1, 20) // 3)
        return change, False, consecutive_plus+1, 0, jackpot_pity+1, False
    if has_rotten_leg:
        if random.random() < 0.6:
            if current_weight and current_weight > 0:
                change = -max(1, int(current_weight * 0.5))
            else:
                change = random.randint(-20, -1)
            return change, True, 0, consecutive_minus+1, jackpot_pity+1, False
        change = boost(random.randint(JACKPOT_MIN, JACKPOT_MAX))
        return change, False, consecutive_plus+1, 0, 0, True
    # обычный режим
    if random.random() < jackpot_chance:
        change = boost(random.randint(JACKPOT_MIN, JACKPOT_MAX))
        return change, False, consecutive_plus+1, 0, 0, True
    if random.random() < minus_chance:
        change = random.randint(-20, -1)
        return change, True, 0, consecutive_minus+1, jackpot_pity+1, False
    change = boost(random.randint(1, 20))
    return change, False, consecutive_plus+1, 0, jackpot_pity+1, False

def apply_case_prize_bonuses(value, prestige_bonus, has_water, case_id):
    # Единая логика для ручного и авто-открытия кейсов.
    # Усиление кейсов уже учтено в open_case, здесь — вода и престиж.
    if has_water and case_id != "daily":
        value = value // 3
    if value > 0:
        value = int(value * prestige_bonus)
    return value

def open_case(case_id, prestige_luck=0, luck_upgrade=0, case_plus_bonus=1.0):
    case = CASES[case_id]
    prizes = case["prizes"]
    total = sum(p["chance"] for p in prizes)
    bonus = (prestige_luck * 100) + (luck_upgrade * LUCK_CASE_BONUS_PER_LEVEL)
    modified = []
    for p in prizes:
        np = p.copy()  # не портим глобальные таблицы призов
        np["norm"] = (p["chance"] / total) * 100
        if (isinstance(np["value"], int) and np["value"] >= 100) or np["value"] in ["rotten_leg","water"]:
            np["norm"] += bonus
        modified.append(np)
    total_norm = sum(p["norm"] for p in modified)
    for p in modified:
        p["norm"] = (p["norm"] / total_norm) * 100
    r = random.random() * 100
    cum = 0
    for p in modified:
        cum += p["norm"]
        if r < cum:
            if isinstance(p["value"], int) and p["value"] > 0:
                p["value"] = int(p["value"] * case_plus_bonus)
            return p
    return modified[-1]

def get_item_price(item_name):
    if item_name in LEGENDARY_UPGRADE_PRICES:
        return LEGENDARY_UPGRADE_PRICES[item_name]
    for si in SHOP_ITEMS:
        if si["name"] == item_name:
            return si["price"]
    return 0

def get_possible_upgrades(item_name, item_count):
    if item_count <= 0:
        return []
    cur_price = get_item_price(item_name)
    if cur_price == 0:
        return []
    upgrades = []
    seen = set()
    all_items = set(i["name"] for i in SHOP_ITEMS) | set(LEGENDARY_UPGRADE_PRICES.keys())
    for si in SHOP_ITEMS:
        name = si["name"]
        if name in seen:
            continue
        tgt = get_item_price(name)
        if tgt <= cur_price:
            continue
        chance = cur_price / tgt
        if chance < 0.01:
            continue
        upgrades.append({"name": name, "price": tgt, "chance": chance, "emoji": ITEM_EMOJIS.get(name, "🎁")})
        seen.add(name)
    if cur_price >= 1000:
        for leg, leg_price in LEGENDARY_UPGRADE_PRICES.items():
            if leg in seen:
                continue
            if leg not in all_items:
                continue
            if leg_price <= cur_price:
                continue
            chance = cur_price / leg_price
            if chance < 0.01:
                continue
            upgrades.append({"name": leg, "price": leg_price, "chance": chance, "emoji": ITEM_EMOJIS.get(leg, "✨")})
            seen.add(leg)
    upgrades.sort(key=lambda x: x["price"])
    return upgrades

def generate_shop_items():
    slots = []
    used = set()
    available_cases = [cid for cid, c in CASES.items() if cid != "daily" and c.get("shop_chance",0)>0]
    
    for _ in range(4):
        if random.random() < 0.7 and available_cases:
            choices = []
            for cid in available_cases:
                w = int(CASES[cid]["shop_chance"] * 100)
                choices.extend([cid]*w)
            if choices:
                chosen = random.choice(choices)
                c = CASES[chosen]
                amt = random.randint(c["min_shop"], c["max_shop"])
                
                # Исправлено: проверяем есть ли числовые призы
                numeric_prizes = [p["value"] for p in c["prizes"] if isinstance(p["value"], int)]
                if numeric_prizes:
                    minp = min(numeric_prizes)
                    maxp = max(numeric_prizes)
                    desc = f"{c['emoji']} Содержит призы от {minp} до {maxp}кг"
                else:
                    desc = f"{c['emoji']} Содержит различные предметы"
                
                slots.append({
                    "type":"case",
                    "case_id":chosen,
                    "name":c["name"],
                    "amount":amt,
                    "price":c["price"],
                    "description":desc,
                    "emoji":c['emoji']
                })
            else:
                slots.append(None)
        else:
            slots.append(None)

    item_slots = []
    for _ in range(6):
        chosen = None
        for _ in range(50):
            idx = random.randint(0,len(SHOP_ITEMS)-1)
            if idx in used:
                continue
            item = SHOP_ITEMS[idx]
            if random.random() < item["chance"]:
                chosen = item
                used.add(idx)
                break
        if chosen:
            amt = random.randint(chosen["min_amount"], chosen["max_amount"])
            item_slots.append({
                "type":"item",
                "name":chosen["name"],
                "amount":amt,
                "price":chosen["price"],
                "description":chosen["description"],
                "gain_per_24h":chosen.get("gain_per_24h",0),
                "emoji":ITEM_EMOJIS.get(chosen["name"],"📦")
            })
        else:
            item_slots.append(None)

    # Слоты 1-4 — кейсы, 5-10 — предметы (как написано в магазине),
    # поэтому перемешиваем только внутри своих групп
    random.shuffle(slots)
    random.shuffle(item_slots)
    return slots + item_slots

def get_shop_data(guild_id):
    safe_init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT slots, last_update, next_update FROM shop WHERE guild_id = ?', (str(guild_id),))
    row = cursor.fetchone()
    conn.close()
    if row:
        slots_json, lu, nu = row
        try:
            slots = json.loads(slots_json) if slots_json else []
            for s in slots:
                if s and "type" not in s:
                    if "case_id" in s:
                        s["type"] = "case"
                    else:
                        s["type"] = "item"
            return slots, lu, nu
        except:
            return [], None, None
    return None, None, None

def update_shop_data(guild_id, slots, last_update, next_update):
    safe_init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    clean = []
    for s in slots:
        if s:
            if "type" not in s:
                if "case_id" in s:
                    s["type"] = "case"
                else:
                    s["type"] = "item"
            clean.append(s)
        else:
            clean.append(None)
    js = json.dumps(clean)
    lu_str = last_update.isoformat() if last_update else None
    nu_str = next_update.isoformat() if next_update else None
    cursor.execute('INSERT OR REPLACE INTO shop (guild_id, slots, last_update, next_update) VALUES (?,?,?,?)', (str(guild_id), js, lu_str, nu_str))
    conn.commit()
    conn.close()

async def ensure_shop_updated(guild_id):
    res = get_shop_data(guild_id)
    now = datetime.now()
    if res[0]:
        slots, lu_str, nu_str = res
        lu = to_datetime(lu_str)
        nu = to_datetime(nu_str)
        if not nu or now >= nu:
            new = generate_shop_items()
            lu = now
            nu = now + timedelta(hours=SHOP_UPDATE_HOURS)
            update_shop_data(guild_id, new, lu, nu)
            return new, lu, nu
        else:
            return slots, lu, nu
    else:
        new = generate_shop_items()
        lu = now
        nu = now + timedelta(hours=SHOP_UPDATE_HOURS)
        update_shop_data(guild_id, new, lu, nu)
        return new, lu, nu

async def update_user_nick(guild_id, user_id, user_name=None):
    try:
        guild = bot.get_guild(guild_id)
        if not guild:
            return
        member = guild.get_member(int(user_id))
        if not member:
            return
        data = get_user_data(guild_id, user_id, user_name)
        if not data:
            return
        display = member.display_name
        clean = display.split("kg",1)[-1].strip() if "kg" in display else display
        if not clean or len(clean)>30:
            clean = data.get('user_name', str(user_id))
        new_nick = format_nick_with_prestige(data.get('prestige',0), data['current_number'], clean)
        if len(new_nick)>32:
            new_nick = new_nick[:32]
        if member.nick != new_nick:
            await member.edit(nick=new_nick)
        return True
    except discord.Forbidden:
        # Нет прав менять ник (владелец сервера / роль выше бота) — это нормально
        return False
    except Exception as e:
        logger.debug(f"Не удалось обновить ник {user_id}: {e}")
        return False

DUEL_STALE_SECONDS = 180

def can_duel(user_data):
    if not user_data.get('duel_active', 0):
        return True
    # Дуэль, зависшая после перезапуска бота, не должна блокировать навсегда
    start = to_datetime(user_data.get('duel_start_time'))
    return start is None or (datetime.now() - start).total_seconds() > DUEL_STALE_SECONDS

def get_duel_info(user_data):
    return {'active': user_data.get('duel_active', 0), 'opponent': user_data.get('duel_opponent'), 'amount': user_data.get('duel_amount', 0), 'message_id': user_data.get('duel_message_id'), 'channel_id': user_data.get('duel_channel_id'), 'initiator': user_data.get('duel_initiator', 0), 'start_time': user_data.get('duel_start_time')}

# ============================================
# ========== GIF-АНИМАЦИИ (РУЛЕТКА) ==========
# ============================================
# Анимация рисуется один раз в GIF и отправляется одним сообщением,
# вместо 20 правок сообщения, которые быстро упираются в лимиты Discord.

try:
    from PIL import Image, ImageDraw, ImageFont
    ANIMATIONS_AVAILABLE = True
except ImportError:
    ANIMATIONS_AVAILABLE = False
    logger.warning("Pillow не установлен — GIF-анимации отключены (pip install Pillow)")

ANIM_W, ANIM_H = 520, 170
TILE_W, TILE_H, TILE_GAP = 104, 128, 8
STRIP_LEN = 48
WIN_INDEX = 42
ANIM_FRAMES = 54
ANIM_FRAME_MS = 50
ANIM_FINAL_MS = 15000
ANIM_BG = (30, 31, 34)
ANIM_TILE_BG = (43, 45, 49)
ANIM_GOLD = (255, 200, 40)

RARITY_COLORS = {
    "common": (176, 195, 217),
    "uncommon": (75, 105, 255),
    "rare": (136, 71, 255),
    "epic": (211, 44, 230),
    "mythic": (235, 75, 75),
    "legendary": (255, 200, 40),
    "minus": (150, 60, 50),
    "empty": (90, 92, 98),
    "success": (60, 200, 90),
    "fail": (220, 60, 60),
    "silver": (190, 195, 205),
    "draw": (155, 89, 182),
}

FONT_CANDIDATES = [
    os.environ.get("FONT_PATH", ""),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts", "DejaVuSans-Bold.ttf"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts", "font.ttf"),
    "C:/Windows/Fonts/segoeuib.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
]

_font_path = None
_font_cache = {}

def _find_font_path():
    global _font_path
    if _font_path is not None:
        return _font_path
    for path in FONT_CANDIDATES:
        if path and os.path.exists(path):
            _font_path = path
            return path
    try:
        import matplotlib
        path = os.path.join(matplotlib.get_data_path(), "fonts", "ttf", "DejaVuSans-Bold.ttf")
        if os.path.exists(path):
            _font_path = path
            return path
    except Exception:
        pass
    logger.warning("Не найден шрифт с кириллицей для анимаций. Положите любой .ttf в папку fonts/ под именем font.ttf")
    _font_path = ""
    return ""

def _font(size):
    if size not in _font_cache:
        path = _find_font_path()
        try:
            _font_cache[size] = ImageFont.truetype(path, size) if path else ImageFont.load_default(size)
        except Exception:
            _font_cache[size] = ImageFont.load_default()
    return _font_cache[size]

def _text_width(draw, text, font):
    return draw.textlength(text, font=font)

def _fit_font(draw, text, max_w, start, minimum):
    size = start
    while size > minimum and _text_width(draw, text, _font(size)) > max_w:
        size -= 1
    return _font(size)

def _wrap(draw, text, font, max_w, max_lines):
    words = text.split()
    lines, cur = [], ""
    for w in words:
        test = (cur + " " + w).strip()
        if _text_width(draw, test, font) <= max_w or not cur:
            cur = test
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip(".") + "..."
    while lines and _text_width(draw, lines[-1], font) > max_w and len(lines[-1]) > 4:
        lines[-1] = lines[-1][:-4] + "..."
    return lines

def _mix(c1, c2, k):
    return tuple(int(a * (1 - k) + b * k) for a, b in zip(c1, c2))

def _render_tile(tile):
    color = tile["color"]
    img = Image.new("RGBA", (TILE_W, TILE_H), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    # фон плитки с оттенком редкости + цветная полоса снизу (как в кейсах CS)
    d.rounded_rectangle((0, 0, TILE_W - 1, TILE_H - 1), radius=10, fill=_mix(ANIM_TILE_BG, color, 0.22))
    d.rounded_rectangle((0, 0, TILE_W - 1, 46), radius=10, fill=_mix(ANIM_TILE_BG, color, 0.38))
    d.rectangle((0, 36, TILE_W - 1, 46), fill=_mix(ANIM_TILE_BG, color, 0.30))
    d.rounded_rectangle((0, TILE_H - 9, TILE_W - 1, TILE_H - 1), radius=4, fill=color)
    inner = TILE_W - 12
    title = tile["title"]
    tf = _fit_font(d, title, inner, 28, 11)
    tw = _text_width(d, title, tf)
    d.text(((TILE_W - tw) / 2 + 1, 25), title, font=tf, fill=(0, 0, 0), anchor="lm")
    d.text(((TILE_W - tw) / 2, 24), title, font=tf, fill=(255, 255, 255), anchor="lm")
    sub = tile.get("subtitle", "")
    if sub:
        sf = _font(13)
        lines = _wrap(d, sub, sf, inner, 3)
        y = 64
        for line in lines:
            lw = _text_width(d, line, sf)
            d.text(((TILE_W - lw) / 2, y), line, font=sf, fill=(225, 225, 230), anchor="lm")
            y += 15
    return img

def _build_overlay():
    overlay = Image.new("RGBA", (ANIM_W, ANIM_H), (0, 0, 0, 0))
    fade_w = 80
    fade = Image.new("L", (fade_w, ANIM_H), 0)
    fd = ImageDraw.Draw(fade)
    for x in range(fade_w):  # слева непрозрачно -> к центру прозрачно
        fd.line((x, 0, x, ANIM_H), fill=int(235 * (1 - x / fade_w)))
    left = Image.new("RGBA", (fade_w, ANIM_H), ANIM_BG + (0,))
    left.putalpha(fade)
    overlay.alpha_composite(left, (0, 0))
    overlay.alpha_composite(left.transpose(Image.Transpose.FLIP_LEFT_RIGHT), (ANIM_W - fade_w, 0))
    d = ImageDraw.Draw(overlay)
    cx = ANIM_W // 2
    d.rectangle((cx - 1, 6, cx + 1, ANIM_H - 7), fill=ANIM_GOLD + (255,))
    d.polygon([(cx - 9, 0), (cx + 9, 0), (cx, 12)], fill=ANIM_GOLD + (255,))
    d.polygon([(cx - 9, ANIM_H - 1), (cx + 9, ANIM_H - 1), (cx, ANIM_H - 13)], fill=ANIM_GOLD + (255,))
    return overlay

def render_roulette_gif(strip):
    """strip = {"tiles": [...], "win_index": int}. Возвращает (BytesIO, длительность прокрутки в секундах)."""
    tiles = strip["tiles"]
    win = strip["win_index"]
    step = TILE_W + TILE_GAP
    pad = ANIM_W // 2
    base = Image.new("RGB", (pad * 2 + len(tiles) * step, ANIM_H), ANIM_BG)
    y = (ANIM_H - TILE_H) // 2
    cache = {}
    for i, tile in enumerate(tiles):
        key = (tile["title"], tile.get("subtitle", ""), tile["color"])
        if key not in cache:
            cache[key] = _render_tile(tile)
        img = cache[key]
        base.paste(img, (pad + i * step, y), img)

    overlay = _build_overlay()
    start = pad - TILE_GAP - TILE_W // 2
    jitter = random.uniform(-0.38, 0.38) * TILE_W
    final = pad + win * step + TILE_W / 2 + jitter - ANIM_W / 2

    rgb_frames, durations = [], []
    for k in range(ANIM_FRAMES):
        t = k / (ANIM_FRAMES - 1)
        eased = 1 - (1 - t) ** 4
        off = int(start + (final - start) * eased)
        frame = base.crop((off, 0, off + ANIM_W, ANIM_H)).convert("RGBA")
        frame.alpha_composite(overlay)
        rgb_frames.append(frame.convert("RGB"))
        durations.append(ANIM_FRAME_MS)

    # Финальный кадр: выигрыш подсвечен, остальное затемнено
    off = int(final)
    frame = base.crop((off, 0, off + ANIM_W, ANIM_H)).convert("RGBA")
    shade = Image.new("RGBA", (ANIM_W, ANIM_H), (0, 0, 0, 150))
    wx = pad + win * step - off
    mask = Image.new("L", (ANIM_W, ANIM_H), 255)
    ImageDraw.Draw(mask).rounded_rectangle((wx, y, wx + TILE_W, y + TILE_H), radius=10, fill=0)
    shade.putalpha(Image.eval(mask, lambda v: int(v * 150 / 255)))
    frame.alpha_composite(shade)
    ImageDraw.Draw(frame).rounded_rectangle((wx - 3, y - 3, wx + TILE_W + 2, y + TILE_H + 2), radius=12, outline=ANIM_GOLD + (255,), width=3)
    rgb_frames.append(frame.convert("RGB"))
    durations.append(ANIM_FINAL_MS)

    # Общая палитра по нескольким реальным кадрам — без полос и мерцания цветов
    samples = [rgb_frames[0], rgb_frames[ANIM_FRAMES // 3], rgb_frames[ANIM_FRAMES - 1], rgb_frames[-1]]
    palette_src = Image.new("RGB", (ANIM_W, ANIM_H * len(samples)))
    for i, sample in enumerate(samples):
        palette_src.paste(sample, (0, ANIM_H * i))
    palette = palette_src.quantize(colors=160, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE)
    frames = [f.quantize(palette=palette, dither=Image.Dither.NONE) for f in rgb_frames]

    buf = io.BytesIO()
    # loop не указываем: GIF проигрывается один раз и останавливается на результате
    frames[0].save(buf, format="GIF", save_all=True, append_images=frames[1:], duration=durations, disposal=1, optimize=False)
    buf.seek(0)
    return buf, ANIM_FRAMES * ANIM_FRAME_MS / 1000

def _rarity_by_share(share):
    if share >= 15:
        return RARITY_COLORS["common"]
    if share >= 7:
        return RARITY_COLORS["uncommon"]
    if share >= 3:
        return RARITY_COLORS["rare"]
    if share >= 1:
        return RARITY_COLORS["epic"]
    if share >= 0.3:
        return RARITY_COLORS["mythic"]
    return RARITY_COLORS["legendary"]

def _prize_tile(prize, total, value_override=None):
    value = prize["value"] if value_override is None else value_override
    share = prize["chance"] / total * 100 if total else 0
    if value == "rotten_leg":
        value = "Гнилая ножка KFC"
    elif value == "water":
        value = "Стакан воды"
    if isinstance(value, int):
        if value < 0:
            return {"title": str(value), "subtitle": "кг", "color": RARITY_COLORS["minus"]}
        if value == 0:
            return {"title": "0", "subtitle": "ничего", "color": RARITY_COLORS["empty"]}
        return {"title": f"+{value}", "subtitle": "кг", "color": _rarity_by_share(share)}
    words = str(value).split()
    return {"title": words[0], "subtitle": " ".join(words[1:]), "color": _rarity_by_share(share)}

def build_case_strip(case_id, winner_prize, winner_value=None):
    prizes = CASES[case_id]["prizes"]
    total = sum(p["chance"] for p in prizes)
    picks = random.choices(prizes, weights=[p["chance"] for p in prizes], k=STRIP_LEN)
    tiles = [_prize_tile(p, total) for p in picks]
    tiles[WIN_INDEX] = _prize_tile(winner_prize, total, winner_value)
    return {"tiles": tiles, "win_index": WIN_INDEX}

def build_upgrade_strip(success, chance, target_name, lose_text="сгорело"):
    win_tile = {"title": "УСПЕХ", "subtitle": target_name, "color": RARITY_COLORS["success"]}
    lose_tile = {"title": "ПРОВАЛ", "subtitle": lose_text, "color": RARITY_COLORS["fail"]}
    visual = min(max(chance, 0.08), 0.92)
    tiles = [win_tile if random.random() < visual else lose_tile for _ in range(STRIP_LEN)]
    tiles[WIN_INDEX] = win_tile if success else lose_tile
    return {"tiles": tiles, "win_index": WIN_INDEX}

def build_duel_strip(result, challenger_name, opponent_name):
    options = [
        {"title": challenger_name, "subtitle": "побеждает", "color": RARITY_COLORS["legendary"]},
        {"title": opponent_name, "subtitle": "побеждает", "color": RARITY_COLORS["silver"]},
        {"title": "НИЧЬЯ", "subtitle": "все при своих", "color": RARITY_COLORS["draw"]},
    ]
    tiles = [random.choice(options) for _ in range(STRIP_LEN)]
    tiles[WIN_INDEX] = options[result]
    return {"tiles": tiles, "win_index": WIN_INDEX}

async def show_roulette(destination, embed, strip, filename, animations_enabled, message=None):
    """Показывает рулетку GIF-ом (одна загрузка вместо десятков правок сообщения).
    Если анимации выключены или Pillow нет — сразу показывает embed. Возвращает сообщение."""
    gif = None
    duration = 1
    if animations_enabled and ANIMATIONS_AVAILABLE:
        try:
            gif, duration = await asyncio.to_thread(render_roulette_gif, strip)
        except Exception as e:
            log_error(e, "Генерация GIF-анимации")
    if gif:
        embed.set_image(url=f"attachment://{filename}")
        file = discord.File(gif, filename=filename)
        if message:
            await message.edit(embed=embed, attachments=[file])
        else:
            message = await destination.send(embed=embed, file=file)
        await asyncio.sleep(duration + 0.3)
    else:
        if message:
            await message.edit(embed=embed)
        else:
            message = await destination.send(embed=embed)
        await asyncio.sleep(1)
    return message


async def migrate_old_burgers_to_prestige():
    burger_to_prestige = {0:1,1:2,2:3,3:4}
    converted = 0
    for guild in bot.guilds:
        guild_id = guild.id
        db_path = get_db_path(guild_id)
        if not os.path.exists(db_path):
            continue
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(user_fat)")
            cols = [col[1] for col in cursor.fetchall()]
            if 'legendary_burger' in cols and 'prestige' in cols:
                cursor.execute("SELECT user_id, user_name, legendary_burger FROM user_fat WHERE legendary_burger >= 0")
                rows = cursor.fetchall()
                for uid, uname, lvl in rows:
                    if lvl in burger_to_prestige:
                        cursor.execute("UPDATE user_fat SET prestige = ?, legendary_burger = -1 WHERE user_id = ?", (burger_to_prestige[lvl], uid))
                        converted += 1
                conn.commit()
            conn.close()
        except Exception as e:
            pass

async def migrate_old_autoburgers_to_auto_fat():
    converted = 0
    for guild in bot.guilds:
        guild_id = guild.id
        db_path = get_db_path(guild_id)
        if not os.path.exists(db_path):
            continue
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(user_fat)")
            cols = [col[1] for col in cursor.fetchall()]
            if 'autoburger_count' in cols and 'auto_fat_level' in cols:
                cursor.execute("SELECT user_id, user_name, autoburger_count FROM user_fat WHERE autoburger_count > 0")
                rows = cursor.fetchall()
                for uid, uname, cnt in rows:
                    if cnt > 0:
                        new_lvl = min(cnt, AUTO_FAT_MAX_LEVEL)
                        interval = get_auto_fat_interval(new_lvl)
                        next_time = datetime.now() + timedelta(hours=interval) if interval else None
                        cursor.execute("UPDATE user_fat SET auto_fat_level = ?, next_auto_fat_time = ?, autoburger_count = 0 WHERE user_id = ?", (new_lvl, next_time, uid))
                        converted += 1
                conn.commit()
            conn.close()
        except Exception as e:
            pass

async def duel_animation(msg, challenger, opponent, result):
    c_name = challenger.display_name[:15]
    o_name = opponent.display_name[:15]
    anim_embed = discord.Embed(title="⚔️ ДУЭЛЬ ⚔️", description=f"**{c_name}** ⚔️ **{o_name}**", color=0xff5500)
    await show_roulette(msg.channel, anim_embed, build_duel_strip(result, c_name, o_name), "duel.gif", True, message=msg)

def roll_upgrade(guild_id, user_id, member_name, target_item, prestige_luck, luck_upgrade):
    # Бросок апгрейда с «теневым» шансом: после неудач он растёт, после успеха падает
    data = get_user_data(guild_id, user_id, member_name)
    shadow = data.get('shadow_upgrade_chance', 0)
    prestige_bonus = 1 + prestige_luck
    luck_bonus = 1 + (luck_upgrade * LUCK_UPGRADE_BONUS_PER_LEVEL / 100)
    base = target_item['chance']
    real = min(base * prestige_bonus * luck_bonus + shadow / 100, 1.0)
    display = min(base * prestige_bonus * luck_bonus, 1.0)
    success = random.random() < real
    new_shadow = max(0, shadow - 8) if success else min(32, shadow + 4)
    return success, new_shadow, display

async def upgrade_animation(ctx, member, source_item, target_item, item_count, prestige_luck=0, luck_upgrade=0, animations_enabled=True):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    success, new_shadow, display = roll_upgrade(guild_id, user_id, member.name, target_item, prestige_luck, luck_upgrade)
    source_emoji = ITEM_EMOJIS.get(source_item, '📦')
    anim_embed = discord.Embed(title="🔧 АПГРЕЙД 🔧", description=f"**{member.display_name}** улучшает:\n{source_emoji} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\nШанс: {display*100:.1f}%", color=0xff5500)
    await show_roulette(ctx, anim_embed, build_upgrade_strip(success, display, target_item['name'], "предмет утерян"), "upgrade.gif", animations_enabled)
    cur_data = get_user_data(guild_id, user_id, member.name)
    items = get_user_items(cur_data['item_counts'])
    if success:
        items[target_item['name']] = items.get(target_item['name'], 0) + 1
        update_user_data(guild_id, user_id, item_counts=save_user_items(items), shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        desc = f"✅ Поздравляем!\n\n{source_emoji} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\nПредмет успешно улучшен!"
        lvl_gained, kg_reward, new_lvl = add_xp(guild_id, user_id, XP_PER_UPGRADE)
        if lvl_gained > 0:
            desc += f"\n\n⭐ ПОВЫШЕНИЕ УРОВНЯ! +{kg_reward} кг! Теперь у вас {new_lvl} уровень!"
        await update_user_nick(guild_id, user_id, member.name)
    else:
        update_user_data(guild_id, user_id, shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        desc = f"❌ Неудача!\n\n{source_emoji} **{source_item}** был утерян в процессе улучшения!"
    log_user_action(user_id, member.name, "Апгрейд предмета", f"{source_item} -> {target_item['name']}: {'успех' if success else 'провал'}")
    result_embed = discord.Embed(title="🔧 РЕЗУЛЬТАТ АПГРЕЙДА 🔧", description=f"{'✅ УСПЕХ! ✅' if success else '❌ НЕУДАЧА! ❌'}\n\n{desc}", color=0x00ff00 if success else 0xff0000)
    result_embed.set_footer(text=f"Шанс был: {display*100:.1f}%")
    await ctx.send(embed=result_embed)

async def upgrade_kg_animation(ctx, member, amount, target_item, prestige_luck=0, luck_upgrade=0, animations_enabled=True):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    success, new_shadow, display = roll_upgrade(guild_id, user_id, member.name, target_item, prestige_luck, luck_upgrade)
    anim_embed = discord.Embed(title="💱 АПГРЕЙД КГ 💱", description=f"**{member.display_name}** улучшает {amount} кг в:\n{target_item['emoji']} **{target_item['name']}**\n\nШанс: {display*100:.1f}%", color=0xff5500)
    await show_roulette(ctx, anim_embed, build_upgrade_strip(success, display, target_item['name'], f"{amount} кг сгорели"), "upgrade.gif", animations_enabled)
    cur_data = get_user_data(guild_id, user_id, member.name)
    if success:
        if target_item.get("is_case", False):
            cases = cur_data.get('cases_dict', {}).copy()
            cases[target_item["case_id"]] = cases.get(target_item["case_id"], 0) + 1
            update_user_data(guild_id, user_id, cases_dict=cases, shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        else:
            items = get_user_items(cur_data['item_counts'])
            items[target_item["name"]] = items.get(target_item["name"], 0) + 1
            update_user_data(guild_id, user_id, item_counts=save_user_items(items), shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        desc = f"✅ Поздравляем!\n\n{amount} кг → {target_item['emoji']} **{target_item['name']}**\n\nПредмет успешно получен!"
        lvl_gained, kg_reward, new_lvl = add_xp(guild_id, user_id, XP_PER_UPGRADE_KG)
        if lvl_gained > 0:
            desc += f"\n\n⭐ ПОВЫШЕНИЕ УРОВНЯ! +{kg_reward} кг! Теперь у вас {new_lvl} уровень!"
        await update_user_nick(guild_id, user_id, member.name)
    else:
        update_user_data(guild_id, user_id, shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        desc = f"❌ Неудача!\n\n{amount} кг сгорели в процессе улучшения!"
    log_user_action(user_id, member.name, "Апгрейд кг", f"{amount} кг -> {target_item['name']}: {'успех' if success else 'провал'}")
    result_embed = discord.Embed(title="💱 РЕЗУЛЬТАТ АПГРЕЙДА 💱", description=f"{'✅ УСПЕХ! ✅' if success else '❌ НЕУДАЧА! ❌'}\n\n{desc}", color=0x00ff00 if success else 0xff0000)
    result_embed.set_footer(text=f"Шанс был: {display*100:.1f}%")
    await ctx.send(embed=result_embed)

# ============================================
# ========== ФОНОВЫЕ ЗАДАЧИ ==========
# ============================================

def iter_guild_dbs():
    for guild in bot.guilds:
        db_path = get_db_path(guild.id)
        if os.path.exists(db_path):
            safe_init_guild_database(guild.id, guild.name)
            yield guild, db_path

def fetch_rows(db_path, query, params=()):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()

def execute_db(db_path, query, params=()):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(query, params)
        conn.commit()
    finally:
        conn.close()

async def daily_case_accumulation_loop():
    await bot.wait_until_ready()
    log_background("Ежедневные кейсы", "цикл запущен")
    while not bot.is_closed():
        try:
            current_time = datetime.now()
            for guild, db_path in iter_guild_dbs():
                try:
                    users = fetch_rows(db_path, "SELECT user_id, daily_case_last_time, case_cd_upgrade, item_counts FROM user_fat")
                    for user_id, last_time_str, case_cd_upgrade, items_str in users:
                        try:
                            # КД кейса с учётом прокачки и апельсинов
                            cooldown_minutes = get_case_cooldown_minutes(case_cd_upgrade, get_user_items(items_str))
                            last_time = to_datetime(last_time_str)
                            if not last_time:
                                execute_db(db_path, "UPDATE user_fat SET daily_case_count = COALESCE(daily_case_count, 0) + 1, daily_case_last_time = ? WHERE user_id = ?", (current_time.isoformat(), user_id))
                                continue
                            diff_minutes = (current_time - last_time).total_seconds() / 60
                            intervals = int(diff_minutes // cooldown_minutes)
                            if intervals > 0:
                                new_last_time = last_time + timedelta(minutes=cooldown_minutes * intervals)
                                execute_db(db_path, "UPDATE user_fat SET daily_case_count = COALESCE(daily_case_count, 0) + ?, daily_case_last_time = ? WHERE user_id = ?", (intervals, new_last_time.isoformat(), user_id))
                        except Exception as e:
                            log_error(e, f"Ежедневный кейс для {user_id}")
                except Exception as e:
                    log_error(e, f"Ежедневные кейсы, сервер {guild.id}")
        except Exception as e:
            log_error(e, "Цикл ежедневных кейсов")
        await asyncio.sleep(60)

async def auto_fat_loop():
    await bot.wait_until_ready()
    log_background("Авто-жир", "цикл запущен")
    while not bot.is_closed():
        try:
            for guild, db_path in iter_guild_dbs():
                try:
                    users = fetch_rows(db_path, '''SELECT user_id, user_name, auto_fat_level, next_auto_fat_time
                                                 FROM user_fat WHERE auto_fat_level > 0''')
                    for user_id, user_name, auto_fat_level, next_time_str in users:
                        try:
                            interval = get_auto_fat_interval(auto_fat_level)
                            if not interval:
                                continue
                            current_time = datetime.now()
                            next_time = to_datetime(next_time_str)
                            if next_time is None:
                                # Время не задано (старые данные) — назначаем
                                execute_db(db_path, 'UPDATE user_fat SET next_auto_fat_time = ? WHERE user_id = ?',
                                           ((current_time + timedelta(hours=interval)).isoformat(), user_id))
                                continue
                            if current_time >= next_time:
                                await apply_auto_fat(user_id, guild.id, user_name)
                                execute_db(db_path, 'UPDATE user_fat SET next_auto_fat_time = ? WHERE user_id = ?',
                                           ((current_time + timedelta(hours=interval)).isoformat(), user_id))
                        except Exception as e:
                            log_error(e, f"Авто-жир для {user_name}")
                except Exception as e:
                    log_error(e, f"Авто-жир, сервер {guild.id}")
        except Exception as e:
            log_error(e, "Цикл авто-жира")
        await asyncio.sleep(60)

def calc_daily_item_income(items):
    total = 0
    for name, cnt in items.items():
        for si in SHOP_ITEMS:
            if si["name"] == name:
                total += si.get("gain_per_24h", 0) * cnt
                break
    return total

async def passive_income_loop():
    # Проверяем каждые 10 минут, начисляем раз в 24 часа на пользователя.
    # (Раньше цикл спал 24 часа, и при перезапусках бота доход не приходил вообще.)
    await bot.wait_until_ready()
    log_background("Пассивный доход", "цикл запущен")
    while not bot.is_closed():
        try:
            for guild, db_path in iter_guild_dbs():
                try:
                    users = fetch_rows(db_path, "SELECT user_id, user_name, item_counts, income_upgrade, prestige, last_passive_income FROM user_fat WHERE item_counts IS NOT NULL AND item_counts != '{}'")
                    for user_id, user_name, item_counts_str, income_upgrade, prestige, last_income in users:
                        try:
                            now = datetime.now()
                            last_income = to_datetime(last_income)
                            if not last_income:
                                execute_db(db_path, 'UPDATE user_fat SET last_passive_income = ? WHERE user_id = ?', (now.isoformat(), user_id))
                                continue
                            periods = int((now - last_income).total_seconds() // 86400)
                            if periods <= 0:
                                continue
                            new_last = (last_income + timedelta(days=periods)).isoformat()
                            total = calc_daily_item_income(get_user_items(item_counts_str)) * periods
                            if total <= 0:
                                execute_db(db_path, 'UPDATE user_fat SET last_passive_income = ? WHERE user_id = ?', (new_last, user_id))
                                continue
                            final = int(total * get_income_bonus(income_upgrade or 0) * get_prestige_bonus(prestige or 0))
                            add_user_weight(guild.id, user_id, final, last_passive_income=new_last)
                            log_background("Пассивный доход", f"{user_name}: +{final} кг")
                            await update_user_nick(guild.id, user_id, user_name)
                        except Exception as e:
                            log_error(e, f"Пассивный доход для {user_id}")
                except Exception as e:
                    log_error(e, f"Пассивный доход, сервер {guild.id}")
        except Exception as e:
            log_error(e, "Цикл пассивного дохода")
        await asyncio.sleep(600)

async def apply_snatcher_effect(guild_id, user_id, user_name):
    try:
        data = get_user_data(guild_id, user_id, user_name)
        items = get_user_items(data['item_counts'])
        if items.get("Снатчер", 0) == 0:
            return
        now = datetime.now()
        last = to_datetime(data.get('snatcher_last_time'))
        if last and (now - last).total_seconds() < 21600:
            return
        if random.random() > 0.2:
            update_user_data(guild_id, user_id, snatcher_last_time=now)
            return
        used = set()
        slots = []
        for _ in range(10):
            chosen = None
            for _ in range(50):
                idx = random.randint(0, len(SHOP_ITEMS)-1)
                if idx in used:
                    continue
                item = SHOP_ITEMS[idx]
                if random.random() < item["chance"]:
                    chosen = item
                    used.add(idx)
                    break
            if chosen:
                amt = random.randint(chosen["min_amount"], chosen["max_amount"])
                slots.append({"name": chosen["name"], "amount": amt})
            else:
                slots.append(None)
        chosen_slot = random.randint(0,9)
        sel = slots[chosen_slot]
        if not sel:
            update_user_data(guild_id, user_id, snatcher_last_time=now)
            return
        items[sel["name"]] = items.get(sel["name"], 0) + 1
        update_user_data(guild_id, user_id, item_counts=save_user_items(items), snatcher_last_time=now)
        log_user_action(user_id, user_name, "Снатчер сработал", f"+1 {sel['name']} (слот {chosen_slot+1})")
        # ЛС ОТКЛЮЧЕНЫ
    except Exception as e:
        log_error(e, f"Снатчер для {user_id}")

async def snatcher_loop():
    await bot.wait_until_ready()
    log_background("Снатчер", "цикл запущен")
    while not bot.is_closed():
        try:
            for guild, db_path in iter_guild_dbs():
                try:
                    # Раньше искали через LIKE '%Снатчер%', но json.dumps хранил кириллицу
                    # как \uXXXX, и снатчер не находился никогда. Теперь разбираем JSON.
                    rows = fetch_rows(db_path, "SELECT user_id, user_name, item_counts FROM user_fat WHERE item_counts IS NOT NULL AND item_counts != '{}'")
                    for uid, uname, items_str in rows:
                        if get_user_items(items_str).get("Снатчер", 0) > 0:
                            await apply_snatcher_effect(guild.id, uid, uname)
                            await asyncio.sleep(1)
                except Exception as e:
                    log_error(e, f"Снатчер, сервер {guild.id}")
        except Exception as e:
            log_error(e, "Цикл снатчера")
        await asyncio.sleep(1800)

async def apply_auto_fat(user_id, guild_id, user_name):
    try:
        data = get_user_data(guild_id, user_id, user_name)
        items_dict = get_user_items(data['item_counts'])

        prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
        fat_plus_bonus = get_fat_plus_bonus(data.get('fat_plus_upgrade', 0))
        jackpot_bonus = get_jackpot_bonus(data.get('jackpot_upgrade', 0))
        luck_upgrade = data.get('luck_upgrade', 0)

        change, was_minus, new_plus, new_minus, new_pity, was_jackpot = get_change_with_pity_and_jackpot(
            data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'],
            luck_upgrade, prestige_bonus, fat_plus_bonus, jackpot_bonus, items_dict, data['current_number'])

        new_number = data['current_number'] + change

        # Авто-жир не трогает КД ручного !жир
        update_user_data(guild_id, user_id, number=new_number, consecutive_plus=new_plus,
                         consecutive_minus=new_minus, jackpot_pity=new_pity)

        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_FAT)
        await update_user_nick(guild_id, user_id, user_name)

        logger.debug(f"Авто-жир для {user_name}: {change:+d} кг, новый вес {new_number} кг")

        if levels_gained > 0:
            logger.info(f"Авто-жир повысил уровень {user_name} до {new_level} (+{kg_reward} кг)")

    except Exception as e:
        log_error(e, f"apply_auto_fat для {user_name}")

async def apply_hourly_effects():
    await bot.wait_until_ready()
    log_background("Почасовой доход", "цикл запущен")
    while not bot.is_closed():
        try:
            for guild, db_path in iter_guild_dbs():
                try:
                    rows = fetch_rows(db_path, "SELECT user_id, user_name, item_counts, income_upgrade, prestige, last_hourly_income FROM user_fat WHERE item_counts IS NOT NULL AND item_counts != '{}'")
                    for uid, uname, items_str, inc_up, pres, last_hour in rows:
                        try:
                            items = get_user_items(items_str)
                            if not items.get("Автохолестерол") and not items.get("Холестеринимус"):
                                continue
                            now = datetime.now()
                            last_hour = to_datetime(last_hour)
                            if not last_hour:
                                execute_db(db_path, 'UPDATE user_fat SET last_hourly_income = ? WHERE user_id = ?', (now.isoformat(), uid))
                                continue
                            hours = int((now - last_hour).total_seconds() // 3600)
                            if hours <= 0:
                                continue
                            hours = min(hours, 24)  # не больше суток за раз после долгого простоя
                            total = 0
                            for _ in range(hours):
                                total += sum(random.randint(1, 10) for _ in range(items.get("Автохолестерол", 0)))
                                total += sum(random.randint(1, 5) for _ in range(items.get("Холестеринимус", 0)))
                            final = int(total * get_income_bonus(inc_up or 0) * get_prestige_bonus(pres or 0))
                            add_user_weight(guild.id, uid, final, last_hourly_income=now)
                            await update_user_nick(guild.id, uid, uname)
                        except Exception as e:
                            log_error(e, f"Почасовой доход для {uid}")
                except Exception as e:
                    log_error(e, f"Почасовой доход, сервер {guild.id}")
        except Exception as e:
            log_error(e, "Цикл почасового дохода")
        await asyncio.sleep(300)

async def open_all_cases(guild_id, user_id, user_name):
    try:
        data = get_user_data(guild_id, user_id, user_name)
        prestige_luck = get_prestige_luck(data.get('prestige', 0))
        luck_upgrade = data.get('luck_upgrade', 0)
        case_plus_bonus = get_case_plus_bonus(data.get('case_plus_upgrade', 0))
        prestige_bonus = get_prestige_bonus(data.get('prestige', 0))

        items_dict = get_user_items(data['item_counts'])
        has_water = items_dict.get("Стакан воды", 0) > 0

        total_gain = 0
        opened_count = 0

        to_open = []
        daily_count = data.get('daily_case_count', 0) or 0
        if daily_count > 0:
            to_open.append(("daily", daily_count))
        cases_dict = data.get('cases_dict', {}).copy()
        for case_id, count in cases_dict.items():
            if count and count > 0 and case_id in CASES and case_id != "daily":
                to_open.append((case_id, count))
                cases_dict[case_id] = 0
        if not to_open:
            return 0, 0

        for case_id, count in to_open:
            for _ in range(count):
                prize = open_case(case_id, prestige_luck, luck_upgrade, case_plus_bonus)
                prize_value = prize["value"]
                opened_count += 1
                if prize_value == "rotten_leg":
                    items_dict["Гнилая ножка KFC"] = items_dict.get("Гнилая ножка KFC", 0) + 1
                elif prize_value == "water":
                    items_dict["Стакан воды"] = items_dict.get("Стакан воды", 0) + 1
                elif isinstance(prize_value, str):
                    items_dict[prize_value] = items_dict.get(prize_value, 0) + 1
                else:
                    # Усиление кейсов уже применено в open_case (раньше применялось дважды)
                    total_gain += apply_case_prize_bonuses(prize_value, prestige_bonus, has_water, case_id)

        # Всё одним обновлением, вес — атомарно
        update_user_data(guild_id, user_id, daily_case_count=0, cases_dict=cases_dict, item_counts=save_user_items(items_dict))
        add_user_weight(guild_id, user_id, total_gain)
        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_CASE * opened_count)
        await update_user_nick(guild_id, user_id, user_name)
        log_user_action(user_id, user_name, "Авто-открытие кейсов", f"открыто {opened_count}, {total_gain:+d} кг")

        # ЛС ОТКЛЮЧЕНЫ - удалён весь код отправки сообщений

        return opened_count, total_gain

    except Exception as e:
        log_error(e, f"Открытие всех кейсов для {user_id}")
        return 0, 0

async def auto_open_new_cases_loop():
    await bot.wait_until_ready()
    log_background("Авто-открытие кейсов", "цикл запущен")
    while not bot.is_closed():
        try:
            for guild, db_path in iter_guild_dbs():
                try:
                    users = fetch_rows(db_path, "SELECT user_id, user_name FROM user_fat WHERE auto_open_cases = 1")
                    for user_id, user_name in users:
                        try:
                            user_data = get_user_data(guild.id, user_id)
                            if user_data.get('prestige', 0) < AUTO_OPEN_CASES_REQUIRED_PRESTIGE:
                                continue
                            has_cases = (user_data.get('daily_case_count', 0) or 0) > 0 or any(
                                (c or 0) > 0 for c in user_data.get('cases_dict', {}).values())
                            if has_cases:
                                await open_all_cases(guild.id, user_id, user_name)
                                await asyncio.sleep(0.5)
                        except Exception as e:
                            log_error(e, f"Авто-открытие для {user_id}")
                except Exception as e:
                    log_error(e, f"Авто-открытие, сервер {guild.id}")
        except Exception as e:
            log_error(e, "Цикл авто-открытия")
        await asyncio.sleep(60)

async def backup_loop():
    # Фоновые задачи пишут в базу напрямую, поэтому периодически делаем полный бэкап
    await bot.wait_until_ready()
    while not bot.is_closed():
        await asyncio.sleep(300)
        for db in glob.glob(os.path.join(DB_FOLDER, "*.db")):
            backup_guild_db(db, force=True)

async def clear_stale_states():
    # После перезапуска бота никакие дуэли/открытия кейсов не идут — снимаем блокировки
    for guild, db_path in iter_guild_dbs():
        try:
            execute_db(db_path, "UPDATE user_fat SET duel_active = 0, duel_opponent = NULL, duel_amount = 0, duel_message_id = NULL, duel_channel_id = NULL, duel_initiator = 0, duel_start_time = NULL, active_case_message_id = NULL, active_case_channel_id = NULL WHERE duel_active = 1 OR active_case_message_id IS NOT NULL")
        except Exception as e:
            log_error(e, f"Сброс зависших состояний, сервер {guild.id}")

# ============================================
# ========== ОСНОВНЫЕ КОМАНДЫ ==========
# ============================================

@bot.command(name='жир')
async def fat_command(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    data = get_user_data(guild_id, user_id, user_name)
    items_dict = get_user_items(data['item_counts'])
    actual_cooldown = get_fat_cooldown_hours(data, items_dict)
    can_use, remaining = check_cooldown(data['fat_cooldown_time'], actual_cooldown)
    if not can_use:
        embed = discord.Embed(title="⏳ Подождите!", description=f"{member.mention}, вы уже использовали команду недавно!", color=0xff0000)
        embed.add_field(name="Осталось подождать", value=format_time(remaining), inline=True)
        embed.add_field(name="Кулдаун", value=f"{actual_cooldown*60:.0f} мин", inline=True)
        await ctx.send(embed=embed)
        return
    prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
    fat_plus_bonus = get_fat_plus_bonus(data.get('fat_plus_upgrade', 0))
    jackpot_bonus = get_jackpot_bonus(data.get('jackpot_upgrade', 0))
    change, was_minus, new_plus, new_minus, new_pity, was_jackpot = get_change_with_pity_and_jackpot(
        data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
        data.get('luck_upgrade', 0), prestige_bonus, fat_plus_bonus, jackpot_bonus, items_dict, data['current_number'])
    new_number = data['current_number'] + change
    update_user_data(guild_id, user_id, number=new_number, consecutive_plus=new_plus, consecutive_minus=new_minus, jackpot_pity=new_pity, fat_cooldown_time=datetime.now())
    levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_FAT)
    await update_user_nick(guild_id, user_id, user_name)
    rank_name, rank_emoji = get_rank(new_number)
    embed_color = 0xffd700 if was_jackpot else (0xff9933 if new_number >= 0 else 0x66ccff)
    embed_title = "💰 ДЖЕКПОТ! 💰" if was_jackpot else "🍔 Набор массы"
    embed = discord.Embed(title=embed_title, description=f"**{member.mention}** теперь весит **{new_number}kg** на сервере **{ctx.guild.name}**!", color=embed_color)
    if was_jackpot:
        embed.add_field(name="💰 ДЖЕКПОТ!", value=f"+{change} кг", inline=True)
    elif change > 0:
        embed.add_field(name="📈 Изменение", value=f"+{change} кг", inline=True)
    elif change < 0:
        embed.add_field(name="📉 Изменение", value=f"{change} кг", inline=True)
    embed.add_field(name="🍖 Текущий вес", value=f"{new_number}kg", inline=True)
    embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
    if levels_gained > 0:
        embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", inline=False)
    embed.add_field(name="⏰ Следующая команда", value=f"через {actual_cooldown*60:.0f} мин", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='жиркейс')
async def fat_case_command(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    data = get_user_data(guild_id, user_id, user_name)
    if data.get('active_case_message_id'):
        try:
            channel = bot.get_channel(int(data['active_case_channel_id'])) if data.get('active_case_channel_id') else None
            if channel:
                try:
                    old_msg = await channel.fetch_message(int(data['active_case_message_id']))
                    if old_msg:
                        # created_at в UTC — сравниваем тоже с UTC, а не с местным временем
                        time_since = datetime.now(timezone.utc) - old_msg.created_at
                        if time_since.total_seconds() < 60:
                            embed = discord.Embed(title="⚠️ Кейс уже открыт!", description=f"{member.mention}, у вас уже есть активный кейс!", color=0xffaa00)
                            await ctx.send(embed=embed)
                            return
                except:
                    pass
        except:
            pass
    daily_count = data.get('daily_case_count', 0)
    cases_dict = data.get('cases_dict', {}).copy()
    case_to_open = None
    case = None
    if daily_count > 0:
        case_to_open = "daily"
        case = CASES["daily"]
    else:
        for case_id, count in cases_dict.items():
            if count > 0:
                case_to_open = case_id
                case = CASES[case_id]
                break
    if not case_to_open:
        embed = discord.Embed(title="📭 Нет кейсов!", description=f"{member.mention}, у вас нет кейсов для открытия!\n\nКупить кейсы можно в магазине (`!магазин`)", color=0xff0000)
        await ctx.send(embed=embed)
        return
    update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=case_to_open)
    prize_emojis = []
    for prize in case["prizes"]:
        if "emoji" in prize:
            emoji = prize["emoji"]
        elif prize["value"] == "rotten_leg":
            emoji = "💀"
        elif prize["value"] == "water":
            emoji = "💧"
        elif isinstance(prize["value"], int):
            if prize["value"] < 0:
                emoji = "📉"
            elif prize["value"] == 0:
                emoji = "🔄"
            elif prize["value"] < 50:
                emoji = "📈"
            elif prize["value"] < 100:
                emoji = "⬆️"
            elif prize["value"] < 500:
                emoji = "🚀"
            elif prize["value"] < 1000:
                emoji = "⭐"
            else:
                emoji = "💥"
        else:
            emoji = "🎁"
        if emoji not in prize_emojis:
            prize_emojis.append(emoji)
    case_emoji = case["emoji"]
    case_embed = discord.Embed(title=f"{case_emoji} **{case['name']}** {case_emoji}", description=f"{member.mention}, у вас есть кейс!\n\n**Нажмите на 🖱️ чтобы открыть**\n**Нажмите на ❌ чтобы отменить**", color=0xffaa00)
    case_embed.set_footer(text="У вас 30 секунд!")
    case_msg = await ctx.send(embed=case_embed)
    await case_msg.add_reaction("🖱️")
    await case_msg.add_reaction("❌")
    update_user_data(guild_id, user_id, active_case_message_id=str(case_msg.id), active_case_channel_id=str(ctx.channel.id))
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["🖱️", "❌"] and reaction.message.id == case_msg.id
    try:
        reaction, user = await bot.wait_for('reaction_add', timeout=30.0, check=check)
        if str(reaction.emoji) == "❌":
            update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None)
            try:
                await case_msg.clear_reactions()
            except:
                pass
            cancel_embed = discord.Embed(title="❌ Отмена", description=f"{member.mention}, вы отменили открытие. Кейс сохранён!", color=0xffaa00)
            await case_msg.edit(embed=cancel_embed)
            return
        current_data = get_user_data(guild_id, user_id, user_name)
        if case_to_open == "daily":
            current_daily = current_data.get('daily_case_count', 0)
            if current_daily <= 0:
                await ctx.send(f"{member.mention}, у вас нет ежедневных кейсов!")
                await case_msg.delete()
                update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None)
                return
            update_user_data(guild_id, user_id, daily_case_count=current_daily - 1)
        else:
            current_cases = current_data.get('cases_dict', {}).copy()
            if current_cases.get(case_to_open, 0) <= 0:
                await ctx.send(f"{member.mention}, у вас больше нет этого кейса!")
                await case_msg.delete()
                update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None)
                return
            current_cases[case_to_open] -= 1
            update_user_data(guild_id, user_id, cases_dict=current_cases)
        try:
            await case_msg.clear_reactions()
        except:
            pass
        prestige_luck = get_prestige_luck(current_data.get('prestige', 0))
        luck_upgrade = current_data.get('luck_upgrade', 0)
        case_plus_bonus = get_case_plus_bonus(current_data.get('case_plus_upgrade', 0))
        prize = open_case(case_to_open, prestige_luck, luck_upgrade, case_plus_bonus)
        if "emoji" in prize:
            prize_emoji = prize["emoji"]
        elif prize["value"] == "rotten_leg":
            prize_emoji = "💀"
        elif prize["value"] == "water":
            prize_emoji = "💧"
        elif isinstance(prize["value"], int):
            if prize["value"] < 0:
                prize_emoji = "📉"
            elif prize["value"] == 0:
                prize_emoji = "🔄"
            elif prize["value"] < 50:
                prize_emoji = "📈"
            elif prize["value"] < 100:
                prize_emoji = "⬆️"
            elif prize["value"] < 500:
                prize_emoji = "🚀"
            elif prize["value"] < 1000:
                prize_emoji = "⭐"
            else:
                prize_emoji = "💥"
        else:
            prize_emoji = "🎁"
        # Значение на плитке — уже с бонусами престижа/воды, как будет начислено
        preview_value = None
        if isinstance(prize["value"], int):
            pre_items = get_user_items(current_data['item_counts'])
            preview_value = apply_case_prize_bonuses(prize["value"], get_prestige_bonus(current_data.get('prestige', 0)),
                                                     pre_items.get("Стакан воды", 0) > 0, case_to_open)
        anim_embed = discord.Embed(title=f"🎰 **{case['name']}** 🎰", description=f"{member.mention} открывает кейс...", color=0xffaa00)
        await show_roulette(ctx, anim_embed, build_case_strip(case_to_open, prize, preview_value), "case.gif",
                            are_animations_enabled(current_data), message=case_msg)
        current_data = get_user_data(guild_id, user_id, user_name)
        items_dict = get_user_items(current_data['item_counts'])
        prize_value = prize["value"]
        prestige_bonus = get_prestige_bonus(current_data.get('prestige', 0))
        has_water = items_dict.get("Стакан воды", 0) > 0
        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_CASE)
        after_xp_data = get_user_data(guild_id, user_id, user_name)
        after_xp_weight = after_xp_data['current_number']
        if prize_value == "rotten_leg":
            items_dict["Гнилая ножка KFC"] = items_dict.get("Гнилая ножка KFC", 0) + 1
            new_number = after_xp_weight
            result_display = f"💀 **Гнилая ножка KFC!** 💀"
            result_color = 0x993366
        elif prize_value == "water":
            items_dict["Стакан воды"] = items_dict.get("Стакан воды", 0) + 1
            new_number = after_xp_weight
            result_display = f"💧 **Стакан воды!** 💧"
            result_color = 0x66ccff
        elif isinstance(prize_value, str):
            items_dict[prize_value] = items_dict.get(prize_value, 0) + 1
            new_number = after_xp_weight
            result_display = f"🎁 **{prize_value}** {prize_emoji}"
            result_color = 0x9b59b6
        else:
            prize_value = apply_case_prize_bonuses(prize_value, prestige_bonus, has_water, case_to_open)
            new_number = after_xp_weight + prize_value
            result_display = f"🎉 **{prize_value:+d} кг** {prize_emoji}"
            result_color = 0xffaa00
        update_data = {'number': new_number, 'user_name': user_name, 'item_counts': save_user_items(items_dict),
                       'active_case_message_id': None, 'active_case_channel_id': None}
        update_user_data(guild_id, user_id, **update_data)
        await update_user_nick(guild_id, user_id, user_name)
        rank_name, rank_emoji = get_rank(new_number)
        final_embed = discord.Embed(title=f"{case['emoji']} Открытие {case['name']}", description=f"**{member.mention}** открыл кейс и получил:", color=result_color)
        final_embed.add_field(name="🎁 Приз", value=result_display, inline=True)
        if not isinstance(prize_value, str) and prize_value not in ["rotten_leg", "water"]:
            final_embed.add_field(name="🍖 Новый вес", value=f"{new_number}kg", inline=True)
            final_embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
        if levels_gained > 0:
            final_embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", inline=False)
        if case_to_open == "daily":
            remaining = after_xp_data.get('daily_case_count', 0)
            if remaining > 0:
                final_embed.add_field(name="📦 Осталось ежедневных кейсов", value=f"{remaining} шт", inline=False)
        else:
            remaining = after_xp_data.get('cases_dict', {}).get(case_to_open, 0)
            if remaining > 0:
                final_embed.add_field(name="📦 Осталось кейсов", value=f"{case['emoji']} {case['name']}: {remaining} шт", inline=False)
        final_embed.set_footer(text=f"{case['emoji']} Удачи в следующий раз!")
        await ctx.send(embed=final_embed)
    except asyncio.TimeoutError:
        update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None)
        try:
            await case_msg.clear_reactions()
        except:
            pass
        timeout_embed = discord.Embed(title="⏰ Время вышло", description=f"{member.mention}, вы не открыли кейс вовремя. Кейс сохранён в инвентаре!", color=0xff0000)
        await case_msg.edit(embed=timeout_embed)

@bot.command(name='профиль')
async def profile_command(ctx, member: discord.Member = None):
    target = member or ctx.author
    guild_id = ctx.guild.id
    user_id = str(target.id)
    user_name = target.name
    
    def create_profile_embed(data, animations_status):
        rank_name, rank_emoji = get_rank(data['current_number'])
        
        items_dict = get_user_items(data['item_counts'])
        actual_fat_cooldown = get_fat_cooldown_hours(data, items_dict)
        
        total_passive_income = 0
        for item_name, count in items_dict.items():
            for shop_item in SHOP_ITEMS:
                if shop_item["name"] == item_name:
                    gain = shop_item.get("gain_per_24h", 0)
                    if gain > 0:
                        total_passive_income += gain * count
                    break
        
        income_bonus = get_income_bonus(data.get('income_upgrade', 0))
        prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
        total_passive_income = int(total_passive_income * income_bonus * prestige_bonus)
        
        total_xp = data.get('user_xp', 0)
        level, current_xp = get_level_and_xp(total_xp)
        next_level_xp = get_xp_for_next_level(level)
        
        fat_cd_level = data.get('fat_cd_upgrade', 0)
        case_cd_level = data.get('case_cd_upgrade', 0)
        luck_level = data.get('luck_upgrade', 0)
        income_level = data.get('income_upgrade', 0)
        prestige_level = data.get('prestige', 0)
        auto_fat_level = data.get('auto_fat_level', 0)
        jackpot_level = data.get('jackpot_upgrade', 0)
        fat_plus_level = data.get('fat_plus_upgrade', 0)
        case_plus_level = data.get('case_plus_upgrade', 0)
        xp_multiplier_level = data.get('xp_multiplier_upgrade', 0)
        auto_open_cases = data.get('auto_open_cases', 0)
        
        fat_cd_cost = get_upgrade_cost("fat_cd", fat_cd_level)
        case_cd_cost = get_upgrade_cost("case_cd", case_cd_level)
        luck_cost = get_upgrade_cost("luck", luck_level)
        income_cost = get_upgrade_cost("income", income_level)
        prestige_cost = get_upgrade_cost("prestige", prestige_level)
        auto_fat_cost = get_upgrade_cost("auto_fat", auto_fat_level)
        jackpot_cost = get_upgrade_cost("jackpot", jackpot_level)
        fat_plus_cost = get_upgrade_cost("fat_plus", fat_plus_level)
        case_plus_cost = get_upgrade_cost("case_plus", case_plus_level)
        xp_multiplier_cost = get_upgrade_cost("xp_multiplier", xp_multiplier_level)
        
        fat_cd_bonus = int(get_fat_cd_reduction(fat_cd_level))
        case_cd_bonus = int(get_case_cd_reduction(case_cd_level))
        auto_fat_interval = get_auto_fat_interval(auto_fat_level)
        auto_fat_text = f"{auto_fat_interval} ч" if auto_fat_interval else "Не куплен"
        jackpot_bonus = get_jackpot_bonus(jackpot_level) * 100
        fat_plus_bonus = (get_fat_plus_bonus(fat_plus_level) - 1) * 100
        case_plus_bonus = (get_case_plus_bonus(case_plus_level) - 1) * 100
        xp_multiplier_bonus = (get_xp_multiplier_bonus(xp_multiplier_level) - 1) * 100
        
        can_use_fat, fat_remaining = check_cooldown(data['fat_cooldown_time'], actual_fat_cooldown)
        fat_status = f"✅ Доступен" if can_use_fat else f"⏳ {format_time(fat_remaining)}"
        
        daily_count = data.get('daily_case_count', 0)
        if daily_count > 0:
            case_status = f"✅ {daily_count} шт в инвентаре"
        else:
            last_time = data.get('daily_case_last_time')
            if last_time:
                if isinstance(last_time, str):
                    last_time = datetime.fromisoformat(last_time)
                cd_min = get_case_cooldown_minutes(data.get('case_cd_upgrade', 0), items_dict)
                next_time = last_time + timedelta(minutes=cd_min)
                if next_time > datetime.now():
                    remaining = (next_time - datetime.now()).total_seconds()
                    case_status = f"⏳ Через {format_time(remaining)}"
                else:
                    case_status = "✅ Доступен (обновите инвентарь)"
            else:
                case_status = "✅ Доступен"
        
        embed = discord.Embed(
            title=f"⭐ **ПРОФИЛЬ** ⭐",
            description=f"**{target.display_name}**\n{'🎬 Анимации: ВКЛ' if animations_status == 1 else '🔇 Анимации: ВЫКЛ'}",
            color=0xffaa00
        )
        
        xp_bar_length = 20
        xp_progress = int((current_xp / next_level_xp) * xp_bar_length) if next_level_xp > 0 else 0
        xp_bar = "█" * xp_progress + "░" * (xp_bar_length - xp_progress)
        
        embed.add_field(
            name="📊 **ОСНОВНАЯ ИНФОРМАЦИЯ**",
            value=f"🍖 Вес: **{data['current_number']}kg**\n"
                  f"🎖️ Звание: {rank_emoji} **{rank_name}**\n"
                  f"📈 Уровень: **{level}**\n"
                  f"✨ Опыт: {current_xp} / {next_level_xp}\n`{xp_bar}`\n"
                  f"🌟 Престиж: **{prestige_level}** (+{prestige_level*10}% к кг, +{prestige_level}% к шансам, +{prestige_level*50}% к опыту)",
            inline=False
        )
        
        embed.add_field(
            name="⏰ **КОМАНДЫ**",
            value=f"**!жир:** {fat_status} (КД {actual_fat_cooldown*60:.0f} мин)\n"
                  f"**!жиркейс:** {case_status}",
            inline=False
        )
        
        if total_passive_income > 0:
            embed.add_field(name="💰 **ПАССИВНЫЙ ДОХОД**", value=f"{total_passive_income} кг/24ч", inline=True)
        
        if data.get('auto_fat_level', 0) > 0:
            embed.add_field(name="🤖 **АВТО-ЖИР**", value=f"{auto_fat_level} уровень (каждые {auto_fat_text})", inline=True)
        
        stats_text = ""
        
        fat_cd_color = "🟢" if data['current_number'] >= fat_cd_cost else "🔴"
        fat_cd_max_text = " (МАКС)" if fat_cd_level >= FAT_CD_MAX_LEVEL else ""
        stats_text += f"{fat_cd_color} **⏰ КД !жир** — ур.{fat_cd_level}{fat_cd_max_text} (-{fat_cd_bonus} мин)\n   Стоимость: `{fat_cd_cost} кг`\n\n"
        
        case_cd_color = "🟢" if data['current_number'] >= case_cd_cost else "🔴"
        case_cd_max_text = " (МАКС)" if case_cd_level >= CASE_CD_MAX_LEVEL else ""
        stats_text += f"{case_cd_color} **📦 КД кейса** — ур.{case_cd_level}{case_cd_max_text} (-{case_cd_bonus} мин)\n   Стоимость: `{case_cd_cost} кг`\n\n"
        
        luck_color = "🟢" if data['current_number'] >= luck_cost else "🔴"
        stats_text += f"{luck_color} **🍀 Удача** — ур.{luck_level} (+{luck_level * LUCK_CASE_BONUS_PER_LEVEL:.2f}% к редким, +{luck_level * LUCK_UPGRADE_BONUS_PER_LEVEL:.2f}% к апгрейдам)\n   Стоимость: `{luck_cost} кг`\n\n"
        
        income_color = "🟢" if data['current_number'] >= income_cost else "🔴"
        stats_text += f"{income_color} **📈 Прибавка** — ур.{income_level} (+{(income_bonus-1)*100:.0f}% к доходу)\n   Стоимость: `{income_cost} кг`\n\n"
        
        prestige_color = "🟢" if data['current_number'] >= prestige_cost else "🔴"
        stats_text += f"{prestige_color} **🌟 Престиж** — ур.{prestige_level}\n   Стоимость: `{prestige_cost} кг`\n\n"
        
        auto_fat_color = "🟢" if data['current_number'] >= auto_fat_cost else "🔴"
        auto_fat_max_text = " (МАКС)" if auto_fat_level >= AUTO_FAT_MAX_LEVEL else ""
        stats_text += f"{auto_fat_color} **🤖 Авто-жир** — ур.{auto_fat_level}{auto_fat_max_text} (каждые {auto_fat_text})\n   Стоимость: `{auto_fat_cost} кг`\n\n"
        
        jackpot_color = "🟢" if data['current_number'] >= jackpot_cost else "🔴"
        jackpot_max_text = " (МАКС)" if jackpot_level >= JACKPOT_MAX_LEVEL else ""
        stats_text += f"{jackpot_color} **💰 Шанс джекпота** — ур.{jackpot_level}{jackpot_max_text} (+{jackpot_bonus:.2f}% к шансу)\n   Стоимость: `{jackpot_cost} кг`\n\n"
        
        fat_plus_color = "🟢" if data['current_number'] >= fat_plus_cost else "🔴"
        fat_plus_max_text = " (МАКС)" if fat_plus_level >= FAT_PLUS_MAX_LEVEL else ""
        stats_text += f"{fat_plus_color} **📈 Усиление !жир** — ур.{fat_plus_level}{fat_plus_max_text} (+{fat_plus_bonus:.0f}% к плюсу)\n   Стоимость: `{fat_plus_cost} кг`\n\n"
        
        case_plus_color = "🟢" if data['current_number'] >= case_plus_cost else "🔴"
        case_plus_max_text = " (МАКС)" if case_plus_level >= CASE_PLUS_MAX_LEVEL else ""
        stats_text += f"{case_plus_color} **🎁 Усиление кейсов** — ур.{case_plus_level}{case_plus_max_text} (+{case_plus_bonus:.0f}% к призам)\n   Стоимость: `{case_plus_cost} кг`\n\n"
        
        xp_multiplier_color = "🟢" if data['current_number'] >= xp_multiplier_cost else "🔴"
        xp_multiplier_max_text = " (МАКС)" if xp_multiplier_level >= XP_MULTIPLIER_MAX_LEVEL else ""
        stats_text += f"{xp_multiplier_color} **⭐ Множитель опыта** — ур.{xp_multiplier_level}{xp_multiplier_max_text} (+{xp_multiplier_bonus:.0f}% к опыту)\n   Стоимость: `{xp_multiplier_cost} кг`\n\n"
        
        auto_open_color = "🟢" if data['current_number'] >= AUTO_OPEN_CASES_PRICE and prestige_level >= AUTO_OPEN_CASES_REQUIRED_PRESTIGE else "🔴"
        # auto_open_cases: 0 — не куплено, 1 — включено, 2 — куплено, но выключено
        if auto_open_cases == 1:
            stats_text += "🟢 **🔄 Авто-открытие кейсов** — ✅ Включено (🔄 — выключить)\n\n"
        elif auto_open_cases == 2:
            stats_text += "🟢 **🔄 Авто-открытие кейсов** — ❌ Выключено (🔄 — включить бесплатно)\n\n"
        else:
            stats_text += f"{auto_open_color} **🔄 Авто-открытие кейсов** — не куплено\n   Стоимость: `{AUTO_OPEN_CASES_PRICE} кг` (требуется {AUTO_OPEN_CASES_REQUIRED_PRESTIGE}+ престиж)\n\n"
        
        # Лимит поля embed — 1024 символа, поэтому делим прокачку на несколько полей
        blocks = [b for b in stats_text.split("\n\n") if b.strip()]
        chunk = ""
        part = 1
        for block in blocks:
            if len(chunk) + len(block) + 2 > 1000:
                embed.add_field(name="⚡ **ПРОКАЧКА**" if part == 1 else "⚡ **ПРОКАЧКА (продолжение)**", value=chunk, inline=False)
                chunk = ""
                part += 1
            chunk += block + "\n\n"
        if chunk:
            embed.add_field(name="⚡ **ПРОКАЧКА**" if part == 1 else "⚡ **ПРОКАЧКА (продолжение)**", value=chunk, inline=False)
        
        pity_emojis = []
        if data['consecutive_plus'] > 0:
            pity_emojis.append(f"🔥{data['consecutive_plus']}")
        if data['consecutive_minus'] > 0:
            pity_emojis.append(f"❄️{data['consecutive_minus']}")
        if data['jackpot_pity'] > 0:
            pity_emojis.append(f"💰{data['jackpot_pity']}")
        if pity_emojis:
            embed.add_field(name="📊 **СЧЁТЧИКИ**", value=" ".join(pity_emojis), inline=True)
        
        cases_text = ""
        daily_count = data.get('daily_case_count', 0)
        if daily_count > 0:
            cases_text += f"📦 Ежедневный кейс: {daily_count} шт\n"
        
        cases_dict = data.get('cases_dict', {})
        for case_id, count in cases_dict.items():
            if count > 0 and case_id in CASES:
                cases_text += f"{CASES[case_id]['emoji']} {CASES[case_id]['name']}: {count}\n"
        
        if cases_text:
            embed.add_field(name="📦 **КЕЙСЫ**", value=cases_text, inline=False)
        
        embed.set_footer(text="🟢 Нажмите на реакцию для улучшения | 🎬 Нажмите 🎬 для переключения анимаций | 🔄 Авто-открытие кейсов")
        return embed
    
    data = get_user_data(guild_id, user_id, user_name)
    animations_status = data.get('animations_enabled', 1)
    embed = create_profile_embed(data, animations_status)
    msg = await ctx.send(embed=embed)

    # Прокачивать можно только свой профиль (раньше можно было тратить чужие кг)
    if target.id != ctx.author.id:
        return

    upgrade_reactions = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟", "🔄"]
    for reaction in upgrade_reactions + ["🎬"]:
        await msg.add_reaction(reaction)

    upgrade_map = {
        "1️⃣": "fat_cd", "2️⃣": "case_cd", "3️⃣": "luck", "4️⃣": "income",
        "5️⃣": "prestige", "6️⃣": "auto_fat", "7️⃣": "jackpot", "8️⃣": "fat_plus",
        "9️⃣": "case_plus", "🔟": "xp_multiplier", "🔄": "auto_open"
    }
    # тип: (поле в БД, макс. уровень или None, название)
    upgrade_info = {
        "fat_cd": ("fat_cd_upgrade", FAT_CD_MAX_LEVEL, "КД !жир"),
        "case_cd": ("case_cd_upgrade", CASE_CD_MAX_LEVEL, "КД кейса"),
        "luck": ("luck_upgrade", None, "Удача"),
        "income": ("income_upgrade", None, "Прибавка"),
        "prestige": ("prestige", None, "Престиж"),
        "auto_fat": ("auto_fat_level", AUTO_FAT_MAX_LEVEL, "Авто-жир"),
        "jackpot": ("jackpot_upgrade", JACKPOT_MAX_LEVEL, "Шанс джекпота"),
        "fat_plus": ("fat_plus_upgrade", FAT_PLUS_MAX_LEVEL, "Усиление !жир"),
        "case_plus": ("case_plus_upgrade", CASE_PLUS_MAX_LEVEL, "Усиление кейсов"),
        "xp_multiplier": ("xp_multiplier_upgrade", XP_MULTIPLIER_MAX_LEVEL, "Множитель опыта"),
    }

    def check(reaction, user):
        return user == ctx.author and reaction.message.id == msg.id and str(reaction.emoji) in upgrade_reactions + ["🎬"]

    async def refresh_profile():
        fresh = get_user_data(guild_id, user_id, user_name)
        await msg.edit(embed=create_profile_embed(fresh, fresh.get('animations_enabled', 1)))

    async def notify(title, description, color, seconds=3):
        await ctx.send(embed=discord.Embed(title=title, description=description, color=color), delete_after=seconds)

    while True:
        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)
        except asyncio.TimeoutError:
            try:
                await msg.clear_reactions()
            except Exception:
                pass
            break

        emoji = str(reaction.emoji)
        # Снимаем реакцию сразу, чтобы её можно было нажать снова (в т.ч. после ошибки)
        try:
            await msg.remove_reaction(reaction, user)
        except Exception:
            pass

        if emoji == "🎬":
            current_data = get_user_data(guild_id, user_id, user_name)
            new_status = 0 if current_data.get('animations_enabled', 1) == 1 else 1
            update_user_data(guild_id, user_id, animations_enabled=new_status)
            await refresh_profile()
            await ctx.send(f"Анимации {'включены 🎬' if new_status == 1 else 'выключены 🔇'}!", delete_after=2)
            continue

        upgrade_type = upgrade_map.get(emoji)
        if not upgrade_type:
            continue

        current_data = get_user_data(guild_id, user_id, user_name)
        prestige_level = current_data.get('prestige', 0)

        if upgrade_type == "auto_open":
            current_auto_open = current_data.get('auto_open_cases', 0) or 0
            spent = 0
            if current_auto_open == 1:
                # Выключение — бесплатно, покупка сохраняется
                update_user_data(guild_id, user_id, auto_open_cases=2)
                await refresh_profile()
                await notify("🔄 **АВТО-ОТКРЫТИЕ КЕЙСОВ**", f"{target.mention} выключено ❌\nКейсы будут сохраняться в инвентаре.", 0xffaa00)
                continue
            if current_auto_open == 0:
                if prestige_level < AUTO_OPEN_CASES_REQUIRED_PRESTIGE:
                    await notify("❌ Требуется престиж!", f"Для покупки авто-открытия кейсов требуется {AUTO_OPEN_CASES_REQUIRED_PRESTIGE}+ уровень престижа! У вас {prestige_level} уровень.", 0xff0000)
                    continue
                if current_data['current_number'] < AUTO_OPEN_CASES_PRICE:
                    await notify("❌ Недостаточно кг!", f"Для покупки авто-открытия кейсов нужно **{AUTO_OPEN_CASES_PRICE} кг**, у вас: **{current_data['current_number']} кг**", 0xff0000)
                    continue
                add_user_weight(guild_id, user_id, -AUTO_OPEN_CASES_PRICE)
                spent = AUTO_OPEN_CASES_PRICE
            update_user_data(guild_id, user_id, auto_open_cases=1)
            log_user_action(user_id, user_name, "Авто-открытие кейсов включено", f"потрачено {spent} кг")
            opened_count, total_gain = await open_all_cases(guild_id, user_id, user_name)
            await update_user_nick(guild_id, user_id, user_name)
            await refresh_profile()

            fresh = get_user_data(guild_id, user_id, user_name)
            desc = f"{target.mention} включено ✅!\n\n"
            if spent:
                desc += f"**Потрачено:** {spent} кг\n"
            desc += f"**Текущий вес:** {fresh['current_number']} кг\n\n"
            if opened_count > 0:
                desc += f"📦 **Открыто кейсов:** {opened_count} шт\n💰 **Получено кг:** {total_gain:+d} кг\n"
            desc += "Теперь все новые кейсы будут открываться автоматически!"
            await notify("🔄 **АВТО-ОТКРЫТИЕ КЕЙСОВ**", desc, 0x00ff00, seconds=8)
            continue

        field, max_level, title = upgrade_info[upgrade_type]
        current_level = current_data.get(field, 0) or 0
        if max_level is not None and current_level >= max_level:
            await notify("❌ Максимальный уровень!", f"{title} уже на максимальном ({max_level}) уровне!", 0xff0000, seconds=2)
            continue
        cost = get_upgrade_cost(upgrade_type, current_level)

        if current_data['current_number'] < cost:
            await notify("❌ Недостаточно кг!", f"Для улучшения нужно **{cost} кг**, у вас: **{current_data['current_number']} кг**", 0xff0000, seconds=2)
            continue

        if upgrade_type == "prestige":
            confirm_embed = discord.Embed(
                title="⚠️ **ПРЕСТИЖ** ⚠️",
                description=f"{target.mention}, вы уверены, что хотите получить престиж?\n\n"
                           f"**Что произойдёт:**\n"
                           f"• Вес сбросится до 0\n"
                           f"• Все предметы и кейсы исчезнут\n"
                           f"• Опыт и уровень сохранятся\n"
                           f"• Улучшения (КД, удача, прибавка, авто-жир, шанс джекпота, усиления, множитель опыта, авто-открытие) **СОХРАНЯТСЯ**\n"
                           f"• Престиж увеличится на 1\n\n"
                           f"Напишите `да` в течение 15 секунд для подтверждения.",
                color=0xff5500
            )
            confirm_msg = await ctx.send(embed=confirm_embed)

            def confirm_check(m):
                return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() == "да"

            try:
                await bot.wait_for('message', timeout=15.0, check=confirm_check)
            except asyncio.TimeoutError:
                await confirm_msg.edit(content="❌ Престиж отменён.", embed=None, delete_after=5)
                continue

            # Перепроверяем: за 15 секунд вес мог измениться
            current_data = get_user_data(guild_id, user_id, user_name)
            if current_data['current_number'] < cost or current_data.get('prestige', 0) != current_level:
                await notify("❌ Престиж отменён", "Данные изменились, попробуйте ещё раз.", 0xff0000)
                continue

            new_prestige = current_level + 1
            # Все кейсы обнуляем явно (раньше передавался пустой dict и кейсы оставались)
            empty_cases = {cid: 0 for cid in CASES if cid != "daily"}
            update_user_data(
                guild_id, user_id,
                number=0,
                item_counts='{}',
                cases_dict=empty_cases,
                daily_case_count=0,
                prestige=new_prestige,
                consecutive_plus=0,
                consecutive_minus=0,
                jackpot_pity=0,
                shadow_upgrade_chance=0
            )

            auto_fat_lvl = current_data.get('auto_fat_level', 0)
            if auto_fat_lvl > 0:
                interval = get_auto_fat_interval(auto_fat_lvl)
                if interval:
                    update_user_data(guild_id, user_id, next_auto_fat_time=datetime.now() + timedelta(hours=interval))

            log_user_action(user_id, user_name, "Престиж", f"уровень {new_prestige}")
            await update_user_nick(guild_id, user_id, user_name)
            await refresh_profile()
            await notify("🌟 **ПРЕСТИЖ ПОЛУЧЕН!** 🌟",
                         f"{target.mention} достиг **{new_prestige}** уровня престижа!\n\n"
                         f"Вес сброшен до 0\nВсе предметы и кейсы удалены\nУлучшения **СОХРАНЕНЫ**!\nОпыт и уровень сохранены!",
                         0xffd700, seconds=5)
            continue

        new_level = current_level + 1
        add_user_weight(guild_id, user_id, -cost)
        if upgrade_type == "auto_fat":
            interval = get_auto_fat_interval(new_level)
            next_time = datetime.now() + timedelta(hours=interval) if interval else None
            update_user_data(guild_id, user_id, auto_fat_level=new_level, next_auto_fat_time=next_time)
        else:
            update_user_data(guild_id, user_id, **{field: new_level})
        log_user_action(user_id, user_name, f"Улучшение {upgrade_type}", f"ур.{new_level}, -{cost} кг")

        await update_user_nick(guild_id, user_id, user_name)
        await refresh_profile()
        new_number = get_user_data(guild_id, user_id, user_name)['current_number']

        if upgrade_type == "fat_cd":
            bonus_text = f"КД !жир уменьшен на {int(get_fat_cd_reduction(new_level))} мин"
        elif upgrade_type == "case_cd":
            bonus_text = f"КД кейса уменьшен на {int(get_case_cd_reduction(new_level))} мин"
        elif upgrade_type == "luck":
            bonus_text = f"Удача увеличена до +{new_level * LUCK_CASE_BONUS_PER_LEVEL:.2f}% к редким и +{new_level * LUCK_UPGRADE_BONUS_PER_LEVEL:.2f}% к апгрейдам"
        elif upgrade_type == "income":
            bonus_text = f"Прибавка увеличена до +{(get_income_bonus(new_level)-1)*100:.0f}% к доходу"
        elif upgrade_type == "auto_fat":
            bonus_text = f"Авто-жир будет срабатывать каждые {get_auto_fat_interval(new_level)} час(ов)"
        elif upgrade_type == "jackpot":
            bonus_text = f"Шанс джекпота увеличен на +{get_jackpot_bonus(new_level) * 100:.2f}%"
        elif upgrade_type == "fat_plus":
            bonus_text = f"Плюс от !жир увеличен на +{(get_fat_plus_bonus(new_level) - 1) * 100:.0f}%"
        elif upgrade_type == "case_plus":
            bonus_text = f"Призы из кейсов увеличены на +{(get_case_plus_bonus(new_level) - 1) * 100:.0f}%"
        else:
            bonus_text = f"Множитель опыта увеличен до +{(get_xp_multiplier_bonus(new_level) - 1) * 100:.0f}%"

        await notify("✅ **УЛУЧШЕНИЕ ПОЛУЧЕНО!** ✅",
                     f"{target.mention} улучшил характеристику!\n\n**Потрачено:** {cost} кг\n**Осталось:** {new_number} кг\n\n**{bonus_text}**",
                     0x00ff00, seconds=3)

@bot.command(name='дуэль')
async def duel_command(ctx, opponent: discord.Member, amount: str = None):
    guild_id = ctx.guild.id
    challenger = ctx.author
    if challenger.id == opponent.id:
        await ctx.send("❌ Нельзя вызвать на дуэль самого себя!")
        return
    if opponent.bot:
        await ctx.send("❌ Нельзя вызвать на дуэль бота!")
        return
    challenger_data = get_user_data(guild_id, str(challenger.id), challenger.name)
    opponent_data = get_user_data(guild_id, str(opponent.id), opponent.name)
    if not can_duel(challenger_data):
        await ctx.send(f"❌ {challenger.mention}, вы уже участвуете в дуэли!")
        return
    if not can_duel(opponent_data):
        await ctx.send(f"❌ {opponent.mention} уже участвует в дуэли!")
        return
    challenger_anim = are_animations_enabled(challenger_data)
    opponent_anim = are_animations_enabled(opponent_data)
    both_animations_on = challenger_anim and opponent_anim
    if amount is None or amount.lower() in ("все", "всё"):
        duel_amount = min(challenger_data['current_number'], opponent_data['current_number'])
        if duel_amount <= 0:
            await ctx.send("❌ У одного из участников нет кг для дуэли!")
            return
        amount_text = f"**всё ({duel_amount}кг)**"
    else:
        try:
            duel_amount = int(amount)
            if duel_amount <= 0:
                await ctx.send("❌ Сумма дуэли должна быть положительным числом!")
                return
            amount_text = f"**{duel_amount}кг**"
        except ValueError:
            await ctx.send("❌ Укажите корректное число кг или 'все'!")
            return
    if challenger_data['current_number'] < duel_amount:
        await ctx.send(f"❌ У вас недостаточно кг! Есть: {challenger_data['current_number']}кг, нужно: {duel_amount}кг")
        return
    if opponent_data['current_number'] < duel_amount:
        await ctx.send(f"❌ У {opponent.mention} недостаточно кг! У него: {opponent_data['current_number']}кг, нужно: {duel_amount}кг")
        return
    embed = discord.Embed(title="🔫 **ВЫЗОВ НА ДУЭЛЬ!** 🔫", description=f"{challenger.mention} вызывает {opponent.mention} на дуэль!\n\n**Ставка:** {amount_text}\n\nОба должны нажать ✅ чтобы принять\nЕсли кто-то нажмёт ❌ - дуэль отменяется", color=0xff5500)
    embed.set_footer(text="У вас 30 секунд чтобы принять решение!")
    duel_msg = await ctx.send(embed=embed)
    await duel_msg.add_reaction("✅")
    await duel_msg.add_reaction("❌")
    current_time = datetime.now()
    update_user_data(guild_id, str(challenger.id), duel_active=1, duel_opponent=str(opponent.id), duel_amount=duel_amount, duel_message_id=str(duel_msg.id), duel_channel_id=str(ctx.channel.id), duel_initiator=1, duel_start_time=current_time)
    update_user_data(guild_id, str(opponent.id), duel_active=1, duel_opponent=str(challenger.id), duel_amount=duel_amount, duel_message_id=str(duel_msg.id), duel_channel_id=str(ctx.channel.id), duel_initiator=0, duel_start_time=current_time)
    accepted_users = set()
    duel_cancelled = False
    def check(reaction, user):
        nonlocal duel_cancelled
        if user.id not in [challenger.id, opponent.id]:
            return False
        if reaction.message.id != duel_msg.id:
            return False
        if str(reaction.emoji) == "❌":
            duel_cancelled = True
            return True
        return str(reaction.emoji) == "✅"
    try:
        while len(accepted_users) < 2 and not duel_cancelled:
            reaction, user = await bot.wait_for('reaction_add', timeout=30.0, check=check)
            if duel_cancelled:
                try:
                    await duel_msg.clear_reactions()
                except Exception:
                    pass
                update_user_data(guild_id, str(challenger.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
                update_user_data(guild_id, str(opponent.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
                decline_embed = discord.Embed(title="❌ Дуэль отклонена", description=f"{user.mention} отказался от дуэли!", color=0xff0000)
                await duel_msg.edit(embed=decline_embed)
                return
            if str(reaction.emoji) == "✅" and user.id not in accepted_users:
                accepted_users.add(user.id)
        if duel_cancelled:
            return
        try:
            await duel_msg.clear_reactions()
        except Exception:
            pass
        # Пока ждали подтверждения, вес мог измениться — перечитываем и перепроверяем
        challenger_data = get_user_data(guild_id, str(challenger.id), challenger.name)
        opponent_data = get_user_data(guild_id, str(opponent.id), opponent.name)
        if challenger_data['current_number'] < duel_amount or opponent_data['current_number'] < duel_amount:
            update_user_data(guild_id, str(challenger.id), **DUEL_RESET)
            update_user_data(guild_id, str(opponent.id), **DUEL_RESET)
            await duel_msg.edit(embed=discord.Embed(title="❌ Дуэль отменена", description="У одного из участников уже не хватает кг для ставки!", color=0xff0000))
            return
        result = random.randint(0, 2)
        if both_animations_on and ANIMATIONS_AVAILABLE:
            await duel_animation(duel_msg, challenger, opponent, result)
        else:
            c_name = challenger.display_name[:15] + "..." if len(challenger.display_name) > 15 else challenger.display_name
            o_name = opponent.display_name[:15] + "..." if len(opponent.display_name) > 15 else opponent.display_name
            if result == 0:
                result_text = f"🏆 **Победитель:** {challenger.mention}"
                result_color = 0xffd700
            elif result == 1:
                result_text = f"🏆 **Победитель:** {opponent.mention}"
                result_color = 0xc0c0c0
            else:
                result_text = "🤝 **НИЧЬЯ!** 🤝"
                result_color = 0x9b59b6
            result_embed = discord.Embed(title="⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️", description=f"**{c_name}** ⚔️ **{o_name}**\n\n{result_text}", color=result_color)
            await duel_msg.edit(embed=result_embed)
            await asyncio.sleep(1.5)
        if result == 0:
            winner, loser = challenger, opponent
            winner_new_weight = challenger_data['current_number'] + duel_amount
            loser_new_weight = opponent_data['current_number'] - duel_amount
            add_user_weight(guild_id, str(winner.id), duel_amount)
            add_user_weight(guild_id, str(loser.id), -duel_amount)
            levels_gained, kg_reward, new_level = add_xp(guild_id, str(winner.id), XP_PER_DUEL_WIN)
            result_description = f"**Победитель:** {winner.mention}\n\n📊 **Результаты:**\n{winner.mention}: {challenger_data['current_number']}кг → **{winner_new_weight}кг** (+{duel_amount})\n{loser.mention}: {opponent_data['current_number']}кг → **{loser_new_weight}кг** (-{duel_amount})"
            if levels_gained > 0:
                result_description += f"\n\n⭐ +{kg_reward} кг за повышение уровня! Теперь у вас **{new_level}** уровень!"
        elif result == 1:
            winner, loser = opponent, challenger
            winner_new_weight = opponent_data['current_number'] + duel_amount
            loser_new_weight = challenger_data['current_number'] - duel_amount
            add_user_weight(guild_id, str(winner.id), duel_amount)
            add_user_weight(guild_id, str(loser.id), -duel_amount)
            levels_gained, kg_reward, new_level = add_xp(guild_id, str(winner.id), XP_PER_DUEL_WIN)
            result_description = f"**Победитель:** {winner.mention}\n\n📊 **Результаты:**\n{winner.mention}: {opponent_data['current_number']}кг → **{winner_new_weight}кг** (+{duel_amount})\n{loser.mention}: {challenger_data['current_number']}кг → **{loser_new_weight}кг** (-{duel_amount})"
            if levels_gained > 0:
                result_description += f"\n\n⭐ +{kg_reward} кг за повышение уровня! Теперь у вас **{new_level}** уровень!"
        else:
            result_description = f"🤝 **НИЧЬЯ!** 🤝\n\n📊 **Результаты:**\n{challenger.mention}: {challenger_data['current_number']}кг → **{challenger_data['current_number']}кг** (без изменений)\n{opponent.mention}: {opponent_data['current_number']}кг → **{opponent_data['current_number']}кг** (без изменений)"
        update_user_data(guild_id, str(challenger.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        update_user_data(guild_id, str(opponent.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        await update_user_nick(guild_id, str(winner.id) if result != 2 else str(challenger.id))
        await update_user_nick(guild_id, str(loser.id) if result != 2 else str(opponent.id))
        result_embed = discord.Embed(title="⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️", description=result_description, color=0xffd700 if result == 0 else (0xc0c0c0 if result == 1 else 0x9b59b6))
        result_embed.set_footer(text="⚔️ Следующая дуэль доступна!")
        await ctx.send(embed=result_embed)
    except asyncio.TimeoutError:
        try:
            await duel_msg.clear_reactions()
        except Exception:
            pass
        update_user_data(guild_id, str(challenger.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        update_user_data(guild_id, str(opponent.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        timeout_embed = discord.Embed(title="⏰ Время вышло", description=f"Не все успели принять дуэль. Дуэль отменена.", color=0xffaa00)
        await duel_msg.edit(embed=timeout_embed)

@bot.command(name='отмена')
async def cancel_duel(ctx):
    # В !жирхелп команда описана как общая, поэтому роль тестера не требуется
    data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    duel_info = get_duel_info(data)
    if not duel_info['active']:
        await ctx.send("❌ Вы не участвуете в дуэли!")
        return
    update_user_data(ctx.guild.id, str(ctx.author.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
    update_user_data(ctx.guild.id, duel_info['opponent'], duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
    try:
        if duel_info['message_id'] and duel_info['channel_id']:
            channel = bot.get_channel(int(duel_info['channel_id']))
            if channel:
                msg = await channel.fetch_message(int(duel_info['message_id']))
                await msg.delete()
    except:
        pass
    await ctx.send(f"✅ Дуэль отменена!")

def get_active_upgrade(data):
    # Состояние апгрейда хранится в upgrade_data (а не в last_command, который
    # перезаписывает !магазин). Поддерживаем и старый формат записи.
    if data.get('upgrade_active', 0) != 1:
        return None
    try:
        info = json.loads(data.get('upgrade_data') or '{}')
    except (ValueError, TypeError):
        info = {}
    if 'type' not in info:
        if 'amount' in info:
            info['type'] = 'kg'
        elif 'source_item' in info:
            info['type'] = 'item'
        elif data.get('last_command') == 'upgrade_kg_select':
            info = {'type': 'kg', 'amount': int(data.get('last_command_target') or 0)}
        elif data.get('last_command') == 'upgrade_select':
            info = {'type': 'item', 'source_item': data.get('last_command_target')}
        else:
            info['type'] = None
    if 'time' not in info:
        info['time'] = data.get('last_command_use_time')
    return info

def refund_upgrade(guild_id, user_id, data):
    # Возвращает ресурсы активного апгрейда. Возвращает текст для пользователя или None.
    info = get_active_upgrade(data)
    if info is None:
        return None
    text = "🔧 Апгрейд (отменён)"
    if info['type'] == 'item' and info.get('source_item'):
        items_dict = get_user_items(data['item_counts'])
        items_dict[info['source_item']] = items_dict.get(info['source_item'], 0) + 1
        update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict), **UPGRADE_RESET)
        return f"🔧 Апгрейд предмета ({info['source_item']} возвращён)"
    if info['type'] == 'kg' and info.get('amount'):
        add_user_weight(guild_id, user_id, int(info['amount']))
        update_user_data(guild_id, user_id, **UPGRADE_RESET)
        return f"💱 Апгрейд кг ({info['amount']} кг возвращены)"
    update_user_data(guild_id, user_id, **UPGRADE_RESET)
    return text

def upgrade_expired(info):
    started = to_datetime(info.get('time'))
    return started is None or (datetime.now() - started).total_seconds() > UPGRADE_TIMEOUT_SECONDS

def get_kg_upgrade_targets(amount):
    all_items = set([item["name"] for item in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
    possible_upgrades = []
    for item_name in all_items:
        target_price = get_item_price(item_name)
        if target_price == 0 or target_price < amount:
            continue
        chance = amount / target_price
        if chance < 0.01:
            continue
        possible_upgrades.append({"name": item_name, "price": target_price, "chance": chance, "emoji": ITEM_EMOJIS.get(item_name, "🎁"), "is_case": False, "case_id": None})
    # Сортировка с именем, чтобы порядок номеров был всегда одинаковым
    possible_upgrades.sort(key=lambda x: (x["price"], x["name"]))
    return possible_upgrades

async def check_pending_upgrade(ctx, data):
    # True — можно начинать новый апгрейд. Просроченный апгрейд автоматически возвращается.
    info = get_active_upgrade(data)
    if info is None:
        return True
    if upgrade_expired(info):
        refund_text = refund_upgrade(ctx.guild.id, str(ctx.author.id), data)
        await ctx.send(f"⏰ Прошлый апгрейд истёк, ресурсы возвращены: {refund_text}")
        return True
    await ctx.send("⚠️ У вас уже есть активный апгрейд! Выберите цель через `!выбрать [номер]` или отмените через `!отменавсё`.")
    return False

@bot.command(name='апгрейд')
async def upgrade_command(ctx, choice: str = None):
    guild_id = ctx.guild.id
    member = ctx.author
    data = get_user_data(guild_id, str(member.id), member.name)
    if not await check_pending_upgrade(ctx, data):
        return
    data = get_user_data(guild_id, str(member.id), member.name)
    items_dict = get_user_items(data['item_counts'])
    available_items = [{"name": item_name, "count": count, "price": get_item_price(item_name), "emoji": ITEM_EMOJIS.get(item_name, "📦")} for item_name, count in items_dict.items() if count > 0 and get_possible_upgrades(item_name, count)]
    available_items.sort(key=lambda x: (x["price"], x["name"]))
    if not available_items:
        await ctx.send("❌ У вас нет предметов для улучшения!")
        return
    if choice is None:
        embed = discord.Embed(title="🔧 **АПГРЕЙД ПРЕДМЕТОВ** 🔧", description=f"{member.mention}, выберите предмет для улучшения:\n\nИспользуйте `!апгрейд [номер]`", color=0x3498db)
        max_display = 15
        display_items = available_items[:max_display]
        remaining_count = len(available_items) - max_display
        items_text = "\n".join([f"**{i+1}.** {item['emoji']} **{item['name']}** — {item['count']} шт — {item['price']} кг" for i, item in enumerate(display_items)])
        if remaining_count > 0:
            items_text += f"\n\n... и ещё {remaining_count} предметов"
        embed.add_field(name="📦 Ваши предметы", value=items_text, inline=False)
        embed.set_footer(text="Всего предметов: {}".format(len(available_items)))
        await ctx.send(embed=embed)
        return
    try:
        item_index = int(choice) - 1
    except ValueError:
        await ctx.send("❌ Введите корректный номер!")
        return
    if item_index < 0 or item_index >= len(available_items):
        await ctx.send(f"❌ Неверный номер! Введите число от 1 до {len(available_items)}")
        return
    selected_item = available_items[item_index]
    possible_upgrades = get_possible_upgrades(selected_item["name"], 1)
    items_dict[selected_item["name"]] -= 1
    now = datetime.now()
    update_user_data(guild_id, str(member.id), item_counts=save_user_items(items_dict), last_command="upgrade_select", last_command_target=selected_item["name"], last_command_use_time=now, upgrade_active=1, upgrade_data=json.dumps({'type': 'item', 'source_item': selected_item["name"], 'time': now.isoformat()}, ensure_ascii=False))
    log_user_action(member.id, member.name, "Апгрейд: выбран предмет", selected_item["name"])
    max_display = 15
    display_upgrades = possible_upgrades[:max_display]
    remaining_count = len(possible_upgrades) - max_display
    embed = discord.Embed(title="🔧 **ВЫБОР ЦЕЛИ АПГРЕЙДА** 🔧", description=f"{member.mention}, вы выбрали: **{selected_item['emoji']} {selected_item['name']}**\n\nТеперь выберите цель (используйте `!выбрать [номер]`) в течение 5 минут.\nОтменить: `!отменавсё`", color=0x3498db)
    upgrades_text = "\n".join([f"**{i+1}.** {upgrade['emoji']} **{upgrade['name']}** — {upgrade['chance']*100:.1f}% шанс" for i, upgrade in enumerate(display_upgrades)])
    if remaining_count > 0:
        upgrades_text += f"\n\n... и ещё {remaining_count} предметов"
    embed.add_field(name="📈 Возможные улучшения", value=upgrades_text, inline=False)
    embed.set_footer(text="Всего доступно: {} улучшений".format(len(possible_upgrades)))
    await ctx.send(embed=embed)

@bot.command(name='апгрейдкг')
async def upgrade_kg_command(ctx, amount: int):
    if amount <= 0:
        await ctx.send("❌ Количество кг должно быть больше 0!")
        return
    guild_id = ctx.guild.id
    member = ctx.author
    data = get_user_data(guild_id, str(member.id), member.name)
    if not await check_pending_upgrade(ctx, data):
        return
    data = get_user_data(guild_id, str(member.id), member.name)
    if data['current_number'] < amount:
        await ctx.send(f"❌ У вас недостаточно кг! Есть: {data['current_number']} кг, нужно: {amount} кг")
        return
    possible_upgrades = get_kg_upgrade_targets(amount)
    if not possible_upgrades:
        # Проверяем ДО списания, чтобы не гонять кг туда-обратно
        await ctx.send(f"❌ На {amount} кг нет доступных улучшений!")
        return
    now = datetime.now()
    add_user_weight(guild_id, str(member.id), -amount)
    update_user_data(guild_id, str(member.id), last_command="upgrade_kg_select", last_command_target=str(amount), last_command_use_time=now, upgrade_active=1, upgrade_data=json.dumps({'type': 'kg', 'amount': amount, 'time': now.isoformat()}))
    log_user_action(member.id, member.name, "Апгрейд кг: списано", f"{amount} кг")
    await update_user_nick(guild_id, str(member.id), member.name)
    max_display = 15
    display_upgrades = possible_upgrades[:max_display]
    remaining_count = len(possible_upgrades) - max_display
    embed = discord.Embed(title="💱 **АПГРЕЙД КГ В ПРЕДМЕТЫ** 💱", description=f"{member.mention}, вы потратили **{amount} кг**!\n\nВыберите цель (используйте `!выбрать [номер]`) в течение 5 минут.\nОтменить и вернуть кг: `!отменавсё`", color=0xffaa00)
    upgrades_text = "\n".join([f"**{i+1}.** {upgrade['emoji']} **{upgrade['name']}** — {upgrade['chance']*100:.1f}% шанс (нужно: {upgrade['price']} кг)" for i, upgrade in enumerate(display_upgrades)])
    if remaining_count > 0:
        upgrades_text += f"\n\n... и ещё {remaining_count} предметов"
    embed.add_field(name="📈 Возможные улучшения", value=upgrades_text, inline=False)
    embed.set_footer(text="Кг уже списаны! Всего доступно: {} предметов".format(len(possible_upgrades)))
    await ctx.send(embed=embed)

@bot.command(name='выбрать')
async def choose_upgrade(ctx, choice: str = None):
    if not choice:
        await ctx.send("❌ Укажите номер!")
        return
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    data = get_user_data(guild_id, user_id, member.name)
    info = get_active_upgrade(data)
    if info is None:
        await ctx.send("❌ У вас нет активного апгрейда! Сначала используйте `!апгрейд` или `!апгрейдкг`.")
        return
    if upgrade_expired(info):
        # Раньше ресурсы при этом просто сгорали
        refund_text = refund_upgrade(guild_id, user_id, data)
        await ctx.send(f"❌ Время ожидания истекло. Ресурсы возвращены: {refund_text}. Используйте команду заново!")
        return
    animations_enabled = are_animations_enabled(data)
    prestige_luck = get_prestige_luck(data.get('prestige', 0))
    luck_upgrade = data.get('luck_upgrade', 0)

    if info['type'] == 'kg':
        amount = int(info.get('amount') or 0)
        possible_upgrades = get_kg_upgrade_targets(amount)
    elif info['type'] == 'item' and info.get('source_item'):
        possible_upgrades = get_possible_upgrades(info['source_item'], 1)
    else:
        refund_text = refund_upgrade(guild_id, user_id, data)
        await ctx.send(f"❌ Неизвестный тип апгрейда, апгрейд отменён: {refund_text}")
        return
    if not possible_upgrades:
        refund_text = refund_upgrade(guild_id, user_id, data)
        await ctx.send(f"❌ Доступных улучшений больше нет. {refund_text}")
        return
    try:
        item_index = int(choice) - 1
    except ValueError:
        await ctx.send("❌ Введите корректный номер! Апгрейд всё ещё активен.")
        return
    if item_index < 0 or item_index >= len(possible_upgrades):
        # Апгрейд остаётся активным — можно выбрать ещё раз, ничего не теряется
        await ctx.send(f"❌ Неверный номер! Введите число от 1 до {len(possible_upgrades)}")
        return
    target_item = possible_upgrades[item_index]
    # Снимаем флаг до анимации, чтобы нельзя было запустить апгрейд дважды
    update_user_data(guild_id, user_id, **UPGRADE_RESET)
    log_user_action(member.id, member.name, "Апгрейд: выбрана цель", target_item['name'])
    if info['type'] == 'kg':
        await upgrade_kg_animation(ctx, member, amount, target_item, prestige_luck, luck_upgrade, animations_enabled)
    else:
        await upgrade_animation(ctx, member, info['source_item'], target_item, 1, prestige_luck, luck_upgrade, animations_enabled)

SHOP_REACTIONS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
SHOP_SESSION_SECONDS = 180

def build_shop_embed(slots, last_update, next_update):
    embed = discord.Embed(
        title="🏪 **МАГАЗИН** 🏪",
        description="Нажмите на **цифру слота** под сообщением, чтобы купить 1 шт.\n"
                    "Купить сразу много: `!купить [слот] [количество]`\n"
                    "Покупать может любой участник сервера.",
        color=0xffaa00)
    # Кейсы и предметы — в разных полях, чтобы не упираться в лимит 1024 символа
    cases_text = ""
    items_text = ""
    for i in range(1, SHOP_SLOTS + 1):
        slot = slots[i-1] if i-1 < len(slots) else None
        num = SHOP_REACTIONS[i-1]
        if slot and isinstance(slot, dict):
            if slot.get('amount', 0) > 0:
                line = f"{num} {slot.get('emoji', '📦')} **{slot.get('name', 'Неизвестно')}** — {slot.get('amount', 0)} шт — **{slot.get('price', 0)} кг/шт**\n   └ {slot.get('description', 'Нет описания')}\n"
            else:
                line = f"{num} ~~{slot.get('name', 'Неизвестно')}~~ — распродано\n"
        else:
            line = f"{num} 🕳️ Пусто\n"
        if i <= 4:
            cases_text += line
        else:
            items_text += line
    if len(items_text) > 1024:
        items_text = items_text[:1000].rsplit("\n", 1)[0] + "\n... и ещё товары"
    embed.add_field(name="📦 Кейсы", value=cases_text or "Пусто", inline=False)
    embed.add_field(name="🛒 Предметы", value=items_text or "Пусто", inline=False)
    last_update_str = last_update.strftime("%d.%m.%Y %H:%M") if last_update else "Никогда"
    next_update_str = next_update.strftime("%d.%m.%Y %H:%M") if next_update else "Скоро"
    embed.add_field(name="⏰ Обновление", value=f"Последнее: {last_update_str}\nСледующее: {next_update_str} (каждые {SHOP_UPDATE_HOURS} ч)", inline=False)
    embed.set_footer(text="💸 Тратьте кг с умом!")
    return embed

def shop_slot_names(slots):
    return [s.get("name") if s else None for s in slots]

async def purchase_from_shop(guild_id, member, slot, amount, expected_name=None):
    """Покупка из слота магазина. Возвращает (успех, embed)."""
    def error(text):
        return False, discord.Embed(title="❌ Покупка не удалась", description=f"{member.mention}, {text}", color=0xff0000)
    if slot < 1 or slot > SHOP_SLOTS:
        return error(f"слот должен быть от 1 до {SHOP_SLOTS}!")
    if amount <= 0:
        return error("количество должно быть больше 0!")
    slots, last_update, next_update = await ensure_shop_updated(guild_id)
    item = slots[slot - 1] if slot - 1 < len(slots) else None
    if expected_name is not None and (not item or item.get("name") != expected_name):
        return error("магазин обновился, посмотрите новый ассортимент!")
    if not item or item.get("amount", 0) <= 0:
        return error(f"в слоте {slot} ничего нет!")
    if amount > item["amount"]:
        return error(f"в наличии только {item['amount']} шт!")
    user_id = str(member.id)
    data = get_user_data(guild_id, user_id, member.name)
    total_price = item["price"] * amount
    if data['current_number'] < total_price:
        return error(f"недостаточно кг! Нужно: {total_price} кг, у вас: {data['current_number']} кг")
    if item.get("type") == "case" or "case_id" in item:
        case_id = item.get("case_id")
        if not case_id or case_id not in CASES:
            return error("неизвестный тип кейса!")
        cases_dict = data.get('cases_dict', {}).copy()
        cases_dict[case_id] = cases_dict.get(case_id, 0) + amount
        update_user_data(guild_id, user_id, cases_dict=cases_dict)
        purchase_desc = f"{item.get('emoji', '📦')} {item.get('name', 'Кейс')} x{amount}"
    else:
        items_dict = get_user_items(data['item_counts'])
        items_dict[item["name"]] = items_dict.get(item["name"], 0) + amount
        update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict))
        purchase_desc = f"{item.get('emoji', '📦')} {item['name']} x{amount}"
    add_user_weight(guild_id, user_id, -total_price)
    item["amount"] -= amount
    update_shop_data(guild_id, slots, last_update, next_update)
    log_user_action(member.id, member.name, "Покупка", f"{item['name']} x{amount} за {total_price} кг")
    levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_SHOP_BUY)
    await update_user_nick(guild_id, user_id, member.name)
    new_number = get_user_data(guild_id, user_id)['current_number']
    embed = discord.Embed(title="✅ Покупка совершена!", description=f"**{member.mention}** приобрёл товары!", color=0x00ff00)
    embed.add_field(name="📦 Предмет", value=purchase_desc, inline=True)
    embed.add_field(name="💰 Цена", value=f"{total_price} кг", inline=True)
    embed.add_field(name="💸 Осталось", value=f"{new_number} кг", inline=True)
    if levels_gained > 0:
        embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", inline=False)
    return True, embed

@bot.command(name='магазин')
async def shop_command(ctx):
    guild_id = ctx.guild.id
    slots, last_update, next_update = await ensure_shop_updated(guild_id)
    slots = slots if isinstance(slots, list) else []
    msg = await ctx.send(embed=build_shop_embed(slots, last_update, next_update))
    shown_names = shop_slot_names(slots)
    for i, emoji in enumerate(SHOP_REACTIONS):
        if i < len(slots) and slots[i] and slots[i].get("amount", 0) > 0:
            await msg.add_reaction(emoji)

    def check(reaction, user):
        return reaction.message.id == msg.id and not user.bot and str(reaction.emoji) in SHOP_REACTIONS

    session_end = asyncio.get_running_loop().time() + SHOP_SESSION_SECONDS
    while True:
        timeout = session_end - asyncio.get_running_loop().time()
        if timeout <= 0:
            break
        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=timeout, check=check)
        except asyncio.TimeoutError:
            break
        try:
            await msg.remove_reaction(reaction, user)
        except Exception:
            pass
        slot = SHOP_REACTIONS.index(str(reaction.emoji)) + 1
        member = ctx.guild.get_member(user.id) or user
        ok, result_embed = await purchase_from_shop(guild_id, member, slot, 1, expected_name=shown_names[slot - 1] if slot - 1 < len(shown_names) else None)
        await ctx.send(embed=result_embed, delete_after=15)
        # Обновляем витрину (остатки или новый ассортимент)
        slots, last_update, next_update = await ensure_shop_updated(guild_id)
        shown_names = shop_slot_names(slots)
        try:
            await msg.edit(embed=build_shop_embed(slots, last_update, next_update))
        except Exception:
            pass
    try:
        await msg.clear_reactions()
    except Exception:
        pass
    try:
        expired = build_shop_embed(slots, last_update, next_update)
        expired.set_footer(text="⌛ Реакции больше не работают — откройте !магазин заново")
        await msg.edit(embed=expired)
    except Exception:
        pass

@bot.command(name='купить')
async def buy_command(ctx, slot: int, amount: int = 1):
    ok, result_embed = await purchase_from_shop(ctx.guild.id, ctx.author, slot, amount)
    await ctx.send(embed=result_embed)

@bot.command(name='продать')
async def sell_command(ctx, *, args: str = None):
    if not args:
        embed = discord.Embed(title="❌ Использование", description="`!продать [название предмета] [количество]`\n`!продать всё` - продать всё сразу\n\n💰 **Цена продажи: 70% от стоимости предмета**", color=0xff0000)
        await ctx.send(embed=embed)
        return
    data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    items_dict = get_user_items(data['item_counts'])
    if args.lower() in ["всё", "все"]:
        if not items_dict:
            await ctx.send("📭 У вас нет предметов для продажи!")
            return
        total_gain = 0
        sold_items = []
        for item_name, count in list(items_dict.items()):
            price = get_item_price(item_name)
            if price > 0:
                sell_price = int(price * 0.7)
                item_gain = sell_price * count
                total_gain += item_gain
                sold_items.append(f"{item_name} x{count} — {item_gain} кг")
                del items_dict[item_name]
        if total_gain == 0:
            await ctx.send("❌ Ни один из ваших предметов нельзя продать!")
            return
        new_number = data['current_number'] + total_gain
        update_user_data(ctx.guild.id, str(ctx.author.id), number=new_number, item_counts=save_user_items(items_dict))
        await update_user_nick(ctx.guild.id, str(ctx.author.id), ctx.author.name)
        embed = discord.Embed(title="💰 Продажа всех предметов", description=f"**{ctx.author.mention}** продал всё!", color=0x00ff00)
        sold_text = "\n".join(sold_items[:10])
        if len(sold_items) > 10:
            sold_text += f"\n... и ещё {len(sold_items) - 10} предметов"
        embed.add_field(name="📦 Продано", value=sold_text, inline=False)
        embed.add_field(name="💸 Получено", value=f"{total_gain} кг", inline=True)
        embed.add_field(name="🍖 Новый вес", value=f"{new_number}kg", inline=True)
        await ctx.send(embed=embed)
        return
    parts = args.split()
    if len(parts) == 1:
        item_name = parts[0]
        amount = 1
    else:
        try:
            amount = int(parts[-1])
            item_name = ' '.join(parts[:-1])
        except ValueError:
            item_name = args
            amount = 1
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    found_item = None
    for key in items_dict.keys():
        if key.lower() == item_name.lower():
            found_item = key
            break
    if not found_item:
        for key in items_dict.keys():
            if item_name.lower() in key.lower():
                found_item = key
                break
    if not found_item:
        if items_dict:
            items_list = "\n".join([f"• {item}: {count} шт" for item, count in list(items_dict.items())[:10]])
            await ctx.send(f"❌ У вас нет предмета '{item_name}'!\n\n📦 **Ваши предметы:**\n{items_list}")
        else:
            await ctx.send("❌ У вас нет предметов в инвентаре!")
        return
    if items_dict[found_item] < amount:
        await ctx.send(f"❌ У вас недостаточно '{found_item}'! Есть: {items_dict[found_item]}, нужно: {amount}")
        return
    price = get_item_price(found_item)
    if price == 0:
        await ctx.send(f"❌ Предмет '{found_item}' нельзя продать (нет цены)!")
        return
    sell_price = int(price * 0.7)
    total_gain = sell_price * amount
    items_dict[found_item] -= amount
    if items_dict[found_item] <= 0:
        del items_dict[found_item]
    new_number = data['current_number'] + total_gain
    update_user_data(ctx.guild.id, str(ctx.author.id), number=new_number, item_counts=save_user_items(items_dict))
    await update_user_nick(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    embed = discord.Embed(title="💰 Продажа предмета", description=f"**{ctx.author.mention}** продал предмет!", color=0x00ff00)
    embed.add_field(name="📦 Предмет", value=f"{found_item} x{amount}", inline=True)
    embed.add_field(name="💎 Цена за шт", value=f"{price} кг", inline=True)
    embed.add_field(name="🏷️ Цена продажи", value=f"{sell_price} кг/шт (70%)", inline=True)
    embed.add_field(name="💸 Всего получено", value=f"{total_gain} кг", inline=True)
    embed.add_field(name="🍖 Новый вес", value=f"{new_number}kg", inline=True)
    if found_item in items_dict:
        embed.add_field(name="📦 Осталось", value=f"{items_dict[found_item]} шт", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='датьжир')
async def give_fat(ctx, target: discord.Member, amount: int):
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    if ctx.author.id == target.id:
        await ctx.send("❌ Нельзя передавать кг самому себе!")
        return
    if target.bot:
        await ctx.send("❌ Нельзя передавать кг боту!")
        return
    giver_data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    target_data = get_user_data(ctx.guild.id, str(target.id), target.name)
    if giver_data['current_number'] < amount:
        await ctx.send(f"❌ У вас недостаточно кг! Есть: {giver_data['current_number']} кг, нужно: {amount} кг")
        return
    new_giver_number = giver_data['current_number'] - amount
    new_target_number = target_data['current_number'] + amount
    add_user_weight(ctx.guild.id, str(ctx.author.id), -amount)
    add_user_weight(ctx.guild.id, str(target.id), amount)
    log_user_action(ctx.author.id, ctx.author.name, "Перевод кг", f"{amount} кг -> {target.name}")
    await update_user_nick(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    await update_user_nick(ctx.guild.id, str(target.id), target.name)
    embed = discord.Embed(title="⚖️ Перевод жира", description=f"**{ctx.author.mention}** передал кг **{target.mention}**!", color=0xffaa00)
    giver_rank, giver_rank_emoji = get_rank(new_giver_number)
    target_rank, target_rank_emoji = get_rank(new_target_number)
    embed.add_field(name="📤 Отправитель", value=f"{ctx.author.mention}\nБыло: {giver_data['current_number']}kg\nСтало: {new_giver_number}kg\n{giver_rank_emoji} {giver_rank}", inline=True)
    embed.add_field(name="📥 Получатель", value=f"{target.mention}\nБыло: {target_data['current_number']}kg\nСтало: {new_target_number}kg\n{target_rank_emoji} {target_rank}", inline=True)
    embed.add_field(name="📦 Количество", value=f"{amount} кг", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='датьпредмет')
async def give_item(ctx, target: discord.Member, amount: int, *, item_name: str):
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    if ctx.author.id == target.id:
        await ctx.send("❌ Нельзя передавать предметы самому себе!")
        return
    if target.bot:
        await ctx.send("❌ Нельзя передавать предметы боту!")
        return
    giver_data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    target_data = get_user_data(ctx.guild.id, str(target.id), target.name)
    for case_id, case in CASES.items():
        if case_id != "daily" and case["name"].lower() in item_name.lower():
            if not case["tradable"]:
                await ctx.send(f"❌ Кейс '{case['name']}' нельзя передавать!")
                return
            giver_cases = giver_data.get('cases_dict', {}).copy()
            target_cases = target_data.get('cases_dict', {}).copy()
            if giver_cases.get(case_id, 0) < amount:
                await ctx.send(f"❌ У вас недостаточно кейсов '{case['name']}'! Есть: {giver_cases.get(case_id, 0)}, нужно: {amount}")
                return
            giver_cases[case_id] = giver_cases.get(case_id, 0) - amount
            target_cases[case_id] = target_cases.get(case_id, 0) + amount
            update_user_data(ctx.guild.id, str(ctx.author.id), cases_dict=giver_cases)
            update_user_data(ctx.guild.id, str(target.id), cases_dict=target_cases)
            embed = discord.Embed(title=f"{case['emoji']} Передача кейса", description=f"**{ctx.author.mention}** передал кейс **{target.mention}**!", color=0xffaa00)
            embed.add_field(name="📦 Кейс", value=f"{case['name']} x{amount}", inline=True)
            embed.add_field(name="📤 У вас осталось", value=f"{giver_cases.get(case_id, 0)} шт", inline=True)
            embed.add_field(name="📥 У получателя", value=f"{target_cases.get(case_id, 0)} шт", inline=True)
            await ctx.send(embed=embed)
            return
    giver_items = get_user_items(giver_data['item_counts'])
    target_items = get_user_items(target_data['item_counts'])
    item_name = item_name.strip()
    found_item = None
    for key in giver_items.keys():
        if key.lower() == item_name.lower():
            found_item = key
            break
    if not found_item:
        await ctx.send(f"❌ У вас нет предмета '{item_name}'!")
        return
    if giver_items[found_item] < amount:
        await ctx.send(f"❌ У вас недостаточно '{found_item}'! Есть: {giver_items[found_item]}, нужно: {amount}")
        return
    giver_items[found_item] -= amount
    if giver_items[found_item] <= 0:
        del giver_items[found_item]
    target_items[found_item] = target_items.get(found_item, 0) + amount
    update_user_data(ctx.guild.id, str(ctx.author.id), item_counts=save_user_items(giver_items))
    update_user_data(ctx.guild.id, str(target.id), item_counts=save_user_items(target_items))
    embed = discord.Embed(title="🎁 Передача предмета", description=f"**{ctx.author.mention}** передал предмет **{target.mention}**!", color=0xffaa00)
    embed.add_field(name="📦 Предмет", value=f"**{found_item}** x{amount}", inline=False)
    giver_inv = "\n".join([f"• {item}: {count} шт" for item, count in list(giver_items.items())[:5]])
    if len(giver_items) > 5:
        giver_inv += f"\n... и ещё {len(giver_items) - 5} предметов"
    embed.add_field(name="📤 Ваш инвентарь", value=giver_inv or "Пусто", inline=True)
    target_inv = "\n".join([f"• {item}: {count} шт" for item, count in list(target_items.items())[:5]])
    if len(target_items) > 5:
        target_inv += f"\n... и ещё {len(target_items) - 5} предметов"
    embed.add_field(name="📥 Инвентарь получателя", value=target_inv or "Пусто", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='инвентарь')
async def show_inventory(ctx, member: discord.Member = None):
    target = member or ctx.author
    data = get_user_data(ctx.guild.id, str(target.id), target.name)
    embed = discord.Embed(title=f"🎒 Инвентарь - {target.display_name}", color=0x3498db)
    daily_count = data.get('daily_case_count', 0)
    if daily_count > 0:
        embed.add_field(name="📦 Ежедневные кейсы", value=f"{daily_count} шт", inline=True)
    if data.get('auto_fat_level', 0) > 0:
        interval = get_auto_fat_interval(data['auto_fat_level'])
        embed.add_field(name="🤖 Авто-жир", value=f"{data['auto_fat_level']} уровень (каждые {interval} ч)", inline=True)
    cases_dict = data.get('cases_dict', {})
    cases_text = ""
    for case_id, count in cases_dict.items():
        if count > 0 and case_id in CASES:
            cases_text += f"{CASES[case_id]['emoji']} {CASES[case_id]['name']}: {count}\n"
    if cases_text:
        embed.add_field(name="📦 Кейсы", value=cases_text, inline=False)
    items_dict = get_user_items(data['item_counts'])
    if items_dict:
        items_text = ""
        regular_items = []
        legendary_items = []
        for item_name, count in items_dict.items():
            if item_name in ["Снатчер", "Святой сэндвич", "Гнилая ножка KFC", "Стакан воды", "Автохолестерол", "Холестеринимус", "Яблоко", "Золотое Яблоко", "Апельсин", "Золотой Апельсин", "Драгонфрукт", "Золотой Драгонфрукт"]:
                legendary_items.append(f"• {item_name}: {count} шт")
            else:
                regular_items.append(f"• {item_name}: {count} шт")
        if regular_items:
            items_text += "**Обычные предметы:**\n" + "\n".join(regular_items[:8]) + "\n"
            if len(regular_items) > 8:
                items_text += f"... и ещё {len(regular_items) - 8} предметов\n"
        if legendary_items:
            items_text += "**✨ Легендарные предметы:**\n" + "\n".join(legendary_items)
        embed.add_field(name="📦 Предметы", value=items_text, inline=False)
    embed.set_footer(text="💪 Жир, кейсы и предметы!")
    await ctx.send(embed=embed)

@bot.command(name='жиротрясы')
async def fat_leaderboard(ctx):
    users = get_all_users_sorted(ctx.guild.id)
    if not users:
        await ctx.send(f"📭 На сервере **{ctx.guild.name}** пока никто не участвовал!")
        return
    embed = discord.Embed(title=f"🏆 Таблица жиротрясов - {ctx.guild.name}", description="Рейтинг пользователей по весу", color=0xffaa00)
    leaderboard_text = ""
    for i, user_data in enumerate(users, 1):
        user_name, number, last_update, consecutive_plus, consecutive_minus, jackpot_pity = user_data[:6]
        prestige = user_data[6] if len(user_data) > 6 else 0
        place_icon = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "🔹"
        rank_name, rank_emoji = get_rank(number)
        display_name = f"{prestige}🌟{user_name}" if prestige > 0 else user_name
        pity_emojis = []
        if consecutive_plus > 0:
            pity_emojis.append(f"🔥{consecutive_plus}")
        if consecutive_minus > 0:
            pity_emojis.append(f"❄️{consecutive_minus}")
        if jackpot_pity > 0:
            pity_emojis.append(f"💰{jackpot_pity}")
        pity_str = f" {' '.join(pity_emojis)}" if pity_emojis else ""
        leaderboard_text += f"{place_icon} **{i}.** {display_name} — **{number}kg** {rank_emoji} *{rank_name}*{pity_str}\n"
        if len(leaderboard_text) > 900:
            leaderboard_text += "... и ещё несколько участников"
            break
    embed.description = leaderboard_text
    stats = get_guild_stats(ctx.guild.id)
    embed.add_field(name="📊 Статистика сервера", value=f"Участников: {stats['total_users']}\nСуммарный вес: {stats['total_weight']}kg\nСредний вес: {stats['avg_weight']:.1f}kg\n🔼 Толстых: {stats['positive']} | 🔽 Худых: {stats['negative']} | ⚖️ Нулевых: {stats['zero']}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name='жирхелп')
async def fat_help(ctx):
    embed = discord.Embed(title="🍔 **ЖИРБОТ - ПОМОЩЬ** 🍔", description="Все команды бота", color=0xffaa00)
    embed.add_field(name="🎮 **ОСНОВНЫЕ КОМАНДЫ**", value="`!жир` - изменить свой вес\n`!жиркейс` - открыть кейс\n`!жиротрясы` - таблица рекордов\n`!профиль [@user]` - профиль и прокачка\n`!жир_звания` - список званий\n`!инвентарь [@user]` - посмотреть инвентарь", inline=False)
    embed.add_field(name="⚔️ **ДУЭЛИ**", value="`!дуэль @user [кг/\"все\"]` - вызвать на дуэль\n`!отмена` - отменить текущую дуэль", inline=False)
    embed.add_field(name="🔧 **АПГРЕЙДЫ**", value="`!апгрейд` - улучшить предмет\n`!апгрейдкг [кол-во]` - улучшить кг в предмет\n`!выбрать [номер]` - выбрать цель апгрейда\n`!отменавсё` - отменить все активные действия", inline=False)
    embed.add_field(name="💰 **ЭКОНОМИКА**", value="`!магазин` - магазин (покупка нажатием на цифру слота)\n`!купить [слот] [кол-во]` - купить сразу много\n`!продать [предмет] [кол-во]` - продать предмет\n`!продать всё` - продать всё\n`!датьжир [@user] [кол-во]` - передать кг\n`!датьпредмет [@user] [кол-во] [предмет]` - передать предмет", inline=False)
    embed.add_field(name="⭐ **ХАРАКТЕРИСТИКИ**", value="• **КД !жир** — уменьшает время ожидания\n• **КД кейса** — уменьшает время ожидания кейса\n• **Удача** — повышает шансы в кейсах и апгрейдах\n• **Прибавка** — +5% к доходу от предметов за уровень\n• **Престиж** — +10% ко всем кг, +1% к шансам и +50% к опыту за уровень\n• **Авто-жир** — автоматический !жир\n• **Усиление !жир** — +2% к плюсу за уровень\n• **Усиление кейсов** — +1% к призам за уровень\n• **Шанс джекпота** — +0.05% к шансу за уровень\n• **Множитель опыта** — +1% к опыту за уровень\n• **Авто-открытие кейсов** — автоматически открывает кейсы (покупается один раз: 5+ престиж, 20000 кг; дальше включается/выключается бесплатно)", inline=False)
    embed.add_field(name="🎬 **НАСТРОЙКИ АНИМАЦИЙ**", value="Анимации можно включить/выключить в `!профиль` кнопкой 🎬", inline=False)
    embed.set_footer(text="🔥❄️💰 - следите за показателями!")
    await ctx.send(embed=embed)

@bot.command(name='жир_звания')
async def show_ranks(ctx):
    embed = discord.Embed(title="🎖️ Система званий", description="Чем больше ваш вес, тем выше звание!", color=0xffaa00)
    ranks_text = ""
    for rank in RANKS:
        range_str = f"{rank['min']}" if rank["min"] == rank["max"] else f"{rank['min']} – {rank['max']}"
        ranks_text += f"{rank['emoji']} **{rank['name']}** — {range_str} kg\n"
    embed.add_field(name="Доступные звания", value=ranks_text, inline=False)
    await ctx.send(embed=embed)

@bot.command(name='сброскд')
async def reset_cooldowns(ctx):
    if not has_tester_role(ctx.author) and not has_high_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}** или **{HIGH_TESTER_ROLE_NAME}**")
        return
    db_path = get_db_path(ctx.guild.id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('UPDATE user_fat SET fat_cooldown_time = NULL')
    fat_affected = cursor.rowcount
    cursor.execute('UPDATE user_fat SET last_case_time = NULL')
    case_affected = cursor.rowcount
    cursor.execute('UPDATE user_fat SET daily_case_last_time = NULL')
    daily_affected = cursor.rowcount
    if has_high_tester_role(ctx.author):
        conn.commit()
        conn.close()
        await asyncio.sleep(0.5)
        conn2 = sqlite3.connect(db_path, timeout=10.0)
        cursor2 = conn2.cursor()
        current_time = datetime.now()
        new_slots = generate_shop_items()
        slots_json = json.dumps(new_slots)
        last_update_str = current_time.isoformat()
        next_update_str = (current_time + timedelta(hours=SHOP_UPDATE_HOURS)).isoformat()
        cursor2.execute('''INSERT OR REPLACE INTO shop (guild_id, slots, last_update, next_update) VALUES (?, ?, ?, ?)''', (str(ctx.guild.id), slots_json, last_update_str, next_update_str))
        conn2.commit()
        conn2.close()
        embed = discord.Embed(title="🔄 **ПОЛНЫЙ СБРОС** 🔄", description=f"**{ctx.author.name}** (Высший тестер) выполнил глобальный сброс!", color=0xff5500)
        embed.add_field(name="⏰ Сброс !жир", value=f"Затронуто: {fat_affected} пользователей", inline=True)
        embed.add_field(name="📦 Сброс !жиркейс", value=f"Затронуто: {case_affected} пользователей", inline=True)
        embed.add_field(name="🏪 Магазин", value=f"Принудительно обновлён", inline=True)
    else:
        conn.commit()
        conn.close()
        embed = discord.Embed(title="🔄 Кулдаун сброшен", description=f"**{ctx.author.name}** сбросил кулдауны для всех!", color=0x00ff00)
        embed.add_field(name="⏰ Сброс !жир", value=f"Затронуто: {fat_affected} пользователей", inline=True)
        embed.add_field(name="📦 Сброс !жиркейс", value=f"Затронуто: {case_affected} пользователей", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='сбросвсех')
async def reset_all_users_weight(ctx):
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    await ctx.send(f"⚠️ **Внимание!** Сбросить вес **ВСЕХ** на 0?\nНапишите `да` в течение 30 секунд.")
    try:
        await bot.wait_for('message', timeout=30.0, check=lambda m: m.author == ctx.author and m.content.lower() == "да")
    except asyncio.TimeoutError:
        await ctx.send("❌ Отмена")
        return
    db_path = get_db_path(ctx.guild.id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE user_fat SET current_number = 0, consecutive_plus = 0, consecutive_minus = 0, jackpot_pity = 0, item_counts = '{}'")
    affected = cursor.rowcount
    cursor.execute("SELECT user_id, user_name FROM user_fat")
    db_users = cursor.fetchall()
    conn.commit()
    conn.close()
    log_user_action(ctx.author.id, ctx.author.name, "Глобальный сброс веса", f"затронуто {affected}")
    # Обновляем ники только участникам игры (раньше создавались записи для всех на сервере)
    for uid, uname in db_users:
        if ctx.guild.get_member(int(uid)):
            await update_user_nick(ctx.guild.id, uid, uname)
            await asyncio.sleep(0.5)
    embed = discord.Embed(title="⚖️ Глобальный сброс", description=f"**{ctx.author.name}** обнулил всех!", color=0xff5500)
    embed.add_field(name="Затронуто пользователей", value=str(affected), inline=True)
    await ctx.send(embed=embed)

@bot.command(name='жир_сброс')
async def fat_reset(ctx, member: discord.Member = None):
    if not ctx.author.guild_permissions.administrator and ctx.author != ctx.guild.owner:
        await ctx.send("❌ Эта команда только для администраторов!")
        return
    target = member or ctx.author
    update_user_data(ctx.guild.id, str(target.id), number=0, consecutive_plus=0, consecutive_minus=0, jackpot_pity=0, item_counts='{}')
    await update_user_nick(ctx.guild.id, str(target.id), target.name)
    await ctx.send(f"✅ Вес {target.mention} сброшен на 0kg")

@bot.command(name='выдатьпредмет')
async def give_shop_item(ctx, amount: int, *, item_name: str):
    if not has_high_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{HIGH_TESTER_ROLE_NAME}**")
        return
    if amount <= 0 or amount > 1000:
        await ctx.send("❌ Количество должно быть от 1 до 1000!")
        return
    data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    item_name = item_name.strip()
    for case_id, case in CASES.items():
        if case_id != "daily" and case["name"].lower() == item_name.lower():
            cases_dict = data.get('cases_dict', {}).copy()
            cases_dict[case_id] = cases_dict.get(case_id, 0) + amount
            update_user_data(ctx.guild.id, str(ctx.author.id), cases_dict=cases_dict)
            embed = discord.Embed(title="🎁 Выдача кейса", description=f"**{ctx.author.mention}** выдал себе кейс!", color=0xffaa00)
            embed.add_field(name="📦 Кейс", value=f"**{case['name']}** x{amount}", inline=True)
            await ctx.send(embed=embed)
            return
    found_item = None
    for shop_item in SHOP_ITEMS:
        if shop_item["name"].lower() == item_name.lower():
            found_item = shop_item
            break
    if not found_item:
        await ctx.send(f"❌ Предмет '{item_name}' не найден!")
        return
    items_dict = get_user_items(data['item_counts'])
    items_dict[found_item["name"]] = items_dict.get(found_item["name"], 0) + amount
    update_user_data(ctx.guild.id, str(ctx.author.id), item_counts=save_user_items(items_dict))
    embed = discord.Embed(title="🎁 Выдача предмета", description=f"**{ctx.author.mention}** выдал себе предмет!", color=0xffaa00)
    embed.add_field(name="📦 Предмет", value=f"**{found_item['name']}** x{amount}", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='отменавсё')
async def cancel_all(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    data = get_user_data(guild_id, user_id, member.name)
    cancelled_items = []
    duel_info = get_duel_info(data)
    if duel_info['active']:
        update_user_data(guild_id, user_id, duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        if duel_info['opponent']:
            update_user_data(guild_id, duel_info['opponent'], duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        try:
            if duel_info['message_id'] and duel_info['channel_id']:
                channel = bot.get_channel(int(duel_info['channel_id']))
                if channel:
                    msg = await channel.fetch_message(int(duel_info['message_id']))
                    await msg.delete()
        except:
            pass
        cancelled_items.append("⚔️ Дуэль")
    refund_text = refund_upgrade(guild_id, user_id, data)
    if refund_text:
        cancelled_items.append(refund_text)
        await update_user_nick(guild_id, user_id, member.name)
    if data.get('active_case_message_id'):
        try:
            channel = bot.get_channel(int(data['active_case_channel_id'])) if data.get('active_case_channel_id') else None
            if channel:
                old_msg = await channel.fetch_message(int(data['active_case_message_id']))
                await old_msg.delete()
        except:
            pass
        update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None)
        cancelled_items.append("📦 Открытие кейса")
    if cancelled_items:
        embed = discord.Embed(title="✅ ОТМЕНА", description=f"{member.mention}, отменены следующие действия:", color=0x00ff00)
        embed.add_field(name="Отменено", value="\n".join(cancelled_items), inline=False)
        embed.set_footer(text="Все ресурсы возвращены!")
        await ctx.send(embed=embed)
    else:
        embed = discord.Embed(title="ℹ️ НЕТ АКТИВНЫХ ДЕЙСТВИЙ", description=f"{member.mention}, у вас нет активных действий для отмены.", color=0xffaa00)
        await ctx.send(embed=embed)

@bot.command(name='жирглобал')
async def global_leaderboard(ctx):
    guild_data = []
    for guild in bot.guilds:
        try:
            stats = get_guild_stats(guild.id)
            guild_data.append({'name': guild.name, 'members': stats['total_users'], 'total_weight': stats['total_weight'], 'avg_weight': stats['avg_weight']})
        except Exception as e:
            continue
    if not guild_data:
        await ctx.send("📭 Нет данных по серверам!")
        return
    guild_data.sort(key=lambda x: x['total_weight'], reverse=True)
    embed = discord.Embed(title="🌍 **ГЛОБАЛЬНЫЙ РЕЙТИНГ СЕРВЕРОВ** 🌍", description="Топ серверов по общей массе жира", color=0xffaa00)
    leaderboard_text = ""
    for i, guild in enumerate(guild_data[:10], 1):
        place_icon = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "🔹"
        weight_display = f"{guild['total_weight']/1000:.1f}т" if guild['total_weight'] >= 1000 else f"{guild['total_weight']}кг"
        leaderboard_text += f"{place_icon} **{i}.** {guild['name'][:30]}\n   📦 **{weight_display}** | 👥 {guild['members']} уч.\n   📊 Средний вес: {guild['avg_weight']:.0f}кг\n\n"
        if len(leaderboard_text) > 1900:
            leaderboard_text += "... и ещё несколько серверов"
            break
    embed.description = leaderboard_text
    total_servers = len(guild_data)
    total_global_weight = sum(g['total_weight'] for g in guild_data)
    total_global_members = sum(g['members'] for g in guild_data)
    if total_global_weight >= 1000000:
        global_display = f"{total_global_weight/1000000:.1f}млн кг"
    elif total_global_weight >= 1000:
        global_display = f"{total_global_weight/1000:.1f}т"
    else:
        global_display = f"{total_global_weight}кг"
    embed.add_field(name="📊 **ГЛОБАЛЬНАЯ СТАТИСТИКА**", value=f"🌍 Серверов: **{total_servers}**\n👥 Участников: **{total_global_members}**\n⚖️ Всего массы: **{global_display}**", inline=False)
    embed.set_footer(text="🏆 Топ-10 серверов")
    await ctx.send(embed=embed)

_background_started = False

@bot.check
async def guild_only_check(ctx):
    # Все данные хранятся по серверам, в ЛС команды не работают
    if ctx.guild is None:
        raise commands.NoPrivateMessage()
    return True

@bot.before_invoke
async def log_every_command(ctx):
    log_command(ctx, ctx.command.qualified_name if ctx.command else "?", text=ctx.message.content)

@bot.event
async def on_ready():
    global _background_started
    logger.info(f"Бот успешно запущен как {bot.user}")
    # on_ready вызывается и при каждом переподключении — без этого флага
    # фоновые задачи запускались повторно и доход начислялся несколько раз
    if _background_started:
        return
    _background_started = True
    await migrate_old_burgers_to_prestige()
    await migrate_old_autoburgers_to_auto_fat()
    await clear_stale_states()
    for task in (auto_fat_loop, passive_income_loop, snatcher_loop, apply_hourly_effects,
                 daily_case_accumulation_loop, auto_open_new_cases_loop, backup_loop):
        asyncio.create_task(task())

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.NoPrivateMessage):
        await ctx.send("❌ Команды бота работают только на сервере!")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Не указан обязательный аргумент: {error.param.name}\nСписок команд: `!жирхелп`")
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send("❌ Пользователь не найден!")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("❌ Неверный формат аргумента. Список команд: `!жирхелп`")
    elif isinstance(error, commands.CheckFailure):
        return
    else:
        original = getattr(error, 'original', error)
        logger.error(f"ОШИБКА | команда {ctx.command} | {original}\n" + "".join(traceback.format_exception(type(original), original, original.__traceback__)))
        await ctx.send("❌ Произошла ошибка, она записана в лог.")

if __name__ == "__main__":
    logger.info("Запуск бота...")
    bot.run(TOKEN)
