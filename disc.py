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
import math
import zlib
import base64

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
# Стоимость престижа: 2000, 5000, 10000, 20000, дальше каждый следующий в 2 раза дороже
PRESTIGE_COSTS = [2000, 5000, 10000, 20000]

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

# ============================================
# ========== МОДИФИКАТОРЫ ПРЕДМЕТОВ И ПРАЗДНИЧНЫЕ ИВЕНТЫ ==========
# ============================================
# Модификаторы бывают двух видов:
#   • ивентовые (kind="event") — пока идёт их праздник, сами выпадают на любой полученный
#     предмет (магазин, кейсы, апгрейды, снатчер) и добавляются в пул !мод;
#   • обычные (kind="regular") — только через !мод (выпадают всегда, в праздник — вместе с ивентовыми).
# На одном предмете может быть до MAX_ITEM_MODIFIERS РАЗНЫХ модификаторов (обычные + ивентовые).
# Все модификаторы пишутся перед названием, самый редкий — первым. Цвет предмета — по самому редкому.
#
# Эффекты (модификаторы на одном предмете складываются):
#   production — +доля к производству кг этим предметом (0.5 = +50%)
#   value      — +доля к стоимости (продажа, цена !мод, шанс апгрейда)
#   fat_cd / case_cd — уменьшение КД !жир / ежедневного кейса
#   jackpot    — прибавка к шансу джекпота в !жир (0.01 = +1%)
#   xp         — +доля к получаемому опыту
#   luck       — множитель ко всем шансам, кроме дуэлей (2.5 = в 2,5 раза)
# КД, джекпот, опыт и удача от разных предметов НЕ складываются — действует лучший предмет
# (иначе 20 холодных беконов обнулили бы кулдаун).
#
# forms  — формы для мужского/женского/среднего рода (согласование с предметом), одним словом
# colors — цвет в анимациях (два цвета = наполовину одним, наполовину другим)
# ansi   — цвет текста в !инвентарь/!ивент/!моды (30 серый, 31 красный, 32 зелёный, 33 жёлтый,
#          34 синий, 35 розовый, 36 голубой, 37 белый)

MAX_ITEM_MODIFIERS = 10
MOD_COST_FRACTION = 0.25  # !мод стоит четверть текущей стоимости предмета

def _M(kind, forms, emoji, colors, ansi, chance, **effects):
    return {"kind": kind, "forms": forms, "emoji": emoji, "colors": colors, "ansi": ansi, "chance": chance, **effects}

def _f(stem_m, stem_f, stem_n):
    return (stem_m, stem_f, stem_n)

ITEM_MODIFIERS = {
    # ===== ИВЕНТОВЫЕ =====
    # --- Новый год ---
    "cold": _M("event", _f("Холодный", "Холодная", "Холодное"), "❄️", [(50, 95, 215)], [34], 0.05, production=0.5, value=0.5, fat_cd=0.10),
    "icy": _M("event", _f("Ледяной", "Ледяная", "Ледяное"), "🧊", [(80, 170, 255)], [36], 0.02, production=1.0, value=1.0, fat_cd=0.20, case_cd=0.10),
    "festive": _M("event", _f("Festive", "Festive", "Festive"), "🎄", [(225, 45, 55), (45, 95, 235)], [31, 34], 0.001, production=2.5, value=2.5, fat_cd=0.50),
    # --- День святого Валентина ---
    "loving": _M("event", _f("Влюблённый", "Влюблённая", "Влюблённое"), "💗", [(255, 120, 190)], [35], 0.05, production=0.5, value=0.5, case_cd=0.10),
    "hearty": _M("event", _f("Сердечный", "Сердечная", "Сердечное"), "❤️", [(230, 40, 70)], [31], 0.02, production=1.0, value=1.0, fat_cd=0.10, case_cd=0.20),
    "cupid": _M("event", _f("Купидонов", "Купидонова", "Купидоново"), "💘", [(255, 120, 190), (250, 250, 255)], [35, 37], 0.001, production=2.5, value=2.5, case_cd=0.50),
    # --- 1 апреля ---
    "clown": _M("event", _f("Клоунский", "Клоунская", "Клоунское"), "🤡", [(255, 215, 40)], [33], 0.05, production=0.25, value=1.0, fat_cd=0.05),
    "absurd": _M("event", _f("Абсурдный", "Абсурдная", "Абсурдное"), "🙃", [(90, 220, 90)], [32], 0.02, production=1.0, value=0.5, fat_cd=0.15, case_cd=0.10),
    "jester": _M("event", _f("Шутовской", "Шутовская", "Шутовское"), "🃏", [(255, 215, 40), (220, 40, 50)], [33, 31], 0.001, production=2.5, value=2.5, fat_cd=0.25, case_cd=0.25),
    # --- Хэллоуин ---
    "spooky": _M("event", _f("Жуткий", "Жуткая", "Жуткое"), "🎃", [(255, 140, 30)], [33], 0.05, production=0.5, value=0.5, case_cd=0.10),
    "ghostly": _M("event", _f("Призрачный", "Призрачная", "Призрачное"), "👻", [(170, 90, 255)], [35], 0.02, production=1.0, value=1.0, fat_cd=0.10, case_cd=0.20),
    "cursed": _M("event", _f("Проклятый", "Проклятая", "Проклятое"), "🕸️", [(90, 220, 60), (150, 60, 230)], [32, 35], 0.001, production=2.5, value=2.5, case_cd=0.50),

    # ===== ОБЫЧНЫЕ (только !мод вне ивентов) =====
    # --- обычные ---
    "iron": _M("regular", _f("Железный", "Железная", "Железное"), "⚙️", [(150, 155, 168)], [30], 0.20, production=0.20, value=0.20),
    "wet": _M("regular", _f("Мокрый", "Мокрая", "Мокрое"), "💦", [(40, 75, 170)], [34], 0.18, value=0.50, fat_cd=0.01),
    "fatty": _M("regular", _f("Жирный", "Жирная", "Жирное"), "🧈", [(195, 150, 20)], [33], 0.10, production=0.30, value=0.30),
    "crunchy": _M("regular", _f("Хрустящий", "Хрустящая", "Хрустящее"), "🥨", [(210, 165, 105)], [33], 0.08, production=0.15, value=0.10),
    "scorched": _M("regular", _f("Подгоревший", "Подгоревшая", "Подгоревшее"), "💨", [(95, 85, 80)], [30], 0.06, production=0.40, value=-0.20),
    "salty": _M("regular", _f("Солёный", "Солёная", "Солёное"), "🧂", [(235, 235, 242)], [37], 0.06, value=0.10, fat_cd=0.02),
    "juicy": _M("regular", _f("Сочный", "Сочная", "Сочное"), "🍑", [(255, 145, 50)], [33], 0.05, production=0.25, value=0.15),
    "spicy": _M("regular", _f("Острый", "Острая", "Острое"), "🌶️", [(230, 55, 40)], [31], 0.04, production=0.20, fat_cd=0.03),
    "sweet": _M("regular", _f("Сладкий", "Сладкая", "Сладкое"), "🍬", [(255, 145, 205)], [35], 0.03, value=0.20, case_cd=0.03),
    # --- редкие ---
    "smoked": _M("regular", _f("Копчёный", "Копчёная", "Копчёное"), "🌫️", [(150, 95, 55)], [33], 0.02, production=0.60, value=0.20),
    "cheesy": _M("regular", _f("Сырный", "Сырная", "Сырное"), "🧀", [(255, 205, 70)], [33], 0.015, production=0.40, value=0.40),
    "honey": _M("regular", _f("Медовый", "Медовая", "Медовое"), "🍯", [(240, 170, 30)], [33], 0.012, value=0.30, case_cd=0.05, xp=0.10),
    "gold": _M("regular", _f("Золотой", "Золотая", "Золотое"), "🥇", [(255, 215, 0)], [33], 0.01, production=0.50, value=4.0),
    "silver": _M("regular", _f("Серебряный", "Серебряная", "Серебряное"), "🥈", [(200, 206, 218)], [37], 0.008, value=1.5),
    "chrome": _M("regular", _f("Хромированный", "Хромированная", "Хромированное"), "🔩", [(160, 190, 215)], [36], 0.007, production=0.80, value=0.50),
    "royal": _M("regular", _f("Королевский", "Королевская", "Королевское"), "👑", [(150, 70, 225)], [35], 0.005, production=1.0, value=1.0),
    "lucky": _M("regular", _f("Счастливый", "Счастливая", "Счастливое"), "🍀", [(60, 205, 90)], [32], 0.005, value=0.30, jackpot=0.01),
    # --- очень редкие ---
    "radioactive": _M("regular", _f("Радиоактивный", "Радиоактивная", "Радиоактивное"), "☢️", [(150, 255, 60)], [32], 0.003, production=3.0, fat_cd=0.10),
    "diamond": _M("regular", _f("Алмазный", "Алмазная", "Алмазное"), "💎", [(120, 230, 255)], [36], 0.0025, value=8.0),
    "fiery": _M("regular", _f("Огненный", "Огненная", "Огненное"), "🔥", [(255, 125, 20), (230, 40, 30)], [33, 31], 0.0025, production=1.5, value=1.5, fat_cd=0.10),
    "platinum": _M("regular", _f("Платиновый", "Платиновая", "Платиновое"), "💿", [(228, 232, 242)], [37], 0.001, value=10.0),
    "cosmic": _M("regular", _f("Космический", "Космическая", "Космическое"), "🌌", [(60, 45, 170), (175, 85, 255)], [34, 35], 0.001, production=4.0, value=2.0, case_cd=0.10),
    # --- ультраредкие ---
    "divine": _M("regular", _f("Божественный", "Божественная", "Божественное"), "😇", [(255, 250, 225), (255, 205, 50)], [37, 33], 0.0002, production=10.0, value=10.0, fat_cd=0.25, case_cd=0.25),
    "infinite": _M("regular", _f("Бесконечный", "Бесконечная", "Бесконечное"), "♾️", [(255, 85, 185), (85, 205, 255)], [35, 36], 0.0001, production=20.0, value=5.0, xp=0.20),
    "gambling": _M("regular", _f("Азартный", "Азартная", "Азартное"), "🎰", [(220, 30, 40), (30, 30, 36)], [31, 30], 0.00001, value=20.0, luck=2.5),
}

# Даты указываются как (месяц, день), конец включительно. Ивент может переходить через Новый год.
HOLIDAY_EVENTS = {
    "new_year": {"name": "Новый год", "emoji": "🎄", "start": (12, 20), "end": (1, 14), "modifiers": ["festive", "icy", "cold"]},
    "valentine": {"name": "День святого Валентина", "emoji": "💘", "start": (2, 10), "end": (2, 16), "modifiers": ["cupid", "hearty", "loving"]},
    "april_fools": {"name": "День смеха", "emoji": "🤡", "start": (4, 1), "end": (4, 3), "modifiers": ["jester", "absurd", "clown"]},
    "halloween": {"name": "Хэллоуин", "emoji": "🎃", "start": (10, 25), "end": (11, 3), "modifiers": ["cursed", "ghostly", "spooky"]},
}

# Род предметов для согласования модификатора: m — мужской, f — женский, n — средний
ITEM_GENDERS = {
    "Тарелка макарон": "f", "Тарелка хинкалей": "f", "Пицца": "f", "Бездонная пачка чипсов": "f",
    "Автоматическая система подачи холестерина": "f", "Гнилая ножка KFC": "f",
    "Ведро KFC": "n", "Комбо за 1000!": "n", "Бездонное ведро KFC": "n", "Яблоко": "n", "Золотое Яблоко": "n",
}

EVENT_OVERRIDE_FILE = os.path.join(LOG_FOLDER, "event_override.json")
_event_override = None
_parse_cache = {}
_base_lookup = None
_form_lookup = None

def all_item_names():
    shop_names = [i["name"] for i in SHOP_ITEMS]
    return shop_names + [n for n in LEGENDARY_UPGRADE_PRICES if n not in shop_names]

def mod_value(mod_id, field):
    return ITEM_MODIFIERS[mod_id].get(field, 1.0 if field == "luck" else 0.0)

def modifier_form(mod_id, base_name):
    gender = ITEM_GENDERS.get(base_name, "m")
    return ITEM_MODIFIERS[mod_id]["forms"]["mfn".index(gender)]

def _sort_mods(mods):
    # Самый редкий — первым: он задаёт цвет и стоит в начале названия
    return sorted(set(mods), key=lambda m: (ITEM_MODIFIERS[m]["chance"], m))

def make_item_key(base_name, mods=None):
    """Название предмета с модификаторами: 'Золотая Мокрая Пицца'. mods — id или список id."""
    if not mods:
        return base_name
    if isinstance(mods, str):
        mods = [mods]
    return " ".join([modifier_form(m, base_name) for m in _sort_mods(mods)] + [base_name])

def _parse(text):
    global _base_lookup, _form_lookup
    if _base_lookup is None:
        names = all_item_names()
        _base_lookup = sorted(((n.lower(), n) for n in names), key=lambda x: len(x[0]), reverse=True)
        _form_lookup = {form.lower(): mod_id for mod_id, m in ITEM_MODIFIERS.items() for form in m["forms"]}
    low = text.strip().lower()
    for base_low, base in _base_lookup:
        if low == base_low:
            return (), base
        if low.endswith(" " + base_low):
            words = low[:-len(base_low) - 1].split()
            mods = [_form_lookup.get(w) for w in words]
            if words and all(mods) and len(set(mods)) == len(mods):
                return tuple(_sort_mods(mods)), base
    return None

def parse_item_key(key):
    """'Золотая Мокрая Пицца' -> (('gold', 'wet'), 'Пицца'); 'Пицца' -> ((), 'Пицца')."""
    if key not in _parse_cache:
        _parse_cache[key] = _parse(key) or ((), key)
    return _parse_cache[key]

def normalize_item_key(text):
    """Название, введённое игроком в любом регистре/порядке -> каноничное название или None."""
    parsed = _parse(text)
    return make_item_key(parsed[1], parsed[0]) if parsed else None

def item_mods(key):
    return parse_item_key(key)[0]

def best_mod(key):
    mods = item_mods(key)
    return mods[0] if mods else None

def base_item_name(key):
    return parse_item_key(key)[1]

def base_counts(items_dict):
    """Количество предметов по базовым названиям (модификаторы не мешают эффектам яблок, воды и т.д.)."""
    result = {}
    for key, count in items_dict.items():
        base = base_item_name(key)
        result[base] = result.get(base, 0) + count
    return result

def modifier_bonus(key, field):
    mods = item_mods(key)
    if field == "luck":
        return max((mod_value(m, "luck") for m in mods), default=1.0)
    return sum(mod_value(m, field) for m in mods)

def best_modifier_bonus(items_dict, field):
    default = 1.0 if field == "luck" else 0.0
    best = max((modifier_bonus(k, field) for k, c in items_dict.items() if c > 0), default=default)
    if field in ("fat_cd", "case_cd"):
        best = min(best, 0.9)
    return best

def player_luck(items_dict):
    """Множитель удачи игрока (Азартный и т.п.) — лучший предмет из инвентаря."""
    return max(1.0, best_modifier_bonus(items_dict or {}, "luck"))

def item_emoji(key):
    return ITEM_EMOJIS.get(base_item_name(key), "📦")

def format_item(key):
    """Название для сообщений: эмодзи лучшего модификатора + название."""
    mod_id = best_mod(key)
    return f"{ITEM_MODIFIERS[mod_id]['emoji']} **{key}**" if mod_id else f"**{key}**"

def ansi_text(text, mod_id):
    codes = ITEM_MODIFIERS[mod_id]["ansi"]
    if len(codes) == 1:
        return f"\u001b[1;{codes[0]}m{text}\u001b[0m"
    half = (len(text) + 1) // 2
    return f"\u001b[1;{codes[0]}m{text[:half]}\u001b[1;{codes[1]}m{text[half:]}\u001b[0m"

def ansi_item(key):
    """Цветное название для ```ansi блоков (цвет лучшего модификатора)."""
    mod_id = best_mod(key)
    return ansi_text(key, mod_id) if mod_id else key

def _load_event_override():
    global _event_override
    if _event_override is None:
        _event_override = "auto"
        try:
            with open(EVENT_OVERRIDE_FILE, encoding="utf-8") as f:
                _event_override = json.load(f).get("mode", "auto")
        except Exception:
            pass
    return _event_override

def set_event_override(mode):
    global _event_override
    _event_override = mode
    try:
        with open(EVENT_OVERRIDE_FILE, "w", encoding="utf-8") as f:
            json.dump({"mode": mode}, f)
    except Exception as e:
        log_error(e, "Сохранение режима ивента")

def _date_in_range(now, start, end):
    md = (now.month, now.day)
    return start <= md <= end if start <= end else (md >= start or md <= end)

def get_active_event(now=None):
    """Возвращает (id, ивент) или (None, None)."""
    mode = _load_event_override()
    if mode == "off":
        return None, None
    if mode in HOLIDAY_EVENTS:
        return mode, HOLIDAY_EVENTS[mode]
    now = now or datetime.now()
    for event_id, event in HOLIDAY_EVENTS.items():
        if _date_in_range(now, event["start"], event["end"]):
            return event_id, event
    return None, None

def get_next_event(now=None):
    now = now or datetime.now()
    best = None
    for event_id, event in HOLIDAY_EVENTS.items():
        start = datetime(now.year, *event["start"])
        if start.date() < now.date():
            start = datetime(now.year + 1, *event["start"])
        if best is None or start < best[2]:
            best = (event_id, event, start)
    return best

def _roll_from_pool(pool, luck=1.0):
    """Бросок одного модификатора из списка (шансы * удача). None — ничего не выпало."""
    if not pool:
        return None
    # Идём от самых редких: они получают полный множитель удачи. Если с удачей сумма
    # шансов больше 100%, «не помещаются» только самые частые модификаторы.
    pool = _sort_mods(pool)
    r = random.random()
    cumulative = 0.0
    for mod_id in pool:
        cumulative += ITEM_MODIFIERS[mod_id]["chance"] * luck
        if r < cumulative:
            return mod_id
    return None

def roll_modifier(luck=1.0):
    """Ивентовый модификатор для только что полученного предмета (None — без модификатора)."""
    _, event = get_active_event()
    return _roll_from_pool(event["modifiers"], luck) if event else None

def grant_item(items_dict, base_name, amount=1, roll=True, luck=None):
    """Кладёт предметы в инвентарь, каждому — свой бросок ивентового модификатора.
    Удача берётся из инвентаря, если не передана. Возвращает {ключ: количество}."""
    if luck is None:
        luck = player_luck(items_dict)
    got = {}
    for _ in range(amount):
        key = make_item_key(base_name, roll_modifier(luck) if roll else None)
        items_dict[key] = items_dict.get(key, 0) + 1
        got[key] = got.get(key, 0) + 1
    return got

def describe_got(got):
    return ", ".join(f"{format_item(k)} x{v}" for k, v in got.items())

def mod_pool_for(key):
    """Какие модификаторы может дать !мод этому предмету прямо сейчас."""
    mods, base = parse_item_key(key)
    _, event = get_active_event()
    pool = [m for m, v in ITEM_MODIFIERS.items() if v["kind"] == "regular"]
    if event:
        pool += event["modifiers"]  # во время праздника к обычным добавляются его модификаторы
    names = set(all_item_names())
    # Уже стоящие на предмете не повторяются; и нельзя получить название, совпадающее
    # с другим предметом (например, «Золотое» + «Яблоко» = отдельный предмет «Золотое Яблоко»)
    return [m for m in pool if m not in mods and make_item_key(base, list(mods) + [m]) not in names]

def mod_cost(key):
    return max(1, math.ceil(get_item_price(key) * MOD_COST_FRACTION))

def modifier_effects_text(mod_id):
    parts = []
    production, value = mod_value(mod_id, "production"), mod_value(mod_id, "value")
    if production:
        parts.append(f"{production*100:+.0f}% производства кг")
    if value:
        parts.append(f"{value*100:+.0f}% стоимости")
    if mod_value(mod_id, "fat_cd"):
        parts.append(f"-{mod_value(mod_id, 'fat_cd')*100:g}% КД !жир")
    if mod_value(mod_id, "case_cd"):
        parts.append(f"-{mod_value(mod_id, 'case_cd')*100:g}% КД ежедневного кейса")
    if mod_value(mod_id, "jackpot"):
        parts.append(f"+{mod_value(mod_id, 'jackpot')*100:g}% к шансу джекпота")
    if mod_value(mod_id, "xp"):
        parts.append(f"+{mod_value(mod_id, 'xp')*100:g}% опыта")
    if mod_value(mod_id, "luck") > 1:
        parts.append(f"x{mod_value(mod_id, 'luck'):g} ко всем шансам (кроме дуэлей)")
    return ", ".join(parts)

def chance_text(chance):
    return f"{chance*100:.4f}".rstrip("0").rstrip(".") + "%"


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
    counts = base_counts(items_dict)
    reduction = counts.get("Яблоко", 0) * 0.05 + counts.get("Золотое Яблоко", 0) * 0.10
    # Модификаторы ивентов: действует лучший из инвентаря
    return cooldown * (1 - min(reduction, 0.9)) * (1 - best_modifier_bonus(items_dict, "fat_cd"))

def get_case_cooldown_minutes(case_cd_upgrade, items_dict):
    cooldown = max(10, CASE_COOLDOWN_HOURS * 60 - get_case_cd_reduction(case_cd_upgrade or 0))
    counts = base_counts(items_dict)
    reduction = counts.get("Апельсин", 0) * 0.05 + counts.get("Золотой Апельсин", 0) * 0.10
    cooldown *= (1 - min(reduction, 0.9)) * (1 - best_modifier_bonus(items_dict, "case_cd"))
    return max(10, cooldown)

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
    xp_mod = 1 + best_modifier_bonus(get_user_items(data.get('item_counts')), "xp")  # Медовый, Бесконечный
    xp_amount = int(xp_amount * xp_bonus * xp_multiplier * xp_mod)
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
        if lvl < len(PRESTIGE_COSTS):
            return PRESTIGE_COSTS[lvl]
        return PRESTIGE_COSTS[-1] * 2 ** (lvl - len(PRESTIGE_COSTS) + 1)
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
        'best_item_name': "TEXT",
        'best_item_price': "INTEGER DEFAULT 0",
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
        case_plus_upgrade INTEGER DEFAULT 0, xp_multiplier_upgrade INTEGER DEFAULT 0,
        best_item_name TEXT, best_item_price INTEGER DEFAULT 0)''')
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
    select_cols = [c for c in ['user_id','user_name','current_number','last_command_time','consecutive_plus','consecutive_minus','jackpot_pity','last_case_time','item_counts','last_command','last_command_target','last_command_use_time','fat_cooldown_time','active_case_message_id','active_case_channel_id','daily_case_last_time','snatcher_last_time','duel_active','duel_opponent','duel_amount','duel_message_id','duel_channel_id','duel_initiator','last_case_type','last_case_prize','upgrade_active','upgrade_data','duel_start_time','shadow_upgrade_chance','user_xp','user_level','fat_cd_upgrade','case_cd_upgrade','luck_upgrade','income_upgrade','prestige','auto_fat_level','next_auto_fat_time','animations_enabled','daily_case_count','auto_open_cases','jackpot_upgrade','fat_plus_upgrade','case_plus_upgrade','xp_multiplier_upgrade','best_item_name','best_item_price'] if c in all_cols]
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
            'jackpot_upgrade': 0, 'fat_plus_upgrade': 0, 'case_plus_upgrade': 0, 'xp_multiplier_upgrade': 0, 'best_item_name': None, 'best_item_price': 0,
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
    base = ['user_id','user_name','current_number','last_command_time','consecutive_plus','consecutive_minus','jackpot_pity','last_case_time','item_counts','last_command','last_command_target','last_command_use_time','fat_cooldown_time','active_case_message_id','active_case_channel_id','daily_case_last_time','snatcher_last_time','duel_active','duel_opponent','duel_amount','duel_message_id','duel_channel_id','duel_initiator','last_case_type','last_case_prize','upgrade_active','upgrade_data','duel_start_time','shadow_upgrade_chance','user_xp','user_level','fat_cd_upgrade','case_cd_upgrade','luck_upgrade','income_upgrade','prestige','auto_fat_level','next_auto_fat_time','animations_enabled','daily_case_count','auto_open_cases','jackpot_upgrade','fat_plus_upgrade','case_plus_upgrade','xp_multiplier_upgrade','best_item_name','best_item_price']
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
    if 'item_counts' in kwargs and 'best_item_price' in cols and 'best_item_price' not in kwargs:
        # Рекорд «самый дорогой предмет за всё время» — обновляется при любом получении предмета
        best = most_expensive_item(get_user_items(kwargs['item_counts']))
        if best:
            cursor.execute("SELECT best_item_price FROM user_fat WHERE user_id = ?", (str(user_id),))
            row = cursor.fetchone()
            if row is not None and best[1] > (row[0] or 0):
                kwargs['best_item_name'], kwargs['best_item_price'] = best
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
    raw_items = items_dict or {}
    luck = player_luck(raw_items)
    mod_jackpot = best_modifier_bonus(raw_items, "jackpot")
    items_dict = base_counts(raw_items)
    has_rotten_leg = items_dict.get("Гнилая ножка KFC", 0) > 0
    has_holy_sandwich = items_dict.get("Святой сэндвич", 0) > 0
    has_water = base_counts(items_dict).get("Стакан воды", 0) > 0
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
    # Модификаторы: «Счастливый» добавляет к шансу, «Азартный» умножает все шансы
    jackpot_chance = min((jackpot_chance + mod_jackpot) * luck, 0.9)
    minus_chance = minus_chance / luck

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

def open_case(case_id, prestige_luck=0, luck_upgrade=0, case_plus_bonus=1.0, luck=1.0):
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
            np["norm"] *= luck
        elif isinstance(np["value"], str):
            np["norm"] *= luck  # предметы из магазинного кейса — тоже «редкие» призы
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
    # item_name может быть с модификатором ивента — тогда стоимость выше
    base = base_item_name(item_name)
    price = 0
    if base in LEGENDARY_UPGRADE_PRICES:
        price = LEGENDARY_UPGRADE_PRICES[base]
    else:
        for si in SHOP_ITEMS:
            if si["name"] == base:
                price = si["price"]
                break
    return int(price * (1 + modifier_bonus(item_name, "value")))

def most_expensive_item(items_dict):
    """(название, цена) самого дорогого предмета в инвентаре или None."""
    best = None
    for key, count in items_dict.items():
        if count > 0:
            price = get_item_price(key)
            if best is None or price > best[1]:
                best = (key, price)
    return best

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
    from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps, ImageChops
    ANIMATIONS_AVAILABLE = True
except ImportError:
    ANIMATIONS_AVAILABLE = False
    logger.warning("Pillow не установлен — GIF-анимации отключены (pip install Pillow)")

from functools import lru_cache

ANIM_W = 600
ANIM_HEADER = 40
ANIM_ROW_H = 176
ANIM_FOOTER = 40
ANIM_H = ANIM_HEADER + ANIM_ROW_H + ANIM_FOOTER
TILE_W, TILE_H, TILE_GAP = 112, 150, 10
ICON_PX = 76
STRIP_LEN = 46
WIN_INDEX = 40
ANIM_SPIN_FRAMES = 52
ANIM_REVEAL_FRAMES = 12
ANIM_FINAL_MS = 15000
ANIM_BG_TOP = (32, 30, 40)
ANIM_BG_BOTTOM = (14, 13, 18)
ANIM_GOLD = (255, 200, 40)

RARITY_COLORS = {
    "common": (176, 195, 217),
    "uncommon": (75, 125, 255),
    "rare": (136, 71, 255),
    "epic": (211, 44, 230),
    "mythic": (235, 75, 75),
    "legendary": (255, 200, 40),
    "minus": (190, 60, 50),
    "empty": (110, 112, 120),
    "success": (60, 210, 90),
    "fail": (225, 60, 60),
    "silver": (200, 205, 215),
    "draw": (155, 89, 182),
}

_font_cache = {}
_font_bytes = None

def _get_font_bytes():
    # Шрифт встроен прямо в код (EMBEDDED_FONT_B64 в конце файла), поэтому
    # ничего не нужно класть на хостинг и ничего не скачивается.
    # Свой шрифт можно указать переменной окружения FONT_PATH.
    global _font_bytes
    if _font_bytes is None:
        custom = os.environ.get("FONT_PATH", "")
        if custom and os.path.exists(custom):
            with open(custom, "rb") as f:
                _font_bytes = f.read()
        else:
            _font_bytes = zlib.decompress(base64.b64decode("".join(EMBEDDED_FONT_B64)))
    return _font_bytes

def _font(size):
    size = max(6, int(size))
    if size not in _font_cache:
        try:
            _font_cache[size] = ImageFont.truetype(io.BytesIO(_get_font_bytes()), size)
        except Exception as e:
            log_error(e, "Загрузка шрифта для анимаций")
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

def _shadow_text(draw, xy, text, font, fill, anchor="mm", shadow=(0, 0, 0), off=2):
    draw.text((xy[0] + off, xy[1] + off), text, font=font, fill=shadow, anchor=anchor)
    draw.text(xy, text, font=font, fill=fill, anchor=anchor)

# ---------- Иконки: рисуются кодом в «пространстве» 100x100 с 3x сглаживанием ----------

class _Pen:
    SS = 3

    def __init__(self):
        self.S = 100 * self.SS
        self.k = self.S / 100
        self.img = Image.new("RGBA", (self.S, self.S), (0, 0, 0, 0))
        self.d = ImageDraw.Draw(self.img)

    def P(self, x, y):
        return (x * self.k, y * self.k)

    def B(self, b):
        return [b[0] * self.k, b[1] * self.k, b[2] * self.k, b[3] * self.k]

    def W(self, w):
        return max(1, int(w * self.k))

    def ell(self, b, fill=None, outline=None, w=0):
        self.d.ellipse(self.B(b), fill=fill, outline=outline, width=self.W(w) if outline else 0)

    def rect(self, b, fill=None, r=0, outline=None, w=0):
        self.d.rounded_rectangle(self.B(b), radius=r * self.k, fill=fill, outline=outline, width=self.W(w) if outline else 0)

    def poly(self, pts, fill=None, outline=None, w=0):
        self.d.polygon([self.P(*q) for q in pts], fill=fill, outline=outline, width=self.W(w) if outline else 1)

    def line(self, pts, fill, w):
        self.d.line([self.P(*q) for q in pts], fill=fill, width=self.W(w), joint="curve")

    def arc(self, b, a0, a1, fill, w):
        self.d.arc(self.B(b), a0, a1, fill=fill, width=self.W(w))

    def chord(self, b, a0, a1, fill):
        self.d.chord(self.B(b), a0, a1, fill=fill)

    def text(self, xy, text, size, fill):
        _shadow_text(self.d, self.P(*xy), text, _font(size * self.k), fill, off=self.W(1))

    def glow(self, b, color, alpha=160, blur=8):
        # Полупрозрачное размытое пятно (свечение/блик) поверх уже нарисованного
        layer = Image.new("RGBA", self.img.size, (0, 0, 0, 0))
        ImageDraw.Draw(layer).ellipse(self.B(b), fill=color + (alpha,))
        if blur:
            layer = layer.filter(ImageFilter.GaussianBlur(blur * self.k))
        self.img.alpha_composite(layer)

    def under_glow(self, b, color, alpha=170, blur=8):
        layer = Image.new("RGBA", self.img.size, (0, 0, 0, 0))
        ImageDraw.Draw(layer).ellipse(self.B(b), fill=color + (alpha,))
        layer = layer.filter(ImageFilter.GaussianBlur(blur * self.k))
        layer.alpha_composite(self.img)
        self.img = layer
        self.d = ImageDraw.Draw(self.img)

    def shine(self, b, alpha=110):
        self.glow(b, (255, 255, 255), alpha, blur=2)

    def paste(self, icon_img, b):
        box = self.B(b)
        size = (int(box[2] - box[0]), int(box[3] - box[1]))
        self.img.alpha_composite(icon_img.resize(size, Image.Resampling.LANCZOS), (int(box[0]), int(box[1])))

    def sparkle(self, x, y, r, color=(255, 245, 190)):
        self.poly([(x, y - r), (x + r * 0.25, y - r * 0.25), (x + r, y), (x + r * 0.25, y + r * 0.25),
                   (x, y + r), (x - r * 0.25, y + r * 0.25), (x - r, y), (x - r * 0.25, y - r * 0.25)], color)

def _wave(x0, x1, y, amp, freq, phase=0.0, steps=24):
    return [(x0 + (x1 - x0) * i / steps, y + amp * math.sin(phase + freq * i)) for i in range(steps + 1)]

BUN = (236, 162, 70)
BUN_DARK = (214, 136, 52)
LETTUCE = (110, 200, 70)
CHEESE = (255, 205, 60)
PATTY = (112, 60, 32)
FRIED = (200, 116, 48)
FRIED_LIGHT = (222, 142, 66)

def _ic_burger(p):
    p.chord((8, 14, 92, 78), 180, 360, BUN)
    p.shine((24, 20, 52, 30), 90)
    for x, y in ((30, 28), (44, 22), (58, 27), (70, 33), (38, 37), (54, 36)):
        p.ell((x - 3, y - 1.6, x + 3, y + 1.6), (255, 243, 210))
    p.poly(_wave(10, 90, 46, 0, 0) + list(reversed(_wave(10, 90, 54, 3, 1.3))), LETTUCE)
    p.poly([(14, 52), (86, 52), (82, 62), (72, 56), (62, 64), (52, 56), (42, 63), (30, 56), (20, 61)], CHEESE)
    p.rect((12, 56, 88, 70), PATTY, r=6)
    for x in (26, 42, 58, 74):
        p.line([(x - 4, 60), (x + 4, 66)], (80, 40, 20), 2)
    p.rect((14, 70, 86, 86), BUN, r=7)
    p.rect((14, 70, 86, 74), BUN_DARK, r=2)

def _ic_burger_stack(p):
    b = _icon_raw("burger")
    p.paste(b, (0, 42, 54, 96))
    p.paste(b, (46, 42, 100, 96))
    p.paste(b, (22, 4, 78, 60))
    for x, y in ((10, 20), (90, 16), (88, 40)):
        p.sparkle(x, y, 6)

def _ic_pizza(p):
    p.poly([(50, 94), (12, 24), (88, 24)], (250, 196, 70))
    p.poly([(50, 86), (19, 28), (81, 28)], (255, 214, 95))
    p.rect((8, 12, 92, 30), (214, 140, 60), r=9)
    p.shine((16, 14, 50, 20), 80)
    for x, y, r in ((36, 40, 7), (60, 42, 7), (49, 60, 6.5), (49, 78, 4.5), (64, 31, 4)):
        p.ell((x - r, y - r, x + r, y + r), (200, 52, 40))
        p.ell((x - r * 0.5, y - r * 0.6, x, y - r * 0.1), (232, 96, 80))
    for x, y in ((42, 52), (57, 70), (30, 33)):
        p.ell((x - 3, y - 2, x + 3, y + 2), (90, 160, 60))

def _ic_drumstick(p, rotten=False):
    meat = (120, 132, 60) if rotten else FRIED
    meat2 = (140, 152, 72) if rotten else FRIED_LIGHT
    p.line([(24, 80), (48, 56)], (244, 236, 214), 11)
    p.ell((8, 72, 24, 88), (244, 236, 214))
    p.ell((16, 80, 32, 96), (244, 236, 214))
    p.ell((32, 8, 92, 68), meat)
    p.ell((40, 14, 88, 60), meat2)
    for x, y in ((56, 30), (70, 44), (52, 50), (74, 24), (62, 56)):
        p.ell((x - 3, y - 2, x + 3, y + 2), _mix(meat, (60, 30, 10), 0.35))
    if rotten:
        for x, y, r in ((50, 26, 6), (72, 46, 5), (60, 56, 4)):
            p.ell((x - r, y - r, x + r, y + r), (110, 60, 130))
        for x in (44, 60, 76):
            p.line([(x + 3 * math.sin(i * 1.3), 16 - i * 3) for i in range(6)], (150, 210, 80), 2.5)
        p.glow((30, 0, 96, 30), (140, 220, 60), 90, 6)
    else:
        p.shine((48, 16, 70, 28), 100)

def _ic_water(p):
    p.poly([(22, 8), (78, 8), (71, 94), (29, 94)], (215, 236, 255))
    p.poly([(25, 38), (75, 38), (71, 90), (29, 90)], (60, 150, 250))
    p.ell((25, 33, 75, 43), (130, 195, 255))
    for x, y, r in ((40, 70, 3), (56, 60, 2.5), (48, 80, 2), (60, 76, 3)):
        p.ell((x - r, y - r, x + r, y + r), None, (210, 235, 255), 1)
    p.line([(30, 14), (36, 86)], (255, 255, 255, 200), 3)
    p.line([(22, 8), (78, 8)], (255, 255, 255), 3)

def _ic_pill(p, c1=(225, 50, 60), c2=(250, 250, 250)):
    p.rect((10, 34, 90, 66), c1, r=16)
    p.rect((44, 34, 90, 66), c2, r=16)
    p.rect((44, 34, 60, 66), c2)
    p.line([(50, 34), (50, 66)], _mix(c1, (0, 0, 0), 0.3), 2)
    p.shine((18, 37, 84, 45), 120)
    p.img = p.img.rotate(32, resample=Image.Resampling.BICUBIC)
    p.d = ImageDraw.Draw(p.img)

def _ic_apple(p, color=(222, 40, 48)):
    p.line([(50, 32), (55, 10)], (110, 70, 40), 5)
    p.poly([(55, 20), (70, 8), (84, 12), (68, 24)], (90, 180, 60))
    p.ell((12, 26, 56, 90), color)
    p.ell((44, 26, 88, 90), color)
    p.ell((22, 36, 78, 94), color)
    p.ell((43, 25, 57, 35), _mix(color, (0, 0, 0), 0.35))
    p.shine((22, 36, 36, 56), 140)

def _ic_orange(p, color=(250, 150, 30)):
    p.ell((14, 18, 86, 90), color)
    rnd = random.Random(7)
    for _ in range(26):
        x, y = rnd.uniform(24, 76), rnd.uniform(28, 82)
        p.ell((x - 1.2, y - 1.2, x + 1.2, y + 1.2), _mix(color, (120, 60, 0), 0.25))
    p.poly([(50, 20), (64, 6), (78, 8), (62, 22)], (90, 180, 60))
    p.ell((46, 16, 54, 24), (110, 80, 30))
    p.shine((24, 28, 42, 46), 130)

def _ic_dragonfruit(p, color=(232, 52, 140)):
    p.poly([(50, 4), (42, 22), (58, 22)], (120, 205, 70))
    p.poly([(34, 8), (40, 26), (50, 20)], (120, 205, 70))
    p.poly([(66, 8), (60, 26), (50, 20)], (120, 205, 70))
    p.ell((18, 18, 82, 92), color)
    for ang in range(0, 360, 40):
        a = math.radians(ang)
        cx, cy = 50 + 26 * math.cos(a), 56 + 30 * math.sin(a)
        tx, ty = 50 + 40 * math.cos(a + 0.35), 56 + 44 * math.sin(a + 0.35)
        p.poly([(cx - 5, cy), (cx + 5, cy), (tx, ty)], (130, 210, 70))
    p.shine((28, 30, 44, 50), 120)

def _bacon_strip(p, y0, base, fat, burnt):
    top = _wave(8, 92, y0, 5, 0.55)
    bottom = list(reversed(_wave(8, 92, y0 + 24, 5, 0.55)))
    p.poly(top + bottom, base)
    p.line(_wave(10, 90, y0 + 8, 5, 0.55), fat, 3.5)
    p.line(_wave(10, 90, y0 + 16, 5, 0.55), fat, 2.5)
    if burnt:
        for x, y in ((24, y0 + 10), (52, y0 + 14), (76, y0 + 8)):
            p.ell((x - 4, y - 3, x + 4, y + 3), (30, 18, 12))

def _ic_bacon(p, burnt=True):
    base = (120, 52, 36) if burnt else (205, 85, 70)
    fat = (180, 130, 100) if burnt else (250, 205, 180)
    _bacon_strip(p, 16, base, fat, burnt)
    _bacon_strip(p, 52, base, fat, burnt)
    if burnt:
        for x in (30, 52, 72):
            p.glow((x - 6, 2, x + 6, 16), (120, 120, 120), 120, 3)

def _ic_sandwich(p, burnt=False, holy=False):
    bread = (150, 92, 50) if burnt else (240, 200, 130)
    crust = (90, 50, 25) if burnt else (200, 140, 70)
    top = 22 if holy else 14
    if holy:
        p.under_glow((10, 10, 90, 92), (255, 220, 120), 170, 10)
        p.ell((28, 2, 72, 16), None, ANIM_GOLD, 3.5)
    p.rect((10, top, 90, top + 20), bread, r=10, outline=crust, w=2.5)
    p.poly(_wave(12, 88, top + 20, 0, 0) + list(reversed(_wave(12, 88, top + 27, 3, 1.4))), LETTUCE)
    p.rect((14, top + 26, 86, top + 33), (232, 64, 52), r=3)
    p.poly([(12, top + 33), (88, top + 33), (84, top + 41), (70, top + 36), (56, top + 43), (40, top + 36), (24, top + 42)], CHEESE)
    p.rect((12, top + 38, 88, top + 47), (240, 140, 150), r=4)
    p.rect((10, top + 47, 90, top + 66), bread, r=10, outline=crust, w=2.5)
    if burnt:
        for x, y in ((26, top + 6), (60, top + 10), (44, top + 56), (74, top + 54)):
            p.ell((x - 4, y - 3, x + 4, y + 3), (35, 20, 10))
    else:
        p.shine((20, top + 3, 50, top + 9), 110)
    if holy:
        for x, y in ((8, 30), (92, 24), (90, 80), (12, 84)):
            p.sparkle(x, y, 6)

def _ic_wrap(p, burnt=True):
    tort = (160, 110, 60) if burnt else (236, 200, 128)
    p.rect((6, 30, 82, 70), tort, r=20)
    for x in (22, 38, 54):
        p.line([(x, 34), (x + 10, 66)], _mix(tort, (60, 30, 10), 0.45), 3)
    p.ell((64, 27, 94, 73), _mix(tort, (255, 255, 255), 0.25))
    p.ell((68, 32, 90, 68), (215, 70, 50))
    for x, y, c in ((74, 42, LETTUCE), (82, 56, LETTUCE), (78, 50, (245, 200, 120)), (84, 40, (245, 200, 120))):
        p.ell((x - 3, y - 3, x + 3, y + 3), c)
    if burnt:
        for x, y in ((18, 44), (46, 58), (30, 62)):
            p.ell((x - 4, y - 3, x + 4, y + 3), (40, 22, 10))

def _plate(p):
    p.ell((4, 54, 96, 92), (210, 212, 222))
    p.ell((14, 58, 86, 86), (245, 246, 250))

def _ic_pasta(p):
    _plate(p)
    p.ell((18, 32, 82, 76), (245, 205, 90))
    for i, (x0, x1, y) in enumerate(((24, 76, 46), (22, 78, 56), (26, 74, 66), (30, 70, 40))):
        p.line(_wave(x0, x1, y, 2.5, 1.2, phase=i), (222, 172, 58), 2.5)
    p.ell((34, 28, 66, 50), (205, 52, 36))
    p.ell((40, 31, 54, 39), (235, 100, 80))
    p.poly([(52, 24), (62, 18), (66, 24), (58, 30)], (80, 170, 60))
    for x, y in ((30, 60), (66, 58), (46, 68)):
        p.ell((x - 2, y - 2, x + 2, y + 2), (255, 255, 240))

def _dumpling(p, cx, cy, r):
    c = (244, 230, 198)
    p.ell((cx - r, cy - r * 0.6, cx + r, cy + r), c)
    p.poly([(cx - r * 0.55, cy - r * 0.35), (cx + r * 0.55, cy - r * 0.35), (cx, cy - r * 1.25)], c)
    p.ell((cx - r * 0.22, cy - r * 1.4, cx + r * 0.22, cy - r * 1.0), (230, 214, 178))
    for dx in (-0.45, -0.15, 0.15, 0.45):
        p.line([(cx, cy - r * 1.1), (cx + dx * r * 1.6, cy + r * 0.2)], (214, 194, 150), 1.6)
    p.shine((cx - r * 0.7, cy - r * 0.2, cx - r * 0.2, cy + r * 0.3), 90)

def _ic_khinkali(p):
    _plate(p)
    _dumpling(p, 50, 44, 18)
    _dumpling(p, 30, 62, 17)
    _dumpling(p, 70, 62, 17)

def _ic_bucket(p, bottomless=False):
    if bottomless:
        p.under_glow((10, 60, 90, 100), (150, 70, 255), 220, 8)
    for x, y in ((28, 20), (50, 14), (70, 20)):
        p.ell((x - 13, y - 11, x + 13, y + 13), FRIED)
        p.ell((x - 9, y - 8, x + 7, y + 6), FRIED_LIGHT)
    p.poly([(16, 32), (84, 32), (73, 94), (27, 94)], (250, 250, 250))
    for i in (0, 2, 4):
        t0, t1 = i / 6, (i + 1) / 6
        p.poly([(16 + 68 * t0, 32), (16 + 68 * t1, 32), (27 + 46 * t1, 94), (27 + 46 * t0, 94)], (215, 30, 40))
    p.rect((12, 26, 88, 37), (236, 236, 242), r=4)
    p.ell((38, 50, 62, 74), (250, 250, 250))
    p.text((50, 62), "KFC", 9, (215, 30, 40))
    if bottomless:
        for x, y in ((12, 50), (88, 60), (50, 98)):
            p.sparkle(x, y, 6, (210, 170, 255))

def _ic_chips(p, bottomless=False):
    if bottomless:
        p.under_glow((10, 60, 90, 100), (150, 70, 255), 220, 8)
    top = [(16 + i * 6, 14 + (4 if i % 2 else 0)) for i in range(12)]
    bottom = [(82 - i * 6, 88 + (4 if i % 2 else 0)) for i in range(12)]
    p.poly(top + [(86, 20), (88, 52), (86, 86)] + bottom + [(14, 86), (12, 52), (14, 20)], (255, 205, 40))
    p.poly([(13, 30), (87, 30), (88, 46), (12, 46)], (225, 40, 45))
    p.text((50, 38), "ЧИПСЫ", 9, (255, 255, 255))
    p.ell((32, 54, 68, 82), (250, 222, 120))
    for x, y in ((42, 62), (56, 70), (48, 74)):
        p.ell((x - 2, y - 2, x + 2, y + 2), (220, 170, 70))
    p.shine((18, 50, 28, 80), 100)
    if bottomless:
        for x, y in ((8, 40), (92, 30), (90, 92)):
            p.sparkle(x, y, 6, (210, 170, 255))

def _ic_combo(p):
    p.rect((2, 74, 98, 94), (200, 60, 50), r=6)
    p.rect((62, 24, 90, 84), (230, 50, 50), r=4)
    p.rect((60, 18, 92, 27), (250, 250, 250), r=3)
    p.line([(78, 18), (84, 2)], (250, 250, 250), 4)
    p.ell((70, 44, 84, 60), (250, 250, 250))
    for x in (14, 20, 26, 32, 38):
        p.line([(x, 46), (x - 2 + (x % 3), 20)], (255, 210, 70), 4)
    p.poly([(8, 40), (44, 40), (40, 82), (12, 82)], (225, 40, 45))
    p.paste(_icon_raw("burger"), (22, 44, 70, 92))

def _ic_machine(p):
    teeth = 8
    pts = []
    for i in range(teeth * 2):
        a = math.pi * 2 * i / (teeth * 2)
        r = 42 if i % 2 == 0 else 33
        pts.append((44 + r * math.cos(a), 52 + r * math.sin(a)))
    p.poly(pts, (150, 156, 170))
    p.ell((24, 32, 64, 72), (120, 126, 140))
    p.ell((34, 42, 54, 62), (40, 42, 50))
    p.shine((20, 22, 40, 34), 90)
    p.ell((70, 50, 94, 74), (255, 214, 60))
    p.poly([(72, 58), (92, 58), (82, 34)], (255, 214, 60))
    p.shine((75, 56, 82, 66), 140)

def _ic_snatcher(p):
    body = (150, 70, 230)
    p.under_glow((6, 4, 94, 96), (170, 90, 255), 140, 9)
    p.ell((14, 8, 86, 72), body)
    p.rect((14, 40, 86, 82), body)
    zig = [(14 + i * 8, 82 + (10 if i % 2 else 0)) for i in range(10)]
    p.poly([(14, 80), (86, 80)] + list(reversed(zig)), body)
    p.ell((26, 26, 46, 48), (255, 255, 255))
    p.ell((54, 26, 74, 48), (255, 255, 255))
    p.ell((33, 33, 43, 45), (20, 10, 30))
    p.ell((61, 33, 71, 45), (20, 10, 30))
    p.rect((30, 56, 70, 68), (50, 15, 70), r=5)
    for x in (36, 46, 56, 64):
        p.poly([(x - 3, 56), (x + 3, 56), (x, 62)], (255, 255, 255))
    p.line([(14, 50), (2, 36), (6, 30)], body, 6)
    p.line([(86, 50), (98, 36), (94, 30)], body, 6)
    p.shine((26, 12, 50, 22), 110)

def _ic_kettlebell(p, color=(70, 72, 82)):
    p.ell((28, 4, 72, 48), None, (60, 62, 72), 9)
    p.ell((12, 26, 88, 96), color)
    p.shine((24, 36, 44, 54), 130)
    p.text((50, 64), "КГ", 22, (255, 255, 255))

def _ic_arrow_down(p):
    p.rect((36, 8, 64, 56), (225, 60, 60), r=4)
    p.poly([(14, 50), (86, 50), (50, 92)], (225, 60, 60))
    p.shine((40, 12, 50, 50), 90)

def _ic_empty(p):
    p.poly([(26, 34), (74, 34), (82, 46), (18, 46)], (120, 84, 50))
    p.poly([(18, 46), (82, 46), (80, 90), (20, 90)], (205, 152, 92))
    p.poly([(18, 46), (26, 34), (8, 22), (2, 36)], (185, 135, 80))
    p.poly([(82, 46), (74, 34), (92, 22), (98, 36)], (185, 135, 80))
    p.line([(50, 46), (50, 90)], (170, 120, 70), 2)
    for x, y, r in ((40, 18, 5), (52, 12, 6), (62, 20, 4)):
        p.glow((x - r, y - r, x + r, y + r), (200, 200, 210), 150, 2)

def _ic_crown(p, color=ANIM_GOLD):
    p.poly([(14, 74), (14, 26), (32, 48), (50, 16), (68, 48), (86, 26), (86, 74)], color)
    p.rect((12, 70, 88, 86), _mix(color, (0, 0, 0), 0.15), r=4)
    for x, c in ((30, (220, 40, 60)), (50, (60, 120, 255)), (70, (40, 200, 110))):
        p.ell((x - 5, 73, x + 5, 83), c)
    for x, y in ((14, 24), (50, 14), (86, 24)):
        p.ell((x - 4, y - 4, x + 4, y + 4), _mix(color, (255, 255, 255), 0.4))
    p.shine((22, 40, 36, 66), 90)

def _ic_swords(p):
    for a, b in (((16, 84), (80, 14)), ((84, 84), (20, 14))):
        p.line([a, b], (215, 222, 235), 8)
        p.line([a, ((a[0] * 3 + b[0]) / 4, (a[1] * 3 + b[1]) / 4)], (120, 70, 40), 9)
    p.line([(18, 60), (38, 80)], ANIM_GOLD, 7)
    p.line([(82, 60), (62, 80)], ANIM_GOLD, 7)

def _ic_fail(p):
    p.ell((10, 10, 90, 90), (205, 42, 48))
    p.line([(32, 32), (68, 68)], (255, 255, 255), 12)
    p.line([(68, 32), (32, 68)], (255, 255, 255), 12)
    p.shine((22, 18, 46, 30), 80)

def _ic_unknown(p):
    p.rect((14, 22, 86, 90), (205, 152, 92), r=6)
    p.rect((10, 14, 90, 34), (225, 172, 110), r=6)
    p.rect((44, 14, 56, 90), (225, 60, 60))
    p.shine((18, 18, 40, 26), 90)

ICON_DRAWERS = {
    "burger": _ic_burger, "burger_stack": _ic_burger_stack, "pizza": _ic_pizza,
    "drumstick": _ic_drumstick, "rotten_leg": lambda p: _ic_drumstick(p, rotten=True),
    "water": _ic_water, "pill_red": _ic_pill, "pill_blue": lambda p: _ic_pill(p, (60, 120, 240), (250, 250, 250)),
    "apple": _ic_apple, "orange": _ic_orange, "dragonfruit": _ic_dragonfruit,
    "bacon": _ic_bacon, "sandwich_burnt": lambda p: _ic_sandwich(p, burnt=True),
    "sandwich_holy": lambda p: _ic_sandwich(p, holy=True), "wrap": _ic_wrap,
    "pasta": _ic_pasta, "khinkali": _ic_khinkali, "bucket": _ic_bucket,
    "bucket_bottomless": lambda p: _ic_bucket(p, bottomless=True),
    "chips_bottomless": lambda p: _ic_chips(p, bottomless=True), "combo": _ic_combo,
    "machine": _ic_machine, "snatcher": _ic_snatcher, "kettlebell": _ic_kettlebell,
    "kettlebell_gold": lambda p: _ic_kettlebell(p, (230, 170, 30)),
    "minus": _ic_arrow_down, "empty": _ic_empty, "crown": _ic_crown,
    "crown_silver": lambda p: _ic_crown(p, (205, 210, 222)), "swords": _ic_swords,
    "fail": _ic_fail, "unknown": _ic_unknown,
}

ITEM_ICONS = {
    "Горелый бекон": ("bacon", None), "Горелый бутерброд": ("sandwich_burnt", None),
    "Горелый додстер": ("wrap", None), "Тарелка макарон": ("pasta", None),
    "Тарелка хинкалей": ("khinkali", None), "Бургер": ("burger", None), "Пицца": ("pizza", None),
    "Ведро KFC": ("bucket", None), "Комбо за 1000!": ("combo", None),
    "Бездонное ведро KFC": ("bucket_bottomless", None), "Бездонная пачка чипсов": ("chips_bottomless", None),
    "Пожизненный запас чикенбургеров": ("burger_stack", None),
    "Автоматическая система подачи холестерина": ("machine", None),
    "Святой сэндвич": ("sandwich_holy", None), "Гнилая ножка KFC": ("rotten_leg", None),
    "Стакан воды": ("water", None), "Автохолестерол": ("pill_red", None), "Холестеринимус": ("pill_blue", None),
    "Яблоко": ("apple", None), "Золотое Яблоко": ("apple", "gold"),
    "Апельсин": ("orange", None), "Золотой Апельсин": ("orange", "gold"),
    "Драгонфрукт": ("dragonfruit", None), "Золотой Драгонфрукт": ("dragonfruit", "gold"),
    "Снатчер": ("snatcher", None),
}

@lru_cache(maxsize=64)
def _icon_raw(name, variant=None):
    """Иконка без обводки, 300x300 RGBA."""
    p = _Pen()
    ICON_DRAWERS.get(name, _ic_unknown)(p)
    img = p.img
    if variant == "gold":
        gray = ImageOps.grayscale(img.convert("RGB"))
        gold = ImageOps.colorize(gray, black=(110, 66, 0), mid=(235, 170, 30), white=(255, 246, 190)).convert("RGBA")
        gold.putalpha(img.getchannel("A"))
        img = gold
        p2 = _Pen()
        p2.img = img
        p2.d = ImageDraw.Draw(img)
        for x, y, r in ((12, 18, 7), (88, 26, 6), (84, 86, 5)):
            p2.sparkle(x, y, r)
        img = p2.img
    return img

def _two_tone(size, colors, alpha=255):
    """Заливка одним цветом или «наполовину/наполовину» (слева первый цвет, справа второй)."""
    img = Image.new("RGBA", size, tuple(colors[0]) + (alpha,))
    if len(colors) > 1:
        ImageDraw.Draw(img).rectangle((size[0] // 2, 0, size[0], size[1]), fill=tuple(colors[1]) + (alpha,))
    return img

def _colored_text(frame, xy, text, font, colors, off=2):
    """Текст цвета модификатора; двухцветный — левая половина одним цветом, правая другим."""
    d = ImageDraw.Draw(frame)
    # Слишком тёмные цвета (чёрная половина «Азартного») для текста слегка осветляем, чтобы читались
    colors = [c if (0.3 * c[0] + 0.59 * c[1] + 0.11 * c[2]) > 70 else _mix(c, (150, 150, 165), 0.6) for c in colors]
    if len(colors) == 1:
        _shadow_text(d, xy, text, font, tuple(colors[0]), off=off)
        return
    d.text((xy[0] + off, xy[1] + off), text, font=font, fill=(0, 0, 0), anchor="mm")
    mask = Image.new("L", frame.size, 0)
    ImageDraw.Draw(mask).text(xy, text, font=font, fill=255, anchor="mm")
    fill = Image.new("RGBA", frame.size, tuple(colors[0]) + (255,))
    ImageDraw.Draw(fill).rectangle((int(xy[0]), 0, frame.size[0], frame.size[1]), fill=tuple(colors[1]) + (255,))
    frame.paste(fill, (0, 0), mask)

@lru_cache(maxsize=160)
def get_icon(name, variant=None, px=ICON_PX, outline_colors=None):
    """Готовая иконка-стикер: обводка (тёмная или цвета модификатора) + мягкая тень, сглаженная до px."""
    img = _icon_raw(name, variant)
    alpha = img.getchannel("A")
    dil = alpha.filter(ImageFilter.MaxFilter(13 if outline_colors else 9))
    out = Image.new("RGBA", img.size, (0, 0, 0, 0))
    shadow = Image.new("RGBA", img.size, (0, 0, 0, 0))
    shadow.putalpha(dil.filter(ImageFilter.GaussianBlur(8)).point(lambda v: int(v * 0.55)))
    out.alpha_composite(ImageChops.offset(shadow, 0, 10))
    if outline_colors:
        outline = _two_tone(img.size, list(outline_colors))
    else:
        outline = Image.new("RGBA", img.size, (24, 20, 30, 0))
    outline.putalpha(dil)
    out.alpha_composite(outline)
    out.alpha_composite(img)
    return out.resize((px, px), Image.Resampling.LANCZOS)

# ---------- Плитки и кадры ----------

@lru_cache(maxsize=1)
def _background():
    bg = Image.new("RGB", (ANIM_W, ANIM_H), ANIM_BG_BOTTOM)
    grad = Image.linear_gradient("L").resize((ANIM_W, ANIM_H))
    top = Image.new("RGB", (ANIM_W, ANIM_H), ANIM_BG_TOP)
    bg = Image.composite(bg, top, grad)
    glow = Image.new("RGBA", (ANIM_W, ANIM_H), (0, 0, 0, 0))
    ImageDraw.Draw(glow).ellipse((ANIM_W * 0.2, ANIM_HEADER, ANIM_W * 0.8, ANIM_H + 40), fill=(90, 70, 140, 70))
    bg = bg.convert("RGBA")
    bg.alpha_composite(glow.filter(ImageFilter.GaussianBlur(40)))
    d = ImageDraw.Draw(bg)
    d.rectangle((0, 0, ANIM_W, ANIM_HEADER), fill=(18, 17, 24, 255))
    d.line((0, ANIM_HEADER, ANIM_W, ANIM_HEADER), fill=ANIM_GOLD + (255,), width=2)
    fy = ANIM_HEADER + ANIM_ROW_H
    d.rectangle((0, fy, ANIM_W, ANIM_H), fill=(18, 17, 24, 255))
    d.line((0, fy, ANIM_W, fy), fill=ANIM_GOLD + (255,), width=2)
    return bg

def _header(frame, title):
    d = ImageDraw.Draw(frame)
    f = _fit_font(d, title, ANIM_W - 40, 20, 12)
    _shadow_text(d, (ANIM_W / 2, ANIM_HEADER / 2 + 1), title, f, (255, 226, 140))

@lru_cache(maxsize=1)
def _overlay():
    overlay = Image.new("RGBA", (ANIM_W, ANIM_H), (0, 0, 0, 0))
    fade_w = 90
    fade = Image.new("L", (fade_w, ANIM_ROW_H), 0)
    fd = ImageDraw.Draw(fade)
    for x in range(fade_w):
        fd.line((x, 0, x, ANIM_ROW_H), fill=int(240 * (1 - x / fade_w) ** 1.6))
    left = Image.new("RGBA", (fade_w, ANIM_ROW_H), ANIM_BG_BOTTOM + (0,))
    left.putalpha(fade)
    overlay.alpha_composite(left, (0, ANIM_HEADER))
    overlay.alpha_composite(left.transpose(Image.Transpose.FLIP_LEFT_RIGHT), (ANIM_W - fade_w, ANIM_HEADER))
    cx = ANIM_W // 2
    glow = Image.new("RGBA", (ANIM_W, ANIM_H), (0, 0, 0, 0))
    fy = ANIM_HEADER + ANIM_ROW_H
    ImageDraw.Draw(glow).rectangle((cx - 4, ANIM_HEADER, cx + 4, fy), fill=ANIM_GOLD + (150,))
    overlay.alpha_composite(glow.filter(ImageFilter.GaussianBlur(5)))
    d = ImageDraw.Draw(overlay)
    d.rectangle((cx - 1, ANIM_HEADER + 4, cx + 1, fy - 4), fill=(255, 240, 180, 255))
    d.polygon([(cx - 10, ANIM_HEADER + 1), (cx + 10, ANIM_HEADER + 1), (cx, ANIM_HEADER + 14)], fill=ANIM_GOLD + (255,))
    d.polygon([(cx - 10, fy - 1), (cx + 10, fy - 1), (cx, fy - 14)], fill=ANIM_GOLD + (255,))
    return overlay

def _render_tile(tile):
    color = tile["color"]
    img = Image.new("RGBA", (TILE_W, TILE_H), (0, 0, 0, 0))
    grad = Image.linear_gradient("L").resize((TILE_W, TILE_H))
    top = Image.new("RGBA", (TILE_W, TILE_H), _mix((26, 26, 32), color, 0.34) + (255,))
    bottom = Image.new("RGBA", (TILE_W, TILE_H), _mix((20, 20, 26), color, 0.10) + (255,))
    body = Image.composite(bottom, top, grad)
    mask = Image.new("L", (TILE_W, TILE_H), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, TILE_W - 1, TILE_H - 1), radius=12, fill=255)
    img.paste(body, (0, 0), mask)
    mod_colors = tile.get("mod_colors")
    if mod_colors:
        # Предмет с модификатором ивента: свечение его цвета(ов)
        glow_mask = Image.new("L", (TILE_W, TILE_H), 0)
        ImageDraw.Draw(glow_mask).ellipse((6, 0, TILE_W - 6, 100), fill=200)
        glow_mask = ImageChops.multiply(glow_mask.filter(ImageFilter.GaussianBlur(12)), mask)
        glow = _two_tone((TILE_W, TILE_H), mod_colors)
        glow.putalpha(glow_mask)
    else:
        glow = Image.new("RGBA", (TILE_W, TILE_H), (0, 0, 0, 0))
        ImageDraw.Draw(glow).ellipse((16, 6, TILE_W - 16, 90), fill=color + (120,))
        glow = glow.filter(ImageFilter.GaussianBlur(12))
        glow.putalpha(ImageChops.multiply(glow.getchannel("A"), mask))
    img.alpha_composite(glow)
    icon = get_icon(tile["icon"][0], tile["icon"][1], ICON_PX, tuple(tuple(c) for c in mod_colors) if mod_colors else None)
    img.alpha_composite(icon, ((TILE_W - ICON_PX) // 2, 8))
    d = ImageDraw.Draw(img)
    if tile.get("badge"):
        ok = tile["badge"] == "ok"
        bx, by = TILE_W - 26, 62
        d.ellipse((bx, by, bx + 20, by + 20), fill=(50, 190, 80) if ok else (205, 45, 50), outline=(20, 20, 26), width=2)
        if ok:
            d.line([(bx + 5, by + 10), (bx + 9, by + 14), (bx + 15, by + 6)], fill=(255, 255, 255), width=3)
        else:
            d.line([(bx + 6, by + 6), (bx + 14, by + 14)], fill=(255, 255, 255), width=3)
            d.line([(bx + 14, by + 6), (bx + 6, by + 14)], fill=(255, 255, 255), width=3)
    label = tile.get("label", "")
    inner = TILE_W - 12
    if tile.get("big_label"):
        f = _fit_font(d, label, inner, 19, 11)
        _shadow_text(d, (TILE_W / 2, 108), label, f, (255, 255, 255))
        if tile.get("sub"):
            _shadow_text(d, (TILE_W / 2, 126), tile["sub"], _font(11), (200, 200, 210), off=1)
    elif label and mod_colors:
        # Модификатор отдельной строкой его цветом, ниже — название предмета
        mod_word, _, rest = label.partition(" ")
        _colored_text(img, (TILE_W / 2, 101), mod_word, _fit_font(d, mod_word, inner, 14, 9), mod_colors, off=1)
        f = _font(11)
        lines = _wrap(d, rest, f, inner, 2)
        y = 117 if len(lines) == 2 else 122
        for line in lines:
            _shadow_text(d, (TILE_W / 2, y), line, f, (240, 240, 245), off=1)
            y += 13
    elif label:
        f = _font(12)
        lines = _wrap(d, label, f, inner, 2)
        y = 106 if len(lines) == 2 else 112
        for line in lines:
            _shadow_text(d, (TILE_W / 2, y), line, f, (240, 240, 245), off=1)
            y += 15
    d.rounded_rectangle((3, TILE_H - 7, TILE_W - 4, TILE_H - 3), radius=2, fill=color)
    if mod_colors:
        border = Image.new("L", (TILE_W, TILE_H), 0)
        ImageDraw.Draw(border).rounded_rectangle((0, 0, TILE_W - 1, TILE_H - 1), radius=12, outline=255, width=3)
        img.paste(_two_tone((TILE_W, TILE_H), mod_colors), (0, 0), border)
    else:
        d.rounded_rectangle((0, 0, TILE_W - 1, TILE_H - 1), radius=12, outline=_mix(color, (0, 0, 0), 0.35), width=1)
    return img

def _star(draw, x, y, r, color):
    draw.polygon([(x, y - r), (x + r * 0.28, y - r * 0.28), (x + r, y), (x + r * 0.28, y + r * 0.28),
                  (x, y + r), (x - r * 0.28, y + r * 0.28), (x - r, y), (x - r * 0.28, y - r * 0.28)], fill=color)

def render_roulette_gif(strip):
    """strip = {"tiles", "win_index", "title", "result_text", "result_color"}.
    Возвращает (BytesIO, длительность прокрутки в секундах)."""
    tiles = strip["tiles"]
    win = strip["win_index"]
    step = TILE_W + TILE_GAP
    pad = ANIM_W // 2
    strip_img = Image.new("RGBA", (pad * 2 + len(tiles) * step, TILE_H), (0, 0, 0, 0))
    cache = {}
    tile_imgs = []
    for i, tile in enumerate(tiles):
        key = (tile["icon"], tile.get("label", ""), tile.get("sub", ""), tile["color"], tile.get("badge"), tile.get("big_label"), tuple(tuple(c) for c in (tile.get("mod_colors") or ())))
        if key not in cache:
            cache[key] = _render_tile(tile)
        tile_imgs.append(cache[key])
        strip_img.alpha_composite(cache[key], (pad + i * step, 0))

    bg = _background().copy()
    _header(bg, strip.get("title", ""))
    overlay = _overlay()
    row_y = ANIM_HEADER + (ANIM_ROW_H - TILE_H) // 2
    footer_cy = ANIM_HEADER + ANIM_ROW_H + ANIM_FOOTER / 2 + 1
    start = pad - TILE_GAP - TILE_W // 2 + step * 2
    jitter = random.uniform(-0.36, 0.36) * TILE_W
    final = pad + win * step + TILE_W / 2 + jitter - ANIM_W / 2

    def crop_at(off, blur_px):
        off = int(off)
        base = strip_img.crop((off, 0, off + ANIM_W, TILE_H))
        if blur_px < 4:
            return base
        # Размытие в движении только по горизонтали: сжимаем ширину и растягиваем обратно
        factor = min(10.0, 1 + blur_px / 7)
        small = base.resize((max(1, int(ANIM_W / factor)), TILE_H), Image.Resampling.BOX)
        return small.resize((ANIM_W, TILE_H), Image.Resampling.BILINEAR)

    rgb_frames, durations = [], []
    prev = start
    for k in range(ANIM_SPIN_FRAMES):
        t = k / (ANIM_SPIN_FRAMES - 1)
        eased = 1 - (1 - t) ** 4
        off = start + (final - start) * eased
        speed = abs(off - prev)
        prev = off
        frame = bg.copy()
        frame.alpha_composite(crop_at(off, speed * 0.6), (0, row_y))
        frame.alpha_composite(overlay)
        dots = "•" * (1 + (k // 4) % 3)
        _shadow_text(ImageDraw.Draw(frame), (ANIM_W / 2, footer_cy), dots, _font(20), (150, 140, 110), off=1)
        rgb_frames.append(frame.convert("RGB"))
        durations.append(40 if t < 0.6 else 50 if t < 0.85 else 70)

    # Финал: выигрыш увеличивается, остальное темнеет, разлетаются искры, выезжает плашка
    off = int(final)
    win_img = tile_imgs[win]
    wx = pad + win * step - off
    wcx, wcy = wx + TILE_W / 2, row_y + TILE_H / 2
    color = strip.get("result_color") or tiles[win]["color"]
    rnd = random.Random()
    particles = [(rnd.uniform(0, math.tau), rnd.uniform(70, 150), rnd.uniform(4, 8),
                  rnd.choice([ANIM_GOLD, color, (255, 255, 255)])) for _ in range(18)]
    result_text = strip.get("result_text", "")
    base_row = bg.copy()
    base_row.alpha_composite(strip_img.crop((off, 0, off + ANIM_W, TILE_H)), (0, row_y))
    for k in range(ANIM_REVEAL_FRAMES):
        f = (k + 1) / ANIM_REVEAL_FRAMES
        ease = 1 - (1 - f) ** 3
        frame = base_row.copy()
        frame.alpha_composite(Image.new("RGBA", (ANIM_W, ANIM_ROW_H - 2), (0, 0, 0, int(165 * ease))), (0, ANIM_HEADER + 1))
        halo = Image.new("RGBA", (ANIM_W, ANIM_H), (0, 0, 0, 0))
        r = 60 + 30 * ease
        ImageDraw.Draw(halo).ellipse((wcx - r * 1.2, wcy - r, wcx + r * 1.2, wcy + r), fill=color + (int(150 * ease),))
        frame.alpha_composite(halo.filter(ImageFilter.GaussianBlur(18)))
        # «выпрыгивание» с небольшим перелётом
        scale = 1 + 0.16 * (1 - math.cos(min(1, f * 1.3) * math.pi)) / 2 + (0.03 * math.sin(f * math.pi * 2) if f > 0.5 else 0)
        sw, sh = int(TILE_W * scale), int(TILE_H * scale)
        big = win_img.resize((sw, sh), Image.Resampling.LANCZOS)
        frame.alpha_composite(big, (int(wcx - sw / 2), int(wcy - sh / 2) - int(6 * ease)))
        d = ImageDraw.Draw(frame)
        d.rounded_rectangle((wcx - sw / 2 - 3, wcy - sh / 2 - 3 - 6 * ease, wcx + sw / 2 + 2, wcy + sh / 2 + 2 - 6 * ease),
                            radius=14, outline=ANIM_GOLD, width=3)
        for ang, dist, size, pc in particles:
            dd = dist * ease
            px, py = wcx + math.cos(ang) * dd, wcy + math.sin(ang) * dd * 0.7
            if ANIM_HEADER < py < ANIM_HEADER + ANIM_ROW_H:
                _star(d, px, py, size * (1 - 0.6 * f), pc)
        if result_text:
            # Плашка с результатом в нижней полосе: проявляется и чуть «вырастает»
            fy = ANIM_HEADER + ANIM_ROW_H
            band = Image.new("RGBA", (ANIM_W, ANIM_FOOTER - 2), color + (int(70 * ease),))
            frame.alpha_composite(band, (0, fy + 2))
            d.line((0, fy, ANIM_W, fy), fill=_mix(ANIM_GOLD, color, ease), width=2)
            tf = _fit_font(d, result_text, ANIM_W - 40, int(14 + 9 * ease), 11)
            if strip.get("result_mod_colors") and ease > 0.5:
                _colored_text(frame, (ANIM_W / 2, footer_cy), result_text, tf, strip["result_mod_colors"])
            else:
                _shadow_text(d, (ANIM_W / 2, footer_cy), result_text, tf, _mix((150, 140, 110), _mix(color, (255, 255, 255), 0.45), ease))
        rgb_frames.append(frame.convert("RGB"))
        durations.append(55)
    durations[-1] = ANIM_FINAL_MS

    # Общая палитра по нескольким реальным кадрам — без мерцания цветов
    samples = [rgb_frames[0], rgb_frames[ANIM_SPIN_FRAMES // 2], rgb_frames[ANIM_SPIN_FRAMES - 1], rgb_frames[-1]]
    palette_src = Image.new("RGB", (ANIM_W, ANIM_H * len(samples)))
    for i, sample in enumerate(samples):
        palette_src.paste(sample, (0, ANIM_H * i))
    palette = palette_src.quantize(colors=160, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE)
    frames = [fr.quantize(palette=palette, dither=Image.Dither.NONE) for fr in rgb_frames]

    buf = io.BytesIO()
    # loop не указываем: GIF проигрывается один раз и останавливается на результате
    frames[0].save(buf, format="GIF", save_all=True, append_images=frames[1:], duration=durations, disposal=1, optimize=False)
    buf.seek(0)
    spin_seconds = sum(durations[:-1]) / 1000
    return buf, spin_seconds

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

def _item_icon(name):
    return ITEM_ICONS.get(base_item_name(name), ("unknown", None))

def _mod_colors(key):
    mod_id = best_mod(key)
    return [tuple(c) for c in ITEM_MODIFIERS[mod_id]["colors"]] if mod_id else None

def _prize_tile(prize, total, value_override=None):
    value = prize["value"] if value_override is None else value_override
    share = prize["chance"] / total * 100 if total else 0
    if value == "rotten_leg":
        value = "Гнилая ножка KFC"
    elif value == "water":
        value = "Стакан воды"
    color = _rarity_by_share(share)
    if isinstance(value, int):
        if value < 0:
            return {"icon": ("minus", None), "label": f"{value} кг", "big_label": True, "color": RARITY_COLORS["minus"]}
        if value == 0:
            return {"icon": ("empty", None), "label": "Пусто", "big_label": True, "color": RARITY_COLORS["empty"]}
        icon = ("kettlebell_gold", None) if value >= 1000 else ("kettlebell", None)
        return {"icon": icon, "label": f"+{value} кг", "big_label": True, "color": color}
    return {"icon": _item_icon(value), "label": str(value), "color": color, "mod_colors": _mod_colors(str(value))}

def _prize_result_text(prize, value_override=None):
    value = prize["value"] if value_override is None else value_override
    if value == "rotten_leg":
        return "ГНИЛАЯ НОЖКА KFC"
    if value == "water":
        return "СТАКАН ВОДЫ"
    if isinstance(value, int):
        return f"{value:+d} КГ" if value else "ПУСТО"
    return str(value).upper()

def build_case_strip(case_id, winner_prize, winner_value=None):
    case = CASES[case_id]
    prizes = case["prizes"]
    total = sum(p["chance"] for p in prizes)
    picks = random.choices(prizes, weights=[p["chance"] for p in prizes], k=STRIP_LEN)
    tiles = [_prize_tile(p, total) for p in picks]
    tiles[WIN_INDEX] = _prize_tile(winner_prize, total, winner_value)
    win_mod = tiles[WIN_INDEX].get("mod_colors")
    return {"tiles": tiles, "win_index": WIN_INDEX, "title": case["name"].upper(),
            "result_text": _prize_result_text(winner_prize, winner_value), "result_color": (win_mod or [tiles[WIN_INDEX]["color"]])[0],
            "result_mod_colors": win_mod}

def build_upgrade_strip(success, chance, target_name, lose_text="сгорело"):
    win_tile = {"icon": _item_icon(target_name), "label": target_name, "color": RARITY_COLORS["success"], "badge": "ok",
                "mod_colors": _mod_colors(target_name) if success else None}
    plain_win = dict(win_tile, label=base_item_name(target_name), mod_colors=None)
    lose_tile = {"icon": ("fail", None), "label": "ПРОВАЛ", "sub": lose_text, "big_label": True, "color": RARITY_COLORS["fail"]}
    visual = min(max(chance, 0.08), 0.92)
    # На ленте — обычный предмет, модификатор виден только у выигрыша
    tiles = [plain_win if random.random() < visual else lose_tile for _ in range(STRIP_LEN)]
    tiles[WIN_INDEX] = win_tile if success else lose_tile
    return {"tiles": tiles, "win_index": WIN_INDEX, "title": f"АПГРЕЙД  •  ШАНС {chance * 100:.1f}%",
            "result_text": f"УСПЕХ! {target_name.upper()}" if success else f"ПРОВАЛ — {lose_text.upper()}",
            "result_color": RARITY_COLORS["success"] if success else RARITY_COLORS["fail"],
            "result_mod_colors": win_tile["mod_colors"]}

def build_duel_strip(result, challenger_name, opponent_name):
    options = [
        {"icon": ("crown", None), "label": challenger_name, "big_label": True, "sub": "побеждает", "color": RARITY_COLORS["legendary"]},
        {"icon": ("crown_silver", None), "label": opponent_name, "big_label": True, "sub": "побеждает", "color": RARITY_COLORS["silver"]},
        {"icon": ("swords", None), "label": "НИЧЬЯ", "big_label": True, "sub": "все при своих", "color": RARITY_COLORS["draw"]},
    ]
    tiles = [random.choice(options) for _ in range(STRIP_LEN)]
    tiles[WIN_INDEX] = options[result]
    result_text = "НИЧЬЯ!" if result == 2 else f"ПОБЕДА: {(challenger_name, opponent_name)[result]}"
    return {"tiles": tiles, "win_index": WIN_INDEX, "title": f"ДУЭЛЬ:  {challenger_name}  vs  {opponent_name}",
            "result_text": result_text, "result_color": options[result]["color"]}

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
    luck = player_luck(get_user_items(data['item_counts']))
    base = target_item['chance']
    real = min((base * prestige_bonus * luck_bonus + shadow / 100) * luck, 1.0)
    display = min(base * prestige_bonus * luck_bonus * luck, 1.0)
    success = random.random() < real
    new_shadow = max(0, shadow - 8) if success else min(32, shadow + 4)
    return success, new_shadow, display, luck

async def upgrade_animation(ctx, member, source_item, target_item, item_count, prestige_luck=0, luck_upgrade=0, animations_enabled=True):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    success, new_shadow, display, luck = roll_upgrade(guild_id, user_id, member.name, target_item, prestige_luck, luck_upgrade)
    # Полученный предмет тоже может выпасть с модификатором ивента (бросаем до анимации)
    won_key = make_item_key(target_item['name'], roll_modifier(luck)) if success else target_item['name']
    source_emoji = item_emoji(source_item)
    anim_embed = discord.Embed(title="🔧 АПГРЕЙД 🔧", description=f"**{member.display_name}** улучшает:\n{source_emoji} {format_item(source_item)} → {target_item['emoji']} **{target_item['name']}**\n\nШанс: {display*100:.1f}%", color=0xff5500)
    await show_roulette(ctx, anim_embed, build_upgrade_strip(success, display, won_key, "предмет утерян"), "upgrade.gif", animations_enabled)
    cur_data = get_user_data(guild_id, user_id, member.name)
    items = get_user_items(cur_data['item_counts'])
    if success:
        items[won_key] = items.get(won_key, 0) + 1
        update_user_data(guild_id, user_id, item_counts=save_user_items(items), shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        desc = f"✅ Поздравляем!\n\n{source_emoji} {format_item(source_item)} → {target_item['emoji']} {format_item(won_key)}\n\nПредмет успешно улучшен!"
        lvl_gained, kg_reward, new_lvl = add_xp(guild_id, user_id, XP_PER_UPGRADE)
        if lvl_gained > 0:
            desc += f"\n\n⭐ ПОВЫШЕНИЕ УРОВНЯ! +{kg_reward} кг! Теперь у вас {new_lvl} уровень!"
        await update_user_nick(guild_id, user_id, member.name)
    else:
        update_user_data(guild_id, user_id, shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        desc = f"❌ Неудача!\n\n{source_emoji} {format_item(source_item)} был утерян в процессе улучшения!"
    log_user_action(user_id, member.name, "Апгрейд предмета", f"{source_item} -> {won_key}: {'успех' if success else 'провал'}")
    result_embed = discord.Embed(title="🔧 РЕЗУЛЬТАТ АПГРЕЙДА 🔧", description=f"{'✅ УСПЕХ! ✅' if success else '❌ НЕУДАЧА! ❌'}\n\n{desc}", color=0x00ff00 if success else 0xff0000)
    result_embed.set_footer(text=f"Шанс был: {display*100:.1f}%")
    await ctx.send(embed=result_embed)

async def upgrade_kg_animation(ctx, member, amount, target_item, prestige_luck=0, luck_upgrade=0, animations_enabled=True):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    success, new_shadow, display, luck = roll_upgrade(guild_id, user_id, member.name, target_item, prestige_luck, luck_upgrade)
    won_key = make_item_key(target_item['name'], roll_modifier(luck)) if success and not target_item.get("is_case") else target_item['name']
    anim_embed = discord.Embed(title="💱 АПГРЕЙД КГ 💱", description=f"**{member.display_name}** улучшает {amount} кг в:\n{target_item['emoji']} **{target_item['name']}**\n\nШанс: {display*100:.1f}%", color=0xff5500)
    await show_roulette(ctx, anim_embed, build_upgrade_strip(success, display, won_key, f"{amount} кг сгорели"), "upgrade.gif", animations_enabled)
    cur_data = get_user_data(guild_id, user_id, member.name)
    if success:
        if target_item.get("is_case", False):
            cases = cur_data.get('cases_dict', {}).copy()
            cases[target_item["case_id"]] = cases.get(target_item["case_id"], 0) + 1
            update_user_data(guild_id, user_id, cases_dict=cases, shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        else:
            items = get_user_items(cur_data['item_counts'])
            items[won_key] = items.get(won_key, 0) + 1
            update_user_data(guild_id, user_id, item_counts=save_user_items(items), shadow_upgrade_chance=new_shadow, **UPGRADE_RESET)
        desc = f"✅ Поздравляем!\n\n{amount} кг → {target_item['emoji']} {format_item(won_key)}\n\nПредмет успешно получен!"
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
    for key, cnt in items.items():
        base = base_item_name(key)
        for si in SHOP_ITEMS:
            if si["name"] == base:
                total += si.get("gain_per_24h", 0) * cnt * (1 + modifier_bonus(key, "production"))
                break
    return int(total)

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
        if base_counts(items).get("Снатчер", 0) == 0:
            return
        now = datetime.now()
        last = to_datetime(data.get('snatcher_last_time'))
        if last and (now - last).total_seconds() < 21600:
            return
        if random.random() > min(1.0, 0.2 * player_luck(items)):
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
        got = grant_item(items, sel["name"])
        update_user_data(guild_id, user_id, item_counts=save_user_items(items), snatcher_last_time=now)
        log_user_action(user_id, user_name, "Снатчер сработал", f"+1 {list(got)[0]} (слот {chosen_slot+1})")
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
                        if base_counts(get_user_items(items_str)).get("Снатчер", 0) > 0:
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
                            counts = base_counts(items)
                            if not counts.get("Автохолестерол") and not counts.get("Холестеринимус"):
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
                            hourly_ranges = {"Автохолестерол": 10, "Холестеринимус": 5}
                            for key, cnt in items.items():
                                top = hourly_ranges.get(base_item_name(key))
                                if not top:
                                    continue
                                mult = 1 + modifier_bonus(key, "production")
                                total += sum(random.randint(1, top) for _ in range(cnt * hours)) * mult
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
        has_water = base_counts(items_dict).get("Стакан воды", 0) > 0
        luck = player_luck(items_dict)

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
                prize = open_case(case_id, prestige_luck, luck_upgrade, case_plus_bonus, luck)
                prize_value = prize["value"]
                opened_count += 1
                if isinstance(prize_value, str):
                    # Предметы из кейсов тоже могут получить модификатор ивента
                    grant_item(items_dict, {"rotten_leg": "Гнилая ножка KFC", "water": "Стакан воды"}.get(prize_value, prize_value))
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
        prize = open_case(case_to_open, prestige_luck, luck_upgrade, case_plus_bonus, player_luck(get_user_items(current_data['item_counts'])))
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
        prize_key = None
        if isinstance(prize["value"], int):
            pre_items = get_user_items(current_data['item_counts'])
            preview_value = apply_case_prize_bonuses(prize["value"], get_prestige_bonus(current_data.get('prestige', 0)),
                                                     base_counts(pre_items).get("Стакан воды", 0) > 0, case_to_open)
        else:
            # Модификатор бросаем до анимации, чтобы рулетка показала его цвет
            base_prize = {"rotten_leg": "Гнилая ножка KFC", "water": "Стакан воды"}.get(prize["value"], prize["value"])
            prize_key = make_item_key(base_prize, roll_modifier(player_luck(get_user_items(current_data['item_counts']))))
            preview_value = prize_key
        anim_embed = discord.Embed(title=f"🎰 **{case['name']}** 🎰", description=f"{member.mention} открывает кейс...", color=0xffaa00)
        await show_roulette(ctx, anim_embed, build_case_strip(case_to_open, prize, preview_value), "case.gif",
                            are_animations_enabled(current_data), message=case_msg)
        current_data = get_user_data(guild_id, user_id, user_name)
        items_dict = get_user_items(current_data['item_counts'])
        prize_value = prize["value"]
        prestige_bonus = get_prestige_bonus(current_data.get('prestige', 0))
        has_water = base_counts(items_dict).get("Стакан воды", 0) > 0
        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_CASE)
        after_xp_data = get_user_data(guild_id, user_id, user_name)
        after_xp_weight = after_xp_data['current_number']
        if prize_key:
            items_dict[prize_key] = items_dict.get(prize_key, 0) + 1
            new_number = after_xp_weight
            mod_id = best_mod(prize_key)
            if mod_id:
                mod = ITEM_MODIFIERS[mod_id]
                result_display = f"{mod['emoji']} **{prize_key}** {prize_emoji}\n✨ *Модификатор ивента: {modifier_effects_text(mod_id)}*"
                result_color = discord.Color.from_rgb(*mod["colors"][0])
            elif prize_value == "rotten_leg":
                result_display = f"💀 **Гнилая ножка KFC!** 💀"
                result_color = 0x993366
            elif prize_value == "water":
                result_display = f"💧 **Стакан воды!** 💧"
                result_color = 0x66ccff
            else:
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
        
        # С учётом модификаторов ивентов (+% производства)
        total_passive_income = calc_daily_item_income(items_dict)

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

        # Самый дорогой предмет сейчас и рекорд за всё время
        best_now = most_expensive_item(items_dict)
        best_name, best_price = data.get('best_item_name'), data.get('best_item_price') or 0
        if best_now and best_now[1] > best_price:
            best_name, best_price = best_now
        if best_now or best_name:
            now_text = f"{item_emoji(best_now[0])} {format_item(best_now[0])} — **{best_now[1]} кг**" if best_now else "инвентарь пуст"
            ever_text = f"{item_emoji(best_name)} {format_item(best_name)} — **{best_price} кг**" if best_name else "—"
            embed.add_field(name="💎 **ЦЕННОСТИ**", value=f"Самый дорогой сейчас: {now_text}\nРекорд за всё время: {ever_text}", inline=False)

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
    # Для старых игроков рекорд ещё не записан — заполняем по текущему инвентарю
    best_now = most_expensive_item(get_user_items(data['item_counts']))
    if best_now and best_now[1] > (data.get('best_item_price') or 0):
        update_user_data(guild_id, user_id, best_item_name=best_now[0], best_item_price=best_now[1])
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
                           f"• Кейсы исчезнут, а **предметы сохранятся**\n"
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
            # Кейсы обнуляем явно, предметы при престиже теперь сохраняются
            empty_cases = {cid: 0 for cid in CASES if cid != "daily"}
            update_user_data(
                guild_id, user_id,
                number=0,
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
                         f"Вес сброшен до 0\nКейсы удалены\nПредметы, улучшения, опыт и уровень **СОХРАНЕНЫ**!",
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
    available_items = [{"name": item_name, "count": count, "price": get_item_price(item_name), "emoji": item_emoji(item_name)} for item_name, count in items_dict.items() if count > 0 and get_possible_upgrades(item_name, count)]
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
    _, event = get_active_event()
    if event:
        embed.description += f"\n\n{event['emoji']} **Идёт ивент «{event['name']}»** — купленные предметы могут получить модификатор! (`!ивент`)"
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
        # Каждая купленная штука бросает свой модификатор ивента
        got = grant_item(items_dict, item["name"], amount)
        update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict))
        purchase_desc = f"{item.get('emoji', '📦')} {describe_got(got)}"
        if any(parse_item_key(k)[0] for k in got):
            purchase_desc += "\n✨ Выпал модификатор ивента!"
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
        embed = discord.Embed(title="❌ Использование", description="`!продать [название предмета] [количество]`\n`!продать всё` - продать всё сразу (кроме предметов с модификаторами)\n\n💰 **Цена продажи: 70% от стоимости предмета**", color=0xff0000)
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
            if parse_item_key(item_name)[0]:
                continue  # предметы с модификаторами «всё» не продаёт — только по названию
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
        # По частичному названию сначала ищем обычные предметы, чтобы случайно не продать редкий ивентовый
        matches = [key for key in items_dict.keys() if item_name.lower() in key.lower()]
        matches.sort(key=lambda k: bool(parse_item_key(k)[0]))
        found_item = matches[0] if matches else None
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
        event_items = []
        for item_name, count in items_dict.items():
            if parse_item_key(item_name)[0]:
                event_items.append((item_name, count))
            elif item_name in ["Снатчер", "Святой сэндвич", "Гнилая ножка KFC", "Стакан воды", "Автохолестерол", "Холестеринимус", "Яблоко", "Золотое Яблоко", "Апельсин", "Золотой Апельсин", "Драгонфрукт", "Золотой Драгонфрукт"]:
                legendary_items.append(f"• {item_name}: {count} шт")
            else:
                regular_items.append(f"• {item_name}: {count} шт")
        if regular_items:
            items_text += "**Обычные предметы:**\n" + "\n".join(regular_items[:8]) + "\n"
            if len(regular_items) > 8:
                items_text += f"... и ещё {len(regular_items) - 8} предметов\n"
        if legendary_items:
            items_text += "**✨ Легендарные предметы:**\n" + "\n".join(legendary_items)
        if items_text:
            embed.add_field(name="📦 Предметы", value=items_text[:1024], inline=False)
        if event_items:
            # Цветные названия через ```ansi (цвета видны в Discord на ПК)
            event_items.sort(key=lambda kv: ITEM_MODIFIERS[best_mod(kv[0])]["chance"])
            lines = []
            for key, count in event_items:
                line = f"{ansi_item(key)} x{count}"
                if sum(len(l) + 1 for l in lines) + len(line) > 950:
                    lines.append(f"... и ещё {len(event_items) - len(lines)}")
                    break
                lines.append(line)
            embed.add_field(name="✨ Предметы с модификаторами", value="```ansi\n" + "\n".join(lines) + "\n```", inline=False)
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
    embed.add_field(name="🎉 **ИВЕНТЫ И МОДИФИКАТОРЫ**", value="`!ивент` - текущий праздник и его модификаторы\n`!мод [номер/название]` - накрутить модификатор на предмет (¼ стоимости)\n`!моды` - список всех модификаторов", inline=False)
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
    # Можно выдать и с модификатором ивента: "!выдатьпредмет 1 Холодная Пицца"
    found_key = normalize_item_key(item_name)
    if not found_key:
        await ctx.send(f"❌ Предмет '{item_name}' не найден!")
        return
    items_dict = get_user_items(data['item_counts'])
    items_dict[found_key] = items_dict.get(found_key, 0) + amount
    update_user_data(ctx.guild.id, str(ctx.author.id), item_counts=save_user_items(items_dict))
    embed = discord.Embed(title="🎁 Выдача предмета", description=f"**{ctx.author.mention}** выдал себе предмет!", color=0xffaa00)
    embed.add_field(name="📦 Предмет", value=f"{format_item(found_key)} x{amount}", inline=True)
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

def _event_dates_text(event):
    (sm, sd), (em, ed) = event["start"], event["end"]
    return f"{sd:02d}.{sm:02d} – {ed:02d}.{em:02d}"

def event_modifiers_field(event):
    lines = []
    for mod_id in event["modifiers"]:
        m = ITEM_MODIFIERS[mod_id]
        sample = ansi_text(m["forms"][0], mod_id)
        lines.append(f"{sample} — {chance_text(m['chance'])}\n  {modifier_effects_text(mod_id)}")
    return "```ansi\n" + "\n".join(lines) + "\n```"

@bot.command(name='ивент')
async def event_command(ctx, action: str = None, event_id: str = None):
    # Управление (только Высший тестер): !ивент включить <id> | !ивент выключить | !ивент авто
    if action:
        if not has_high_tester_role(ctx.author):
            await ctx.send(f"❌ Управлять ивентами может только **{HIGH_TESTER_ROLE_NAME}**")
            return
        action = action.lower()
        if action == "включить":
            if event_id not in HOLIDAY_EVENTS:
                ids = ", ".join(f"`{eid}` ({e['name']})" for eid, e in HOLIDAY_EVENTS.items())
                await ctx.send(f"❌ Укажите ивент: {ids}")
                return
            set_event_override(event_id)
            await ctx.send(f"✅ Ивент **{HOLIDAY_EVENTS[event_id]['name']}** включён принудительно (`!ивент авто` — вернуть по датам)")
        elif action == "выключить":
            set_event_override("off")
            await ctx.send("✅ Все ивенты выключены (`!ивент авто` — вернуть по датам)")
        elif action == "авто":
            set_event_override("auto")
            await ctx.send("✅ Ивенты снова включаются по датам праздников")
        else:
            await ctx.send("❌ Используйте: `!ивент включить <id>`, `!ивент выключить`, `!ивент авто`")
        log_user_action(ctx.author.id, ctx.author.name, "Управление ивентом", f"{action} {event_id or ''}")
        return

    active_id, event = get_active_event()
    if event:
        embed = discord.Embed(
            title=f"{event['emoji']} ИВЕНТ: {event['name'].upper()} {event['emoji']}",
            description="Каждый полученный предмет (магазин, кейсы, апгрейды, снатчер) может получить **модификатор**!\n"
                        "Модификатор пишется перед названием и усиливает предмет.",
            color=discord.Color.from_rgb(*ITEM_MODIFIERS[event["modifiers"][0]]["colors"][0]))
        embed.add_field(name="✨ Модификаторы", value=event_modifiers_field(event), inline=False)
        embed.add_field(name="📅 Даты", value=_event_dates_text(event), inline=True)
    else:
        next_id, next_event, start = get_next_event()
        days = (start.date() - datetime.now().date()).days
        embed = discord.Embed(title="📅 Сейчас ивентов нет",
                              description=f"Следующий: {next_event['emoji']} **{next_event['name']}** — через {days} дн. ({_event_dates_text(next_event)})",
                              color=0x95a5a6)
        embed.add_field(name="✨ Его модификаторы", value=event_modifiers_field(next_event), inline=False)
    embed.add_field(name="ℹ️ Как работают", value="• +% производства — больше кг от предмета\n"
                                                  "• +% стоимости — дороже продажа и выше шанс апгрейда\n"
                                                  "• -% КД — действует лучший модификатор из инвентаря (не складываются)\n"
                                                  "• Во время ивента они добавляются в `!мод` к обычным модификаторам\n"
                                                  "• `!продать всё` не продаёт предметы с модификаторами", inline=False)
    all_events = "\n".join(f"{e['emoji']} {e['name']} — {_event_dates_text(e)}" for e in HOLIDAY_EVENTS.values())
    embed.add_field(name="🗓️ Все ивенты", value=all_events, inline=False)
    embed.set_footer(text="Цветные названия видны в Discord на компьютере")
    await ctx.send(embed=embed)

XP_PER_MOD = 25

def build_mod_strip(item_key, won_mod, pool):
    """Рулетка !мод: плитки с модификаторами из пула и «пусто»."""
    base = base_item_name(item_key)
    icon = _item_icon(base)

    def mod_tile(m):
        mod = ITEM_MODIFIERS[m]
        return {"icon": icon, "label": f"{modifier_form(m, base)} {base}", "color": _rarity_by_share(mod["chance"] * 100),
                "mod_colors": [tuple(c) for c in mod["colors"]]}

    empty = {"icon": ("empty", None), "label": "ПУСТО", "sub": "без модификатора", "big_label": True, "color": RARITY_COLORS["empty"]}
    total = sum(ITEM_MODIFIERS[m]["chance"] for m in pool)
    # Для зрелищности редкие показываем на ленте чаще, чем они реально выпадают (вес = корень шанса)
    options = [mod_tile(m) for m in pool] + [empty]
    weights = [ITEM_MODIFIERS[m]["chance"] ** 0.5 for m in pool] + [max(0.05, 1 - total) ** 0.5]
    tiles = random.choices(options, weights=weights, k=STRIP_LEN)
    tiles[WIN_INDEX] = mod_tile(won_mod) if won_mod else empty
    if won_mod:
        result_text = f"{modifier_form(won_mod, base).upper()}!  ({chance_text(ITEM_MODIFIERS[won_mod]['chance'])})"
        result_colors = [tuple(c) for c in ITEM_MODIFIERS[won_mod]["colors"]]
    else:
        result_text = "НИЧЕГО НЕ ВЫПАЛО"
        result_colors = None
    return {"tiles": tiles, "win_index": WIN_INDEX, "title": f"МОДИФИКАЦИЯ  •  {base.upper()}",
            "result_text": result_text, "result_color": result_colors[0] if result_colors else RARITY_COLORS["empty"],
            "result_mod_colors": result_colors}

def _mod_pool_title():
    _, event = get_active_event()
    if event:
        return f"обычные + {event['emoji']} ивентовые «{event['name']}»"
    return "обычные модификаторы"

def _find_item_key(items_dict, text):
    exact = normalize_item_key(text)
    if exact and items_dict.get(exact, 0) > 0:
        return exact
    low = text.strip().lower()
    for key in items_dict:
        if key.lower() == low and items_dict[key] > 0:
            return key
    matches = [k for k, c in items_dict.items() if c > 0 and low in k.lower()]
    matches.sort(key=lambda k: (len(item_mods(k)), k))  # сначала предмет без модификаторов
    return matches[0] if matches else None

@bot.command(name='мод')
async def mod_command(ctx, *, choice: str = None):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    data = get_user_data(guild_id, user_id, member.name)
    items_dict = get_user_items(data['item_counts'])
    available = sorted([(k, c) for k, c in items_dict.items() if c > 0], key=lambda kv: (get_item_price(kv[0]), kv[0]))
    if not available:
        await ctx.send("❌ У вас нет предметов для модификации!")
        return

    if choice is None:
        embed = discord.Embed(
            title="✨ **МОДИФИКАЦИЯ ПРЕДМЕТОВ** ✨",
            description=f"{member.mention}, накрутите модификатор на предмет за **¼ его стоимости**.\n"
                        f"Используйте `!мод [номер]` или `!мод [название]`.\n\n"
                        f"Сейчас выпадают: **{_mod_pool_title()}**\n"
                        f"На предмете может быть до **{MAX_ITEM_MODIFIERS}** разных модификаторов. Список — `!моды`",
            color=0x9b59b6)
        lines = []
        for i, (key, count) in enumerate(available[:15], 1):
            mods_n = len(item_mods(key))
            lines.append(f"**{i}.** {item_emoji(key)} {format_item(key)} — {count} шт — **{mod_cost(key)} кг** — {mods_n}/{MAX_ITEM_MODIFIERS}")
        text = "\n".join(lines)
        if len(available) > 15:
            text += f"\n... и ещё {len(available) - 15} предметов (укажите название)"
        embed.add_field(name="📦 Ваши предметы (цена модификации)", value=text[:1024], inline=False)
        await ctx.send(embed=embed)
        return

    key = None
    if choice.strip().isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(available):
            key = available[idx][0]
    else:
        key = _find_item_key(items_dict, choice)
    if not key:
        await ctx.send(f"❌ Предмет не найден! Посмотрите список: `!мод`")
        return
    mods = item_mods(key)
    if len(mods) >= MAX_ITEM_MODIFIERS:
        await ctx.send(f"❌ На {format_item(key)} уже {MAX_ITEM_MODIFIERS} модификаторов — это максимум!")
        return
    pool = mod_pool_for(key)
    if not pool:
        await ctx.send(f"❌ На {format_item(key)} уже стоят все {_mod_pool_title()}, которые можно получить сейчас!")
        return
    cost = mod_cost(key)
    if data['current_number'] < cost:
        await ctx.send(f"❌ Недостаточно кг! Модификация {format_item(key)} стоит **{cost} кг**, у вас: **{data['current_number']} кг**")
        return

    # Удача считается по инвентарю до того, как предмет забрали на модификацию
    luck = player_luck(items_dict)
    old_price = get_item_price(key)
    # Сразу списываем кг и забираем 1 шт. предмета, чтобы нельзя было потратить его дважды
    items_dict[key] -= 1
    update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict))
    add_user_weight(guild_id, user_id, -cost)
    won = _roll_from_pool(pool, luck)
    base = base_item_name(key)
    new_key = make_item_key(base, list(mods) + [won]) if won else key

    anim_embed = discord.Embed(title="✨ МОДИФИКАЦИЯ ✨",
                               description=f"**{member.display_name}** модифицирует {item_emoji(key)} {format_item(key)}\nЦена: **{cost} кг**",
                               color=0x9b59b6)
    await show_roulette(ctx, anim_embed, build_mod_strip(key, won, pool), "mod.gif", are_animations_enabled(data))

    cur = get_user_data(guild_id, user_id, member.name)
    cur_items = get_user_items(cur['item_counts'])
    cur_items[new_key] = cur_items.get(new_key, 0) + 1
    update_user_data(guild_id, user_id, item_counts=save_user_items(cur_items))
    levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_MOD)
    await update_user_nick(guild_id, user_id, member.name)
    log_user_action(member.id, member.name, "Модификация", f"{key} -> {new_key} за {cost} кг")

    if won:
        mod = ITEM_MODIFIERS[won]
        embed = discord.Embed(title=f"{mod['emoji']} ВЫПАЛ МОДИФИКАТОР! {mod['emoji']}",
                              description=f"{member.mention} получил {mod['emoji']} **{modifier_form(won, base)}** ({chance_text(mod['chance'])})!\n\n"
                                          f"Теперь: {item_emoji(new_key)} {format_item(new_key)}",
                              color=discord.Color.from_rgb(*mod["colors"][0]))
        embed.add_field(name="✨ Эффект модификатора", value=modifier_effects_text(won) or "—", inline=False)
        embed.add_field(name="💎 Стоимость предмета", value=f"{old_price} → **{get_item_price(new_key)} кг**", inline=True)
        embed.add_field(name="🔢 Модификаторов", value=f"{len(item_mods(new_key))}/{MAX_ITEM_MODIFIERS}", inline=True)
    else:
        embed = discord.Embed(title="💨 Ничего не выпало",
                              description=f"{member.mention}, модификатор не выпал. {format_item(key)} остался без изменений.",
                              color=0x7f8c8d)
    embed.add_field(name="💸 Потрачено", value=f"{cost} кг", inline=True)
    if levels_gained > 0:
        embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", value=f"+{kg_reward} кг! Теперь у вас **{new_level}** уровень!", inline=False)
    embed.set_footer(text="Ещё раз: !мод " + new_key)
    await ctx.send(embed=embed)

def _mods_lines(mod_ids):
    lines = []
    for m in mod_ids:
        mod = ITEM_MODIFIERS[m]
        lines.append(f"{ansi_text(mod['forms'][0], m)} {chance_text(mod['chance'])}\n  {modifier_effects_text(m)}")
    return lines

def _add_ansi_fields(embed, title, lines):
    chunk = []
    part = 0
    for line in lines + [None]:
        if line is None or sum(len(x) + 1 for x in chunk) + len(line) > 960:
            if chunk:
                embed.add_field(name=title if part == 0 else f"{title} (продолжение)", value="```ansi\n" + "\n".join(chunk) + "\n```", inline=False)
                part += 1
            chunk = []
        if line is not None:
            chunk.append(line)

@bot.command(name='моды')
async def mods_list_command(ctx):
    regular = sorted([m for m, v in ITEM_MODIFIERS.items() if v["kind"] == "regular"], key=lambda m: -ITEM_MODIFIERS[m]["chance"])
    embed = discord.Embed(title="✨ **МОДИФИКАТОРЫ ПРЕДМЕТОВ** ✨",
                          description=f"`!мод` — накрутить модификатор за ¼ стоимости предмета. До {MAX_ITEM_MODIFIERS} разных на предмет, эффекты складываются.\n"
                                      "Обычные выпадают всегда, а во время праздника к ним добавляются **ивентовые** (они же сами падают на любые предметы).\n"
                                      "КД, джекпот, опыт и удача от разных предметов не складываются — работает лучший.\n"
                                      f"Сейчас: **{_mod_pool_title()}**",
                          color=0x9b59b6)
    tiers = [("⚪ Обычные", lambda c: c >= 0.03), ("🔵 Редкие", lambda c: 0.005 <= c < 0.03),
             ("🟣 Очень редкие", lambda c: 0.0005 <= c < 0.005), ("🟡 Ультраредкие", lambda c: c < 0.0005)]
    for title, cond in tiers:
        _add_ansi_fields(embed, title, _mods_lines([m for m in regular if cond(ITEM_MODIFIERS[m]["chance"])]))
    event_lines = []
    for event in HOLIDAY_EVENTS.values():
        event_lines.append(f"{event['emoji']} {event['name']}: " + ", ".join(
            f"{ITEM_MODIFIERS[m]['forms'][0]} {chance_text(ITEM_MODIFIERS[m]['chance'])}" for m in event["modifiers"]))
    embed.add_field(name="🎉 Ивентовые (подробнее — `!ивент`)", value="\n".join(event_lines), inline=False)
    embed.set_footer(text="Цветные названия видны в Discord на компьютере")
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

# Встроенный шрифт для GIF-анимаций: DejaVu Sans Bold (свободная лицензия DejaVu/Bitstream Vera),
# только латиница, кириллица и цифры, сжато zlib + base64 (~27 КБ).
EMBEDDED_FONT_B64 = (
    "eNrtvQlcVFX7OH7OXWZwVPZF9hkGGBARAQF3RRE3FlPE3QBhZBFQFhU3RDMzUzOXFK2UyMzUlMzS3M3cUvM1NHPLfM1KM+stV+bM"
    "7znn3jsMiNX7fd/e9/1/Pn+vd+69557lOc92nvM851wQRgjZolmIR2l94nrH86uFYQgteQlS/foMTB5cMXRhGjxvRcg5sM/gIT0N"
    "1d0PIfTWl/B+RfLgsIjNR92mIISz4Tl1bH76hPFZP3ggZFMHz7vHpRdPQGo40JJZ8Nxi3PgyY8/s7QkItYQ6R+7OzkrP9ElOnQjv"
    "bsMZnQ0Jti42t6A+f3j2z84vmfLqG7sPwvN+hNSdxheOTf9Jp+qCULOPEOI35KdPmYC/RivgfV/Iry1Iz8+qePiWDUI5XggFxU0o"
    "LC4xV6AR0H4afY9oX7nhq767slj7rF2X35AvZIV/Z2baXqTXbx+8WVg3l7hptOqpkFd6Sf9BOXU+8UZIsxreZ2ukmqz+4eM0BZ9F"
    "SUhEvZHQ6D2POISF5dxueIvESHEV5PWRrvx5ZOQcAazmNjyvEjhOmIU4wFgPS+sDjb0zAXZtoErlTJxxpTofX4fkV6W3wkkofwzq"
    "WY99hRB0SLUdLRCXoLU2AuqpeoSmccXoEL8BbYWzWghDwfD+Aq9B+fwgVAjXMxxgHvJnwHkTzio458M5Cs7X4SyXn2fAmctHohtw"
    "VtA6lFOoRgsB1HmqcKRRuaPd4lVkVFXDdbJ0qpbAcw3azT2ip3mhKhLSIZ/6HryDdBXAL16WrqpgeHcILRVLoa5ySIM6bc6irqoQ"
    "ZBBPmG+LJ9Eo2hcKM1wXQPunBYoDhMaIRjRM3IU2CXvZdZSYj4bx06Acva9Bm7i99DTvFgdJ9zapaCNNF8ulcjQf/wuUPwT9PIs8"
    "4d1aMQb5qjNRXzEY+cK9u7CB1oURdxsL9Er7r+Ae8LOF4ckN9YerO80DcNnCedgGYU9hDuAzBL3O8A+4Z2nwDs4allYBNAFeBdbJ"
    "F69DXW6ogtIHnlWQPg3Kl0L5AzZ9UaZ8ZlPcM7w3cdpsAboCLRQ6KCfQwVWiBeLhjIW2dQodnji3Qx9PolBGC6uT0eImXA8A3ije"
    "mzhtMtAwRov8hifQoFamxQ246hn+FTo0Oile4JrEaGF9Ai0YzeBK+0rba3ylfWftP+XKeBRoTvvPeITih8L4B1fKz6zc066AS7HG"
    "/KP6OOBqEFoPONZCPxmu4eoP1zZwtWM0oHiQr+KvUG46fTYvpHzK5AR4lZ3uch7puoRe+XPSs1ACV725mntgvii1jTY1vqovoxwV"
    "xTFNo/JGcdvo2iwVGW0OwDPIIJUD+bpEeaZySWXjqVeQWSY3ja6MX4Bmf/ZK5Z3JHOUxSmdZ7pnsNbpS+aa4AjmpADmaJ59b4bwO"
    "cpMK12o4DyBkGoBQ3Vy4fgFnhiILajXwiqyzaD9kuYilNGjWBY1pppHyqF6UdVUI6syusq5j+o7qMi2aRutrdgL5iO+BLqyR89Ny"
    "b6Ex1rqO0eSy3CbVc1Cn+idIBx2nHg95JqO3mn2DdmtWQ73PgZz3l9u5iuY0y4T7YDSH8fcKOO1BX7UFWk1CxZQ3oR0fYT3KFFcD"
    "vehZAOclNBHg2CTOk2VsHtN1TE8KbyIbimP1XTSR6bg78G4mes6mHPTjl1Avj4LUXSDNjuWfrz5CeQ11t8jzDhRE22d1AQ9Ico30"
    "lCbqO2iEej3k/xoFMThWS/JK22djD5ygJwuAPgU20kmHTaozEdDzLKUpnEvpVeyJzsK5G86l9Ap5ouCMFL2xgZ7cRhi3UlEV1FGF"
    "U0k26MsOzdai3uo2aC49geb5dYfhrK6rqZsHx1bT5Lq18sgsnWAvIA2cmfCE2bOA+jLLyAbGbhWaicuFZ7WBOtdAldnMbIm1AtLa"
    "w7NoNpuvmfebEq+99vXWiwcbWwaoBfsdJj+pWFmEZkpWBQIocDlc09gohoRn4TkRstjDGQj3mxHSuUon/hShQLAfAqEOfAQyu8DR"
    "FvWEuk5jL5yHy/AK/D6+gH/heM6B8+VCudHcJK6Ku8oRPowv4GfznwkawVeIERKEZ4Vi4V1RJ3YXJ4nV4hnxlopTRasGqsaoJquW"
    "qqpVh1SX1C3U4WqjeqX6S/VtGzsbf5t4m3E25Tav2my02Wnzmc03Nj/ZmJt1apbXbGWzY83qNN6aMZp5mjWanZpazfXm3s17Ny9u"
    "Xt28tvlvLSJbpLaY1WJ5i+0tzrb4R8vmLbu2HNByRsvNcByxVdlG2s6w3WR7wM7Hrp3dG3ZX7Ii9rb2X/VL7tfan7X9yaOUwyCHd"
    "YZnDew57HK44/Oro5xjnmOKY6VjiuMnxI8dPHD93vOT4nZPeKdFpuFOW00SnGU7znZY7rXPa7LTT6a5TnbONs7Oz1rmNcwfnEc7V"
    "zluddzsfdf7C+WsXG5dnXEa75LiUulS4LHRZ5fLAVXC1d/VyDXJt79rDNcF1mGum67uuH7oecj3tetH1pus/3Ozchrlluk1wm+72"
    "gtsyt8NuZ9wuu33v9lsr3KplK/dWAa3CW3Vt1a/VkFbprfJblbWa22pJq1Wtzra62upWq/vuvLudu6e7wT3Svbv7APeh7mPdC92n"
    "uc9zX+r+hvu77h+6H3K/5eHtEewR5RHrkegx3CPLY4LHNI95Hss91nls9tjpcdjjjMdlj4eeoqeDp7dnsGeUZ6xnoudwz8meSzxf"
    "89zgud1zv+dnnl96/t3L2SvRa7hXltdErxle872We53y9vNu693Ru7f3M96jvbO9S7zLvRd4r/Cu8t7svdP7sPcZ70ve33n/6uPo"
    "E+uT6DPcJ8tnos8Mn/k+y33W+Wz22elz2OeMz2Wf731+88W+LX3dff192/l28e3rO9i30Heu70LfN3zf9z3p+w+tk7andpjWqC3U"
    "ztAu0L6qfVN7SfudzkXnrQvQheqidLm61bpjulrdNd2Pfio/W7/Ofr38BvtN85vjt9Xvjl6td9KH6xP1Q/Sj9Vn6Mn2Ffr5+iX67"
    "/h/6x/7O/uH+A/zz/Jf6v+F/zP/XgHYBIwPGBmwK+Drgp0DXQJ/AAYHTA5cHrgvcF3g58JHB2eBlaGfoYOhhGGwYaXjHsM1wwnAz"
    "CAdpg+KCBgaVBVUHXQ5WBzsF+wY/Ezw6+MXgV4Irg6uCNwZfad28dUzrXq2TW49sXdp6TusFrVe1frv1B60Ptj7V+qvWt1ubQmxD"
    "tCFdQ0aETA5ZGrIp5POQh23UbVzahLTp1ya1zcQ2O9vUhqpCnUMNodGheaFTQ18KfSN0Z+iR0CugPYxkhWAUq0GzgF3v5KBzCNA5"
    "6IwCqivmPetukBVq2we/FIFe57AvzAPmibUwS2gGMq5z4HU8ZNdhX5wdSUgkziYrxVrTITIGV5k+I2ug7kP8Rj4bSrC6sYOeHXz2"
    "jyd+hIy1XAg9oVK0AFRPsLgXeSJoA/M6p0geqMNOvRM7o3TsxJDIL07DNumX07EL2dEbe5GtaZfTyIO0q2nke9w3ntzAg9LwQPIg"
    "HtuIe0k5riDlteTOeVKBy+l5HjvW4grTRnIHoFpLzoowb0F61BqFQ8t6PjDQ4ODq6uZgCAyMah8dExPpAk8uNNXN1dXFWaXmHVQq"
    "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"
)

if __name__ == "__main__":
    logger.info("Запуск бота...")
    bot.run(TOKEN)
