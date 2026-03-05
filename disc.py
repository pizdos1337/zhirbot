import discord
from discord.ext import commands
import random
import sqlite3
import os
from datetime import datetime, timedelta
import time
import math
import asyncio
import shutil
import glob
import json

# ===== НАСТРОЙКИ =====
TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
PREFIX = "!"
DB_FOLDER = "/app/data/guild_databases"
COOLDOWN_HOURS = 1
TESTER_ROLE_NAME = "тестер"  # Название роли для доступа к обычным тестерским командам
HIGH_TESTER_ROLE_NAME = "Высший тестер"  # Название роли для доступа к расширенным командам

# Настройки вероятностей
BASE_MINUS_CHANCE = 0.2
MAX_MINUS_CHANCE = 0.6
PITY_INCREMENT = 0.1

# Настройки накопления на плюс от минусов
CONSECUTIVE_MINUS_BOOST = 0.2
MAX_CONSECUTIVE_MINUS_BOOST = 0.8

# Настройки джекпота
BASE_JACKPOT_CHANCE = 0.001
JACKPOT_PITY_INCREMENT = 0.001
MAX_JACKPOT_CHANCE = 0.05
JACKPOT_MIN = 100
JACKPOT_MAX = 500

# Настройки кейса
CASE_COOLDOWN_HOURS = 24

# Призы в кейсе
CASE_PRIZES = [
    {"value": 0, "chance": 20.0, "emoji": "🔄", "name": "Ничего"},
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
    {"value": "autoburger", "chance": 1.0, "emoji": "🍔", "name": "АВТОБУРГЕР"},
]

total_chance = sum(prize["chance"] for prize in CASE_PRIZES)
for prize in CASE_PRIZES:
    prize["normalized_chance"] = (prize["chance"] / total_chance) * 100

# Настройки Автобургера
AUTOBURGER_INTERVALS = [6, 4, 2, 1]
AUTOBURGER_MAX_BONUS = 0.6
AUTOBURGER_GROWTH_RATE = 0.03

# ===== НАСТРОЙКИ ЛЕГЕНДАРНЫХ БУРГЕРОВ =====
BURGER_RANKS = [
    {"name": "Железный бургер", "emoji": "⬛", "multiplier": 1.5,
     "fat_cooldown": 45, "case_cooldown": 16, "weight_required": 3600, "chance": 0.7},
    {"name": "Золотой бургер", "emoji": "🟨", "multiplier": 2.0,
     "fat_cooldown": 30, "case_cooldown": 12, "weight_required": 4300, "chance": 0.5},
    {"name": "Платиновый бургер", "emoji": "⬜", "multiplier": 2.5,
     "fat_cooldown": 20, "case_cooldown": 6, "weight_required": 6000, "chance": 0.3},
    {"name": "Алмазный бургер", "emoji": "🟦", "multiplier": 3.0,
     "fat_cooldown": 10, "case_cooldown": 3, "weight_required": 8000, "chance": 0.2,
     "plus_bonus": 0.1, "rare_multiplier": 2.0},
]

IRON_BURGER = 0
GOLD_BURGER = 1
PLATINUM_BURGER = 2
DIAMOND_BURGER = 3

# ===== НАСТРОЙКИ МАГАЗИНА =====
SHOP_ITEMS = [
    {
        "name": "Горелый бекон",
        "chance": 1.0,
        "min_amount": 3,
        "max_amount": 20,
        "price": 20,
        "gain_per_24h": 1,
        "description": "🏭 Даёт +1 кг каждые 24 часа"
    },
    {
        "name": "Горелый бутерброд",
        "chance": 0.4,
        "min_amount": 1,
        "max_amount": 5,
        "price": 70,
        "gain_per_24h": 3,
        "description": "🥪 Даёт +3 кг каждые 24 часа"
    },
    {
        "name": "Горелый додстер",
        "chance": 0.4,
        "min_amount": 1,
        "max_amount": 3,
        "price": 100,
        "gain_per_24h": 5,
        "description": "🌯 Даёт +5 кг каждые 24 часа"
    }
]

SHOP_SLOTS = 6
SHOP_UPDATE_HOURS = 12
# Продолжение списка SHOP_ITEMS
SHOP_ITEMS.extend([
    {
        "name": "Тарелка макарон",
        "chance": 0.3,
        "min_amount": 1,
        "max_amount": 2,
        "price": 200,
        "gain_per_24h": 10,
        "description": "🍝 Даёт +10 кг каждые 24 часа"
    },
    {
        "name": "Тарелка хинкалей",
        "chance": 0.2,
        "min_amount": 1,
        "max_amount": 2,
        "price": 300,
        "gain_per_24h": 15,
        "description": "🥟 Даёт +15 кг каждые 24 часа"
    },
    {
        "name": "Бургер",
        "chance": 0.15,
        "min_amount": 1,
        "max_amount": 2,
        "price": 400,
        "gain_per_24h": 20,
        "description": "🍔 Даёт +20 кг каждые 24 часа"
    },
    {
        "name": "Пицца",
        "chance": 0.1,
        "min_amount": 1,
        "max_amount": 2,
        "price": 500,
        "gain_per_24h": 30,
        "description": "🍕 Даёт +30 кг каждые 24 часа"
    },
    {
        "name": "Ведро KFC",
        "chance": 0.08,
        "min_amount": 1,
        "max_amount": 2,
        "price": 800,
        "gain_per_24h": 50,
        "description": "🍗 Даёт +50 кг каждые 24 часа"
    },
    {
        "name": "Комбо за 1000!",
        "chance": 0.06,
        "min_amount": 1,
        "max_amount": 2,
        "price": 1000,
        "gain_per_24h": 100,
        "description": "🍱 Даёт +100 кг каждые 24 часа"
    },
    {
        "name": "Бездонное ведро KFC",
        "chance": 0.04,
        "min_amount": 1,
        "max_amount": 1,
        "price": 1500,
        "gain_per_24h": 150,
        "description": "🪣 Даёт +150 кг каждые 24 часа"
    },
    {
        "name": "Бездонная пачка чипсов",
        "chance": 0.03,
        "min_amount": 1,
        "max_amount": 1,
        "price": 3000,
        "gain_per_24h": 250,
        "description": "🥨 Даёт +250 кг каждые 24 часа"
    },
    {
        "name": "Пожизненный запас чикенбургеров",
        "chance": 0.02,
        "min_amount": 1,
        "max_amount": 1,
        "price": 5000,
        "gain_per_24h": 500,
        "description": "🍔🍔🍔 Даёт +500 кг каждые 24 часа"
    },
    {
        "name": "Автоматическая система подачи холестерина",
        "chance": 0.01,
        "min_amount": 1,
        "max_amount": 1,
        "price": 7000,
        "gain_per_24h": 1000,
        "description": "⚙️💉 Даёт +1000 кг каждые 24 часа"
    },
    {
        "name": "Святой сэндвич",
        "chance": 0.005,
        "min_amount": 1,
        "max_amount": 1,
        "price": 10000,
        "gain_per_24h": 0,
        "description": "✨ **ЛЕГЕНДАРНО** ✨\nУвеличивает шанс джекпота до 30% за шт"
    },
    {
        "name": "Гнилая ножка KFC",
        "chance": 0.005,
        "min_amount": 1,
        "max_amount": 5,
        "price": 1,
        "gain_per_24h": 0,
        "description": "💀 **ПРОКЛЯТО** 💀\n50% шанс потерять 1/3 массы при каждом !жир"
    },
    {
        "name": "Стакан воды",
        "chance": 0.005,
        "min_amount": 1,
        "max_amount": 5,
        "price": 1,
        "gain_per_24h": 0,
        "description": "💧 **ОЧИЩЕНИЕ** 💧\nНет минусов, но весь прирост в 3 раза меньше"
    },
])

SHOP_SLOTS = 6
SHOP_UPDATE_HOURS = 12

print("="*60)
print("🍔 ЖИРНЫЙ БОТ - ЗАПУСК")
print("="*60)
print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 Папка БД: {DB_FOLDER}")
print("="*60)

if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найдена переменная окружения DISCORD_BOT_TOKEN!")
    print("📌 Убедитесь, что на хостинге установлена переменная окружения с токеном бота")
    exit(1)
# ===== ФУНКЦИИ ДЛЯ РАБОТЫ С JSON ПРЕДМЕТАМИ =====
def get_user_items(item_counts_str):
    """Получает словарь с предметами пользователя из JSON строки"""
    try:
        return json.loads(item_counts_str) if item_counts_str and item_counts_str != '{}' else {}
    except:
        return {}

def save_user_items(items_dict):
    """Сохраняет словарь предметов в JSON строку"""
    return json.dumps(items_dict)

def add_user_item(items_dict, item_name, amount=1):
    """Добавляет предмет пользователю"""
    items_dict[item_name] = items_dict.get(item_name, 0) + amount
    return items_dict

def remove_user_item(items_dict, item_name, amount=1):
    """Удаляет предмет у пользователя"""
    if item_name in items_dict:
        items_dict[item_name] -= amount
        if items_dict[item_name] <= 0:
            del items_dict[item_name]
    return items_dict

# ===== ФУНКЦИЯ ФОРМАТИРОВАНИЯ НИКА СО ЗНАЧКОМ =====
def format_nick_with_icon(user_number, user_name, legendary_burger=-1):
    """Форматирует ник с учётом легендарного бургера"""
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        icon = BURGER_RANKS[legendary_burger]["emoji"]
        return f"{icon}{user_number}kg {user_name}"
    else:
        return f"{user_number}kg {user_name}"

# ===== ФУНКЦИЯ ПРОВЕРКИ ВОЗВЫШЕНИЯ =====
def check_ascension_available(current_weight, legendary_burger):
    """Проверяет, доступно ли возвышение для пользователя"""
    if legendary_burger >= DIAMOND_BURGER:
        return False, -1, None, 0, 0
    
    next_burger = legendary_burger + 1 if legendary_burger >= 0 else 0
    
    if next_burger < len(BURGER_RANKS):
        burger = BURGER_RANKS[next_burger]
        if current_weight >= burger["weight_required"]:
            return True, next_burger, burger["name"], burger["weight_required"], burger["chance"]
    
    return False, -1, None, 0, 0

# ===== ФУНКЦИИ БЕЗОПАСНОЙ РАБОТЫ С БД =====
def repair_database(db_path):
    """Пытается восстановить повреждённую базу данных"""
    if not os.path.exists(db_path):
        return False
    
    backup_path = db_path + f".corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(db_path, backup_path)
    print(f"⚠️ Создан бекап повреждённой БД: {backup_path}")
    
    os.remove(db_path)
    print("🗑️ Повреждённая БД удалена")
    return True

def backup_and_restore_db():
    """Сохраняет БД при обновлении и восстанавливает при запуске"""
    main_db_folder = DB_FOLDER
    backup_folder = "/tmp/guild_databases_backup"
    
    print(f"📁 Основная папка БД: {main_db_folder}")
    print(f"📁 Папка бекапов: {backup_folder}")
    
    if not os.path.exists(main_db_folder):
        os.makedirs(main_db_folder)
        print(f"📁 Создана папка для БД: {main_db_folder}")
        return
    
    if os.path.exists(backup_folder):
        print("🔄 Восстанавливаю базы данных из бекапа...")
        restored = 0
        for db_file in glob.glob(os.path.join(backup_folder, "*.db")):
            dest = os.path.join(main_db_folder, os.path.basename(db_file))
            shutil.copy2(db_file, dest)
            print(f"  ✅ Восстановлено: {os.path.basename(db_file)}")
            restored += 1
        if restored == 0:
            print("  ⚠️ Бекапов не найдено")
    else:
        print("📦 Создаю бекап баз данных...")
        os.makedirs(backup_folder, exist_ok=True)
        backed_up = 0
        for db_file in glob.glob(os.path.join(main_db_folder, "*.db")):
            dest = os.path.join(backup_folder, os.path.basename(db_file))
            shutil.copy2(db_file, dest)
            print(f"  ✅ Сбэкаплено: {os.path.basename(db_file)}")
            backed_up += 1
        if backed_up == 0:
            print("  📭 Нет файлов для бекапа")

print("🚀 Запуск системы бекапов...")
backup_and_restore_db()
print("✅ Система бекапов инициализирована")

# ===== СИСТЕМА ЗВАНИЙ =====
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
    {"name": "Арчжирмезис", "min": 5000, "max": 10000, "emoji": "♛"},
    {"name": "ЖИРНАЯ ТОЛСТАЯ ОГРОМНАЯ СВИНЬЯ", "min": 10001, "max": 99999999, "emoji": "🐖"},
]

def migrate_database_if_needed(guild_id):
    """Проверяет и добавляет недостающие колонки в существующую БД"""
    db_path = get_db_path(guild_id)
    if not os.path.exists(db_path):
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Получаем список существующих колонок
    cursor.execute("PRAGMA table_info(user_fat)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'fat_cooldown_time' not in columns:
        print(f"📦 Добавляю колонку fat_cooldown_time для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN fat_cooldown_time TIMESTAMP")

    if 'legendary_burger' not in columns:
        print(f"📦 Добавляю колонку legendary_burger для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN legendary_burger INTEGER DEFAULT -1")
    
    if 'item_counts' not in columns:
        print(f"📦 Добавляю колонку item_counts для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN item_counts TEXT DEFAULT '{}'")
    
    if 'last_command' not in columns:
        print(f"📦 Добавляю колонку last_command для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_command TEXT")
    
    if 'last_command_target' not in columns:
        print(f"📦 Добавляю колонку last_command_target для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_command_target TEXT")
    
    if 'last_command_use_time' not in columns:
        print(f"📦 Добавляю колонку last_command_use_time для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_command_use_time TIMESTAMP")
    
    # Проверяем таблицу shop
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shop'")
    if not cursor.fetchone():
        print(f"📦 Создаю таблицу shop для сервера {guild_id}")
        cursor.execute('''
            CREATE TABLE shop (
                guild_id TEXT PRIMARY KEY,
                slots TEXT,
                last_update TIMESTAMP,
                next_update TIMESTAMP
            )
        ''')
    
    conn.commit()
    conn.close()
    
def get_rank(weight):
    for rank in RANKS:
        if rank["min"] <= weight <= rank["max"]:
            return rank["name"], rank["emoji"]
    if weight > 99999999:
        return "🌀 Бесконечность", "🌀"
    if weight < -999:
        return "Черная дыра", "💀"
    return "❓ Неопределённый", "❓"

if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)
    print(f"📁 Создана папка для баз данных: {DB_FOLDER}")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# ===== РАБОТА С БАЗОЙ ДАННЫХ =====
def get_db_path(guild_id):
    return os.path.join(DB_FOLDER, f"guild_{guild_id}.db")

def init_guild_database(guild_id):
    """Создаёт таблицы в базе данных для конкретного сервера"""
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_fat (
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            current_number INTEGER DEFAULT 0,
            last_command_time TIMESTAMP,
            consecutive_plus INTEGER DEFAULT 0,
            consecutive_minus INTEGER DEFAULT 0,
            jackpot_pity INTEGER DEFAULT 0,
            autoburger_count INTEGER DEFAULT 0,
            last_case_time TIMESTAMP,
            next_autoburger_time TIMESTAMP,
            total_autoburger_activations INTEGER DEFAULT 0,
            total_autoburger_gain INTEGER DEFAULT 0,
            last_autoburger_result TEXT,
            last_autoburger_time TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shop (
            guild_id TEXT PRIMARY KEY,
            slots TEXT,
            last_update TIMESTAMP,
            next_update TIMESTAMP
        )
    ''')
    
    # Получаем список существующих колонок
    cursor.execute("PRAGMA table_info(user_fat)")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Добавляем недостающие колонки
    if 'fat_cooldown_time' not in columns:  # ДОБАВЬТЕ ЭТОТ БЛОК
        print(f"📦 Добавляю колонку fat_cooldown_time для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN fat_cooldown_time TIMESTAMP")

    if 'legendary_burger' not in columns:
        print(f"📦 Добавляю колонку legendary_burger для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN legendary_burger INTEGER DEFAULT -1")
    
    if 'item_counts' not in columns:
        print(f"📦 Добавляю колонку item_counts для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN item_counts TEXT DEFAULT '{}'")
    
    if 'last_command' not in columns:
        print(f"📦 Добавляю колонку last_command для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_command TEXT")
    
    if 'last_command_target' not in columns:
        print(f"📦 Добавляю колонку last_command_target для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_command_target TEXT")
    
    if 'last_command_use_time' not in columns:
        print(f"📦 Добавляю колонку last_command_use_time для сервера {guild_id}")
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_command_use_time TIMESTAMP")
    
    # Проверяем таблицу shop
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shop'")
    if not cursor.fetchone():
        print(f"📦 Создаю таблицу shop для сервера {guild_id}")
        cursor.execute('''
            CREATE TABLE shop (
                guild_id TEXT PRIMARY KEY,
                slots TEXT,
                last_update TIMESTAMP,
                next_update TIMESTAMP
            )
        ''')
    
    conn.commit()
    conn.close()
    
    print(f"✅ База данных инициализирована для сервера {guild_id}")

def get_user_data(guild_id, user_id, user_name=None):
    """Получает данные пользователя из БД конкретного сервера"""
    safe_init_guild_database(guild_id, f"Guild_{guild_id}")
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # ДОБАВЛЯЕМ fat_cooldown_time в SELECT
    cursor.execute('''
        SELECT current_number, last_command_time, consecutive_plus, consecutive_minus,
               jackpot_pity, autoburger_count, last_case_time, next_autoburger_time,
               total_autoburger_activations, total_autoburger_gain,
               last_autoburger_result, last_autoburger_time,
               legendary_burger, item_counts, last_command, last_command_target, last_command_use_time,
               fat_cooldown_time
        FROM user_fat WHERE user_id = ?
    ''', (str(user_id),))
    result = cursor.fetchone()
    
    if result:
        number = result[0]
        last_time = result[1]
        consecutive_plus = result[2] or 0
        consecutive_minus = result[3] or 0
        jackpot_pity = result[4] or 0
        autoburger_count = result[5] or 0
        last_case_time = result[6]
        next_autoburger_time = result[7]
        total_activations = result[8] or 0
        total_gain = result[9] or 0
        last_result = result[10]
        last_activation_time = result[11]
        legendary_burger = result[12] if result[12] is not None else -1
        item_counts = result[13] or '{}'
        last_command = result[14]
        last_command_target = result[15]
        last_command_use_time = result[16]
        fat_cooldown_time = result[17]  # НОВОЕ ПОЛЕ
    else:
        number = 0
        last_time = None
        consecutive_plus = 0
        consecutive_minus = 0
        jackpot_pity = 0
        autoburger_count = 0
        last_case_time = None
        next_autoburger_time = None
        total_activations = 0
        total_gain = 0
        last_result = None
        last_activation_time = None
        legendary_burger = -1
        item_counts = '{}'
        last_command = None
        last_command_target = None
        last_command_use_time = None
        fat_cooldown_time = None  # НОВОЕ ПОЛЕ
        
        # ДОБАВЛЯЕМ fat_cooldown_time в INSERT
        cursor.execute('''
            INSERT INTO user_fat (
                user_id, user_name, current_number, last_command_time,
                consecutive_plus, consecutive_minus, jackpot_pity,
                autoburger_count, last_case_time, next_autoburger_time,
                total_autoburger_activations, total_autoburger_gain,
                last_autoburger_result, last_autoburger_time,
                legendary_burger, item_counts,
                last_command, last_command_target, last_command_use_time,
                fat_cooldown_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", number, last_time,
              consecutive_plus, consecutive_minus, jackpot_pity,
              autoburger_count, last_case_time, next_autoburger_time,
              total_activations, total_gain, last_result, last_activation_time,
              legendary_burger, item_counts,
              last_command, last_command_target, last_command_use_time,
              fat_cooldown_time))
        conn.commit()
    
    conn.close()
    # ВОЗВРАЩАЕМ 18 ЗНАЧЕНИЙ
    return (number, last_time, consecutive_plus, consecutive_minus,
            jackpot_pity, autoburger_count, last_case_time, next_autoburger_time,
            total_activations, total_gain, last_result, last_activation_time,
            legendary_burger, item_counts, last_command, last_command_target, last_command_use_time,
            fat_cooldown_time)

def safe_init_guild_database(guild_id, guild_name="Unknown"):
    """Безопасная инициализация БД с обработкой ошибок"""
    db_path = get_db_path(guild_id)
    
    # Проверяем существует ли файл БД
    if os.path.exists(db_path):
        try:
            # Пробуем открыть БД для проверки
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            # Проверяем есть ли таблица user_fat
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_fat'")
            if not cursor.fetchone():
                # Таблицы нет - создаём заново
                conn.close()
                os.remove(db_path)
                print(f"⚠️ Таблица user_fat не найдена в БД сервера {guild_name}, создаю заново")
                return create_new_database(db_path, guild_id, guild_name)
            
            # Проверяем есть ли нужные колонки
            cursor.execute("PRAGMA table_info(user_fat)")
            columns = [col[1] for col in cursor.fetchall()]
            conn.close()
            
            # Добавляем недостающие колонки
            add_missing_columns(db_path, columns)
            
            print(f"✅ База данных для сервера {guild_name} в порядке")
            return True
            
        except sqlite3.DatabaseError:
            print(f"⚠️ Обнаружена повреждённая БД для сервера {guild_name} (ID: {guild_id})")
            repair_database(db_path)
            return create_new_database(db_path, guild_id, guild_name)
    else:
        print(f"📁 Создаю новую БД для сервера {guild_name} (ID: {guild_id})")
        return create_new_database(db_path, guild_id, guild_name)

def create_new_database(db_path, guild_id, guild_name):
    """Создаёт новую базу данных со всеми полями"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Создаём таблицу СРАЗУ со всеми нужными полями
    cursor.execute('''
        CREATE TABLE user_fat (
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            current_number INTEGER DEFAULT 0,
            last_command_time TIMESTAMP,
            consecutive_plus INTEGER DEFAULT 0,
            consecutive_minus INTEGER DEFAULT 0,
            jackpot_pity INTEGER DEFAULT 0,
            autoburger_count INTEGER DEFAULT 0,
            last_case_time TIMESTAMP,
            next_autoburger_time TIMESTAMP,
            total_autoburger_activations INTEGER DEFAULT 0,
            total_autoburger_gain INTEGER DEFAULT 0,
            last_autoburger_result TEXT,
            last_autoburger_time TIMESTAMP,
            legendary_burger INTEGER DEFAULT -1,
            item_counts TEXT DEFAULT '{}',
            last_command TEXT,
            last_command_target TEXT,
            last_command_use_time TIMESTAMP
            fat_cooldown_time TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shop (
            guild_id TEXT PRIMARY KEY,
            slots TEXT,
            last_update TIMESTAMP,
            next_update TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"✅ Новая база данных создана для сервера {guild_name}")
    return True

def add_missing_columns(db_path, existing_columns):
    """Добавляет недостающие колонки в существующую таблицу"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Список всех нужных колонок
    required_columns = {
        'legendary_burger': "INTEGER DEFAULT -1",
        'item_counts': "TEXT DEFAULT '{}'",
        'last_command': "TEXT",
        'last_command_target': "TEXT",
        'last_command_use_time': "TIMESTAMP",
        'fat_cooldown_time': "TIMESTAMP",
    }
    
    for col_name, col_type in required_columns.items():
        if col_name not in existing_columns:
            try:
                print(f"📦 Добавляю колонку {col_name}")
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col_name} {col_type}")
            except Exception as e:
                print(f"⚠️ Ошибка при добавлении колонки {col_name}: {e}")
    
    # Проверяем таблицу shop
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shop'")
    if not cursor.fetchone():
        print(f"📦 Создаю таблицу shop")
        cursor.execute('''
            CREATE TABLE shop (
                guild_id TEXT PRIMARY KEY,
                slots TEXT,
                last_update TIMESTAMP,
                next_update TIMESTAMP
            )
        ''')
    
    conn.commit()
    conn.close()
    
def update_user_data(guild_id, user_id, new_number, user_name=None,
                     consecutive_plus=None, consecutive_minus=None,
                     jackpot_pity=None, autoburger_count=None,
                     last_case_time=None, next_autoburger_time=None,
                     total_activations=None, total_gain=None,
                     last_result=None, last_activation_time=None,
                     legendary_burger=None, item_counts=None,
                     last_command=None, last_command_target=None, last_command_use_time=None, fat_cooldown_time=None):
    """Обновляет данные пользователя в БД"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    current_time = datetime.now()
    
    updates = ["current_number = ?", "user_name = ?", "last_command_time = ?"]
    values = [new_number, user_name or "Unknown", current_time]
    
    if fat_cooldown_time is not None:
        updates.append("fat_cooldown_time = ?")
        values.append(fat_cooldown_time)

    if consecutive_plus is not None:
        updates.append("consecutive_plus = ?")
        values.append(consecutive_plus)
    
    if consecutive_minus is not None:
        updates.append("consecutive_minus = ?")
        values.append(consecutive_minus)
    
    if jackpot_pity is not None:
        updates.append("jackpot_pity = ?")
        values.append(jackpot_pity)
    
    if autoburger_count is not None:
        updates.append("autoburger_count = ?")
        values.append(autoburger_count)
    
    if last_case_time is not None:
        updates.append("last_case_time = ?")
        values.append(last_case_time)
    
    if next_autoburger_time is not None:
        updates.append("next_autoburger_time = ?")
        values.append(next_autoburger_time)
    
    if total_activations is not None:
        updates.append("total_autoburger_activations = ?")
        values.append(total_activations)
    
    if total_gain is not None:
        updates.append("total_autoburger_gain = ?")
        values.append(total_gain)
    
    if last_result is not None:
        updates.append("last_autoburger_result = ?")
        values.append(last_result)
    
    if last_activation_time is not None:
        updates.append("last_autoburger_time = ?")
        values.append(last_activation_time)
    
    if legendary_burger is not None:
        updates.append("legendary_burger = ?")
        values.append(legendary_burger)
    
    if item_counts is not None:
        updates.append("item_counts = ?")
        values.append(item_counts)
    
    if last_command is not None:
        updates.append("last_command = ?")
        values.append(last_command)
    
    if last_command_target is not None:
        updates.append("last_command_target = ?")
        values.append(last_command_target)
    
    if last_command_use_time is not None:
        updates.append("last_command_use_time = ?")
        values.append(last_command_use_time)
    
    values.append(str(user_id))
    
    query = f"UPDATE user_fat SET {', '.join(updates)} WHERE user_id = ?"
    cursor.execute(query, values)
    
    if cursor.rowcount == 0:
        cursor.execute('''
            INSERT INTO user_fat (
                user_id, user_name, current_number, last_command_time,
                consecutive_plus, consecutive_minus, jackpot_pity,
                autoburger_count, last_case_time, next_autoburger_time,
                total_autoburger_activations, total_autoburger_gain,
                last_autoburger_result, last_autoburger_time,
                legendary_burger, item_counts,
                last_command, last_command_target, last_command_use_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", new_number, current_time,
              consecutive_plus or 0, consecutive_minus or 0, jackpot_pity or 0,
              autoburger_count or 0, last_case_time, next_autoburger_time,
              total_activations or 0, total_gain or 0, last_result, last_activation_time,
              legendary_burger or -1, item_counts or '{}',
              last_command, last_command_target, last_command_use_time))
    
    conn.commit()
    conn.close()
    
    try:
        backup_folder = "/tmp/guild_databases_backup"
        os.makedirs(backup_folder, exist_ok=True)
        db_name = os.path.basename(db_path)
        backup_path = os.path.join(backup_folder, db_name)
        shutil.copy2(db_path, backup_path)
    except:
        pass
    
    return current_time

def reset_all_cooldowns(guild_id):
    """Сбрасывает кулдаун для всех пользователей"""
    init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('UPDATE user_fat SET fat_cooldown_time = NULL')  # Только это поле
    affected_rows = cursor.rowcount
    conn.commit()
    conn.close()
    return affected_rows

def reset_all_weights(guild_id):
    init_guild_database(guild_id)
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('UPDATE user_fat SET current_number = 0, consecutive_plus = 0, consecutive_minus = 0, jackpot_pity = 0, autoburger_count = 0, total_autoburger_activations = 0, total_autoburger_gain = 0, last_autoburger_result = NULL, last_autoburger_time = NULL, legendary_burger = -1, item_counts = "{}"')
    affected_rows = cursor.rowcount
    conn.commit()
    conn.close()
    return affected_rows
def get_all_users_sorted(guild_id):
    """Получает всех пользователей сервера отсортированных по числу (убывание)"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT user_name, current_number, last_command_time,
               consecutive_plus, consecutive_minus, jackpot_pity,
               autoburger_count, total_autoburger_activations, total_autoburger_gain,
               legendary_burger
        FROM user_fat 
        ORDER BY current_number DESC
    ''')
    
    results = cursor.fetchall()
    conn.close()
    return results

def get_guild_stats(guild_id):
    """Получает статистику по серверу"""
    users = get_all_users_sorted(guild_id)
    
    total_users = len(users)
    total_weight = sum(u[1] for u in users)
    avg_weight = total_weight / total_users if total_users > 0 else 0
    
    positive = sum(1 for u in users if u[1] > 0)
    negative = sum(1 for u in users if u[1] < 0)
    zero = sum(1 for u in users if u[1] == 0)
    
    total_autoburgers = sum(u[6] for u in users)
    total_activations = sum(u[7] for u in users)
    total_gain = sum(u[8] for u in users)
    
    burger_counts = [0, 0, 0, 0]
    for u in users:
        burger_idx = u[9] if len(u) > 9 and u[9] is not None and u[9] >= 0 else -1
        if burger_idx >= 0 and burger_idx < len(burger_counts):
            burger_counts[burger_idx] += 1
    
    return {
        'total_users': total_users,
        'total_weight': total_weight,
        'avg_weight': avg_weight,
        'positive': positive,
        'negative': negative,
        'zero': zero,
        'total_autoburgers': total_autoburgers,
        'total_activations': total_activations,
        'total_gain': total_gain,
        'burger_counts': burger_counts
    }

def get_shop_data(guild_id):
    """Получает данные магазина для сервера"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('SELECT slots, last_update, next_update FROM shop WHERE guild_id = ?', (str(guild_id),))
    result = cursor.fetchone()
    conn.close()
    
    return result  # Возвращаем как есть (строки)

def update_shop_data(guild_id, slots, last_update, next_update):
    """Обновляет данные магазина для сервера"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Преобразуем datetime объекты в строки для хранения
    last_update_str = last_update.isoformat() if last_update else None
    next_update_str = next_update.isoformat() if next_update else None
    
    cursor.execute('''
        INSERT OR REPLACE INTO shop (guild_id, slots, last_update, next_update)
        VALUES (?, ?, ?, ?)
    ''', (str(guild_id), json.dumps(slots), last_update_str, next_update_str))
    
    conn.commit()
    conn.close()

def check_cooldown(last_command_time, cooldown_hours):
    if last_command_time is None:
        return True, 0
    
    try:
        if isinstance(last_command_time, str):
            last_time = datetime.fromisoformat(last_command_time)
        else:
            last_time = last_command_time
        
        time_diff = datetime.now() - last_time
        cooldown_seconds = cooldown_hours * 3600
        
        if time_diff.total_seconds() >= cooldown_seconds:
            return True, 0
        else:
            remaining = cooldown_seconds - time_diff.total_seconds()
            return False, remaining
    except:
        return True, 0

def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    if hours > 0:
        return f"{hours} ч {minutes} мин"
    elif minutes > 0:
        return f"{minutes} мин {seconds} сек"
    else:
        return f"{seconds} сек"

def has_tester_role(member):
    """Проверяет, есть ли у участника роль "тестер" """
    if not member:
        return False
    
    for role in member.roles:
        if role.name.lower() == TESTER_ROLE_NAME.lower():
            return True
    
    return False

def has_high_tester_role(member):
    """Проверяет, есть ли у участника роль "Высший тестер" """
    if not member:
        return False
    
    for role in member.roles:
        if role.name.lower() == HIGH_TESTER_ROLE_NAME.lower():
            return True
    
    return False

def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count=0, legendary_burger=-1, items_dict=None, current_weight=None):
    """
    Определяет изменение веса с учётом всех механик
    Приоритет легендарных предметов: Стакан воды > Гнилая ножка KFC
    Святой сэндвич работает всегда (не конфликтует)
    """
    if items_dict is None:
        items_dict = {}
    
    # Проверяем наличие легендарных предметов
    has_rotten_leg = items_dict.get("Гнилая ножка KFC", 0) > 0
    has_holy_sandwich = items_dict.get("Святой сэндвич", 0) > 0
    has_water = items_dict.get("Стакан воды", 0) > 0
    
    # Определяем активный легендарный предмет (по приоритету)
    active_legendary_item = None
    if has_water:
        active_legendary_item = "water"
    elif has_rotten_leg:
        active_legendary_item = "rotten_leg"
    
    # Множитель от легендарного бургера
    multiplier = 1.0
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        multiplier = BURGER_RANKS[legendary_burger]["multiplier"]
    
    # Экспоненциальный бонус от автобургеров
    if autoburger_count > 0:
        autoburger_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count))
    else:
        autoburger_boost = 0
    
    # Бонус от минусов подряд
    minus_boost = min(consecutive_minus * CONSECUTIVE_MINUS_BOOST, MAX_CONSECUTIVE_MINUS_BOOST)
    
    # Дополнительный бонус от алмазного бургера
    diamond_bonus = 0
    if legendary_burger == DIAMOND_BURGER:
        diamond_bonus = 0.1
    
    # Рассчитываем текущий шанс на минус
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - autoburger_boost - minus_boost - diamond_bonus
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))
    
    # Рассчитываем текущий шанс на джекпот
    jackpot_chance = BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT)
    
    # Применяем бонус от алмазного бургера (x2 к шансу)
    if legendary_burger == DIAMOND_BURGER:
        jackpot_chance *= 2
    
    # Применяем бонус от Святого сэндвича (+30% за каждый)
    if has_holy_sandwich:
        sandwich_count = items_dict.get("Святой сэндвич", 0)
        # Для святого сэндвича игнорируем обычный лимит
        sandwich_bonus = 0.3 * sandwich_count
        jackpot_chance = max(jackpot_chance, sandwich_bonus)
        # Ограничиваем только до 90% (чтобы не было 100% гарантии)
        jackpot_chance = min(jackpot_chance, 0.9)
    else:
        # Обычный лимит для всех остальных случаев
        jackpot_chance = min(jackpot_chance, MAX_JACKPOT_CHANCE)
    
    # Обработка в зависимости от активного легендарного предмета
    if active_legendary_item == "water":
        # СТАКАН ВОДЫ: нет минусов, весь прирост в 3 раза меньше
        # Проверяем джекпот
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = change // 3  # В 3 раза меньше
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            # Обычный плюс (минусов нет)
            change = random.randint(1, 20)
            change = change // 3  # В 3 раза меньше
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    elif active_legendary_item == "rotten_leg":
        # ГНИЛАЯ НОЖКА KFC: 60% потерять 50% массы, 40% обычный джекпот
        if random.random() < 0.6:  # 60% шанс на потерю
            # Потеря 50% массы
            if current_weight is not None:
                loss = int(current_weight * 0.5)  # 50% массы
                change = -loss
            else:
                change = -int(consecutive_plus * 0.5)  # Запасной вариант
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            # 40% шанс на обычный джекпот (без умножения)
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)  # Обычный джекпот
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    else:
        # НЕТ АКТИВНЫХ ЛЕГЕНДАРНЫХ ПРЕДМЕТОВ - обычная обработка
        # Проверяем джекпот
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        
        # Проверяем минус/плюс
        roll = random.random()
        
        if roll < minus_chance:
            change = random.randint(-20, -1)
            change = int(change * multiplier)
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
        else:
            change = random.randint(1, 20)
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
        
        return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot

def get_case_prize(legendary_burger=-1):
    """Определяет приз из кейса с учётом бонуса алмазного бургера"""
    roll = random.random() * 100
    
    # Для алмазного бургера удваиваем шансы редких предметов
    if legendary_burger == DIAMOND_BURGER:
        # Создаём копию призов с удвоенными шансами для редких
        modified_prizes = []
        for prize in CASE_PRIZES:
            p = prize.copy()
            if p["value"] == "autoburger" or (isinstance(p["value"], int) and p["value"] >= 1000):
                p["normalized_chance"] = prize["normalized_chance"] * 2
            modified_prizes.append(p)
        
        # Перенормируем
        total = sum(p["normalized_chance"] for p in modified_prizes)
        for p in modified_prizes:
            p["normalized_chance"] = (p["normalized_chance"] / total) * 100
        
        cumulative = 0
        for prize in modified_prizes:
            cumulative += prize["normalized_chance"]
            if roll < cumulative:
                return prize
    else:
        cumulative = 0
        for prize in CASE_PRIZES:
            cumulative += prize["normalized_chance"]
            if roll < cumulative:
                return prize
    
    return CASE_PRIZES[-1]

def get_autoburger_interval(autoburger_count):
    if autoburger_count <= 0:
        return None
    elif autoburger_count == 1:
        return AUTOBURGER_INTERVALS[0]
    elif autoburger_count == 2:
        return AUTOBURGER_INTERVALS[1]
    elif autoburger_count == 3:
        return AUTOBURGER_INTERVALS[2]
    else:
        return AUTOBURGER_INTERVALS[3]
        
async def apply_autoburger(user_id, guild_id, user_name):
    """Фоновое применение команды !жир для автобургера"""
    try:
        (current_number, _, consecutive_plus, consecutive_minus, jackpot_pity,
         autoburger_count, _, _, total_activations, total_gain, _, _,
         legendary_burger, item_counts, _, _, _) = get_user_data(guild_id, user_id, user_name)
        
        # ДОБАВЬТЕ ЭТУ СТРОКУ
        items_dict = get_user_items(item_counts)
        
        change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
            consecutive_plus, consecutive_minus, jackpot_pity, 
            autoburger_count, legendary_burger, items_dict, current_number  # 7 параметров
        )
        
        new_number = current_number + change
        
        new_total_activations = total_activations + 1
        new_total_gain = total_gain + change
        new_last_result = f"{change:+d} кг"
        new_last_activation_time = datetime.now()
        
        update_user_data(
            guild_id, user_id, new_number, user_name,
            new_consecutive_plus, new_consecutive_minus, new_jackpot_pity,
            autoburger_count, None, None,
            new_total_activations, new_total_gain, new_last_result, new_last_activation_time,
            legendary_burger, item_counts
        )
        
        guild = bot.get_guild(guild_id)
        if guild:
            member = guild.get_member(int(user_id))
            if member:
                display_name = member.display_name
                clean_name = display_name
                if "kg" in display_name:
                    parts = display_name.split("kg", 1)
                    if len(parts) > 1:
                        clean_name = parts[1].strip()
                        if not clean_name:
                            clean_name = user_name
                else:
                    clean_name = display_name
                
                if not clean_name or len(clean_name) > 30:
                    clean_name = user_name
                
                new_nick = format_nick_with_icon(new_number, clean_name, legendary_burger)
                if len(new_nick) > 32:
                    new_nick = new_nick[:32]
                
                try:
                    await member.edit(nick=new_nick)
                except:
                    pass
        
        print(f"🤖 Автобургер сработал для {user_name}: {change:+d} кг")
        
    except Exception as e:
        print(f"❌ Ошибка в автобургере: {e}")

async def autoburger_loop():
    """Фоновый цикл для проверки и запуска автобургеров"""
    await bot.wait_until_ready()
    
    while not bot.is_closed():
        try:
            current_time = datetime.now()
            
            for guild in bot.guilds:
                guild_id = guild.id
                
                db_path = get_db_path(guild_id)
                if not os.path.exists(db_path):
                    continue
                
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT user_id, user_name, autoburger_count, next_autoburger_time
                    FROM user_fat 
                    WHERE autoburger_count > 0 AND next_autoburger_time IS NOT NULL
                ''')
                
                users = cursor.fetchall()
                conn.close()
                
                for user_id, user_name, autoburger_count, next_time_str in users:
                    try:
                        if next_time_str:
                            if isinstance(next_time_str, str):
                                next_time = datetime.fromisoformat(next_time_str)
                            else:
                                next_time = next_time_str
                            
                            if current_time >= next_time:
                                await apply_autoburger(user_id, guild_id, user_name)
                                
                                interval = get_autoburger_interval(autoburger_count)
                                if interval:
                                    new_next_time = current_time + timedelta(hours=interval)
                                    
                                    conn = sqlite3.connect(db_path)
                                    cursor = conn.cursor()
                                    cursor.execute('''
                                        UPDATE user_fat 
                                        SET next_autoburger_time = ?
                                        WHERE user_id = ?
                                    ''', (new_next_time, user_id))
                                    conn.commit()
                                    conn.close()
                    except Exception as e:
                        print(f"❌ Ошибка обработки автобургера для {user_id}: {e}")
            
        except Exception as e:
            print(f"❌ Ошибка в цикле автобургеров: {e}")
        
        await asyncio.sleep(60)

async def passive_income_loop():
    """Фоновый цикл для начисления пассивного дохода от предметов (каждые 24 часа)"""
    await bot.wait_until_ready()
    
    while not bot.is_closed():
        try:
            current_time = datetime.now()
            print(f"💰 Начисление пассивного дохода: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            for guild in bot.guilds:
                guild_id = guild.id
                db_path = get_db_path(guild_id)
                
                if not os.path.exists(db_path):
                    continue
                
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Получаем всех пользователей
                cursor.execute('SELECT user_id, user_name, current_number, item_counts, legendary_burger FROM user_fat')
                users = cursor.fetchall()
                conn.close()
                
                for user_id, user_name, current_number, item_counts_str, legendary_burger in users:
                    try:
                        items_dict = get_user_items(item_counts_str)
                        if not items_dict:
                            continue
                        
                        # Рассчитываем общий прирост за 24 часа
                        total_gain = 0
                        gained_items = []
                        
                        for item_name, count in items_dict.items():
                            # Ищем предмет в списке магазина
                            for shop_item in SHOP_ITEMS:
                                if shop_item["name"] == item_name:
                                    gain = shop_item["gain_per_24h"] * count
                                    if gain > 0:
                                        total_gain += gain
                                        gained_items.append(f"{item_name} x{count} (+{gain}кг)")
                                    break
                        
                        if total_gain > 0:
                            # Применяем множитель от легендарного бургера
                            multiplier = 1.0
                            if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
                                multiplier = BURGER_RANKS[legendary_burger]["multiplier"]
                            
                            final_gain = int(total_gain * multiplier)
                            new_number = current_number + final_gain
                            
                            # Обновляем данные в БД
                            conn = sqlite3.connect(db_path)
                            c = conn.cursor()
                            c.execute('UPDATE user_fat SET current_number = ? WHERE user_id = ?', 
                                     (new_number, user_id))
                            conn.commit()
                            conn.close()
                            
                            # Пытаемся обновить ник
                            try:
                                guild_obj = bot.get_guild(guild_id)
                                if guild_obj:
                                    member = guild_obj.get_member(int(user_id))
                                    if member:
                                        display_name = member.display_name
                                        clean_name = display_name
                                        if "kg" in display_name:
                                            parts = display_name.split("kg", 1)
                                            if len(parts) > 1:
                                                clean_name = parts[1].strip()
                                                if not clean_name:
                                                    clean_name = user_name
                                        else:
                                            clean_name = display_name
                                        
                                        if not clean_name or len(clean_name) > 30:
                                            clean_name = user_name
                                        
                                        new_nick = format_nick_with_icon(new_number, clean_name, legendary_burger)
                                        if len(new_nick) > 32:
                                            new_nick = new_nick[:32]
                                        
                                        await member.edit(nick=new_nick)
                            except:
                                pass
                            
                            print(f"💰 {user_name} получил {final_gain}кг от предметов: {', '.join(gained_items)}")
                    
                    except Exception as e:
                        print(f"❌ Ошибка при начислении дохода для {user_id}: {e}")
            
        except Exception as e:
            print(f"❌ Ошибка в цикле пассивного дохода: {e}")
        
        # Ждём 24 часа до следующего начисления
        await asyncio.sleep(24 * 60 * 60)  # 24 часа в секундах

def check_databases_on_startup():
    """Проверяет все базы данных при запуске"""
    print("\n🔍 ** ПРОВЕРКА БАЗ ДАННЫХ ** 🔍")
    print("-" * 40)
    
    existing_dbs = 0
    new_dbs = 0
    corrupted_dbs = 0
    recovered_dbs = 0
    
    for guild in bot.guilds:
        db_path = get_db_path(guild.id)
        
        if os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT count(*) FROM user_fat")
                user_count = cursor.fetchone()[0]
                conn.close()
                existing_dbs += 1
                print(f"✅ {guild.name}: БД существует, пользователей: {user_count}")
            except sqlite3.DatabaseError:
                corrupted_dbs += 1
                print(f"⚠️ {guild.name}: БД ПОВРЕЖДЕНА - будет восстановлена")
                if safe_init_guild_database(guild.id, guild.name):
                    recovered_dbs += 1
                    print(f"   ✅ Восстановлена")
        else:
            new_dbs += 1
            print(f"📁 {guild.name}: БД отсутствует - будет создана")
            safe_init_guild_database(guild.id, guild.name)
    
    print("-" * 40)
    print(f"📊 ИТОГИ ПРОВЕРКИ:")
    print(f"   ✅ Существовало БД: {existing_dbs}")
    if corrupted_dbs > 0:
        print(f"   ⚠️  Было повреждено: {corrupted_dbs}")
        print(f"   🔧 Восстановлено: {recovered_dbs}")
    if new_dbs > 0:
        print(f"   📁 Создано новых БД: {new_dbs}")
    
    return existing_dbs, new_dbs, corrupted_dbs

@bot.event
async def on_ready():
    print(f"\n{'='*60}")
    print(f"✅ Бот успешно запущен как {bot.user}")
    print(f"📊 ID бота: {bot.user.id}")
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    existing, new, corrupted = check_databases_on_startup()
    
    print(f"\n📋 Серверы, на которых присутствует бот:")
    for guild in bot.guilds:
        print(f"  - {guild.name} (ID: {guild.id}, участников: {guild.member_count})")
    
    print(f"\n⚙️ НАСТРОЙКИ БОТА:")
    print(f"  ⏰ Кулдаун !жир: {COOLDOWN_HOURS} ч")
    print(f"  📦 Кулдаун кейса: {CASE_COOLDOWN_HOURS} ч")
    print(f"  🎭 Роль тестера: {TESTER_ROLE_NAME}")
    print(f"  📁 Папка БД: {DB_FOLDER}")
    print(f"  🍔 Бонус автобургеров: +{AUTOBURGER_MAX_BONUS*100}% макс")
    
    if new > 0 or corrupted > 0:
        print(f"\n⚠️ ВНИМАНИЕ: Произошли изменения в базах данных!")
        if new > 0:
            print(f"   📁 Добавлено новых БД: {new}")
        if corrupted > 0:
            print(f"   🔧 Восстановлено повреждённых: {corrupted}")
        print(f"   📝 Некоторые данные могли быть сброшены")
    else:
        print(f"\n✅ Все базы данных в порядке, данные сохранены")
    
    print(f"\n{'-'*40}")
    print(f"🎮 Доступные команды: !жирхелп")
    print(f"{'='*60}\n")
    
    bot.loop.create_task(autoburger_loop())
    bot.loop.create_task(passive_income_loop())

@bot.event
async def on_guild_join(guild):
    print(f"✅ Бот добавлен на новый сервер: {guild.name} (ID: {guild.id})")
    init_guild_database(guild.id)
    
    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            embed = discord.Embed(
                title="🍔 Жирбот прибыл!",
            )
            await channel.send(embed=embed)
            break

@bot.command(name='жир')
async def fat_command(ctx):
    """Меняет Display Name пользователя"""
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, _, _, total_activations, total_gain, _, _,
     legendary_burger, item_counts, _, _, _, fat_cooldown_time) = get_user_data(guild_id, user_id, user_name)
    
    # Определяем актуальный кулдаун с учётом бургера
    actual_cooldown = COOLDOWN_HOURS
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        actual_cooldown = BURGER_RANKS[legendary_burger]["fat_cooldown"] / 60  # переводим минуты в часы
    
    can_use, remaining = check_cooldown(fat_cooldown_time, actual_cooldown)
    
    if not can_use:
        embed = discord.Embed(
            title="⏳ Подождите!",
            description=f"{member.mention}, вы уже использовали команду недавно!",
            color=0xff0000
        )
        embed.add_field(name="Осталось подождать", value=format_time(remaining), inline=True)
        embed.add_field(name="Кулдаун", value=f"{actual_cooldown*60:.0f} мин", inline=True)
        embed.set_footer(text="Приходите взвешиваться позже!")
        await ctx.send(embed=embed)
        return
    
    # Получаем предметы пользователя для обработки легендарных предметов
    items_dict = get_user_items(item_counts)
    
    change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
        consecutive_plus, consecutive_minus, jackpot_pity, 
        autoburger_count, legendary_burger, items_dict, current_number
    )
    new_number = current_number + change
    
    # ВСЕГДА обновляем данные в БД, независимо от смены ника
    update_user_data(
        guild_id, user_id, new_number, user_name,
        new_consecutive_plus, new_consecutive_minus, new_jackpot_pity,
        autoburger_count, None, None,
        total_activations, total_gain, None, None,
        legendary_burger, item_counts, None, None, None,
        datetime.now()  # Это и есть fat_cooldown_time
    )
    
    # ПЫТАЕМСЯ обновить ник, но не прерываем выполнение если не получилось
    nick_updated = False
    try:
        display_name = member.display_name
        clean_name = display_name
        if "kg" in display_name:
            parts = display_name.split("kg", 1)
            if len(parts) > 1:
                clean_name = parts[1].strip()
                if not clean_name:
                    clean_name = user_name
        else:
            clean_name = display_name
        
        if not clean_name or len(clean_name) > 30:
            clean_name = user_name
        
        new_nick = format_nick_with_icon(new_number, clean_name, legendary_burger)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        
        await member.edit(nick=new_nick)
        nick_updated = True
    except discord.Forbidden:
        # Не хватает прав - просто логируем и продолжаем
        print(f"⚠️ Не удалось сменить ник для {user_name} (недостаточно прав)")
    except Exception as e:
        # Другие ошибки тоже не критичны
        print(f"⚠️ Ошибка при смене ника для {user_name}: {e}")
    
    # ВСЕГДА показываем сообщение о результате
    rank_name, rank_emoji = get_rank(new_number)
    
    if was_jackpot:
        embed_color = 0xffd700
        embed_title = "💰 ДЖЕКПОТ! 💰"
    else:
        embed_color = 0xff9933 if new_number >= 0 else 0x66ccff
        embed_title = "🍔 Набор массы"
    
    embed = discord.Embed(
        title=embed_title,
        description=f"**{member.mention}** теперь весит **{abs(new_number)}kg** на сервере **{ctx.guild.name}**!",
        color=embed_color
    )
    
    if was_jackpot:
        embed.add_field(name="💰 ДЖЕКПОТ!", value=f"+{change} кг", inline=True)
    elif change > 0:
        embed.add_field(name="📈 Изменение", value=f"+{change} кг", inline=True)
    elif change < 0:
        embed.add_field(name="📉 Изменение", value=f"{change} кг", inline=True)
    else:
        embed.add_field(name="⚖️ Изменение", value="0 кг", inline=True)
    
    embed.add_field(name="🍖 Текущий вес", value=f"{new_number}kg", inline=True)
    embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
    
    pity_info = []
    if was_jackpot:
        pity_info.append("💰 Джекпот сброшен!")
    elif was_minus:
        if consecutive_plus > 0:
            pity_info.append(f"❌ Серия плюсов ({consecutive_plus}) прервана!")
        pity_info.append(f"📉 Минусов подряд: {new_consecutive_minus}")
    else:
        if new_consecutive_plus > 0:
            pity_info.append(f"🔥 Плюсов подряд: {new_consecutive_plus}")
        if consecutive_minus > 0:
            pity_info.append(f"✅ Серия минусов ({consecutive_minus}) прервана!")
    
    if pity_info:
        embed.add_field(name="📊 Статистика", value="\n".join(pity_info), inline=False)
    
    if autoburger_count > 0:
        interval = get_autoburger_interval(autoburger_count)
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count)) * 100
        embed.add_field(name="🍔 Автобургеры", 
                       value=f"{autoburger_count} шт (каждые {interval} ч)\n⚡ Бонус к плюсу: +{current_boost:.1f}%", 
                       inline=True)
    
    # Добавляем информацию о возвышении если доступно
    available, burger_idx, burger_name, req_weight, chance = check_ascension_available(new_number, legendary_burger)
    if available:
        embed.add_field(
            name="✨ **ВОЗВЫШЕНИЕ ДОСТУПНО!** ✨",
            value=f"Вы достигли {req_weight}кг! Используйте `!возвышение`\n"
                  f"Шанс получить {BURGER_RANKS[burger_idx]['emoji']} {burger_name}: {chance*100:.0f}%",
            inline=False
        )
    
    # Если ник не обновился, добавляем предупреждение
    if not nick_updated:
        embed.add_field(
            name="⚠️ **ВНИМАНИЕ**",
            value="Не удалось обновить ник (недостаточно прав).\n"
                  "Вес в базе данных обновлён, но в нике не отображается.",
            inline=False
        )
    
    embed.add_field(name="⏰ Следующая команда", value=f"через {actual_cooldown*60:.0f} мин", inline=True)
    
    # Если ник обновился, показываем его, иначе показываем сообщение об ошибке
    if nick_updated:
        embed.set_footer(text=f"Новый ник: {new_nick}")
    else:
        embed.set_footer(text="⚡ Вес обновлён в БД, но ник не изменён")
    
    await ctx.send(embed=embed)

@bot.command(name='жиркейс')
async def fat_case(ctx):
    """Открывает кейс с анимацией в стиле CS:GO"""
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    # Получаем данные пользователя
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, last_command, last_command_target, last_command_use_time) = get_user_data(guild_id, user_id, user_name)
    
    # Определяем актуальный кулдаун с учётом бургера
    actual_case_cooldown = CASE_COOLDOWN_HOURS
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        actual_case_cooldown = BURGER_RANKS[legendary_burger]["case_cooldown"]
    
    can_use, remaining = check_cooldown(last_case_time, actual_case_cooldown)
    
    if not can_use:
        embed = discord.Embed(
            title="⏳ Подождите!",
            description=f"{member.mention}, вы уже открывали кейс недавно!",
            color=0xff0000
        )
        embed.add_field(name="Осталось подождать", value=format_time(remaining), inline=True)
        embed.add_field(name="Кулдаун кейса", value=f"{actual_case_cooldown} часов", inline=True)
        await ctx.send(embed=embed)
        return
    
    # СОЗДАЁМ КРАСИВЫЙ КЕЙС С РЕАКЦИЯМИ
    case_embed = discord.Embed(
        title="📦 **ЖИРКЕЙС** 📦",
        description=(
            f"{member.mention}, у вас есть кейс!\n\n"
            f"**Нажмите на 🖱️ чтобы открыть**\n\n"
            f"┌───────────────┐\n"
            f"│----🍔🥤🍟-------│\n"
            f"│----Ж И Р-----------│\n"
            f"│----К Е Й С---------│\n"
            f"│----🍕🌭🍗-------│\n"
            f"└───────────────┘"
        ),
        color=0xffaa00
    )
    case_embed.set_footer(text="У вас 30 секунд чтобы открыть кейс!")
    
    case_msg = await ctx.send(embed=case_embed)
    await case_msg.add_reaction("🖱️")
    
    # Собираем эмодзи из возможных призов для анимации
    prize_emojis = []
    for prize in CASE_PRIZES:
        if prize["emoji"] not in prize_emojis:
            prize_emojis.append(prize["emoji"])
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) == "🖱️" and reaction.message.id == case_msg.id
    
    try:
        reaction, user = await bot.wait_for('reaction_add', timeout=30.0, check=check)
        
        # ПЫТАЕМСЯ УДАЛИТЬ РЕАКЦИИ, НО НЕ ПАДАЕМ, ЕСЛИ НЕТ ПРАВ
        try:
            await case_msg.clear_reactions()
        except discord.Forbidden:
            print(f"⚠️ Нет прав на удаление реакций на сервере {ctx.guild.name}")
        except Exception as e:
            print(f"⚠️ Ошибка при удалении реакций: {e}")
        
        # ПОЛУЧАЕМ ПРИЗ ЗАРАНЕЕ
        prize = get_case_prize(legendary_burger)
        
        # Проверяем наличие стакана воды у пользователя
        items_dict = get_user_items(item_counts)
        has_water = items_dict.get("Стакан воды", 0) > 0
        
        # Рассчитываем изменения ДО анимации с учётом стакана воды
        new_autoburger_count = autoburger_count
        new_number = current_number
        new_next_autoburger_time = next_autoburger_time
        actual_prize_value = prize["value"]
        
        if prize["value"] == "autoburger":
            new_autoburger_count = autoburger_count + 1
            interval = get_autoburger_interval(new_autoburger_count)
            if interval:
                new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
            result_display = f"🎉 **АВТОБУРГЕР!** 🍔✨"
            result_color = 0xffd700
            
            # Автобургер тоже уменьшается от стакана воды? Нет, это отдельный предмет
            # Но сам автобургер будет давать кг позже через apply_autoburger
        else:
            # Применяем эффект стакана воды если есть
            if has_water:
                actual_prize_value = prize["value"] // 3
                new_number = current_number + actual_prize_value
            else:
                new_number = current_number + prize["value"]
            
            result_display = f"🎉 **{actual_prize_value:+d} кг** {prize['emoji']}"
            result_color = 0xffaa00
        
        # ОБНОВЛЯЕМ ДАННЫЕ В БД ДО АНИМАЦИИ
        current_time = datetime.now()
        update_user_data(
            guild_id, user_id, new_number, user_name,
            consecutive_plus, consecutive_minus, jackpot_pity,
            new_autoburger_count, current_time, new_next_autoburger_time,
            total_activations, total_gain, last_result, last_activation_time,
            legendary_burger, item_counts,
            last_command, last_command_target, last_command_use_time
        )
        
                # ГЕНЕРИРУЕМ ЛИНИЮ ИЗ 50 ЭМОДЗИ
        line = []
        for i in range(50):
            line.append(random.choice(prize_emojis))
        
        # СТАВИМ ПРИЗ НА 39 МЕСТО (индекс 38)
        line[38] = prize['emoji']
        
        # ТЕКУЩАЯ ПОЗИЦИЯ ПРОКРУТКИ (начинаем с 0)
        position = 0
        
        # Embed для анимации
        anim_embed = discord.Embed(
            title="🎰 **ЖИРКЕЙС** 🎰",
            description="",
            color=0xffaa00
        )
        
        # ОПТИМИЗИРОВАННАЯ АНИМАЦИЯ (7 секунд)
        animation_frames = [
            # (пропуск, скорость)
            (12, 0.3),  # Кадр 1 - быстро
            (8, 0.4),   # Кадр 2 - средне-быстро
            (5, 0.5),   # Кадр 3 - средне
            (3, 0.6),   # Кадр 4 - средне-медленно
            (2, 0.7),   # Кадр 5 - медленно
            (1, 0.8),   # Кадр 6 - очень медленно
            (0, 1.2),   # Кадр 7 - стоп
        ]
        
        for skip, speed in animation_frames:
            if skip > 0:
                # Пропускаем эмодзи
                for _ in range(skip):
                    position += 1
                    # Добавляем случайный эмодзи в конец для бесконечности
                    line.append(random.choice(prize_emojis))
                    line.pop(0)
            
            # Показываем 9 эмодзи
            visible = line[position:position+9]
            display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
            
            anim_embed.description = f"**{display_line}**"
            await case_msg.edit(embed=anim_embed)
            await asyncio.sleep(speed)
        
        # ПОКАЗЫВАЕМ РЕЗУЛЬТАТ
        result_embed = discord.Embed(
            title="🎯 **РЕЗУЛЬТАТ** 🎯",
            description=f"**{display_line}**\n\n**{result_display}**",
            color=result_color
        )
        await case_msg.edit(embed=result_embed)
        
        # Держим результат 1.5 секунды
        await asyncio.sleep(1.5)
        
        # Обновляем ник, если изменился вес
        if prize["value"] != "autoburger" and prize["value"] != 0:
            try:
                display_name = member.display_name
                clean_name = display_name
                if "kg" in display_name:
                    parts = display_name.split("kg", 1)
                    if len(parts) > 1:
                        clean_name = parts[1].strip()
                        if not clean_name:
                            clean_name = user_name
                else:
                    clean_name = display_name
                
                if not clean_name or len(clean_name) > 30:
                    clean_name = user_name
                
                new_nick = format_nick_with_icon(new_number, clean_name, legendary_burger)
                if len(new_nick) > 32:
                    new_nick = new_nick[:32]
                
                await member.edit(nick=new_nick)
            except:
                pass
        
        # Финальный embed с детальной информацией
        rank_name, rank_emoji = get_rank(new_number)
        
        if prize["value"] == "autoburger":
            final_embed = discord.Embed(
                title="🍔✨ **А В Т О Б У Р Г Е Р** ✨🍔",
                description=f"""
# 🎉🎉🎉 **ПОЗДРАВЛЯЕМ!** 🎉🎉🎉

**{member.mention}** выиграл главный приз!

## 🍔 **АВТОБУРГЕР** 🍔

Теперь команда **!жир** будет автоматически применяться
каждые **{get_autoburger_interval(new_autoburger_count)} часов**!

### 📊 Статистика:
- Всего автобургеров: **{new_autoburger_count}**
- Интервал: **каждые {get_autoburger_interval(new_autoburger_count)} ч**
- Бонус к шансу плюса: **+{AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * new_autoburger_count)) * 100:.1f}%**

*Автобургеры складываются, увеличивая бонус и уменьшая интервал!*
                """,
                color=0xffaa00
            )
            final_embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/1085819476236259459.png")
            final_embed.set_footer(text="✨ Удачи в наборе массы! ✨")
        else:
            final_embed = discord.Embed(
                title="📦 Открытие кейса",
                description=f"**{member.mention}** открыл кейс и получил:",
                color=0xffaa00
            )
            
            final_embed.add_field(name="🎁 Приз", value=f"**{actual_prize_value:+d} кг** {prize['emoji']}", inline=False)
            final_embed.add_field(name="🍖 Новый вес", value=f"{new_number}kg", inline=True)
            final_embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
            
            if new_autoburger_count > autoburger_count:
                final_embed.add_field(name="🍔 Автобургеры", value=f"+1! Теперь: {new_autoburger_count}", inline=True)
            
            # Добавляем информацию о стакане воды если применимо
            if has_water and prize["value"] != "autoburger":
                final_embed.add_field(
                    name="💧 Эффект стакана воды",
                    value=f"Исходный приз: {prize['value']:+d} кг\nУменьшен в 3 раза",
                    inline=False
                )
        
        final_embed.add_field(name="⏰ Следующий кейс", value=f"через {actual_case_cooldown} часов", inline=False)
        
        await ctx.send(embed=final_embed)
        
    except asyncio.TimeoutError:
        await case_msg.clear_reactions()
        timeout_embed = discord.Embed(
            title="⏰ Время вышло",
            description=f"{member.mention}, вы не открыли кейс вовремя. Попробуйте снова!",
            color=0xff0000
        )
        await case_msg.edit(embed=timeout_embed)

@bot.command(name='жиркейс_шансы')
async def fat_case_chances(ctx):
    """
    Показывает шансы выпадения призов в кейсе
    """
    embed = discord.Embed(
        title="📊 **ШАНСЫ В КЕЙСЕ** 📊",
        description="Вероятность выпадения каждого приза:",
        color=0xffaa00
    )
    
    # Сортируем призы по редкости (самые редкие внизу)
    sorted_prizes = sorted(CASE_PRIZES, key=lambda x: x['chance'] if x['chance'] > 0 else 999, reverse=True)
    
    chances_text = ""
    rare_text = ""
    legendary_text = ""
    
    for prize in sorted_prizes:
        if prize["value"] == "autoburger":
            legendary_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']:.5f}%\n"
        elif prize["value"] >= 1000:
            rare_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']}%\n"
        else:
            chances_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']}%\n"
    
    if chances_text:
        embed.add_field(name="📦 **Обычные призы**", value=chances_text, inline=False)
    
    if rare_text:
        embed.add_field(name="✨ **Редкие призы**", value=rare_text, inline=False)
    
    if legendary_text:
        embed.add_field(name="🌟 **Легендарные призы**", value=legendary_text, inline=False)
    
    # Добавляем информацию о кулдауне
    embed.add_field(
        name="⏰ **Информация**",
        value=(
            f"• Кулдаун кейса: **{CASE_COOLDOWN_HOURS} часов**\n"
            f"• Команда: `!жиркейс`\n"
            f"• Для открытия нажмите на 🖱️ после использования команды"
        ),
        inline=False
    )
    
    # Добавляем информацию о бонусах от алмазного бургера
    embed.add_field(
        name="💎 **Бонус алмазного бургера**",
        value=(
            f"• Шансы на редкие призы **x2**\n"
            f"• Шанс на автобургер: **{CASE_PRIZES[-1]['chance'] * 2:.5f}%**\n"
            f"• Шанс на +5000кг: **{CASE_PRIZES[-2]['chance'] * 2}%**"
        ),
        inline=False
    )
    
    embed.set_footer(text="🎰 Удачи в открытии кейсов!")
    
    await ctx.send(embed=embed)

@bot.command(name='жиротрясы')
async def fat_leaderboard(ctx):
    """Таблица рекордов"""
    guild_id = ctx.guild.id
    guild_name = ctx.guild.name
    
    users = get_all_users_sorted(guild_id)
    
    if not users:
        await ctx.send(f"📭 На сервере **{guild_name}** пока никто не участвовал!")
        return
    
    embed = discord.Embed(
        title=f"🏆 Таблица жиротрясов - {guild_name}",
        description="Рейтинг пользователей по весу (от самых толстых до самых худых)",
        color=0xffaa00
    )
    
    leaderboard_text = ""
    for i, (user_name, number, last_update, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, total_acts, total_gain, legendary_burger) in enumerate(users, 1):
        if i == 1:
            place_icon = "🥇"
        elif i == 2:
            place_icon = "🥈"
        elif i == 3:
            place_icon = "🥉"
        else:
            place_icon = "🔹"
        
        rank_name, rank_emoji = get_rank(number)
        
        # ФОРМИРУЕМ НИК С ЭМОДЗИ БУРГЕРА В НАЧАЛЕ
        display_name = user_name
        if legendary_burger is not None and legendary_burger >= 0:
            burger_emoji = BURGER_RANKS[legendary_burger]["emoji"]
            display_name = f"{burger_emoji}{user_name}"
        
        # Добавляем информацию о накоплениях (в конец)
        pity_emojis = []
        if consecutive_plus and consecutive_plus > 0:
            pity_emojis.append("🔥")
        if consecutive_minus and consecutive_minus > 0:
            pity_emojis.append("❄️")
        if jackpot_pity and jackpot_pity > 0:
            pity_emojis.append("💰")
        if autoburger_count and autoburger_count > 0:
            pity_emojis.append(f"🍔{autoburger_count}")
        if total_acts and total_acts > 0:
            pity_emojis.append(f"⚡{total_acts}")
        
        pity_str = f" {' '.join(pity_emojis)}" if pity_emojis else ""
        
        # ТЕПЕРЬ БУРГЕР В НАЧАЛЕ, А НАКОПЛЕНИЯ В КОНЦЕ
        leaderboard_text += f"{place_icon} **{i}.** {display_name} — **{number}kg** {rank_emoji} *{rank_name}*{pity_str}\n"
        
        if len(leaderboard_text) > 900:
            leaderboard_text += "... и ещё несколько участников"
            break
    
    embed.description = leaderboard_text
    
    stats = get_guild_stats(guild_id)
    
    # Статистика по бургерам
    burger_stats = ""
    for i, count in enumerate(stats['burger_counts']):
        if count > 0:
            burger_stats += f"{BURGER_RANKS[i]['emoji']} {BURGER_RANKS[i]['name']}: {count}\n"
    
    embed.add_field(name="📊 Статистика сервера", 
                   value=f"Участников: {stats['total_users']}\n"
                         f"Суммарный вес: {stats['total_weight']}kg\n"
                         f"Средний вес: {stats['avg_weight']:.1f}kg\n"
                         f"🔼 Толстых: {stats['positive']} | 🔽 Худых: {stats['negative']} | ⚖️ Нулевых: {stats['zero']}\n"
                         f"🍔 Всего автобургеров: {stats['total_autoburgers']}\n"
                         f"⚡ Всего срабатываний: {stats['total_activations']}\n"
                         f"📈 Всего набрано: {stats['total_gain']} кг", 
                   inline=False)
    
    if burger_stats:
        embed.add_field(name="✨ Легендарные бургеры", value=burger_stats, inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='жирстат')
async def fat_stats(ctx, member: discord.Member = None):
    """Показывает подробную статистику автобургеров"""
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, _, _, _) = get_user_data(guild_id, user_id, target.name)
    
    embed = discord.Embed(
        title=f"📊 Статистика автобургеров - {target.display_name}",
        color=0x3498db
    )
    
    embed.add_field(name="🍔 Всего автобургеров", value=str(autoburger_count), inline=True)
    embed.add_field(name="⚡ Срабатываний", value=str(total_activations), inline=True)
    embed.add_field(name="📈 Всего набрано", value=f"{total_gain} кг", inline=True)
    
    if total_activations > 0:
        avg_gain = total_gain / total_activations
        embed.add_field(name="📊 Средний прирост", value=f"{avg_gain:.1f} кг", inline=True)
    
    if last_result and last_activation_time:
        try:
            if isinstance(last_activation_time, str):
                last_time = datetime.fromisoformat(last_activation_time)
            else:
                last_time = last_activation_time
            time_diff = datetime.now() - last_time
            hours = time_diff.total_seconds() / 3600
            embed.add_field(name="🕒 Последнее", 
                           value=f"{last_result} ({hours:.1f} ч назад)", 
                           inline=False)
        except:
            pass
    
    if autoburger_count > 0:
        interval = get_autoburger_interval(autoburger_count)
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count)) * 100
        embed.add_field(name="⚡ Текущий бонус", 
                       value=f"+{current_boost:.1f}% к плюсу (каждые {interval} ч)", 
                       inline=False)
    
    if next_autoburger_time:
        try:
            if isinstance(next_autoburger_time, str):
                next_time = datetime.fromisoformat(next_autoburger_time)
            else:
                next_time = next_autoburger_time
            time_diff = next_time - datetime.now()
            if time_diff.total_seconds() > 0:
                embed.add_field(name="⏰ Следующий автобургер", 
                               value=f"через {format_time(time_diff.total_seconds())}", 
                               inline=False)
        except:
            pass
    
    await ctx.send(embed=embed)

@bot.command(name='жир_инфо')
async def fat_info(ctx, member: discord.Member = None):
    """Информация о пользователе"""
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    (number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, last_command, last_command_target, last_command_use_time) = get_user_data(guild_id, user_id, target.name)
    
    rank_name, rank_emoji = get_rank(number)
    
    # Определяем актуальные кулдауны с учётом бургера
    actual_fat_cooldown = COOLDOWN_HOURS
    actual_case_cooldown = CASE_COOLDOWN_HOURS
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        actual_fat_cooldown = BURGER_RANKS[legendary_burger]["fat_cooldown"] / 60
        actual_case_cooldown = BURGER_RANKS[legendary_burger]["case_cooldown"]
    
    embed = discord.Embed(
        title=f"🍔 Информация о {target.display_name} на сервере {ctx.guild.name}",
        color=0x00ff00
    )
    
    embed.add_field(name="Текущий вес", value=f"{number}kg", inline=True)
    embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
    
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        burger = BURGER_RANKS[legendary_burger]
        embed.add_field(name=f"{burger['emoji']} Легендарный бургер", 
                       value=f"**{burger['name']}**\nМножитель: x{burger['multiplier']}", 
                       inline=True)
    
    pity_emojis = []
    if consecutive_plus > 0:
        pity_emojis.append(f"🔥{consecutive_plus}")
    if consecutive_minus > 0:
        pity_emojis.append(f"❄️{consecutive_minus}")
    if jackpot_pity > 0:
        pity_emojis.append(f"💰{jackpot_pity}")
    
    if pity_emojis:
        embed.add_field(name="📊 Счётчики", value=" ".join(pity_emojis), inline=True)
    
    if autoburger_count > 0:
        interval = get_autoburger_interval(autoburger_count)
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count)) * 100
        embed.add_field(name="🍔 Автобургеры", 
                       value=f"{autoburger_count} шт (каждые {interval} ч)\n⚡ Бонус: +{current_boost:.1f}%\n⚡ Срабатываний: {total_activations}", 
                       inline=True)
        
        if next_autoburger_time:
            try:
                if isinstance(next_autoburger_time, str):
                    next_time = datetime.fromisoformat(next_autoburger_time)
                else:
                    next_time = next_autoburger_time
                time_diff = next_time - datetime.now()
                if time_diff.total_seconds() > 0:
                    embed.add_field(name="⏰ Следующий автобургер", 
                                   value=f"через {format_time(time_diff.total_seconds())}", 
                                   inline=True)
            except:
                pass
    
    can_use, remaining = check_cooldown(last_time, actual_fat_cooldown)
    if can_use:
        cooldown_status = f"✅ !жир доступен (КД {actual_fat_cooldown*60:.0f} мин)"
    else:
        cooldown_status = f"⏳ !жир через {format_time(remaining)}"
    
    can_use_case, case_remaining = check_cooldown(last_case_time, actual_case_cooldown)
    if can_use_case:
        case_status = f"✅ !жиркейс доступен (КД {actual_case_cooldown} ч)"
    else:
        case_status = f"⏳ !жиркейс через {format_time(case_remaining)}"
    
    embed.add_field(name="Команды", value=f"{cooldown_status}\n{case_status}", inline=False)
    
    available, burger_idx, burger_name, req_weight, chance = check_ascension_available(number, legendary_burger)
    if available:
        embed.add_field(
            name="✨ **ВОЗВЫШЕНИЕ ДОСТУПНО!** ✨",
            value=f"Цель: {req_weight}кг\nШанс: {chance*100:.0f}%\nИспользуйте `!возвышение`",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name='возвышение')
async def ascension_command(ctx):
    """
    Попытка получить легендарный бургер
    Доступно при достижении определённого веса
    """
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, last_command, last_command_target, last_command_use_time) = get_user_data(guild_id, user_id, user_name)
    
    available, burger_idx, burger_name, req_weight, chance = check_ascension_available(current_number, legendary_burger)
    
    if not available:
        if legendary_burger >= DIAMOND_BURGER:
            embed = discord.Embed(
                title="❌ Возвышение недоступно",
                description=f"{member.mention}, у вас уже есть **Алмазный бургер**!\n"
                           f"Это максимальный уровень возвышения.",
                color=0xff0000
            )
        else:
            next_burger = legendary_burger + 1 if legendary_burger >= 0 else 0
            if next_burger < len(BURGER_RANKS):
                req = BURGER_RANKS[next_burger]["weight_required"]
                embed = discord.Embed(
                    title="❌ Возвышение недоступно",
                    description=f"{member.mention}, вам нужно достичь **{req}кг**\n"
                               f"Текущий вес: **{current_number}кг**",
                    color=0xff0000
                )
            else:
                embed = discord.Embed(
                    title="❌ Возвышение недоступно",
                    description=f"{member.mention}, для вас больше нет возвышений!",
                    color=0xff0000
                )
        await ctx.send(embed=embed)
        return
    
    roll = random.random()
    success = roll < chance
    
    items_dict = get_user_items(item_counts)
    
    if success:
        new_burger_idx = burger_idx
        new_number = 0
        new_autoburger_count = 0
        new_next_autoburger_time = None
        
        display_name = member.display_name
        clean_name = display_name
        if "kg" in display_name:
            parts = display_name.split("kg", 1)
            if len(parts) > 1:
                clean_name = parts[1].strip()
                if not clean_name:
                    clean_name = user_name
        else:
            clean_name = display_name
        
        if not clean_name or len(clean_name) > 30:
            clean_name = user_name
        
        new_nick = format_nick_with_icon(new_number, clean_name, new_burger_idx)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        
        try:
            await member.edit(nick=new_nick)
        except:
            pass
        
        update_user_data(
            guild_id, user_id, new_number, user_name,
            consecutive_plus, consecutive_minus, jackpot_pity,
            new_autoburger_count, last_case_time, new_next_autoburger_time,
            total_activations, total_gain, last_result, last_activation_time,
            new_burger_idx, save_user_items(items_dict),
            last_command, last_command_target, last_command_use_time
        )
        
        burger_emoji = BURGER_RANKS[new_burger_idx]["emoji"]
        embed = discord.Embed(
            title="✨ **ВОЗВЫШЕНИЕ УСПЕШНО!** ✨",
            description=f"**{member.mention}** получил {burger_emoji} **{burger_name}**!",
            color=0x00ff00
        )
        
        embed.add_field(name="📊 Результат", 
                       value=f"Вес сброшен до **0кг**\n"
                             f"Автобургеры обнулены\n"
                             f"Новый ник: {new_nick}",
                       inline=False)
        
        burger = BURGER_RANKS[new_burger_idx]
        bonus_text = f"Множитель: x{burger['multiplier']}\n"
        bonus_text += f"КД !жир: {burger['fat_cooldown']} мин\n"
        bonus_text += f"КД !жиркейс: {burger['case_cooldown']} ч"
        
        if new_burger_idx == DIAMOND_BURGER:
            bonus_text += f"\n+10% к шансу плюса\nРедкие предметы x2"
        
        embed.add_field(name="⚡ Полученные бонусы", value=bonus_text, inline=False)
        
    else:
        new_number = current_number // 2
        
        display_name = member.display_name
        clean_name = display_name
        if "kg" in display_name:
            parts = display_name.split("kg", 1)
            if len(parts) > 1:
                clean_name = parts[1].strip()
                if not clean_name:
                    clean_name = user_name
        else:
            clean_name = display_name
        
        if not clean_name or len(clean_name) > 30:
            clean_name = user_name
        
        new_nick = format_nick_with_icon(new_number, clean_name, legendary_burger)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        
        try:
            await member.edit(nick=new_nick)
        except:
            pass
        
        update_user_data(
            guild_id, user_id, new_number, user_name,
            consecutive_plus, consecutive_minus, jackpot_pity,
            autoburger_count, last_case_time, next_autoburger_time,
            total_activations, total_gain, last_result, last_activation_time,
            legendary_burger, save_user_items(items_dict),
            last_command, last_command_target, last_command_use_time
        )
        
        embed = discord.Embed(
            title="💔 **ВОЗВЫШЕНИЕ НЕ УДАЛОСЬ** 💔",
            description=f"**{member.mention}** попытался возвыситься, но потерпел неудачу!",
            color=0xff0000
        )
        
        embed.add_field(name="📊 Результат", 
                       value=f"Вес уменьшен вдвое: **{new_number}кг**\n"
                             f"Шанс был: {chance*100:.0f}%\n"
                             f"Повезёт в следующий раз!",
                       inline=False)
    
    embed.set_footer(text="💪 Продолжайте набирать массу!")
    await ctx.send(embed=embed)

def generate_shop_items():
    """Генерирует новый набор предметов для магазина"""
    slots = []
    used_indices = set()
    
    for slot in range(SHOP_SLOTS):
        # Пытаемся найти предмет для этого слота
        chosen_item = None
        for _ in range(50):  # Максимум 50 попыток найти предмет
            item_idx = random.randint(0, len(SHOP_ITEMS) - 1)
            if item_idx in used_indices:
                continue
            
            item = SHOP_ITEMS[item_idx]
            if random.random() < item["chance"]:
                chosen_item = item
                used_indices.add(item_idx)
                break
        
        if chosen_item:
            amount = random.randint(chosen_item["min_amount"], chosen_item["max_amount"])
            slots.append({
                "name": chosen_item["name"],
                "amount": amount,
                "price": chosen_item["price"],
                "description": chosen_item["description"],
                "gain_per_24h": chosen_item.get("gain_per_24h", 0)
            })
        else:
            slots.append(None)
    
    return slots

async def ensure_shop_updated(guild_id):
    """Проверяет и обновляет магазин если нужно"""
    result = get_shop_data(guild_id)
    current_time = datetime.now()
    
    if result:
        slots_json, last_update_str, next_update_str = result
        
        # Преобразуем строки обратно в datetime объекты
        last_update = None
        next_update = None
        if last_update_str:
            try:
                last_update = datetime.fromisoformat(last_update_str)
            except:
                last_update = None
        if next_update_str:
            try:
                next_update = datetime.fromisoformat(next_update_str)
            except:
                next_update = None
        
        if next_update and current_time >= next_update:
            # Время обновлять магазин
            new_slots = generate_shop_items()
            last_update = current_time
            next_update = current_time + timedelta(hours=SHOP_UPDATE_HOURS)
            update_shop_data(guild_id, new_slots, last_update, next_update)
            return new_slots, last_update, next_update
        else:
            # Магазин ещё актуален
            slots = json.loads(slots_json) if slots_json else []
            return slots, last_update, next_update
    else:
        # Первое создание магазина
        new_slots = generate_shop_items()
        last_update = current_time
        next_update = current_time + timedelta(hours=SHOP_UPDATE_HOURS)
        update_shop_data(guild_id, new_slots, last_update, next_update)
        return new_slots, last_update, next_update

@bot.command(name='магазин')
async def shop_command(ctx):
    """
    Показывает доступные предметы в магазине
    Используйте !купить [слот] [количество] после просмотра
    """
    guild_id = ctx.guild.id
    member = ctx.author
    
    # Запоминаем, что пользователь использовал !магазин
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, _, _, _) = get_user_data(guild_id, str(member.id), member.name)
    
    # Обновляем last_command для пользователя
    update_user_data(
        guild_id, str(member.id), current_number, member.name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        autoburger_count, last_case_time, next_autoburger_time,
        total_activations, total_gain, last_result, last_activation_time,
        legendary_burger, item_counts,
        "shop", None, datetime.now()
    )
    
    slots, last_update, next_update = await ensure_shop_updated(guild_id)
    
    embed = discord.Embed(
        title="🏪 **МАГАЗИН** 🏪",
        description="Доступные предметы (используйте `!купить [слот] [количество]`):",
        color=0xffaa00
    )
    
    items_text = ""
    for i, slot in enumerate(slots, 1):
        if slot:
            items_text += f"**{i}.** {slot['name']} — {slot['amount']} шт — **{slot['price']} кг/шт**\n"
            items_text += f"   └ {slot['description']}\n"
        else:
            items_text += f"**{i}.** 🕳️ Пусто\n"
    
    embed.add_field(name="📦 Товары", value=items_text, inline=False)
    
    last_update_str = last_update.strftime("%d.%m.%Y %H:%M") if last_update else "Никогда"
    next_update_str = next_update.strftime("%d.%m.%Y %H:%M") if next_update else "Скоро"
    
    embed.add_field(name="⏰ Обновление", 
                   value=f"Последнее: {last_update_str}\nСледующее: {next_update_str}", 
                   inline=False)
    
    embed.set_footer(text="💸 Тратьте кг с умом!")
    
    await ctx.send(embed=embed)

@bot.command(name='купить')
async def buy_command(ctx, slot: int, amount: int = 1):
    """
    Покупает предмет из магазина
    Использование: !купить [номер слота] [количество]
    Пример: !купить 1 2 - купить 2 штуки из первого слота
    """
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    
    # Проверяем, что слот корректен
    if slot < 1 or slot > SHOP_SLOTS:
        await ctx.send(f"❌ Слот должен быть от 1 до {SHOP_SLOTS}!")
        return
    
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    
    # Получаем данные пользователя
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, last_command, last_command_target, last_command_use_time_str) = get_user_data(guild_id, user_id, member.name)
    
    # ПРЕОБРАЗУЕМ СТРОКУ В DATETIME, ЕСЛИ НУЖНО
    last_command_use_time = None
    if last_command_use_time_str:
        try:
            if isinstance(last_command_use_time_str, str):
                last_command_use_time = datetime.fromisoformat(last_command_use_time_str)
            else:
                last_command_use_time = last_command_use_time_str
        except:
            last_command_use_time = None
    
    # Проверяем, что пользователь использовал !магазин недавно (в течение 5 минут)
    if last_command != "shop" or not last_command_use_time:
        await ctx.send("❌ Сначала используйте `!магазин` для просмотра доступных товаров!")
        return
    
    time_since_shop = datetime.now() - last_command_use_time
    if time_since_shop.total_seconds() > 300:  # 5 минут
        await ctx.send("❌ Время ожидания истекло. Используйте `!магазин` заново!")
        # Сбрасываем last_command
        update_user_data(
            guild_id, user_id, current_number, member.name,
            consecutive_plus, consecutive_minus, jackpot_pity,
            autoburger_count, last_case_time, next_autoburger_time,
            total_activations, total_gain, last_result, last_activation_time,
            legendary_burger, item_counts,
            None, None, None
        )
        return
    
    # Получаем актуальный магазин
    slots, last_update, next_update = await ensure_shop_updated(guild_id)
    
    # Проверяем, что слот не пуст
    if slot - 1 >= len(slots) or not slots[slot - 1]:
        await ctx.send(f"❌ В слоте {slot} ничего нет!")
        return
    
    item = slots[slot - 1]
    
    # Проверяем, что есть нужное количество
    if amount > item["amount"]:
        await ctx.send(f"❌ В наличии только {item['amount']} шт!")
        return
    
    # Проверяем, хватает ли кг
    total_price = item["price"] * amount
    if current_number < total_price:
        await ctx.send(f"❌ Недостаточно кг! Нужно: {total_price} кг, у вас: {current_number} кг")
        return
    
    # Выполняем покупку
    new_number = current_number - total_price
    item["amount"] -= amount
    
    # Обновляем инвентарь пользователя
    items_dict = get_user_items(item_counts)
    items_dict[item["name"]] = items_dict.get(item["name"], 0) + amount
    
    # Обновляем магазин в БД
    update_shop_data(guild_id, slots, last_update, next_update)
    
    # Обновляем данные пользователя
    update_user_data(
        guild_id, user_id, new_number, member.name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        autoburger_count, last_case_time, next_autoburger_time,
        total_activations, total_gain, last_result, last_activation_time,
        legendary_burger, save_user_items(items_dict),
        None, None, None  # Сбрасываем last_command
    )
    
    # Обновляем ник
    try:
        display_name = member.display_name
        clean_name = display_name
        if "kg" in display_name:
            parts = display_name.split("kg", 1)
            if len(parts) > 1:
                clean_name = parts[1].strip()
                if not clean_name:
                    clean_name = member.name
        else:
            clean_name = display_name
        
        if not clean_name or len(clean_name) > 30:
            clean_name = member.name
        
        new_nick = format_nick_with_icon(new_number, clean_name, legendary_burger)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        
        await member.edit(nick=new_nick)
    except:
        pass
    
    embed = discord.Embed(
        title="✅ Покупка совершена!",
        description=f"**{member.mention}** приобрёл товары!",
        color=0x00ff00
    )
    
    embed.add_field(name="📦 Предмет", value=f"{item['name']} x{amount}", inline=True)
    embed.add_field(name="💰 Цена", value=f"{total_price} кг", inline=True)
    embed.add_field(name="💸 Осталось", value=f"{new_number} кг", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='датьжир')
async def give_fat(ctx, target: discord.Member, amount: int):
    """Передаёт указанное количество кг другому пользователю"""
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    
    guild_id = ctx.guild.id
    giver = ctx.author
    giver_id = str(giver.id)
    giver_name = giver.name
    target_id = str(target.id)
    target_name = target.name
    
    if giver_id == target_id:
        await ctx.send("❌ Нельзя передавать кг самому себе!")
        return
    
    (giver_number, giver_last_time, giver_plus, giver_minus, giver_jackpot,
     giver_burgers, giver_case_time, giver_next_burger,
     giver_acts, giver_gain, giver_last_res, giver_last_time,
     giver_legendary, giver_items, _, _, _) = get_user_data(guild_id, giver_id, giver_name)
    
    (target_number, target_last_time, target_plus, target_minus, target_jackpot,
     target_burgers, target_case_time, target_next_burger,
     target_acts, target_gain, target_last_res, target_last_time,
     target_legendary, target_items, _, _, _) = get_user_data(guild_id, target_id, target_name)
    
    if giver_number < amount:
        await ctx.send(f"❌ У вас недостаточно кг! Есть: {giver_number} кг, нужно: {amount} кг")
        return
    
    new_giver_number = giver_number - amount
    new_target_number = target_number + amount
    
    update_user_data(guild_id, giver_id, new_giver_number, giver_name,
                    giver_plus, giver_minus, giver_jackpot, giver_burgers,
                    giver_case_time, giver_next_burger,
                    giver_acts, giver_gain, giver_last_res, giver_last_time,
                    giver_legendary, giver_items)
    
    update_user_data(guild_id, target_id, new_target_number, target_name,
                    target_plus, target_minus, target_jackpot, target_burgers,
                    target_case_time, target_next_burger,
                    target_acts, target_gain, target_last_res, target_last_time,
                    target_legendary, target_items)
    
    try:
        display_name = giver.display_name
        clean_name = display_name
        if "kg" in display_name:
            parts = display_name.split("kg", 1)
            if len(parts) > 1:
                clean_name = parts[1].strip()
                if not clean_name:
                    clean_name = giver_name
        else:
            clean_name = display_name
        if not clean_name or len(clean_name) > 30:
            clean_name = giver_name
        new_nick = format_nick_with_icon(new_giver_number, clean_name, giver_legendary)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        await giver.edit(nick=new_nick)
    except:
        pass
    
    try:
        display_name = target.display_name
        clean_name = display_name
        if "kg" in display_name:
            parts = display_name.split("kg", 1)
            if len(parts) > 1:
                clean_name = parts[1].strip()
                if not clean_name:
                    clean_name = target_name
        else:
            clean_name = display_name
        if not clean_name or len(clean_name) > 30:
            clean_name = target_name
        new_nick = format_nick_with_icon(new_target_number, clean_name, target_legendary)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        await target.edit(nick=new_nick)
    except:
        pass
    
    giver_rank, giver_rank_emoji = get_rank(new_giver_number)
    target_rank, target_rank_emoji = get_rank(new_target_number)
    
    embed = discord.Embed(
        title="⚖️ Перевод жира",
        description=f"**{giver.mention}** передал кг **{target.mention}**!",
        color=0xffaa00
    )
    
    embed.add_field(name="📤 Отправитель", 
                   value=f"{giver.mention}\nБыло: {giver_number}kg\nСтало: {new_giver_number}kg\n{giver_rank_emoji} {giver_rank}", 
                   inline=True)
    
    embed.add_field(name="📥 Получатель", 
                   value=f"{target.mention}\nБыло: {target_number}kg\nСтало: {new_target_number}kg\n{target_rank_emoji} {target_rank}", 
                   inline=True)
    
    embed.add_field(name="📦 Количество", value=f"{amount} кг", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='датьпредмет')
async def give_item(ctx, target: discord.Member, amount: int, *, item_name: str):
    """
    Передаёт предметы другому пользователю
    Использование: !датьпредмет @пользователь количество "название предмета"
    """
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    
    guild_id = ctx.guild.id
    giver = ctx.author
    giver_id = str(giver.id)
    giver_name = giver.name
    target_id = str(target.id)
    target_name = target.name
    
    if giver_id == target_id:
        await ctx.send("❌ Нельзя передавать предметы самому себе!")
        return
    
    # Получаем данные обоих пользователей
    (giver_number, giver_last_time, giver_plus, giver_minus, giver_jackpot,
     giver_burgers, giver_case_time, giver_next_burger,
     giver_acts, giver_gain, giver_last_res, giver_last_time,
     giver_legendary, giver_items_str, _, _, _) = get_user_data(guild_id, giver_id, giver_name)
    
    (target_number, target_last_time, target_plus, target_minus, target_jackpot,
     target_burgers, target_case_time, target_next_burger,
     target_acts, target_gain, target_last_res, target_last_time,
     target_legendary, target_items_str, _, _, _) = get_user_data(guild_id, target_id, target_name)
    
    # ПРОВЕРЯЕМ, НЕ АВТОБУРГЕР ЛИ ЭТО
    item_lower = item_name.lower()
    is_autoburger = any(word in item_lower for word in ["автобургер", "бургер", "autoburger"])
    
    if is_autoburger:
        # Передаём автобургеры
        if giver_burgers < amount:
            await ctx.send(f"❌ У вас недостаточно автобургеров! Есть: {giver_burgers}, нужно: {amount}")
            return
        
        # Выполняем передачу
        new_giver_burgers = giver_burgers - amount
        new_target_burgers = target_burgers + amount
        
        # Обновляем время следующего автобургера для получателя
        new_target_next_burger = None
        if new_target_burgers > 0:
            interval = get_autoburger_interval(new_target_burgers)
            if interval:
                new_target_next_burger = datetime.now() + timedelta(hours=interval)
        
        # Обновляем данные
        update_user_data(
            guild_id, giver_id, giver_number, giver_name,
            giver_plus, giver_minus, giver_jackpot, new_giver_burgers,
            giver_case_time, giver_next_burger,
            giver_acts, giver_gain, giver_last_res, giver_last_time,
            giver_legendary, giver_items_str,
            None, None, None
        )
        
        update_user_data(
            guild_id, target_id, target_number, target_name,
            target_plus, target_minus, target_jackpot, new_target_burgers,
            target_case_time, new_target_next_burger,
            target_acts, target_gain, target_last_res, target_last_time,
            target_legendary, target_items_str,
            None, None, None
        )
        
        embed = discord.Embed(
            title="🍔 Передача автобургера",
            description=f"**{giver.mention}** передал автобургер **{target.mention}**!",
            color=0xffaa00
        )
        
        embed.add_field(name="📦 Количество", value=f"{amount} шт", inline=True)
        embed.add_field(name="📤 У вас осталось", value=f"{new_giver_burgers} 🍔", inline=True)
        embed.add_field(name="📥 У получателя", value=f"{new_target_burgers} 🍔", inline=True)
        
        await ctx.send(embed=embed)
        return
    
    # Если не автобургер - работаем с обычными предметами
    giver_items = get_user_items(giver_items_str)
    target_items = get_user_items(target_items_str)
    
    item_name = item_name.strip()
    
    # Ищем предмет
    found_item = None
    for key in giver_items.keys():
        if key.lower() == item_name.lower():
            found_item = key
            break
    
    if not found_item:
        available_items = list(giver_items.keys())
        if available_items:
            items_list = "\n".join([f"• {item}: {count} шт" for item, count in giver_items.items()])
            await ctx.send(f"❌ У вас нет предмета '{item_name}'!\n\n📦 **Ваши предметы:**\n{items_list}")
        else:
            await ctx.send("❌ У вас нет предметов в инвентаре!")
        return
    
    if giver_items[found_item] < amount:
        await ctx.send(f"❌ У вас недостаточно '{found_item}'! Есть: {giver_items[found_item]}, нужно: {amount}")
        return
    
    # Проверяем легендарные бургеры
    legendary_burger_names = ["Железный бургер", "Золотой бургер", "Платиновый бургер", "Алмазный бургер"]
    if found_item in legendary_burger_names:
        await ctx.send(f"❌ Легендарные бургеры нельзя передавать!")
        return
    
    # Выполняем передачу
    giver_items[found_item] -= amount
    if giver_items[found_item] <= 0:
        del giver_items[found_item]
    
    target_items[found_item] = target_items.get(found_item, 0) + amount
    
    update_user_data(
        guild_id, giver_id, giver_number, giver_name,
        giver_plus, giver_minus, giver_jackpot, giver_burgers,
        giver_case_time, giver_next_burger,
        giver_acts, giver_gain, giver_last_res, giver_last_time,
        giver_legendary, save_user_items(giver_items),
        None, None, None
    )
    
    update_user_data(
        guild_id, target_id, target_number, target_name,
        target_plus, target_minus, target_jackpot, target_burgers,
        target_case_time, target_next_burger,
        target_acts, target_gain, target_last_res, target_last_time,
        target_legendary, save_user_items(target_items),
        None, None, None
    )
    
    embed = discord.Embed(
        title="🎁 Передача предмета",
        description=f"**{giver.mention}** передал предмет **{target.mention}**!",
        color=0xffaa00
    )
    
    embed.add_field(name="📦 Предмет", value=f"**{found_item}** x{amount}", inline=False)
    
    # Инвентарь отправителя
    giver_inv = "\n".join([f"• {item}: {count} шт" for item, count in list(giver_items.items())[:5]])
    if len(giver_items) > 5:
        giver_inv += f"\n... и ещё {len(giver_items) - 5} предметов"
    embed.add_field(name="📤 Ваш инвентарь", value=giver_inv or "Пусто", inline=True)
    
    # Инвентарь получателя
    target_inv = "\n".join([f"• {item}: {count} шт" for item, count in list(target_items.items())[:5]])
    if len(target_items) > 5:
        target_inv += f"\n... и ещё {len(target_items) - 5} предметов"
    embed.add_field(name="📥 Инвентарь получателя", value=target_inv or "Пусто", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='инвентарь')
async def show_inventory(ctx, member: discord.Member = None):
    """Показывает инвентарь пользователя"""
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    (number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, _, _, _) = get_user_data(guild_id, user_id, target.name)
    
    embed = discord.Embed(
        title=f"🎒 Инвентарь - {target.display_name}",
        color=0x3498db
    )
    
    embed.add_field(name="🍔 Автобургеры", value=str(autoburger_count), inline=True)
    embed.add_field(name="⚡ Срабатываний", value=str(total_activations), inline=True)
    embed.add_field(name="📈 Всего набрано", value=f"{total_gain} кг", inline=True)
    
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        burger = BURGER_RANKS[legendary_burger]
        embed.add_field(name=f"{burger['emoji']} Легендарный бургер", 
                       value=f"**{burger['name']}**\nМножитель: x{burger['multiplier']}", 
                       inline=False)
    
    items_dict = get_user_items(item_counts)
    if items_dict:
        items_text = ""
        for item_name, count in items_dict.items():
            items_text += f"• {item_name}: {count} шт\n"
        embed.add_field(name="📦 Предметы", value=items_text, inline=False)
    
    if autoburger_count > 0:
        interval = get_autoburger_interval(autoburger_count)
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count)) * 100
        embed.add_field(name="⚡ Текущий бонус", 
                       value=f"+{current_boost:.1f}% к плюсу (каждые {interval} ч)", 
                       inline=True)
        
        if next_autoburger_time:
            try:
                if isinstance(next_autoburger_time, str):
                    next_time = datetime.fromisoformat(next_autoburger_time)
                else:
                    next_time = next_autoburger_time
                time_diff = next_time - datetime.now()
                if time_diff.total_seconds() > 0:
                    embed.add_field(name="⏰ Следующий автобургер", 
                                   value=f"через {format_time(time_diff.total_seconds())}", 
                                   inline=False)
            except:
                pass
    
    embed.set_footer(text="💪 Жир и предметы!")
    await ctx.send(embed=embed)

@bot.command(name='жир_звания')
async def show_ranks(ctx):
    """Список званий"""
    embed = discord.Embed(
        title="🎖️ Система званий",
        description="Чем больше ваш вес, тем выше звание!",
        color=0xffaa00
    )
    
    ranks_text = ""
    for rank in RANKS:
        if rank["min"] == rank["max"]:
            range_str = f"{rank['min']}"
        else:
            range_str = f"{rank['min']} – {rank['max']}"
        ranks_text += f"{rank['emoji']} **{rank['name']}** — {range_str} kg\n"
    
    embed.add_field(name="Доступные звания", value=ranks_text, inline=False)
    await ctx.send(embed=embed)

@bot.command(name='жир_сброс')
async def fat_reset(ctx, member: discord.Member = None):
    """Сброс веса (только админы)"""
    if not ctx.author.guild_permissions.administrator and ctx.author != ctx.guild.owner:
        await ctx.send("❌ Эта команда только для администраторов!")
        return
    
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    update_user_data(guild_id, user_id, 0, target.name, 0, 0, 0, 0, None, None, 0, 0, None, None, -1, '{}')
    
    try:
        new_nick = f"0kg {target.name}"
        await target.edit(nick=new_nick)
        await ctx.send(f"✅ Вес {target.mention} сброшен на 0kg")
    except:
        await ctx.send(f"✅ Вес {target.mention} сброшен на 0kg (ник не изменён)")

@bot.command(name='сброскд')
async def reset_cooldowns(ctx):
    """
    Сброс кулдаунов
    Для обычных тестеров: сброс !жир
    Для Высших тестеров: сброс !жир, !жиркейс и обновление магазина
    """
    guild_id = ctx.guild.id
    member = ctx.author
    
    is_high_tester = has_high_tester_role(member)
    is_regular_tester = has_tester_role(member)
    
    # Проверяем права
    if not is_regular_tester and not is_high_tester:
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}** или **{HIGH_TESTER_ROLE_NAME}**")
        return
    
    if is_high_tester:
        # ВЫСШИЙ ТЕСТЕР: полный сброс
        
        # 1. Сбрасываем кулдаун !жир для всех
        fat_affected = reset_all_cooldowns(guild_id)
        
        # 2. Сбрасываем кулдаун !жиркейс для всех
        db_path = get_db_path(guild_id)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE user_fat SET last_case_time = NULL')
        case_affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        # 3. Обновляем магазин принудительно
        current_time = datetime.now()
        new_slots = generate_shop_items()
        last_update = current_time
        next_update = current_time + timedelta(hours=SHOP_UPDATE_HOURS)
        update_shop_data(guild_id, new_slots, last_update, next_update)
        
        embed = discord.Embed(
            title="🔄 **ПОЛНЫЙ СБРОС** 🔄",
            description=f"**{ctx.author.name}** (Высший тестер) выполнил глобальный сброс!",
            color=0xff5500
        )
        embed.add_field(name="⏰ Сброс !жир", value=f"Затронуто: {fat_affected} пользователей", inline=True)
        embed.add_field(name="📦 Сброс !жиркейс", value=f"Затронуто: {case_affected} пользователей", inline=True)
        embed.add_field(name="🏪 Магазин", value=f"Принудительно обновлён", inline=True)
        
    else:
        # ОБЫЧНЫЙ ТЕСТЕР: только сброс !жир
        affected = reset_all_cooldowns(guild_id)
        
        embed = discord.Embed(
            title="🔄 Кулдаун сброшен",
            description=f"**{ctx.author.name}** сбросил кулдаун !жир для всех!",
            color=0x00ff00
        )
        embed.add_field(name="Затронуто пользователей", value=str(affected), inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='сбросвсех')
async def reset_all_users_weight(ctx):
    """Глобальный сброс веса (только тестеры)"""
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    confirmation = await ctx.send(f"⚠️ **Внимание!** Сбросить вес **ВСЕХ** на 0?\nНапишите `да` в течение 30 секунд.")
    
    def check(msg):
        return msg.author == ctx.author and msg.content.lower() == "да"
    
    try:
        await bot.wait_for('message', timeout=30.0, check=check)
    except:
        await ctx.send("❌ Отмена")
        return
    
    affected = reset_all_weights(guild_id)
    
    embed = discord.Embed(
        title="⚖️ Глобальный сброс",
        description=f"**{ctx.author.name}** обнулил всех!",
        color=0xff5500
    )
    embed.add_field(name="Затронуто пользователей", value=str(affected), inline=True)
    await ctx.send(embed=embed)

@bot.command(name='жир_кулдаун')
async def cooldown_info(ctx):
    """Информация о кулдаунах"""
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    
    (number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, _, _, _) = get_user_data(guild_id, user_id, member.name)
    
    actual_fat_cooldown = COOLDOWN_HOURS
    actual_case_cooldown = CASE_COOLDOWN_HOURS
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        actual_fat_cooldown = BURGER_RANKS[legendary_burger]["fat_cooldown"] / 60
        actual_case_cooldown = BURGER_RANKS[legendary_burger]["case_cooldown"]
    
    fat_can_use, fat_remaining = check_cooldown(last_time, actual_fat_cooldown)
    case_can_use, case_remaining = check_cooldown(last_case_time, actual_case_cooldown)
    
    embed = discord.Embed(
        title=f"⏰ Кулдауны на сервере {ctx.guild.name}",
        description=f"Для {member.mention}",
        color=0x3498db
    )
    
    if fat_can_use:
        fat_status = "✅ Доступна"
    else:
        fat_status = f"⏳ {format_time(fat_remaining)}"
    
    embed.add_field(name="!жир", value=f"Кулдаун: {actual_fat_cooldown*60:.0f} мин\nСтатус: {fat_status}", inline=True)
    
    if case_can_use:
        case_status = "✅ Доступен"
    else:
        case_status = f"⏳ {format_time(case_remaining)}"
    
    embed.add_field(name="!жиркейс", value=f"Кулдаун: {actual_case_cooldown} ч\nСтатус: {case_status}", inline=True)
    
    embed.add_field(name="Текущий вес", value=f"{number}kg", inline=True)
    
    pity_emojis = []
    if consecutive_plus > 0:
        pity_emojis.append(f"🔥{consecutive_plus}")
    if consecutive_minus > 0:
        pity_emojis.append(f"❄️{consecutive_minus}")
    if jackpot_pity > 0:
        pity_emojis.append(f"💰{jackpot_pity}")
    if autoburger_count > 0:
        pity_emojis.append(f"🍔{autoburger_count}")
    if legendary_burger >= 0:
        pity_emojis.append(BURGER_RANKS[legendary_burger]["emoji"])
    
    if pity_emojis:
        embed.add_field(name="Счётчики", value=" ".join(pity_emojis), inline=True)
    
    if has_tester_role(ctx.author):
        embed.add_field(name="Роль", value="🎭 Тестер", inline=True)
    
    if next_autoburger_time:
        try:
            if isinstance(next_autoburger_time, str):
                next_time = datetime.fromisoformat(next_autoburger_time)
            else:
                next_time = next_autoburger_time
            time_diff = next_time - datetime.now()
            if time_diff.total_seconds() > 0:
                embed.add_field(name="🍔 След. автобургер", 
                               value=f"через {format_time(time_diff.total_seconds())}", 
                               inline=True)
        except:
            pass
    
    await ctx.send(embed=embed)

@bot.command(name='жир_серверы')
async def list_guilds(ctx):
    """Статистика по серверам (только админы)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Эта команда только для администраторов!")
        return
    
    embed = discord.Embed(
        title="📊 Статистика по серверам",
        color=0x3498db
    )
    
    for guild in bot.guilds:
        stats = get_guild_stats(guild.id)
        burger_text = ""
        for i, count in enumerate(stats['burger_counts']):
            if count > 0:
                burger_text += f"{BURGER_RANKS[i]['emoji']}:{count} "
        
        embed.add_field(
            name=guild.name,
            value=f"Участников: {stats['total_users']}\n"
                  f"Суммарный вес: {stats['total_weight']}kg\n"
                  f"🍔 Автобургеров: {stats['total_autoburgers']}\n"
                  f"✨ Бургеры: {burger_text if burger_text else 'Нет'}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name='жирхелп')
async def fat_help(ctx):
    """Показывает список всех команд бота"""
    embed = discord.Embed(
        title="🍔 **ЖИРБОТ - ПОМОЩЬ** 🍔",
        description="Все команды",
        color=0xffaa00
    )
    
    # ОСНОВНЫЕ КОМАНДЫ
    embed.add_field(
        name="🎮 **ОСНОВНЫЕ КОМАНДЫ**",
        value="""
        `!жир` - изменить свой вес
        `!жиркейс` - открыть кейс с анимацией
        `!жиркейс_шансы` - шансы в кейсе
        `!жиротрясы` - таблица рекордов на сервере
        `!жирглобал` - топ серверов по общей массе
        `!жир_инфо [@user]` - информация о весе
        `!жир_звания` - список всех званий
        `!жир_кулдаун` - статус кулдаунов
        `!жирстат [@user]` - статистика автобургеров
        `!инвентарь [@user]` - посмотреть инвентарь
        """,
        inline=False
    )
    
    # ЭКОНОМИКА И ТОРГОВЛЯ
    embed.add_field(
        name="💰 **ЭКОНОМИКА**",
        value="""
        `!магазин` - магазин предметов
        `!купить [слот] [кол-во]` - купить предмет
        `!датьжир [@user] [кол-во]` - передать кг
        `!датьпредмет [@user] [кол-во] [предмет]` - передать предмет
        """,
        inline=False
    )
    
    # ВОЗВЫШЕНИЕ И ЛЕГЕНДАРНЫЕ БУРГЕРЫ
    embed.add_field(
        name="✨ **ВОЗВЫШЕНИЕ**",
        value="""
        `!возвышение` - попытка получить легендарный бургер
        
        **⬛ Железный бургер** - 3600кг (70%) - х1.5
        **🟨 Золотой бургер** - 4300кг (50%) - х2.0
        **⬜ Платиновый бургер** - 6000кг (30%) - х2.5
        **🟦 Алмазный бургер** - 8000кг (20%) - х3.0, +10% к плюсу, редкие x2
        """,
        inline=False
    )
    
    # ЛЕГЕНДАРНЫЕ ПРЕДМЕТЫ
    embed.add_field(
        name="💎 **ЛЕГЕНДАРНЫЕ ПРЕДМЕТЫ**",
        value="""
        **🍔 Святой сэндвич** - +30% к шансу джекпота за шт (макс 90%)
        **💀 Гнилая ножка KFC** - 60% потерять 50% массы, 40% джекпот
        **💧 Стакан воды** - нет минусов, весь прирост в 3 раза меньше
        """,
        inline=False
    )
    
    # КОМАНДЫ ДЛЯ АДМИНОВ
    embed.add_field(
        name="👑 **ДЛЯ АДМИНОВ**",
        value="""
        `!жир_сброс [@user]` - сбросить вес пользователя
        """,
        inline=False
    )
    
    # ЛЕГЕНДА - ЧТО ОЗНАЧАЮТ ЭМОДЗИ
    embed.add_field(
        name="📊 **ЛЕГЕНДА**",
        value="""
        `🔥` - серия плюсов подряд
        `❄️` - серия минусов подряд
        `💰` - накопление на джекпот
        `🍔X` - количество автобургеров
        `⚡X` - количество срабатываний автобургеров
        """,
        inline=True
    )
    
    # ПОЛЕЗНЫЕ СОВЕТЫ
    embed.add_field(
        name="💡 **СОВЕТЫ**",
        value="""
        • Копите автобургеры для пассивного дохода
        • Покупайте предметы в магазине каждые 12ч
        • Достигайте 3600кг для первого возвышения
        • Легендарные предметы не стакаются (кроме сэндвича)
        """,
        inline=True
    )
    
    embed.set_footer(text="🔥❄️💰🍔⚡ - следите за показателями! | Версия 3.0")
    
    await ctx.send(embed=embed)

@bot.command(name='автобургер')
async def give_autoburger(ctx, количество: int = 1):
    """Выдаёт автобургеры (только для тестеров)"""
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    if количество <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    
    if количество > 100:
        await ctx.send("⚠️ Слишком много! Максимум 100 автобургеров за раз.")
        количество = 100
    
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, last_command, last_command_target, last_command_use_time) = get_user_data(guild_id, user_id, user_name)
    
    new_autoburger_count = autoburger_count + количество
    
    interval = get_autoburger_interval(new_autoburger_count)
    if interval:
        new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
    else:
        new_next_autoburger_time = None
    
    update_user_data(
        guild_id, user_id, current_number, user_name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        new_autoburger_count, last_case_time, new_next_autoburger_time,
        total_activations, total_gain, last_result, last_activation_time,
        legendary_burger, item_counts,
        last_command, last_command_target, last_command_use_time
    )
    
    embed = discord.Embed(
        title="🍔 Выдача автобургеров",
        description=f"**{member.mention}** получил автобургеры!",
        color=0xffaa00
    )
    
    embed.add_field(name="📦 Получено", value=f"+{количество} 🍔", inline=True)
    embed.add_field(name="📊 Всего", value=f"{new_autoburger_count} 🍔", inline=True)
    
    if new_autoburger_count > 0:
        interval = get_autoburger_interval(new_autoburger_count)
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * new_autoburger_count)) * 100
        embed.add_field(name="⚡ Эффект", 
                       value=f"Авто-жир каждые {interval} ч\nБонус к плюсу: +{current_boost:.1f}%", 
                       inline=False)
    
    embed.set_footer(text="✨ Удачи в наборе массы!")
    await ctx.send(embed=embed)

@bot.command(name='автобургер_сброс')
async def reset_autoburger(ctx, member: discord.Member = None):
    """Сбрасывает количество автобургеров у пользователя (только для тестеров)"""
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, last_command, last_command_target, last_command_use_time) = get_user_data(guild_id, user_id, target.name)
    
    if autoburger_count == 0:
        await ctx.send(f"ℹ️ У {target.mention} нет автобургеров!")
        return
    
    update_user_data(
        guild_id, user_id, current_number, target.name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        0, last_case_time, None,
        total_activations, total_gain, last_result, last_activation_time,
        legendary_burger, item_counts,
        last_command, last_command_target, last_command_use_time
    )
    
    embed = discord.Embed(
        title="🔄 Сброс автобургеров",
        description=f"**{ctx.author.name}** сбросил автобургеры у {target.mention}",
        color=0xff5500
    )
    embed.add_field(name="Было", value=f"{autoburger_count} 🍔", inline=True)
    embed.add_field(name="Стало", value="0 🍔", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='автобургер_инфо')
async def autoburger_info(ctx, member: discord.Member = None):
    """Показывает информацию об автобургерах пользователя (только для тестеров)"""
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts, _, _, _) = get_user_data(guild_id, user_id, target.name)
    
    embed = discord.Embed(
        title=f"🍔 Информация об автобургерах",
        description=f"Для {target.mention}",
        color=0x3498db
    )
    
    embed.add_field(name="Количество", value=f"{autoburger_count} 🍔", inline=True)
    embed.add_field(name="Срабатываний", value=str(total_activations), inline=True)
    embed.add_field(name="Всего набрано", value=f"{total_gain} кг", inline=True)
    
    if total_activations > 0:
        avg_gain = total_gain / total_activations
        embed.add_field(name="Средний прирост", value=f"{avg_gain:.1f} кг", inline=True)
    
    if autoburger_count > 0:
        interval = get_autoburger_interval(autoburger_count)
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count)) * 100
        embed.add_field(name="Интервал", value=f"каждые {interval} ч", inline=True)
        embed.add_field(name="Бонус к плюсу", value=f"+{current_boost:.1f}%", inline=True)
        
        if next_autoburger_time:
            try:
                if isinstance(next_autoburger_time, str):
                    next_time = datetime.fromisoformat(next_autoburger_time)
                else:
                    next_time = next_autoburger_time
                time_diff = next_time - datetime.now()
                if time_diff.total_seconds() > 0:
                    embed.add_field(name="⏰ Следующий", 
                                   value=f"через {format_time(time_diff.total_seconds())}", 
                                   inline=True)
            except:
                pass
    
    if last_result and last_activation_time:
        try:
            if isinstance(last_activation_time, str):
                last_time = datetime.fromisoformat(last_activation_time)
            else:
                last_time = last_activation_time
            time_diff = datetime.now() - last_time
            hours = time_diff.total_seconds() / 3600
            embed.add_field(name="🕒 Последний результат", 
                           value=f"{last_result} ({hours:.1f} ч назад)", 
                           inline=False)
        except:
            pass
    
    await ctx.send(embed=embed)

@bot.command(name='выдатьпредмет')
async def give_shop_item(ctx, amount: int, *, item_name: str):
    """
    Выдаёт себе предмет из магазина (только для Высших тестеров)
    Использование: !выдатьпредмет количество "название предмета"
    Пример: !выдатьпредмет 5 "Горелый бекон"
    """
    # Проверяем, есть ли у пользователя роль Высший тестер
    if not has_high_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{HIGH_TESTER_ROLE_NAME}**")
        return
    
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    
    if amount > 1000:
        await ctx.send("⚠️ Слишком много! Максимум 1000 предметов за раз.")
        amount = 1000
    
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    # Получаем данные пользователя
    (current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity,
     autoburger_count, last_case_time, next_autoburger_time,
     total_activations, total_gain, last_result, last_activation_time,
     legendary_burger, item_counts_str, last_command, last_command_target, last_command_use_time) = get_user_data(guild_id, user_id, user_name)
    
    # Очищаем название предмета от лишних пробелов
    item_name = item_name.strip()
    
    # Ищем предмет в списке магазина
    found_item = None
    for shop_item in SHOP_ITEMS:
        if shop_item["name"].lower() == item_name.lower():
            found_item = shop_item
            break
    
    if not found_item:
        # Показываем список доступных предметов
        items_list = "\n".join([f"• {item['name']}" for item in SHOP_ITEMS[:10]])
        if len(SHOP_ITEMS) > 10:
            items_list += f"\n... и ещё {len(SHOP_ITEMS) - 10} предметов"
        await ctx.send(f"❌ Предмет '{item_name}' не найден в магазине!\n\n📦 **Доступные предметы:**\n{items_list}")
        return
    
    # Преобразуем JSON строку в словарь
    items_dict = get_user_items(item_counts_str)
    
    # Добавляем предмет
    items_dict[found_item["name"]] = items_dict.get(found_item["name"], 0) + amount
    
    # Обновляем данные в БД
    update_user_data(
        guild_id, user_id, current_number, user_name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        autoburger_count, last_case_time, next_autoburger_time,
        total_activations, total_gain, last_result, last_activation_time,
        legendary_burger, save_user_items(items_dict),
        last_command, last_command_target, last_command_use_time
    )
    
    # Создаём красивое сообщение
    embed = discord.Embed(
        title="🎁 Выдача предмета",
        description=f"**{member.mention}** выдал себе предмет!",
        color=0xffaa00
    )
    
    embed.add_field(name="📦 Предмет", value=f"**{found_item['name']}** x{amount}", inline=True)
    
    # Показываем описание предмета
    embed.add_field(name="📝 Описание", value=found_item['description'], inline=False)
    
    # Показываем текущий инвентарь
    items_list = "\n".join([f"• {item}: {count} шт" for item, count in list(items_dict.items())[:8]])
    if len(items_dict) > 8:
        items_list += f"\n... и ещё {len(items_dict) - 8} предметов"
    
    embed.add_field(name="📊 Ваш инвентарь", value=items_list or "Пусто", inline=False)
    
    embed.set_footer(text="✨ Только для высших тестеров!")
    
    await ctx.send(embed=embed)

@bot.command(name='жирглобал')
async def global_leaderboard(ctx):
    """
    Показывает топ серверов по общей жирности
    """
    guild_data = []
    
    for guild in bot.guilds:
        try:
            stats = get_guild_stats(guild.id)
            
            # Получаем только нужные данные
            guild_data.append({
                'name': guild.name,
                'members': stats['total_users'],
                'total_weight': stats['total_weight'],
                'avg_weight': stats['avg_weight'],
                'autoburgers': stats['total_autoburgers'],
                'activations': stats['total_activations'],
                'burger_counts': stats['burger_counts']
            })
        except Exception as e:
            print(f"❌ Ошибка при получении статистики для сервера {guild.name}: {e}")
            continue
    
    if not guild_data:
        await ctx.send("📭 Нет данных по серверам!")
        return
    
    # Сортируем по общей массе (убывание)
    guild_data.sort(key=lambda x: x['total_weight'], reverse=True)
    
    embed = discord.Embed(
        title="🌍 **ГЛОБАЛЬНЫЙ РЕЙТИНГ СЕРВЕРОВ** 🌍",
        description="Топ серверов по общей массе жира",
        color=0xffaa00
    )
    
    # Формируем топ-10 серверов
    leaderboard_text = ""
    for i, guild in enumerate(guild_data[:10], 1):
        if i == 1:
            place_icon = "🥇"
        elif i == 2:
            place_icon = "🥈"
        elif i == 3:
            place_icon = "🥉"
        else:
            place_icon = "🔹"
        
        # Считаем общее количество легендарных бургеров
        total_burgers = sum(guild['burger_counts'])
        
        # Форматируем массу (в тоннах для больших чисел)
        if guild['total_weight'] >= 1000:
            weight_display = f"{guild['total_weight']/1000:.1f}т"
        else:
            weight_display = f"{guild['total_weight']}кг"
        
        leaderboard_text += f"{place_icon} **{i}.** {guild['name'][:30]}\n"
        leaderboard_text += f"   📦 **{weight_display}** | 👥 {guild['members']} уч.\n"
        leaderboard_text += f"   📊 Средний вес: {guild['avg_weight']:.0f}кг\n"
        
        # Добавляем информацию о легендарных бургерах если есть
        if total_burgers > 0:
            burger_icons = []
            for idx, count in enumerate(guild['burger_counts']):
                if count > 0:
                    burger_icons.append(f"{BURGER_RANKS[idx]['emoji']}{count}")
            leaderboard_text += f"   ✨ {' '.join(burger_icons)}\n"
        
        leaderboard_text += "\n"
        
        if len(leaderboard_text) > 1900:
            leaderboard_text += "... и ещё несколько серверов"
            break
    
    embed.description = leaderboard_text
    
    # Общая статистика по всем серверам
    total_servers = len(guild_data)
    total_global_weight = sum(g['total_weight'] for g in guild_data)
    total_global_members = sum(g['members'] for g in guild_data)
    total_global_burgers = sum(sum(g['burger_counts']) for g in guild_data)
    
    if total_global_weight >= 1000000:
        global_display = f"{total_global_weight/1000000:.1f}млн кг"
    elif total_global_weight >= 1000:
        global_display = f"{total_global_weight/1000:.1f}т"
    else:
        global_display = f"{total_global_weight}кг"
    
    embed.add_field(
        name="📊 **ГЛОБАЛЬНАЯ СТАТИСТИКА**",
        value=(
            f"🌍 Серверов: **{total_servers}**\n"
            f"👥 Участников: **{total_global_members}**\n"
            f"⚖️ Всего массы: **{global_display}**\n"
            f"🍔 Автобургеров: **{sum(g['autoburgers'] for g in guild_data)}**\n"
            f"✨ Легендарных бургеров: **{total_global_burgers}**"
        ),
        inline=False
    )
    
    embed.set_footer(text="🏆 Топ-10 серверов")
    
    await ctx.send(embed=embed)
    
# ===== ЗАПУСК БОТА =====
if __name__ == "__main__":
    print("🚀 Запуск бота...")
    bot.run(TOKEN)







