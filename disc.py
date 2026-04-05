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
TESTER_ROLE_NAME = "тестер"
HIGH_TESTER_ROLE_NAME = "Высший тестер"

# Настройки вероятностей
BASE_MINUS_CHANCE = 0.2
MAX_MINUS_CHANCE = 0.6
PITY_INCREMENT = 0.1
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

# Призы в ежедневном кейсе
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
]

total_chance = sum(prize["chance"] for prize in CASE_PRIZES)
for prize in CASE_PRIZES:
    prize["normalized_chance"] = (prize["chance"] / total_chance) * 100

# Настройки авто-жира
AUTO_FAT_INTERVALS = {
    1: 6,   # 6 часов
    2: 3,   # 3 часа
    3: 1    # 1 час
}

# Настройки престижа
PRESTIGE_BONUS_PER_LEVEL = 0.10      # +10% к кг за уровень
PRESTIGE_LUCK_PER_LEVEL = 0.01       # +1% к шансам за уровень

# Настройки прибавки
INCOME_BONUS_PER_LEVEL = 0.05        # +5% к кг от предметов за уровень

# Настройки кейсов
CASES = {
    "daily": {
        "name": "Жиркейс",
        "emoji": "📦",
        "tradable": False,
        "daily": True,
        "prizes": CASE_PRIZES
    },
    "chicken": {
        "name": "Коробка от чикенбургера",
        "emoji": "🍗",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.3,
        "min_shop": 1,
        "max_shop": 3,
        "price": 10,
        "prizes": [
            {"value": -10, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 30, "emoji": "🔄"},
            {"value": 10, "chance": 20, "emoji": "📈"},
            {"value": 15, "chance": 10, "emoji": "📈"},
            {"value": 20, "chance": 10, "emoji": "⬆️"},
            {"value": 25, "chance": 10, "emoji": "⬆️"}
        ]
    },
    "bigmac": {
        "name": "Коробка от Биг Мака",
        "emoji": "🍔",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.25,
        "min_shop": 1,
        "max_shop": 3,
        "price": 15,
        "prizes": [
            {"value": -15, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 30, "emoji": "🔄"},
            {"value": 15, "chance": 20, "emoji": "📈"},
            {"value": 20, "chance": 10, "emoji": "⬆️"},
            {"value": 25, "chance": 10, "emoji": "⬆️"},
            {"value": 30, "chance": 10, "emoji": "🚀"}
        ]
    },
    "whopper": {
        "name": "Коробка от Воппера",
        "emoji": "🔥",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.23,
        "min_shop": 1,
        "max_shop": 3,
        "price": 25,
        "prizes": [
            {"value": -25, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 30, "emoji": "🔄"},
            {"value": 25, "chance": 20, "emoji": "📈"},
            {"value": 30, "chance": 10, "emoji": "🚀"},
            {"value": 40, "chance": 9, "emoji": "🚀"},
            {"value": 50, "chance": 1, "emoji": "💫"}
        ]
    },
    "green_whopper": {
        "name": "Коробка от Зеленого Воппера",
        "emoji": "💚",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.17,
        "min_shop": 1,
        "max_shop": 2,
        "price": 50,
        "prizes": [
            {"value": -25, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 10, "emoji": "🔄"},
            {"value": 10, "chance": 20, "emoji": "📈"},
            {"value": 30, "chance": 10, "emoji": "🚀"},
            {"value": 50, "chance": 10, "emoji": "💫"},
            {"value": 100, "chance": 9, "emoji": "⭐"},
            {"value": 250, "chance": 1, "emoji": "💥"}
        ]
    },
    "burger_pizza": {
        "name": "Коробка от Бургер пиццы",
        "emoji": "🍕",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.15,
        "min_shop": 1,
        "max_shop": 2,
        "price": 100,
        "prizes": [
            {"value": -10, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 10, "emoji": "🔄"},
            {"value": 30, "chance": 20, "emoji": "🚀"},
            {"value": 50, "chance": 30, "emoji": "💫"},
            {"value": 100, "chance": 5, "emoji": "⭐"},
            {"value": 250, "chance": 5, "emoji": "⭐"},
            {"value": 500, "chance": 4, "emoji": "💥"},
            {"value": 1000, "chance": 1, "emoji": "💥"}
        ]
    },
    "mcguffin": {
        "name": "Коробка от МакГаффина",
        "emoji": "🎁",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.1,
        "min_shop": 1,
        "max_shop": 1,
        "price": 200,
        "prizes": [
            {"value": 100, "chance": 80, "emoji": "⭐"},
            {"value": 200, "chance": 5, "emoji": "💥"},
            {"value": 250, "chance": 5, "emoji": "💥"},
            {"value": 500, "chance": 5, "emoji": "💥"},
            {"value": 750, "chance": 1, "emoji": "✨"},
            {"value": 1000, "chance": 1, "emoji": "✨"},
            {"value": 1200, "chance": 1, "emoji": "✨"},
            {"value": 1500, "chance": 1, "emoji": "✨"}
        ]
    },
    "rotten_pack": {
        "name": "Упаковка Гнилой Ножки KFC",
        "emoji": "💀📦",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.1,
        "min_shop": 1,
        "max_shop": 10,
        "price": 100,
        "prizes": [
            {"value": 0, "chance": 98, "emoji": "🔄"},
            {"value": "rotten_leg", "chance": 2, "emoji": "💀"}
        ]
    },
    "water_pack": {
        "name": "Упаковка Стакана Воды",
        "emoji": "💧📦",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.1,
        "min_shop": 1,
        "max_shop": 10,
        "price": 100,
        "prizes": [
            {"value": 0, "chance": 98, "emoji": "🔄"},
            {"value": "water", "chance": 2, "emoji": "💧"}
        ]
    }
}

# Настройки магазина
SHOP_SLOTS = 10
SHOP_UPDATE_HOURS = 12

SHOP_ITEMS = [
    {"name": "Горелый бекон", "chance": 1.0, "min_amount": 3, "max_amount": 20,
     "price": 20, "gain_per_24h": 1, "description": "🏭 Даёт +1 кг каждые 24 часа"},
    {"name": "Горелый бутерброд", "chance": 0.4, "min_amount": 1, "max_amount": 5,
     "price": 70, "gain_per_24h": 3, "description": "🥪 Даёт +3 кг каждые 24 часа"},
    {"name": "Горелый додстер", "chance": 0.4, "min_amount": 1, "max_amount": 3,
     "price": 100, "gain_per_24h": 5, "description": "🌯 Даёт +5 кг каждые 24 часа"},
    {"name": "Тарелка макарон", "chance": 0.3, "min_amount": 1, "max_amount": 2,
     "price": 200, "gain_per_24h": 10, "description": "🍝 Даёт +10 кг каждые 24 часа"},
    {"name": "Тарелка хинкалей", "chance": 0.2, "min_amount": 1, "max_amount": 2,
     "price": 300, "gain_per_24h": 15, "description": "🥟 Даёт +15 кг каждые 24 часа"},
    {"name": "Бургер", "chance": 0.15, "min_amount": 1, "max_amount": 2,
     "price": 400, "gain_per_24h": 20, "description": "🍔 Даёт +20 кг каждые 24 часа"},
    {"name": "Пицца", "chance": 0.1, "min_amount": 1, "max_amount": 2,
     "price": 500, "gain_per_24h": 30, "description": "🍕 Даёт +30 кг каждые 24 часа"},
    {"name": "Ведро KFC", "chance": 0.08, "min_amount": 1, "max_amount": 2,
     "price": 800, "gain_per_24h": 50, "description": "🍗 Даёт +50 кг каждые 24 часа"},
    {"name": "Комбо за 1000!", "chance": 0.06, "min_amount": 1, "max_amount": 2,
     "price": 1000, "gain_per_24h": 100, "description": "🍱 Даёт +100 кг каждые 24 часа"},
    {"name": "Бездонное ведро KFC", "chance": 0.04, "min_amount": 1, "max_amount": 1,
     "price": 1500, "gain_per_24h": 150, "description": "🪣 Даёт +150 кг каждые 24 часа"},
    {"name": "Бездонная пачка чипсов", "chance": 0.03, "min_amount": 1, "max_amount": 1,
     "price": 3000, "gain_per_24h": 250, "description": "🥨 Даёт +250 кг каждые 24 часа"},
    {"name": "Пожизненный запас чикенбургеров", "chance": 0.02, "min_amount": 1, "max_amount": 1,
     "price": 5000, "gain_per_24h": 500, "description": "🍔🍔🍔 Даёт +500 кг каждые 24 часа"},
    {"name": "Автоматическая система подачи холестерина", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 7000, "gain_per_24h": 1000, "description": "⚙️💉 Даёт +1000 кг каждые 24 часа"},
    {"name": "Святой сэндвич", "chance": 0.005, "min_amount": 1, "max_amount": 1,
     "price": 10000, "gain_per_24h": 0, "description": "✨ **ЛЕГЕНДАРНО** ✨\nУвеличивает шанс джекпота до 30% за шт"},
    {"name": "Гнилая ножка KFC", "chance": 0.005, "min_amount": 1, "max_amount": 5,
     "price": 1, "gain_per_24h": 0, "description": "💀 **ПРОКЛЯТО** 💀\n60% потерять 50% массы, 40% джекпот"},
    {"name": "Стакан воды", "chance": 0.005, "min_amount": 1, "max_amount": 5,
     "price": 1, "gain_per_24h": 0, "description": "💧 **ОЧИЩЕНИЕ** 💧\nНет минусов, но весь прирост в 3 раза меньше"},
    {"name": "Автохолестерол", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 10кг в час",
     "effect": "auto_cholesterol", "effect_value": (1, 10), "effect_type": "hourly"},
    {"name": "Холестеринимус", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 500, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 5кг в час",
     "effect": "cholesterol", "effect_value": (1, 5), "effect_type": "hourly"},
    {"name": "Яблоко", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 500, "gain_per_24h": 0, "description": "🍎 Уменьшает кулдаун !жир на 5% за штуку",
     "effect": "fat_cooldown_reduction", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Апельсин", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 750, "gain_per_24h": 0, "description": "🍊 Уменьшает кулдаун !жиркейс на 5% за штуку",
     "effect": "case_cooldown_reduction", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Золотое Яблоко", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "🍎✨ Уменьшает кулдаун !жир на 10% за штуку",
     "effect": "fat_cooldown_reduction", "effect_value": 0.10, "effect_type": "passive"},
    {"name": "Золотой Апельсин", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "🍊✨ Уменьшает кулдаун !жиркейс на 10% за штуку",
     "effect": "case_cooldown_reduction", "effect_value": 0.10, "effect_type": "passive"},
    {"name": "Драгонфрукт", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "🐉🍈 Повышает шанс джекпота на 1% за штуку",
     "effect": "jackpot_boost", "effect_value": 0.01, "effect_type": "passive"},
    {"name": "Золотой Драгонфрукт", "chance": 0.005, "min_amount": 1, "max_amount": 1,
     "price": 3000, "gain_per_24h": 0, "description": "🐉🍈✨ Повышает шанс джекпота на 5% за штуку",
     "effect": "jackpot_boost", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Снатчер", "chance": 0.001, "min_amount": 1, "max_amount": 1,
     "price": 2000, "gain_per_24h": 0, "description": "👾 **СНАТЧЕР** 👾\nКаждые 6 часов с шансом 20% генерирует 1 случайный предмет из магазина"},
]

ITEM_EMOJIS = {
    "Горелый бекон": "🥓",
    "Горелый бутерброд": "🥪", 
    "Горелый додстер": "🌯",
    "Тарелка макарон": "🍝",
    "Тарелка хинкалей": "🥟",
    "Бургер": "🍔",
    "Пицца": "🍕",
    "Ведро KFC": "🍗",
    "Комбо за 1000!": "🍱",
    "Бездонное ведро KFC": "🪣",
    "Бездонная пачка чипсов": "🥨",
    "Пожизненный запас чикенбургеров": "🍔🍔🍔",
    "Автоматическая система подачи холестерина": "⚙️💉",
    "Святой сэндвич": "✨",
    "Гнилая ножка KFC": "💀",
    "Стакан воды": "💧",
    "Автохолестерол": "💊",
    "Холестеринимус": "💊",
    "Яблоко": "🍎",
    "Апельсин": "🍊",
    "Золотое Яблоко": "🍎✨",
    "Золотой Апельсин": "🍊✨",
    "Драгонфрукт": "🐉🍈",
    "Золотой Драгонфрукт": "🐉🍈✨",
    "Снатчер": "👾"
}

# Магазинный кейс
CASES["shop_case"] = {
    "name": "Магазинный кейс",
    "emoji": "🏪",
    "tradable": True,
    "daily": False,
    "shop_chance": 0.3,
    "min_shop": 1,
    "max_shop": 5,
    "price": 150,
    "prizes": []
}

shop_case_prizes = []
for item in SHOP_ITEMS:
    chance_percent = item["chance"] * 100
    emoji = ITEM_EMOJIS.get(item["name"], "🎁")
    shop_case_prizes.append({
        "value": item["name"],
        "chance": chance_percent,
        "emoji": emoji,
        "name": item["name"]
    })

total = sum(p["chance"] for p in shop_case_prizes)
if total < 100:
    shop_case_prizes.append({
        "value": 0,
        "chance": 100 - total,
        "emoji": "🔄",
        "name": "Ничего"
    })
else:
    for prize in shop_case_prizes:
        prize["chance"] = (prize["chance"] / total) * 100

CASES["shop_case"]["prizes"] = shop_case_prizes

# Теневая стоимость
LEGENDARY_UPGRADE_PRICES = {
    "Святой сэндвич": 20000,
    "Гнилая ножка KFC": 5000,
    "Стакан воды": 3000,
    "Автохолестерол": 5000,
    "Холестеринимус": 2500,
    "Яблоко": 1500,
    "Золотое Яблоко": 3000,
    "Апельсин": 2000,
    "Золотой Апельсин": 4000,
    "Драгонфрукт": 4000,
    "Золотой Драгонфрукт": 8000,
    "Снатчер": 20000
}

print("="*60)
print("🍔 ЖИРНЫЙ БОТ - ЗАПУСК")
print("="*60)

if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найдена переменная окружения DISCORD_BOT_TOKEN!")
    exit(1)

# ===== ФУНКЦИИ ДЛЯ РАБОТЫ С JSON =====
def get_user_items(item_counts_str):
    try:
        return json.loads(item_counts_str) if item_counts_str and item_counts_str != '{}' else {}
    except:
        return {}

def save_user_items(items_dict):
    return json.dumps(items_dict)

# ===== ФУНКЦИИ ОПЫТА И УРОВНЕЙ =====
def get_xp_for_next_level(level):
    return (50 * (level + 1)) + ((level + 1) * 5)

def get_level_and_xp(total_xp):
    level = 0
    remaining_xp = total_xp
    while True:
        needed = get_xp_for_next_level(level)
        if remaining_xp < needed:
            break
        remaining_xp -= needed
        level += 1
    return level, remaining_xp

def add_xp(guild_id, user_id, xp_amount):
    data = get_user_data(guild_id, user_id)
    old_level = data.get('user_level', 0)
    new_total_xp = data.get('user_xp', 0) + xp_amount
    new_level, current_xp = get_level_and_xp(new_total_xp)
    
    total_kg_reward = 0
    for level in range(old_level + 1, new_level + 1):
        total_kg_reward += 15 * level
    
    new_weight = data['current_number'] + total_kg_reward
    
    update_user_data(guild_id, user_id, 
                    user_xp=new_total_xp,
                    user_level=new_level,
                    number=new_weight)
    
    return new_level - old_level, total_kg_reward, new_level

def format_nick_with_prestige(prestige, weight, user_name):
    if prestige > 0:
        return f"{prestige}🌟 {weight}kg {user_name}"
    return f"{weight}kg {user_name}"

# ===== ФУНКЦИИ ДЛЯ БОНУСОВ =====
def get_prestige_bonus(prestige):
    return 1 + (prestige * PRESTIGE_BONUS_PER_LEVEL)

def get_prestige_luck(prestige):
    return prestige * PRESTIGE_LUCK_PER_LEVEL

def get_income_bonus(income_upgrade):
    return 1 + (income_upgrade * INCOME_BONUS_PER_LEVEL)

def get_fat_cd_reduction(upgrade_count):
    return upgrade_count * 1

def get_case_cd_reduction(upgrade_count):
    return upgrade_count * 20

def get_auto_fat_interval(auto_fat_level):
    if auto_fat_level <= 0:
        return None
    return AUTO_FAT_INTERVALS.get(auto_fat_level, 1)

def get_auto_fat_cost(current_level):
    return 500 + (current_level * 500)

def get_upgrade_cost(upgrade_type, current_level):
    if upgrade_type == "fat_cd":
        return 100 + (current_level * 50)
    elif upgrade_type == "case_cd":
        return 100 + (current_level * 50)
    elif upgrade_type == "luck":
        return 1000 + (current_level * 500)
    elif upgrade_type == "income":
        return 250 + (current_level * 150)
    elif upgrade_type == "prestige":
        return 2000 + (current_level * 1000)
    elif upgrade_type == "auto_fat":
        return 500 + (current_level * 500)
    return 0

# ===== ФУНКЦИИ БЕЗОПАСНОЙ РАБОТЫ С БД =====
def repair_database(db_path):
    if not os.path.exists(db_path):
        return False
    backup_path = db_path + f".corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(db_path, backup_path)
    print(f"⚠️ Создан бекап повреждённой БД: {backup_path}")
    os.remove(db_path)
    return True

def backup_and_restore_db():
    main_db_folder = DB_FOLDER
    backup_folder = "/tmp/guild_databases_backup"
    if not os.path.exists(main_db_folder):
        os.makedirs(main_db_folder)
        return
    if os.path.exists(backup_folder):
        for db_file in glob.glob(os.path.join(backup_folder, "*.db")):
            dest = os.path.join(main_db_folder, os.path.basename(db_file))
            shutil.copy2(db_file, dest)
    else:
        os.makedirs(backup_folder, exist_ok=True)
        for db_file in glob.glob(os.path.join(main_db_folder, "*.db")):
            dest = os.path.join(backup_folder, os.path.basename(db_file))
            shutil.copy2(db_file, dest)

backup_and_restore_db()

def add_missing_columns(db_path, existing_columns):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    required_columns = {
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
        'next_auto_fat_time': "TIMESTAMP"
    }
    
    for col_name, col_type in required_columns.items():
        if col_name not in existing_columns:
            try:
                print(f"📦 Добавляю колонку {col_name}")
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col_name} {col_type}")
            except Exception as e:
                print(f"⚠️ Ошибка при добавлении колонки {col_name}: {e}")
    
    for case_id in CASES.keys():
        if case_id != "daily":
            col_name = f"case_{case_id}_count"
            if col_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col_name} INTEGER DEFAULT 0")
                except:
                    pass
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shop'")
    if not cursor.fetchone():
        cursor.execute('''CREATE TABLE shop (
            guild_id TEXT PRIMARY KEY, 
            slots TEXT, 
            last_update TIMESTAMP, 
            next_update TIMESTAMP
        )''')
    
    conn.commit()
    conn.close()

def safe_init_guild_database(guild_id, guild_name="Unknown"):
    db_path = get_db_path(guild_id)
    
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_fat'")
            if not cursor.fetchone():
                conn.close()
                return create_new_database(db_path, guild_id, guild_name)
            
            cursor.execute("PRAGMA table_info(user_fat)")
            columns = [col[1] for col in cursor.fetchall()]
            conn.close()
            
            add_missing_columns(db_path, columns)
            return True
        except sqlite3.DatabaseError:
            repair_database(db_path)
            return create_new_database(db_path, guild_id, guild_name)
    else:
        return create_new_database(db_path, guild_id, guild_name)

def create_new_database(db_path, guild_id, guild_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE user_fat (
        user_id TEXT PRIMARY KEY, 
        user_name TEXT, 
        current_number INTEGER DEFAULT 0, 
        last_command_time TIMESTAMP, 
        consecutive_plus INTEGER DEFAULT 0, 
        consecutive_minus INTEGER DEFAULT 0, 
        jackpot_pity INTEGER DEFAULT 0, 
        last_case_time TIMESTAMP, 
        item_counts TEXT DEFAULT '{}',
        last_command TEXT,
        last_command_target TEXT,
        last_command_use_time TIMESTAMP,
        fat_cooldown_time TIMESTAMP,
        active_case_message_id TEXT,
        active_case_channel_id TEXT,
        daily_case_last_time TIMESTAMP,
        snatcher_last_time TIMESTAMP,
        duel_active INTEGER DEFAULT 0,
        duel_opponent TEXT,
        duel_amount INTEGER DEFAULT 0,
        duel_message_id TEXT,
        duel_channel_id TEXT,
        duel_initiator INTEGER DEFAULT 0,
        last_case_type TEXT,
        last_case_prize TEXT,
        upgrade_active INTEGER DEFAULT 0,
        upgrade_data TEXT,
        duel_start_time TIMESTAMP,
        shadow_upgrade_chance INTEGER DEFAULT 0,
        user_xp INTEGER DEFAULT 0,
        user_level INTEGER DEFAULT 0,
        fat_cd_upgrade INTEGER DEFAULT 0,
        case_cd_upgrade INTEGER DEFAULT 0,
        luck_upgrade INTEGER DEFAULT 0,
        income_upgrade INTEGER DEFAULT 0,
        prestige INTEGER DEFAULT 0,
        auto_fat_level INTEGER DEFAULT 0,
        next_auto_fat_time TIMESTAMP
    )''')
    
    for case_id in CASES.keys():
        if case_id != "daily":
            try:
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN case_{case_id}_count INTEGER DEFAULT 0")
            except:
                pass
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS shop (
        guild_id TEXT PRIMARY KEY, 
        slots TEXT, 
        last_update TIMESTAMP, 
        next_update TIMESTAMP
    )''')
    
    conn.commit()
    conn.close()
    return True

def get_db_path(guild_id):
    return os.path.join(DB_FOLDER, f"guild_{guild_id}.db")

def get_user_data(guild_id, user_id, user_name=None):
    safe_init_guild_database(guild_id, f"Guild_{guild_id}")
    db_path = get_db_path(guild_id)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    all_columns = [col[1] for col in cursor.fetchall()]
    
    select_cols = [col for col in [
        'user_id', 'user_name', 'current_number', 'last_command_time',
        'consecutive_plus', 'consecutive_minus', 'jackpot_pity',
        'last_case_time', 'item_counts', 'last_command', 'last_command_target',
        'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id',
        'active_case_channel_id', 'daily_case_last_time', 'snatcher_last_time',
        'duel_active', 'duel_opponent', 'duel_amount', 'duel_message_id', 
        'duel_channel_id', 'duel_initiator', 'last_case_type', 'last_case_prize',
        'upgrade_active', 'upgrade_data', 'duel_start_time', 'shadow_upgrade_chance',
        'user_xp', 'user_level', 'fat_cd_upgrade', 'case_cd_upgrade', 'luck_upgrade',
        'income_upgrade', 'prestige', 'auto_fat_level', 'next_auto_fat_time'
    ] if col in all_columns]
    
    case_cols = []
    for case_id in CASES.keys():
        if case_id != "daily":
            col_name = f"case_{case_id}_count"
            if col_name in all_columns:
                case_cols.append(col_name)
    
    all_select_cols = select_cols + case_cols
    query = f"SELECT {', '.join(all_select_cols)} FROM user_fat WHERE user_id = ?"
    
    cursor.execute(query, (str(user_id),))
    result = cursor.fetchone()
    
    if result:
        data = list(result)
        idx = 0
        user_data = {}
        for col in select_cols:
            user_data[col] = data[idx]
            idx += 1
        
        cases_dict = {}
        for i, case_col in enumerate(case_cols):
            case_id = case_col.replace("case_", "").replace("_count", "")
            cases_dict[case_id] = data[idx + i] or 0
        
        if 'shop' in cases_dict:
            cases_dict['shop_case'] = cases_dict.get('shop_case', 0) + cases_dict['shop']
            del cases_dict['shop']
        
        user_data['cases_dict'] = cases_dict
    else:
        user_data = {
            'user_id': str(user_id),
            'user_name': user_name or "Unknown",
            'current_number': 0,
            'last_command_time': None,
            'consecutive_plus': 0,
            'consecutive_minus': 0,
            'jackpot_pity': 0,
            'last_case_time': None,
            'item_counts': '{}',
            'last_command': None,
            'last_command_target': None,
            'last_command_use_time': None,
            'fat_cooldown_time': None,
            'active_case_message_id': None,
            'active_case_channel_id': None,
            'daily_case_last_time': None,
            'snatcher_last_time': None,
            'duel_active': 0,
            'duel_opponent': None,
            'duel_amount': 0,
            'duel_message_id': None,
            'duel_channel_id': None,
            'duel_initiator': 0,
            'last_case_type': None,
            'last_case_prize': None,
            'upgrade_active': 0,
            'upgrade_data': None,
            'duel_start_time': None,
            'shadow_upgrade_chance': 0,
            'user_xp': 0,
            'user_level': 0,
            'fat_cd_upgrade': 0,
            'case_cd_upgrade': 0,
            'luck_upgrade': 0,
            'income_upgrade': 0,
            'prestige': 0,
            'auto_fat_level': 0,
            'next_auto_fat_time': None,
            'cases_dict': {}
        }
        
        for case_id in CASES.keys():
            if case_id != "daily":
                user_data['cases_dict'][case_id] = 0
        
        create_new_user(cursor, user_data, all_columns)
        conn.commit()
    
    conn.close()
    return user_data

def create_new_user(cursor, user_data, all_columns):
    cols = []
    values = []
    
    base_fields = ['user_id', 'user_name', 'current_number', 'last_command_time',
                   'consecutive_plus', 'consecutive_minus', 'jackpot_pity',
                   'last_case_time', 'item_counts', 'last_command', 'last_command_target',
                   'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id',
                   'active_case_channel_id', 'daily_case_last_time', 'snatcher_last_time',
                   'duel_active', 'duel_opponent', 'duel_amount', 'duel_message_id', 
                   'duel_channel_id', 'duel_initiator', 'last_case_type', 'last_case_prize',
                   'upgrade_active', 'upgrade_data', 'duel_start_time', 'shadow_upgrade_chance',
                   'user_xp', 'user_level', 'fat_cd_upgrade', 'case_cd_upgrade', 'luck_upgrade',
                   'income_upgrade', 'prestige', 'auto_fat_level', 'next_auto_fat_time']
    
    for field in base_fields:
        if field in all_columns:
            cols.append(field)
            values.append(user_data.get(field))
    
    for case_id, count in user_data['cases_dict'].items():
        col_name = f"case_{case_id}_count"
        if col_name in all_columns:
            cols.append(col_name)
            values.append(count)
    
    placeholders = ["?"] * len(cols)
    query = f"INSERT INTO user_fat ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
    cursor.execute(query, values)

def update_user_data(guild_id, user_id, **kwargs):
    safe_init_guild_database(guild_id, f"Guild_{guild_id}")
    db_path = get_db_path(guild_id)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    updates = []
    values = []
    
    for key, value in kwargs.items():
        if key == 'number':
            if 'current_number' in existing_columns:
                updates.append("current_number = ?")
                values.append(value)
        elif key == 'cases_dict' and isinstance(value, dict):
            for case_id, count in value.items():
                col_name = f"case_{case_id}_count"
                if col_name in existing_columns:
                    updates.append(f"{col_name} = ?")
                    values.append(count)
        elif key in existing_columns:
            updates.append(f"{key} = ?")
            values.append(value)
    
    if not updates:
        conn.close()
        return
    
    values.append(str(user_id))
    query = f"UPDATE user_fat SET {', '.join(updates)} WHERE user_id = ?"
    
    try:
        cursor.execute(query, values)
    except sqlite3.OperationalError as e:
        print(f"❌ Ошибка SQL: {e}")
        conn.close()
        return
    
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

def get_rank(weight):
    for rank in RANKS:
        if rank["min"] <= weight <= rank["max"]:
            return rank["name"], rank["emoji"]
    if weight > 99999999:
        return "🌀 Бесконечность", "🌀"
    if weight < -999:
        return "Черная дыра", "💀"
    return "❓ Неопределённый", "❓"

# ===== НАСТРОЙКИ БОТА =====
if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

def get_user_cases(guild_id, user_id):
    data = get_user_data(guild_id, user_id)
    return data.get('cases_dict', {})

def update_user_cases(guild_id, user_id, case_id, change=1):
    data = get_user_data(guild_id, user_id)
    cases_dict = data.get('cases_dict', {}).copy()
    
    if case_id in cases_dict:
        cases_dict[case_id] += change
        if cases_dict[case_id] < 0:
            cases_dict[case_id] = 0
    
    update_user_data(guild_id, user_id, cases_dict=cases_dict)

def can_get_daily_case(guild_id, user_id, custom_cooldown=None):
    data = get_user_data(guild_id, user_id)
    daily_case_last_time = data.get('daily_case_last_time')
    
    if not daily_case_last_time:
        return True, 0
    
    if isinstance(daily_case_last_time, str):
        last_time = datetime.fromisoformat(daily_case_last_time)
    else:
        last_time = daily_case_last_time
    
    time_diff = datetime.now() - last_time
    
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    cd_reduction_minutes = get_case_cd_reduction(case_cd_upgrade)
    actual_cooldown = max(1, CASE_COOLDOWN_HOURS * 60 - cd_reduction_minutes) / 60
    
    cooldown_seconds = actual_cooldown * 3600
    
    if time_diff.total_seconds() >= cooldown_seconds:
        return True, 0
    else:
        remaining = cooldown_seconds - time_diff.total_seconds()
        return False, remaining

def update_daily_case_time(guild_id, user_id):
    update_user_data(guild_id, user_id, daily_case_last_time=datetime.now())

def get_all_users_sorted(guild_id):
    safe_init_guild_database(guild_id, f"Guild_{guild_id}")
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    columns = [col[1] for col in cursor.fetchall()]
    
    select_cols = ['user_name', 'current_number', 'last_command_time', 
                   'consecutive_plus', 'consecutive_minus', 'jackpot_pity']
    
    if 'prestige' in columns:
        select_cols.append('prestige')
    
    query = f"SELECT {', '.join(select_cols)} FROM user_fat ORDER BY current_number DESC"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

def get_guild_stats(guild_id):
    users = get_all_users_sorted(guild_id)
    total_users = len(users)
    total_weight = sum(u[1] for u in users)
    avg_weight = total_weight / total_users if total_users > 0 else 0
    positive = sum(1 for u in users if u[1] > 0)
    negative = sum(1 for u in users if u[1] < 0)
    zero = sum(1 for u in users if u[1] == 0)
    
    return {
        'total_users': total_users, 
        'total_weight': total_weight, 
        'avg_weight': avg_weight, 
        'positive': positive, 
        'negative': negative, 
        'zero': zero
    }

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
    if not member:
        return False
    for role in member.roles:
        if role.name.lower() == TESTER_ROLE_NAME.lower():
            return True
    return False

def has_high_tester_role(member):
    if not member:
        return False
    for role in member.roles:
        if role.name.lower() == HIGH_TESTER_ROLE_NAME.lower():
            return True
    return False

# ===== ФУНКЦИИ ДЛЯ БОЕВОЙ СИСТЕМЫ =====
def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, 
                                      luck_upgrade=0, prestige_bonus=1.0,
                                      items_dict=None, current_weight=None):
    if items_dict is None:
        items_dict = {}
    
    has_rotten_leg = items_dict.get("Гнилая ножка KFC", 0) > 0
    has_holy_sandwich = items_dict.get("Святой сэндвич", 0) > 0
    has_water = items_dict.get("Стакан воды", 0) > 0
    
    minus_boost = min(consecutive_minus * CONSECUTIVE_MINUS_BOOST, MAX_CONSECUTIVE_MINUS_BOOST)
    
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - minus_boost
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))
    
    jackpot_chance = BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT)
    if has_holy_sandwich:
        sandwich_count = items_dict.get("Святой сэндвич", 0)
        sandwich_bonus = 0.3 * sandwich_count
        jackpot_chance = max(jackpot_chance, sandwich_bonus)
        jackpot_chance = min(jackpot_chance, 0.9)
    else:
        jackpot_chance = min(jackpot_chance, MAX_JACKPOT_CHANCE)
    
    if has_water:
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX) // 3
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            change = random.randint(1, 20) // 3
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    elif has_rotten_leg:
        if random.random() < 0.6:
            if current_weight is not None:
                loss = int(current_weight * 0.5)
                change = -loss
            else:
                change = -int(consecutive_plus * 0.5)
            change = int(change * prestige_bonus)
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    else:
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        
        roll = random.random()
        if roll < minus_chance:
            change = random.randint(-20, -1)
            change = int(change * prestige_bonus)
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
        else:
            change = random.randint(1, 20)
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
        
        return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot

def open_case(case_id, prestige_luck=0, luck_upgrade=0):
    case = CASES[case_id]
    prizes = case["prizes"]
    
    total_chance = sum(p["chance"] for p in prizes)
    for prize in prizes:
        prize["normalized_chance"] = (prize["chance"] / total_chance) * 100
    
    prestige_bonus = 1 + prestige_luck
    luck_bonus = 1 + (luck_upgrade * 0.0025)
    
    modified_prizes = []
    for prize in prizes:
        p = prize.copy()
        if (isinstance(p["value"], int) and p["value"] >= 100) or p["value"] in ["rotten_leg", "water"]:
            p["normalized_chance"] = prize["normalized_chance"] * prestige_bonus * luck_bonus
        modified_prizes.append(p)
    
    total = sum(p["normalized_chance"] for p in modified_prizes)
    for p in modified_prizes:
        p["normalized_chance"] = (p["normalized_chance"] / total) * 100
    prizes = modified_prizes
    
    roll = random.random() * 100
    cumulative = 0
    for prize in prizes:
        cumulative += prize["normalized_chance"]
        if roll < cumulative:
            return prize
    
    return prizes[-1]

# ===== АВТО-ЖИР =====
async def apply_auto_fat(user_id, guild_id, user_name, channel_id=None):
    try:
        data = get_user_data(guild_id, user_id, user_name)
        
        items_dict = get_user_items(data['item_counts'])
        prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
        
        change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
            data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
            data.get('luck_upgrade', 0), prestige_bonus, items_dict, data['current_number'])
        
        new_number = data['current_number'] + change
        
        update_data = {
            'number': new_number,
            'user_name': user_name,
            'consecutive_plus': new_consecutive_plus,
            'consecutive_minus': new_consecutive_minus,
            'jackpot_pity': new_jackpot_pity,
            'fat_cooldown_time': datetime.now()
        }
        
        update_user_data(guild_id, user_id, **update_data)
        
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
                new_nick = format_nick_with_prestige(data.get('prestige', 0), new_number, clean_name)
                if len(new_nick) > 32:
                    new_nick = new_nick[:32]
                try:
                    await member.edit(nick=new_nick)
                except:
                    pass
        
        if channel_id:
            channel = bot.get_channel(channel_id)
            if channel:
                rank_name, rank_emoji = get_rank(new_number)
                
                if was_jackpot:
                    embed_color = 0xffd700
                    embed_title = "💰 ДЖЕКПОТ! 💰 (Авто-жир)"
                else:
                    embed_color = 0xff9933 if new_number >= 0 else 0x66ccff
                    embed_title = "🍔 Авто-жир"
                
                embed = discord.Embed(
                    title=embed_title,
                    description=f"**{member.mention if member else user_name}** теперь весит **{abs(new_number)}kg**!",
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
                embed.set_footer(text="⚡ Сработал авто-жир!")
                
                await channel.send(embed=embed)
        
        print(f"🤖 Авто-жир сработал для {user_name}: {change:+d} кг")
    except Exception as e:
        print(f"❌ Ошибка в авто-жире: {e}")

async def auto_fat_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            current_time = datetime.now()
            
            for guild in bot.guilds:
                guild_id = guild.id
                db_path = get_db_path(guild_id)
                if not os.path.exists(db_path):
                    continue
                
                default_channel = None
                for channel in guild.text_channels:
                    if channel.permissions_for(guild.me).send_messages:
                        default_channel = channel
                        break
                
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    cursor.execute("PRAGMA table_info(user_fat)")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    if 'auto_fat_level' not in columns or 'next_auto_fat_time' not in columns:
                        conn.close()
                        continue
                    
                    cursor.execute('''SELECT user_id, user_name, auto_fat_level, next_auto_fat_time 
                                    FROM user_fat WHERE auto_fat_level > 0 AND next_auto_fat_time IS NOT NULL''')
                    users = cursor.fetchall()
                    conn.close()
                    
                    for user_id, user_name, auto_fat_level, next_time_str in users:
                        try:
                            if next_time_str:
                                if isinstance(next_time_str, str):
                                    next_time = datetime.fromisoformat(next_time_str)
                                else:
                                    next_time = next_time_str
                                if current_time >= next_time:
                                    await apply_auto_fat(user_id, guild_id, user_name, default_channel.id if default_channel else None)
                                    
                                    interval = get_auto_fat_interval(auto_fat_level)
                                    if interval:
                                        new_next_time = current_time + timedelta(hours=interval)
                                        conn2 = sqlite3.connect(db_path)
                                        c2 = conn2.cursor()
                                        c2.execute('''UPDATE user_fat SET next_auto_fat_time = ? 
                                                    WHERE user_id = ?''', (new_next_time.isoformat(), user_id))
                                        conn2.commit()
                                        conn2.close()
                        except Exception as e:
                            print(f"❌ Ошибка обработки авто-жира для {user_id}: {e}")
                except Exception as e:
                    print(f"❌ Ошибка при работе с БД сервера {guild_id}: {e}")
        except Exception as e:
            print(f"❌ Ошибка в цикле авто-жира: {e}")
        
        await asyncio.sleep(60)

async def passive_income_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            current_time = datetime.now()
            
            for guild in bot.guilds:
                guild_id = guild.id
                db_path = get_db_path(guild_id)
                if not os.path.exists(db_path):
                    continue
                
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    cursor.execute("PRAGMA table_info(user_fat)")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    if 'item_counts' not in columns or 'last_passive_income' not in columns:
                        conn.close()
                        continue
                    
                    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, 
                                      income_upgrade, prestige, last_passive_income 
                                      FROM user_fat WHERE item_counts != '{}' AND item_counts IS NOT NULL''')
                    users = cursor.fetchall()
                    conn.close()
                    
                    for user_id, user_name, current_number, item_counts_str, income_upgrade, prestige, last_income in users:
                        try:
                            should_pay = False
                            
                            if not last_income:
                                should_pay = False
                            else:
                                if isinstance(last_income, str):
                                    last_time = datetime.fromisoformat(last_income)
                                else:
                                    last_time = last_income
                                
                                time_diff = current_time - last_time
                                if time_diff.total_seconds() >= 24 * 60 * 60:
                                    should_pay = True
                            
                            if should_pay:
                                items_dict = get_user_items(item_counts_str)
                                if not items_dict:
                                    continue
                                
                                total_gain = 0
                                gained_items = []
                                
                                for item_name, count in items_dict.items():
                                    for shop_item in SHOP_ITEMS:
                                        if shop_item["name"] == item_name:
                                            gain = shop_item.get("gain_per_24h", 0) * count
                                            if gain > 0:
                                                total_gain += gain
                                                gained_items.append(f"{item_name} x{count} (+{gain}кг)")
                                            break
                                
                                if total_gain > 0:
                                    income_bonus = get_income_bonus(income_upgrade or 0)
                                    prestige_bonus = get_prestige_bonus(prestige or 0)
                                    final_gain = int(total_gain * income_bonus * prestige_bonus)
                                    new_number = current_number + final_gain
                                    
                                    conn2 = sqlite3.connect(db_path)
                                    c = conn2.cursor()
                                    c.execute('''UPDATE user_fat SET 
                                                current_number = ?, 
                                                last_passive_income = ? 
                                                WHERE user_id = ?''', 
                                             (new_number, current_time, user_id))
                                    conn2.commit()
                                    conn2.close()
                                    
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
                                                new_nick = format_nick_with_prestige(prestige or 0, new_number, clean_name)
                                                if len(new_nick) > 32:
                                                    new_nick = new_nick[:32]
                                                await member.edit(nick=new_nick)
                                    except:
                                        pass
                                    
                                    print(f"💰 {user_name} получил {final_gain}кг от предметов: {', '.join(gained_items)}")
                            else:
                                conn2 = sqlite3.connect(db_path)
                                c = conn2.cursor()
                                c.execute('''UPDATE user_fat SET last_passive_income = ? 
                                           WHERE user_id = ? AND last_passive_income IS NULL''', 
                                         (current_time, user_id))
                                conn2.commit()
                                conn2.close()
                                
                        except Exception as e:
                            print(f"❌ Ошибка при начислении дохода для {user_id}: {e}")
                except Exception as e:
                    print(f"❌ Ошибка при работе с БД сервера {guild_id}: {e}")
        except Exception as e:
            print(f"❌ Ошибка в цикле пассивного дохода: {e}")
        
        await asyncio.sleep(24 * 60 * 60)

async def apply_snatcher_effect(guild_id, user_id, user_name):
    try:
        data = get_user_data(guild_id, user_id, user_name)
        items_dict = get_user_items(data['item_counts'])
        
        snatcher_count = items_dict.get("Снатчер", 0)
        if snatcher_count == 0:
            return
        
        current_time = datetime.now()
        
        if data.get('snatcher_last_time'):
            last_time = data['snatcher_last_time']
            if isinstance(last_time, str):
                last_time = datetime.fromisoformat(last_time)
            
            time_diff = current_time - last_time
            if time_diff.total_seconds() < 6 * 3600:
                return
        
        if random.random() > 0.2:
            update_user_data(guild_id, user_id, snatcher_last_time=current_time)
            return
        
        virtual_slots = []
        used_indices = set()
        
        for _ in range(10):
            chosen_item = None
            for _ in range(50):
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
                virtual_slots.append({
                    "name": chosen_item["name"],
                    "amount": amount,
                    "price": chosen_item["price"],
                    "description": chosen_item["description"],
                    "gain_per_24h": chosen_item.get("gain_per_24h", 0)
                })
            else:
                virtual_slots.append(None)
        
        chosen_slot = random.randint(0, 9)
        selected_item = virtual_slots[chosen_slot]
        
        if not selected_item:
            update_user_data(guild_id, user_id, snatcher_last_time=current_time)
            return
        
        items_dict[selected_item["name"]] = items_dict.get(selected_item["name"], 0) + 1
        update_user_data(
            guild_id, user_id, 
            item_counts=save_user_items(items_dict),
            snatcher_last_time=current_time
        )
        
        try:
            guild = bot.get_guild(guild_id)
            if guild:
                member = guild.get_member(int(user_id))
                if member:
                    embed = discord.Embed(
                        title="👾 **Снатчер сработал!**",
                        description=f"Ваш **Снатчер** сгенерировал предмет из {chosen_slot + 1} слота!",
                        color=0x9b59b6
                    )
                    embed.add_field(name="📦 Получено", value=f"**+1 {selected_item['name']}**", inline=False)
                    await member.send(embed=embed)
        except:
            pass
            
    except Exception as e:
        print(f"❌ Ошибка в работе Снатчера: {e}")

async def snatcher_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            for guild in bot.guilds:
                guild_id = guild.id
                db_path = get_db_path(guild_id)
                if not os.path.exists(db_path):
                    continue
                
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    cursor.execute("PRAGMA table_info(user_fat)")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    if 'snatcher_last_time' not in columns or 'item_counts' not in columns:
                        conn.close()
                        continue
                    
                    cursor.execute('''SELECT user_id, user_name FROM user_fat 
                                    WHERE item_counts LIKE '%"Снатчер"%' ''')
                    users = cursor.fetchall()
                    conn.close()
                    
                    for user_id, user_name in users:
                        try:
                            await apply_snatcher_effect(guild_id, user_id, user_name)
                            await asyncio.sleep(1)
                        except Exception as e:
                            print(f"❌ Ошибка при обработке Снатчера для {user_id}: {e}")
                except Exception as e:
                    print(f"❌ Ошибка при работе с БД сервера {guild_id}: {e}")
            
        except Exception as e:
            print(f"❌ Ошибка в цикле Снатчера: {e}")
        
        await asyncio.sleep(1800)

async def apply_hourly_effects():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            current_time = datetime.now()
            
            for guild in bot.guilds:
                guild_id = guild.id
                db_path = get_db_path(guild_id)
                if not os.path.exists(db_path):
                    continue
                
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    cursor.execute("PRAGMA table_info(user_fat)")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    if 'item_counts' not in columns or 'last_hourly_income' not in columns:
                        conn.close()
                        continue
                    
                    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, 
                                      income_upgrade, prestige, last_hourly_income 
                                      FROM user_fat''')
                    users = cursor.fetchall()
                    conn.close()
                    
                    for user_id, user_name, current_number, item_counts_str, income_upgrade, prestige, last_hourly in users:
                        try:
                            should_pay = False
                            
                            if not last_hourly:
                                should_pay = False
                            else:
                                if isinstance(last_hourly, str):
                                    last_time = datetime.fromisoformat(last_hourly)
                                else:
                                    last_time = last_hourly
                                
                                time_diff = current_time - last_time
                                if time_diff.total_seconds() >= 3600:
                                    should_pay = True
                            
                            if should_pay:
                                items_dict = get_user_items(item_counts_str)
                                if not items_dict:
                                    continue
                                
                                total_gain = 0
                                gained_items = []
                                
                                for item_name, count in items_dict.items():
                                    if item_name == "Автохолестерол":
                                        gain = random.randint(1, 10) * count
                                        total_gain += gain
                                        gained_items.append(f"Автохолестерол x{count} (+{gain}кг)")
                                    elif item_name == "Холестеринимус":
                                        gain = random.randint(1, 5) * count
                                        total_gain += gain
                                        gained_items.append(f"Холестеринимус x{count} (+{gain}кг)")
                                
                                if total_gain > 0:
                                    income_bonus = get_income_bonus(income_upgrade or 0)
                                    prestige_bonus = get_prestige_bonus(prestige or 0)
                                    final_gain = int(total_gain * income_bonus * prestige_bonus)
                                    new_number = current_number + final_gain
                                    
                                    conn2 = sqlite3.connect(db_path)
                                    c = conn2.cursor()
                                    c.execute('''UPDATE user_fat SET 
                                                current_number = ?, 
                                                last_hourly_income = ? 
                                                WHERE user_id = ?''', 
                                             (new_number, current_time, user_id))
                                    conn2.commit()
                                    conn2.close()
                                    
                                    print(f"💊 {user_name} получил {final_gain}кг от почасовых предметов")
                            else:
                                conn2 = sqlite3.connect(db_path)
                                c = conn2.cursor()
                                c.execute('''UPDATE user_fat SET last_hourly_income = ? 
                                           WHERE user_id = ? AND last_hourly_income IS NULL''', 
                                         (current_time, user_id))
                                conn2.commit()
                                conn2.close()
                                
                        except Exception as e:
                            print(f"❌ Ошибка при начислении почасового дохода для {user_id}: {e}")
                except Exception as e:
                    print(f"❌ Ошибка при работе с БД сервера {guild_id}: {e}")
            
        except Exception as e:
            print(f"❌ Ошибка в цикле почасовых эффектов: {e}")
        
        await asyncio.sleep(3600)

def check_databases_on_startup():
    print("\n🔍 ** ПРОВЕРКА БАЗ ДАННЫХ ** 🔍")
    existing_dbs = 0
    for guild in bot.guilds:
        if safe_init_guild_database(guild.id, guild.name):
            existing_dbs += 1
    return existing_dbs

def can_duel(user_data):
    return not user_data.get('duel_active', 0)

def get_duel_info(user_data):
    return {
        'active': user_data.get('duel_active', 0),
        'opponent': user_data.get('duel_opponent'),
        'amount': user_data.get('duel_amount', 0),
        'message_id': user_data.get('duel_message_id'),
        'channel_id': user_data.get('duel_channel_id'),
        'initiator': user_data.get('duel_initiator', 0),
        'start_time': user_data.get('duel_start_time')
    }

async def migrate_old_burgers_to_prestige():
    print("\n🔄 КОНВЕРТАЦИЯ ЛЕГЕНДАРНЫХ БУРГЕРОВ В ПРЕСТИЖ...")
    
    burger_to_prestige = {
        0: 1,
        1: 2,
        2: 3,
        3: 4
    }
    
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
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'legendary_burger' in columns and 'prestige' in columns:
                cursor.execute("SELECT user_id, user_name, legendary_burger FROM user_fat WHERE legendary_burger >= 0")
                users = cursor.fetchall()
                
                for user_id, user_name, burger_level in users:
                    if burger_level in burger_to_prestige:
                        prestige_amount = burger_to_prestige[burger_level]
                        
                        cursor.execute("UPDATE user_fat SET prestige = ?, legendary_burger = -1 WHERE user_id = ?", 
                                      (prestige_amount, user_id))
                        converted += 1
                        print(f"  ✅ {user_name}: бургер {burger_level} -> {prestige_amount} престижа")
                
                conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"❌ Ошибка при конвертации на сервере {guild.name}: {e}")
    
    print(f"✅ Конвертация завершена! Обработано пользователей: {converted}")

async def migrate_old_autoburgers_to_auto_fat():
    print("\n🔄 КОНВЕРТАЦИЯ АВТОБУРГЕРОВ В АВТО-ЖИР...")
    
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
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'autoburger_count' in columns and 'auto_fat_level' in columns:
                cursor.execute("SELECT user_id, user_name, autoburger_count FROM user_fat WHERE autoburger_count > 0")
                users = cursor.fetchall()
                
                for user_id, user_name, autoburger_count in users:
                    if autoburger_count > 0:
                        new_level = min(autoburger_count, 3)
                        interval = get_auto_fat_interval(new_level)
                        next_time = datetime.now() + timedelta(hours=interval) if interval else None
                        cursor.execute("UPDATE user_fat SET auto_fat_level = ?, next_auto_fat_time = ?, autoburger_count = 0 WHERE user_id = ?", 
                                      (new_level, next_time, user_id))
                        converted += 1
                        print(f"  ✅ {user_name}: {autoburger_count} автобургер(ов) -> {new_level} уровень авто-жира")
                
                conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"❌ Ошибка при конвертации на сервере {guild.name}: {e}")
    
    print(f"✅ Конвертация автобургеров завершена! Обработано пользователей: {converted}")

async def duel_animation(msg, challenger, opponent):
    c_name = challenger.display_name[:15] + "..." if len(challenger.display_name) > 15 else challenger.display_name
    o_name = opponent.display_name[:15] + "..." if len(opponent.display_name) > 15 else opponent.display_name
    
    max_len = max(len(c_name), len(o_name))
    c_name = c_name.ljust(max_len)
    o_name = o_name.ljust(max_len)
    
    duel_emojis = ["⬆️", "⬇️", "⚔️"]
    
    line = []
    for i in range(100):
        line.append(random.choice(duel_emojis))
    
    result = random.randint(0, 2)
    
    if result == 0:
        result_emoji = "⬆️"
        result_text = f"🏆 **Победитель:** {challenger.mention}"
        result_color = 0xffd700
    elif result == 1:
        result_emoji = "⬇️"
        result_text = f"🏆 **Победитель:** {opponent.mention}"
        result_color = 0xc0c0c0
    else:
        result_emoji = "⚔️"
        result_text = "🤝 **НИЧЬЯ!** 🤝"
        result_color = 0x9b59b6
    
    line[57] = result_emoji
    
    anim_embed = discord.Embed(
        title="⚔️ **ДУЭЛЬ** ⚔️",
        description="",
        color=0xff5500
    )
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        anim_embed.description = f"**{c_name}**\n**{display_line}**\n**{o_name}**"
        
        await msg.edit(embed=anim_embed)
        await asyncio.sleep(0.5)
    
    result_embed = discord.Embed(
        title="⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️",
        description=f"**{c_name}**\n**{display_line}**\n**{o_name}**\n\n{result_text}",
        color=result_color
    )
    await msg.edit(embed=result_embed)
    await asyncio.sleep(1.5)
    
    return result

def get_item_price(item_name):
    if item_name in LEGENDARY_UPGRADE_PRICES:
        return LEGENDARY_UPGRADE_PRICES[item_name]
    
    for shop_item in SHOP_ITEMS:
        if shop_item["name"] == item_name:
            return shop_item["price"]
    
    return 0

def get_possible_upgrades(item_name, item_count):
    if item_count <= 0:
        return []
    
    current_price = get_item_price(item_name)
    if current_price == 0:
        return []
    
    possible_upgrades = []
    seen_items = set()
    
    all_items = set()
    for shop_item in SHOP_ITEMS:
        all_items.add(shop_item["name"])
    for leg_name in LEGENDARY_UPGRADE_PRICES.keys():
        all_items.add(leg_name)
    
    for shop_item in SHOP_ITEMS:
        item_name_check = shop_item["name"]
        
        if item_name_check in seen_items:
            continue
            
        target_price = get_item_price(item_name_check)
        
        if target_price <= current_price:
            continue
        
        chance = current_price / target_price
        
        if chance < 0.01:
            continue
        
        possible_upgrades.append({
            "name": item_name_check,
            "price": target_price,
            "chance": chance,
            "emoji": ITEM_EMOJIS.get(item_name_check, "🎁")
        })
        seen_items.add(item_name_check)
    
    if current_price >= 1000:
        for leg_name, leg_price in LEGENDARY_UPGRADE_PRICES.items():
            if leg_name in seen_items:
                continue
            
            if leg_name not in all_items:
                continue
            
            if leg_price <= current_price:
                continue
            
            chance = current_price / leg_price
            
            if chance < 0.01:
                continue
            
            possible_upgrades.append({
                "name": leg_name,
                "price": leg_price,
                "chance": chance,
                "emoji": ITEM_EMOJIS.get(leg_name, "✨")
            })
            seen_items.add(leg_name)
    
    possible_upgrades.sort(key=lambda x: x["price"])
    
    return possible_upgrades

async def upgrade_animation(ctx, member, source_item, target_item, item_count, prestige_luck=0, luck_upgrade=0):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    user_name = member.name
    
    data = get_user_data(guild_id, user_id, user_name)
    
    shadow_chance = data.get('shadow_upgrade_chance', 0)
    prestige_bonus = 1 + prestige_luck
    luck_bonus = 1 + (luck_upgrade * 0.005)
    base_chance = target_item['chance']
    real_chance = min(base_chance * prestige_bonus * luck_bonus + shadow_chance / 100, 1.0)
    display_chance = base_chance * prestige_bonus * luck_bonus * 100
    
    upgrade_emojis = ["🟥", "🟩"]
    
    line = []
    for i in range(100):
        line.append(random.choice(upgrade_emojis))
    
    roll = random.random()
    success = roll < real_chance
    
    if success:
        new_shadow = max(0, shadow_chance - 8)
        result_emoji = "🟩"
        result_text = f"✅ **УСПЕХ!** ✅"
        result_color = 0x00ff00
    else:
        new_shadow = min(32, shadow_chance + 4)
        result_emoji = "🟥"
        result_text = f"❌ **НЕУДАЧА!** ❌"
        result_color = 0xff0000
    
    line[57] = result_emoji
    
    anim_embed = discord.Embed(
        title="🔧 **АПГРЕЙД** 🔧",
        description=f"**{member.display_name}** улучшает:\n"
                   f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n"
                   f"Шанс: **{display_chance:.1f}%**",
        color=0xff5500
    )
    
    upgrade_msg = await ctx.send(embed=anim_embed)
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        anim_embed.description = f"**{member.display_name}** улучшает:\n" \
                                 f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n" \
                                 f"**{display_line}**\n\n" \
                                 f"Шанс: **{display_chance:.1f}%**"
        
        await upgrade_msg.edit(embed=anim_embed)
        await asyncio.sleep(0.5)
    
    current_data = get_user_data(guild_id, user_id, user_name)
    items_dict = get_user_items(current_data['item_counts'])
    
    if success:
        items_dict[target_item['name']] = items_dict.get(target_item['name'], 0) + 1
        
        result_description = f"✅ **Поздравляем!**\n\n" \
                            f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n" \
                            f"Предмет успешно улучшен!"
    else:
        result_description = f"❌ **Неудача!**\n\n" \
                            f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** был утерян в процессе улучшения!"
    
    update_data = {
        'item_counts': save_user_items(items_dict),
        'shadow_upgrade_chance': new_shadow,
        'upgrade_active': 0,
        'upgrade_data': None,
        'last_command': None,
        'last_command_target': None,
        'last_command_use_time': None
    }
    
    update_user_data(guild_id, user_id, **update_data)
    
    result_embed = discord.Embed(
        title="🔧 **РЕЗУЛЬТАТ АПГРЕЙДА** 🔧",
        description=f"**{display_line}**\n\n{result_text}\n\n{result_description}",
        color=result_color
    )
    result_embed.set_footer(text=f"Шанс был: {display_chance:.1f}%")
    
    await upgrade_msg.edit(embed=result_embed)

async def upgrade_kg_animation(ctx, member, amount, target_item, prestige_luck=0, luck_upgrade=0):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    user_name = member.name
    
    data = get_user_data(guild_id, user_id, user_name)
    
    shadow_chance = data.get('shadow_upgrade_chance', 0)
    prestige_bonus = 1 + prestige_luck
    luck_bonus = 1 + (luck_upgrade * 0.005)
    base_chance = target_item['chance']
    real_chance = min(base_chance * prestige_bonus * luck_bonus + shadow_chance / 100, 1.0)
    display_chance = base_chance * prestige_bonus * luck_bonus * 100
    
    upgrade_emojis = ["🟥", "🟩"]
    
    line = []
    for i in range(100):
        line.append(random.choice(upgrade_emojis))
    
    roll = random.random()
    success = roll < real_chance
    
    if success:
        new_shadow = max(0, shadow_chance - 8)
        result_emoji = "🟩"
        result_text = f"✅ **УСПЕХ!** ✅"
        result_color = 0x00ff00
    else:
        new_shadow = min(32, shadow_chance + 4)
        result_emoji = "🟥"
        result_text = f"❌ **НЕУДАЧА!** ❌"
        result_color = 0xff0000
    
    line[57] = result_emoji
    
    anim_embed = discord.Embed(
        title="💱 **АПГРЕЙД КГ** 💱",
        description=f"**{member.display_name}** улучшает {amount} кг в:\n"
                   f"{target_item['emoji']} **{target_item['name']}**\n\n"
                   f"Шанс: **{display_chance:.1f}%**",
        color=0xff5500
    )
    
    upgrade_msg = await ctx.send(embed=anim_embed)
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        anim_embed.description = f"**{member.display_name}** улучшает {amount} кг в:\n" \
                                 f"{target_item['emoji']} **{target_item['name']}**\n\n" \
                                 f"**{display_line}**\n\n" \
                                 f"Шанс: **{display_chance:.1f}%**"
        
        await upgrade_msg.edit(embed=anim_embed)
        await asyncio.sleep(0.5)
    
    current_data = get_user_data(guild_id, user_id, user_name)
    
    if success:
        if target_item.get("is_case", False):
            cases_dict = current_data.get('cases_dict', {}).copy()
            cases_dict[target_item["case_id"]] = cases_dict.get(target_item["case_id"], 0) + 1
            update_user_data(
                guild_id, user_id,
                cases_dict=cases_dict,
                shadow_upgrade_chance=new_shadow,
                upgrade_active=0,
                upgrade_data=None
            )
            result_description = f"✅ **Поздравляем!**\n\n" \
                                f"{amount} кг → {target_item['emoji']} **{target_item['name']}**\n\n" \
                                f"Предмет успешно получен!"
        else:
            items_dict = get_user_items(current_data['item_counts'])
            items_dict[target_item["name"]] = items_dict.get(target_item["name"], 0) + 1
            update_user_data(
                guild_id, user_id,
                item_counts=save_user_items(items_dict),
                shadow_upgrade_chance=new_shadow,
                upgrade_active=0,
                upgrade_data=None
            )
            result_description = f"✅ **Поздравляем!**\n\n" \
                                f"{amount} кг → {target_item['emoji']} **{target_item['name']}**\n\n" \
                                f"Предмет успешно получен!"
    else:
        update_user_data(
            guild_id, user_id,
            shadow_upgrade_chance=new_shadow,
            upgrade_active=0,
            upgrade_data=None
        )
        result_description = f"❌ **Неудача!**\n\n" \
                            f"{amount} кг сгорели в процессе улучшения!"
    
    result_embed = discord.Embed(
        title="💱 **РЕЗУЛЬТАТ АПГРЕЙДА** 💱",
        description=f"**{display_line}**\n\n{result_text}\n\n{result_description}",
        color=result_color
    )
    result_embed.set_footer(text=f"Шанс был: {display_chance:.1f}%")
    
    await upgrade_msg.edit(embed=result_embed)

def generate_shop_items():
    slots = []
    used_indices = set()
    
    available_cases = [cid for cid, case in CASES.items() if cid != "daily" and case.get("shop_chance", 0) > 0]
    
    for _ in range(4):
        if random.random() < 0.7 and available_cases:
            case_choices = []
            for cid in available_cases:
                case = CASES[cid]
                weight = case["shop_chance"] * 100
                case_choices.extend([cid] * int(weight))
            
            if case_choices:
                chosen_id = random.choice(case_choices)
                case = CASES[chosen_id]
                amount = random.randint(case["min_shop"], case["max_shop"])
                
                min_prize = 0
                max_prize = 0
                for p in case["prizes"]:
                    if isinstance(p["value"], int):
                        if p["value"] < min_prize:
                            min_prize = p["value"]
                        if p["value"] > max_prize:
                            max_prize = p["value"]
                
                slots.append({
                    "type": "case",
                    "case_id": chosen_id,
                    "name": case["name"],
                    "amount": amount,
                    "price": case["price"],
                    "description": f"{case['emoji']} Содержит случайные призы!\nОт {min_prize}кг до {max_prize}кг",
                    "emoji": case['emoji']
                })
            else:
                slots.append(None)
        else:
            slots.append(None)
    
    for _ in range(6):
        chosen_item = None
        for _ in range(50):
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
                "type": "item",
                "name": chosen_item["name"],
                "amount": amount,
                "price": chosen_item["price"],
                "description": chosen_item["description"],
                "gain_per_24h": chosen_item.get("gain_per_24h", 0),
                "emoji": ITEM_EMOJIS.get(chosen_item["name"], "📦")
            })
        else:
            slots.append(None)
    
    random.shuffle(slots)
    return slots

def get_shop_data(guild_id):
    safe_init_guild_database(guild_id, f"Guild_{guild_id}")
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT slots, last_update, next_update FROM shop WHERE guild_id = ?', (str(guild_id),))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        slots_json, last_update, next_update = result
        try:
            slots = json.loads(slots_json) if slots_json else []
            for slot in slots:
                if slot is not None and "type" not in slot:
                    if "case_id" in slot:
                        slot["type"] = "case"
                    else:
                        slot["type"] = "item"
            return slots, last_update, next_update
        except:
            return [], None, None
    return None, None, None

def update_shop_data(guild_id, slots, last_update, next_update):
    safe_init_guild_database(guild_id, f"Guild_{guild_id}")
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    clean_slots = []
    for slot in slots:
        if slot is not None:
            if "type" not in slot:
                if "case_id" in slot:
                    slot["type"] = "case"
                else:
                    slot["type"] = "item"
            clean_slots.append(slot)
        else:
            clean_slots.append(None)
    
    slots_json = json.dumps(clean_slots)
    last_update_str = last_update.isoformat() if last_update else None
    next_update_str = next_update.isoformat() if next_update else None
    
    cursor.execute('''INSERT OR REPLACE INTO shop (guild_id, slots, last_update, next_update) VALUES (?, ?, ?, ?)''', 
                  (str(guild_id), slots_json, last_update_str, next_update_str))
    conn.commit()
    conn.close()

async def ensure_shop_updated(guild_id):
    result = get_shop_data(guild_id)
    current_time = datetime.now()
    
    if result[0] is not None:
        slots, last_update_str, next_update_str = result
        
        last_update = None
        next_update = None
        if last_update_str:
            try:
                last_update = datetime.fromisoformat(last_update_str) if isinstance(last_update_str, str) else last_update_str
            except:
                last_update = None
        if next_update_str:
            try:
                next_update = datetime.fromisoformat(next_update_str) if isinstance(next_update_str, str) else next_update_str
            except:
                next_update = None
        
        if next_update and current_time >= next_update:
            new_slots = generate_shop_items()
            last_update = current_time
            next_update = current_time + timedelta(hours=SHOP_UPDATE_HOURS)
            update_shop_data(guild_id, new_slots, last_update, next_update)
            return new_slots, last_update, next_update
        else:
            return slots, last_update, next_update
    else:
        new_slots = generate_shop_items()
        last_update = current_time
        next_update = current_time + timedelta(hours=SHOP_UPDATE_HOURS)
        update_shop_data(guild_id, new_slots, last_update, next_update)
        return new_slots, last_update, next_update

# ===== КОМАНДА !апгрейдюзер =====
@bot.command(name='апгрейдюзер')
async def upgrade_user_command(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    def create_upgrade_embed(data):
        fat_cd_level = data.get('fat_cd_upgrade', 0)
        case_cd_level = data.get('case_cd_upgrade', 0)
        luck_level = data.get('luck_upgrade', 0)
        income_level = data.get('income_upgrade', 0)
        prestige_level = data.get('prestige', 0)
        auto_fat_level = data.get('auto_fat_level', 0)
        
        total_xp = data.get('user_xp', 0)
        level, current_xp = get_level_and_xp(total_xp)
        next_level_xp = get_xp_for_next_level(level)
        
        fat_cd_cost = get_upgrade_cost("fat_cd", fat_cd_level)
        case_cd_cost = get_upgrade_cost("case_cd", case_cd_level)
        luck_cost = get_upgrade_cost("luck", luck_level)
        income_cost = get_upgrade_cost("income", income_level)
        prestige_cost = get_upgrade_cost("prestige", prestige_level)
        auto_fat_cost = get_upgrade_cost("auto_fat", auto_fat_level)
        
        fat_cd_bonus = get_fat_cd_reduction(fat_cd_level)
        case_cd_bonus = get_case_cd_reduction(case_cd_level)
        prestige_bonus = get_prestige_bonus(prestige_level)
        income_bonus = get_income_bonus(income_level)
        
        auto_fat_interval = get_auto_fat_interval(auto_fat_level)
        auto_fat_text = f"{auto_fat_interval} ч" if auto_fat_interval else "Не куплен"
        
        embed = discord.Embed(
            title=f"⭐ **ПРОКАЧКА ПЕРСОНАЖА** ⭐",
            description=f"**{member.display_name}**\n\n"
                       f"Выберите характеристику для улучшения, нажав на реакцию ниже:\n"
                       f"🟢 - доступно | 🔴 - недостаточно кг",
            color=0xffaa00
        )
        
        xp_bar_length = 20
        xp_progress = int((current_xp / next_level_xp) * xp_bar_length) if next_level_xp > 0 else 0
        xp_bar = "█" * xp_progress + "░" * (xp_bar_length - xp_progress)
        
        embed.add_field(
            name="📊 **УРОВЕНЬ И ОПЫТ**",
            value=f"Уровень: **{level}**\n"
                  f"Опыт: {current_xp} / {next_level_xp}\n"
                  f"`{xp_bar}`\n"
                  f"Всего опыта: {total_xp}",
            inline=False
        )
        
        stats_text = ""
        
        fat_cd_color = "🟢" if data['current_number'] >= fat_cd_cost else "🔴"
        stats_text += f"{fat_cd_color} **⏰ КД !жир** — ур.{fat_cd_level} (-{fat_cd_bonus} мин)\n"
        stats_text += f"   Стоимость: `{fat_cd_cost} кг`\n\n"
        
        case_cd_color = "🟢" if data['current_number'] >= case_cd_cost else "🔴"
        stats_text += f"{case_cd_color} **📦 КД кейса** — ур.{case_cd_level} (-{case_cd_bonus} мин)\n"
        stats_text += f"   Стоимость: `{case_cd_cost} кг`\n\n"
        
        luck_color = "🟢" if data['current_number'] >= luck_cost else "🔴"
        stats_text += f"{luck_color} **🍀 Удача** — ур.{luck_level} (+{luck_level * 0.25:.2f}% к редким, +{luck_level * 0.5:.2f}% к апгрейдам)\n"
        stats_text += f"   Стоимость: `{luck_cost} кг`\n\n"
        
        income_color = "🟢" if data['current_number'] >= income_cost else "🔴"
        stats_text += f"{income_color} **📈 Прибавка** — ур.{income_level} (+{(income_bonus-1)*100:.0f}% к доходу от предметов)\n"
        stats_text += f"   Стоимость: `{income_cost} кг`\n\n"
        
        prestige_color = "🟢" if data['current_number'] >= prestige_cost else "🔴"
        stats_text += f"{prestige_color} **🌟 Престиж** — ур.{prestige_level} (+{(prestige_bonus-1)*100:.0f}% ко всему, +{prestige_level}% к шансам)\n"
        stats_text += f"   Стоимость: `{prestige_cost} кг`\n\n"
        
        auto_fat_color = "🟢" if data['current_number'] >= auto_fat_cost else "🔴"
        stats_text += f"{auto_fat_color} **🤖 Авто-жир** — ур.{auto_fat_level} (каждые {auto_fat_text})\n"
        stats_text += f"   Стоимость: `{auto_fat_cost} кг`\n\n"
        
        embed.add_field(name="⚡ **ХАРАКТЕРИСТИКИ**", value=stats_text, inline=False)
        
        embed.add_field(
            name="💡 **ЧТО ДАЁТ**",
            value="• **КД !жир** — уменьшает время ожидания команды\n"
                  "• **КД кейса** — уменьшает время ожидания бесплатного кейса\n"
                  "• **Удача** — повышает шанс редких предметов в кейсах (+0.25%/ур) и шанс апгрейдов (+0.5%/ур)\n"
                  "• **Прибавка** — увеличивает получаемые кг от пассивного дохода и почасовых предметов (+5%/ур)\n"
                  "• **Престиж** — сбрасывает всё, но даёт +10% ко всем кг и +1% к шансам за уровень\n"
                  "• **Авто-жир** — автоматически использует !жир каждые 6/3/1 час(ов)",
            inline=False
        )
        
        embed.set_footer(text="💰 Для улучшения нажмите на соответствующую реакцию")
        return embed
    
    data = get_user_data(guild_id, user_id, user_name)
    embed = create_upgrade_embed(data)
    msg = await ctx.send(embed=embed)
    
    reactions = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣"]
    for reaction in reactions:
        await msg.add_reaction(reaction)
    
    upgrade_map = {
        "1️⃣": "fat_cd",
        "2️⃣": "case_cd", 
        "3️⃣": "luck",
        "4️⃣": "income",
        "5️⃣": "prestige",
        "6️⃣": "auto_fat"
    }
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in reactions and reaction.message.id == msg.id
    
    while True:
        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)
            selected = str(reaction.emoji)
            
            if selected not in upgrade_map:
                continue
            
            upgrade_type = upgrade_map[selected]
            
            current_data = get_user_data(guild_id, user_id, user_name)
            
            if upgrade_type == "fat_cd":
                current_level = current_data.get('fat_cd_upgrade', 0)
                cost = get_upgrade_cost("fat_cd", current_level)
            elif upgrade_type == "case_cd":
                current_level = current_data.get('case_cd_upgrade', 0)
                cost = get_upgrade_cost("case_cd", current_level)
            elif upgrade_type == "luck":
                current_level = current_data.get('luck_upgrade', 0)
                cost = get_upgrade_cost("luck", current_level)
            elif upgrade_type == "income":
                current_level = current_data.get('income_upgrade', 0)
                cost = get_upgrade_cost("income", current_level)
            elif upgrade_type == "prestige":
                current_level = current_data.get('prestige', 0)
                cost = get_upgrade_cost("prestige", current_level)
            elif upgrade_type == "auto_fat":
                current_level = current_data.get('auto_fat_level', 0)
                if current_level >= 3:
                    error_embed = discord.Embed(
                        title="❌ Максимальный уровень!",
                        description=f"Авто-жир уже на максимальном (3) уровне!",
                        color=0xff0000
                    )
                    temp_msg = await ctx.send(embed=error_embed)
                    await asyncio.sleep(2)
                    await temp_msg.delete()
                    continue
                cost = get_upgrade_cost("auto_fat", current_level)
            
            if current_data['current_number'] < cost:
                error_embed = discord.Embed(
                    title="❌ Недостаточно кг!",
                    description=f"Для улучшения нужно **{cost} кг**, у вас: **{current_data['current_number']} кг**",
                    color=0xff0000
                )
                temp_msg = await ctx.send(embed=error_embed)
                await asyncio.sleep(2)
                await temp_msg.delete()
                continue
            
            if upgrade_type == "prestige":
                confirm_embed = discord.Embed(
                    title="⚠️ **ПРЕСТИЖ** ⚠️",
                    description=f"{member.mention}, вы уверены, что хотите получить престиж?\n\n"
                               f"**Это действие НЕОБРАТИМО!**\n"
                               f"• Вес сбросится до 0\n"
                               f"• Все предметы и кейсы исчезнут\n"
                               f"• Все улучшения обнулятся\n"
                               f"• Останется только +1 уровень престижа\n\n"
                               f"Напишите `да` в течение 15 секунд для подтверждения.",
                    color=0xff5500
                )
                await ctx.send(embed=confirm_embed)
                
                def confirm_check(m):
                    return m.author == ctx.author and m.content.lower() == "да"
                
                try:
                    await bot.wait_for('message', timeout=15.0, check=confirm_check)
                except asyncio.TimeoutError:
                    await ctx.send("❌ Престиж отменён.")
                    continue
                
                new_prestige = current_level + 1
                update_user_data(
                    guild_id, user_id,
                    current_number=0,
                    item_counts='{}',
                    cases_dict={},
                    fat_cd_upgrade=0,
                    case_cd_upgrade=0,
                    luck_upgrade=0,
                    income_upgrade=0,
                    prestige=new_prestige,
                    auto_fat_level=0,
                    next_auto_fat_time=None,
                    user_xp=0,
                    user_level=0,
                    consecutive_plus=0,
                    consecutive_minus=0,
                    jackpot_pity=0,
                    shadow_upgrade_chance=0
                )
                
                try:
                    new_nick = format_nick_with_prestige(new_prestige, 0, user_name)
                    if len(new_nick) > 32:
                        new_nick = new_nick[:32]
                    await member.edit(nick=new_nick)
                except:
                    pass
                
                new_data = get_user_data(guild_id, user_id, user_name)
                new_embed = create_upgrade_embed(new_data)
                await msg.edit(embed=new_embed)
                
                success_embed = discord.Embed(
                    title="🌟 **ПРЕСТИЖ ПОЛУЧЕН!** 🌟",
                    description=f"{member.mention} достиг **{new_prestige}** уровня престижа!\n\n"
                               f"Вес сброшен до 0\n"
                               f"Все предметы и улучшения обнулены\n"
                               f"Теперь вы получаете +{new_prestige * 10}% ко всему и +{new_prestige}% к шансам!",
                    color=0xffd700
                )
                temp_msg = await ctx.send(embed=success_embed)
                await asyncio.sleep(3)
                await temp_msg.delete()
                continue
            
            new_level = current_level + 1
            new_number = current_data['current_number'] - cost
            
            if upgrade_type == "auto_fat":
                interval = get_auto_fat_interval(new_level)
                next_time = datetime.now() + timedelta(hours=interval) if interval else None
                update_user_data(guild_id, user_id, 
                                number=new_number,
                                auto_fat_level=new_level,
                                next_auto_fat_time=next_time)
            else:
                update_field = {
                    "fat_cd": "fat_cd_upgrade",
                    "case_cd": "case_cd_upgrade",
                    "luck": "luck_upgrade",
                    "income": "income_upgrade"
                }[upgrade_type]
                
                update_user_data(guild_id, user_id, 
                                number=new_number,
                                **{update_field: new_level})
            
            try:
                new_nick = format_nick_with_prestige(current_data.get('prestige', 0), new_number, user_name)
                if len(new_nick) > 32:
                    new_nick = new_nick[:32]
                await member.edit(nick=new_nick)
            except:
                pass
            
            new_data = get_user_data(guild_id, user_id, user_name)
            new_embed = create_upgrade_embed(new_data)
            await msg.edit(embed=new_embed)
            
            if upgrade_type == "fat_cd":
                new_bonus = get_fat_cd_reduction(new_level)
                bonus_text = f"КД !жир уменьшен на {new_bonus} мин"
            elif upgrade_type == "case_cd":
                new_bonus = get_case_cd_reduction(new_level)
                bonus_text = f"КД кейса уменьшен на {new_bonus} мин"
            elif upgrade_type == "luck":
                bonus_text = f"Удача увеличена до +{new_level * 0.25:.2f}% к редким предметам и +{new_level * 0.5:.2f}% к апгрейдам"
            elif upgrade_type == "income":
                new_bonus = get_income_bonus(new_level)
                bonus_text = f"Прибавка увеличена до +{(new_bonus-1)*100:.0f}% к доходу от предметов"
            elif upgrade_type == "auto_fat":
                interval = get_auto_fat_interval(new_level)
                bonus_text = f"Авто-жир будет срабатывать каждые {interval} час(ов)"
            
            success_embed = discord.Embed(
                title="✅ **УЛУЧШЕНИЕ ПОЛУЧЕНО!** ✅",
                description=f"{member.mention} улучшил характеристику!\n\n"
                           f"**Потрачено:** {cost} кг\n"
                           f"**Осталось:** {new_number} кг\n\n"
                           f"**{bonus_text}**",
                color=0x00ff00
            )
            temp_msg = await ctx.send(embed=success_embed)
            await asyncio.sleep(2)
            await temp_msg.delete()
            
            try:
                await msg.remove_reaction(reaction, user)
            except:
                pass
            
        except asyncio.TimeoutError:
            timeout_embed = discord.Embed(
                title="⏰ Режим ожидания",
                description="Нажмите на любую реакцию снова, чтобы продолжить улучшения!",
                color=0xffaa00
            )
            await msg.edit(embed=timeout_embed)
            
            try:
                reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)
                fresh_data = get_user_data(guild_id, user_id, user_name)
                fresh_embed = create_upgrade_embed(fresh_data)
                await msg.edit(embed=fresh_embed)
                continue
            except asyncio.TimeoutError:
                try:
                    await msg.clear_reactions()
                except:
                    pass
                final_embed = create_upgrade_embed(get_user_data(guild_id, user_id, user_name))
                final_embed.set_footer(text="💤 Режим ожидания активирован. Используйте !апгрейдюзер заново для продолжения.")
                await msg.edit(embed=final_embed)
                break

# ===== ОСНОВНЫЕ КОМАНДЫ =====

@bot.command(name='жир')
async def fat_command(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    data = get_user_data(guild_id, user_id, user_name)
    
    fat_cd_upgrade = data.get('fat_cd_upgrade', 0)
    cd_reduction_minutes = get_fat_cd_reduction(fat_cd_upgrade)
    actual_cooldown = max(0.1, COOLDOWN_HOURS * 60 - cd_reduction_minutes) / 60
    
    items_dict = get_user_items(data['item_counts'])
    for item_name, count in items_dict.items():
        if item_name == "Яблоко":
            actual_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотое Яблоко":
            actual_cooldown *= (1 - count * 0.10)
    
    actual_cooldown = max(0.1, actual_cooldown)
    
    can_use, remaining = check_cooldown(data['fat_cooldown_time'], actual_cooldown)
    
    if not can_use:
        embed = discord.Embed(title="⏳ Подождите!", 
                             description=f"{member.mention}, вы уже использовали команду недавно!", 
                             color=0xff0000)
        embed.add_field(name="Осталось подождать", value=format_time(remaining), inline=True)
        embed.add_field(name="Кулдаун", value=f"{actual_cooldown*60:.0f} мин", inline=True)
        await ctx.send(embed=embed)
        return
    
    prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
    
    change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
        data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
        data.get('luck_upgrade', 0), prestige_bonus, items_dict, data['current_number'])
    
    temp_number = data['current_number'] + change
    
    update_user_data(guild_id, user_id, number=temp_number)
    
    levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, 30)
    
    final_data = get_user_data(guild_id, user_id, user_name)
    final_number = final_data['current_number']
    
    update_user_data(guild_id, user_id,
                    user_name=user_name,
                    consecutive_plus=new_consecutive_plus,
                    consecutive_minus=new_consecutive_minus,
                    jackpot_pity=new_jackpot_pity,
                    fat_cooldown_time=datetime.now())
    
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
        
        new_nick = format_nick_with_prestige(data.get('prestige', 0), final_number, clean_name)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        
        await member.edit(nick=new_nick)
    except:
        pass
    
    rank_name, rank_emoji = get_rank(final_number)
    
    if was_jackpot:
        embed_color = 0xffd700
        embed_title = "💰 ДЖЕКПОТ! 💰"
    else:
        embed_color = 0xff9933 if final_number >= 0 else 0x66ccff
        embed_title = "🍔 Набор массы"
    
    embed = discord.Embed(title=embed_title, 
                         description=f"**{member.mention}** теперь весит **{abs(final_number)}kg** на сервере **{ctx.guild.name}**!", 
                         color=embed_color)
    
    if was_jackpot:
        embed.add_field(name="💰 ДЖЕКПОТ!", value=f"+{change} кг", inline=True)
    elif change > 0:
        embed.add_field(name="📈 Изменение", value=f"+{change} кг", inline=True)
    elif change < 0:
        embed.add_field(name="📉 Изменение", value=f"{change} кг", inline=True)
    else:
        embed.add_field(name="⚖️ Изменение", value="0 кг", inline=True)
    
    embed.add_field(name="🍖 Текущий вес", value=f"{final_number}kg", inline=True)
    embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
    
    if levels_gained > 0:
        embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", 
                       value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", 
                       inline=False)
    
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
                        time_since = datetime.now() - old_msg.created_at.replace(tzinfo=None)
                        if time_since.total_seconds() < 120:
                            embed = discord.Embed(
                                title="⚠️ Кейс уже открыт!",
                                description=f"{member.mention}, у вас уже есть активный кейс!\n"
                                           f"Сначала завершите или дождитесь таймаута предыдущего.",
                                color=0xffaa00
                            )
                            await ctx.send(embed=embed)
                            return
                except:
                    pass
        except Exception as e:
            print(f"Ошибка при проверке активного кейса: {e}")
    
    items_dict = get_user_items(data['item_counts'])
    
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    cd_reduction_minutes = get_case_cd_reduction(case_cd_upgrade)
    actual_case_cooldown = max(1, CASE_COOLDOWN_HOURS * 60 - cd_reduction_minutes) / 60
    
    for item_name, count in items_dict.items():
        if item_name == "Апельсин":
            actual_case_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотой Апельсин":
            actual_case_cooldown *= (1 - count * 0.10)
    
    actual_case_cooldown = max(1, int(actual_case_cooldown))
    
    can_get_daily, daily_remaining = can_get_daily_case(guild_id, user_id, actual_case_cooldown)
    
    cases_dict = data.get('cases_dict', {}).copy()
    case_to_open = None
    case = None
    
    if can_get_daily:
        case_to_open = "daily"
        case = CASES["daily"]
    else:
        for case_id, count in cases_dict.items():
            if count > 0:
                case_to_open = case_id
                case = CASES[case_id]
                break
    
    if not case_to_open:
        time_str = format_time(daily_remaining) if daily_remaining > 0 else "скоро"
        embed = discord.Embed(
            title="📭 Нет кейсов!",
            description=f"{member.mention}, у вас нет кейсов для открытия!\n\n"
                       f"**Ежедневный кейс** будет доступен через: {time_str}\n\n"
                       f"Купить кейсы можно в магазине (`!магазин`)",
            color=0xff0000
        )
        await ctx.send(embed=embed)
        return
    
    update_user_data(
        guild_id, user_id,
        active_case_message_id=None,
        active_case_channel_id=None,
        last_case_type=case_to_open
    )
    
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
    
    case_embed = discord.Embed(
        title=f"{case_emoji} **{case['name']}** {case_emoji}",
        description=(
            f"{member.mention}, у вас есть кейс!\n\n"
            f"**Нажмите на 🖱️ чтобы открыть**\n"
            f"**Нажмите на ❌ чтобы отменить**\n\n"
            f"┌───────────────┐\n"
            f"│----{case_emoji}---{case_emoji}---{case_emoji}----│\n"
            f"│----К-Е-Й-С-------│\n"
            f"│----{case['name'][:10]}--│\n"
            f"│----{case_emoji}---{case_emoji}---{case_emoji}----│\n"
            f"└───────────────┘"
        ),
        color=0xffaa00
    )
    case_embed.set_footer(text="У вас 30 секунд чтобы открыть кейс!")
    
    case_msg = await ctx.send(embed=case_embed)
    await case_msg.add_reaction("🖱️")
    await case_msg.add_reaction("❌")
    
    update_user_data(
        guild_id, user_id,
        active_case_message_id=str(case_msg.id),
        active_case_channel_id=str(ctx.channel.id)
    )
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["🖱️", "❌"] and reaction.message.id == case_msg.id
    
    try:
        reaction, user = await bot.wait_for('reaction_add', timeout=30.0, check=check)
        
        if str(reaction.emoji) == "❌":
            update_user_data(
                guild_id, user_id,
                active_case_message_id=None,
                active_case_channel_id=None,
                last_case_type=None
            )
            
            try:
                await case_msg.clear_reactions()
            except:
                pass
            
            cancel_embed = discord.Embed(
                title="❌ Отмена",
                description=f"{member.mention}, вы отменили открытие кейса. Кейс сохранён в инвентаре!",
                color=0xffaa00
            )
            await case_msg.edit(embed=cancel_embed)
            return
        
        if case_to_open == "daily":
            can_get_daily, _ = can_get_daily_case(guild_id, user_id, actual_case_cooldown)
            if not can_get_daily:
                await ctx.send(f"{member.mention}, ежедневный кейс уже использован!")
                await case_msg.delete()
                update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None)
                return
            
            update_daily_case_time(guild_id, user_id)
        else:
            current_data = get_user_data(guild_id, user_id, user_name)
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
        
        prestige_luck = get_prestige_luck(data.get('prestige', 0))
        luck_upgrade = data.get('luck_upgrade', 0)
        prize = open_case(case_to_open, prestige_luck, luck_upgrade)
        
        update_user_data(
            guild_id, user_id,
            active_case_message_id=None,
            active_case_channel_id=None,
            last_case_type=None,
            last_case_prize=None
        )
        
        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, 100)
        
        line = []
        for i in range(100):
            line.append(random.choice(prize_emojis))
        
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
        
        line[56] = prize_emoji
        
        anim_embed = discord.Embed(
            title=f"🎰 **{case['name']}** 🎰",
            description="",
            color=0xffaa00
        )
        
        animation_frames = [
            (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
            (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
            (11, 50), (12, 52), (13, 54), (14, 55), (15, 55),
            (16, 55), (17, 56), (18, 56)
        ]
        
        for frame_num, center_pos in animation_frames:
            visible = line[center_pos-4:center_pos+5]
            display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
            anim_embed.description = f"**{display_line}**"
            
            await case_msg.edit(embed=anim_embed)
            await asyncio.sleep(0.5)
        
        visible = line[52:61]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        anim_embed.description = f"**{display_line}**\n\n**РЕЗУЛЬТАТ!**"
        
        await case_msg.edit(embed=anim_embed)
        await asyncio.sleep(1)
        
        items_dict = get_user_items(data['item_counts'])
        new_number = data['current_number']
        prize_value = prize["value"]
        
        prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
        has_water = items_dict.get("Стакан воды", 0) > 0
        
        if prize_value == "rotten_leg":
            items_dict["Гнилая ножка KFC"] = items_dict.get("Гнилая ножка KFC", 0) + 1
            result_display = f"💀 **Гнилая ножка KFC!** 💀"
            result_color = 0x993366
            
        elif prize_value == "water":
            items_dict["Стакан воды"] = items_dict.get("Стакан воды", 0) + 1
            result_display = f"💧 **Стакан воды!** 💧"
            result_color = 0x66ccff
            
        elif isinstance(prize_value, str):
            items_dict[prize_value] = items_dict.get(prize_value, 0) + 1
            result_display = f"🎁 **{prize_value}** {prize_emoji}"
            result_color = 0x9b59b6
            
        else:
            if has_water and case_to_open != "daily":
                prize_value = prize_value // 3
            prize_value = int(prize_value * prestige_bonus)
            new_number = data['current_number'] + prize_value
            result_display = f"🎉 **{prize_value:+d} кг** {prize_emoji}"
            result_color = 0xffaa00
        
        update_data = {
            'number': new_number,
            'user_name': user_name,
            'item_counts': save_user_items(items_dict)
        }
        
        update_user_data(guild_id, user_id, **update_data)
        
        if isinstance(prize_value, int) and prize_value != 0:
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
                
                new_nick = format_nick_with_prestige(data.get('prestige', 0), new_number, clean_name)
                if len(new_nick) > 32:
                    new_nick = new_nick[:32]
                
                await member.edit(nick=new_nick)
            except:
                pass
        
        rank_name, rank_emoji = get_rank(new_number)
        
        if prize_value == "rotten_leg":
            final_embed = discord.Embed(
                title=f"{case['emoji']} Открытие {case['name']}",
                description=f"**{member.mention}** открыл кейс и получил:",
                color=result_color
            )
            final_embed.add_field(name="🎁 Приз", value=result_display, inline=False)
            
        elif prize_value == "water":
            final_embed = discord.Embed(
                title=f"{case['emoji']} Открытие {case['name']}",
                description=f"**{member.mention}** открыл кейс и получил:",
                color=result_color
            )
            final_embed.add_field(name="🎁 Приз", value=result_display, inline=False)
            
        elif isinstance(prize_value, str):
            final_embed = discord.Embed(
                title=f"{case['emoji']} Открытие {case['name']}",
                description=f"**{member.mention}** открыл кейс и получил предмет!",
                color=result_color
            )
            final_embed.add_field(name="🎁 Приз", value=result_display, inline=False)
            
        else:
            final_embed = discord.Embed(
                title=f"{case['emoji']} Открытие {case['name']}",
                description=f"**{member.mention}** открыл кейс и получил:",
                color=result_color
            )
            final_embed.add_field(name="🎁 Приз", value=result_display, inline=True)
            final_embed.add_field(name="🍖 Новый вес", value=f"{new_number}kg", inline=True)
            final_embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
        
        if levels_gained > 0:
            final_embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", 
                                 value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", 
                                 inline=False)
        
        if case_to_open != "daily":
            current_data = get_user_data(guild_id, user_id)
            remaining = current_data.get('cases_dict', {}).get(case_to_open, 0)
            if remaining > 0:
                final_embed.add_field(
                    name="📦 Осталось кейсов",
                    value=f"{case['emoji']} {case['name']}: {remaining} шт",
                    inline=False
                )
        else:
            final_embed.add_field(
                name="⏰ Следующий ежедневный кейс",
                value=f"через {actual_case_cooldown} часов",
                inline=False
            )
        
        final_embed.set_footer(text=f"{case['emoji']} Удачи в следующий раз!")
        
        await ctx.send(embed=final_embed)
        
    except asyncio.TimeoutError:
        update_user_data(
            guild_id, user_id,
            active_case_message_id=None,
            active_case_channel_id=None,
            last_case_type=None
        )
        
        try:
            await case_msg.clear_reactions()
        except:
            pass
        
        timeout_embed = discord.Embed(
            title="⏰ Время вышло",
            description=f"{member.mention}, вы не открыли кейс вовремя. Кейс сохранён в инвентаре!",
            color=0xff0000
        )
        await case_msg.edit(embed=timeout_embed)

      if len(giver_items) > 5:
        giver_inv += f"\n... и ещё {len(giver_items) - 5} предметов"
    embed.add_field(name="📤 Ваш инвентарь", value=giver_inv or "Пусто", inline=True)
    
    target_inv = "\n".join([f"• {item}: {count} шт" for item, count in list(target_items.items())[:5]])
    if len(target_items) > 5:
        target_inv += f"\n... и ещё {len(target_items) - 5} предметов"
    embed.add_field(name="📥 Инвентарь получателя", value=target_inv or "Пусто", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='магазин')
async def shop_command(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    
    data = get_user_data(guild_id, str(member.id), member.name)
    
    update_user_data(
        guild_id, str(member.id),
        last_command="shop",
        last_command_use_time=datetime.now()
    )
    
    slots, last_update, next_update = await ensure_shop_updated(guild_id)
    
    if not isinstance(slots, list):
        slots = []
    
    embed = discord.Embed(
        title="🏪 **МАГАЗИН** 🏪",
        description="Доступные предметы (используйте `!купить [слот] [количество]`):\n📦 **Слоты 1-4:** Кейсы | 🛒 **Слоты 5-10:** Предметы",
        color=0xffaa00
    )
    
    items_text = ""
    for i in range(1, SHOP_SLOTS + 1):
        slot = slots[i-1] if i-1 < len(slots) else None
        
        if slot is not None and isinstance(slot, dict):
            if slot["type"] == "case":
                prefix = "📦" if i <= 4 else "🎲"
                items_text += f"**{i}.** {prefix} {slot.get('emoji', '📦')} {slot.get('name', 'Неизвестный кейс')} — {slot.get('amount', 0)} шт — **{slot.get('price', 0)} кг/шт**\n"
                items_text += f"   └ {slot.get('description', 'Нет описания')}\n"
            else:
                prefix = "🛒" if i > 4 else "🎁"
                items_text += f"**{i}.** {prefix} {slot.get('name', 'Неизвестный предмет')} — {slot.get('amount', 0)} шт — **{slot.get('price', 0)} кг/шт**\n"
                items_text += f"   └ {slot.get('description', 'Нет описания')}\n"
        else:
            if i <= 4:
                items_text += f"**{i}.** 📦🕳️ Пустой слот для кейса\n"
            else:
                items_text += f"**{i}.** 🛒🕳️ Пустой слот для предмета\n"
    
    embed.add_field(name="📦 Товары", value=items_text, inline=False)
    
    last_update_str = last_update.strftime("%d.%m.%Y %H:%M") if last_update else "Никогда"
    next_update_str = next_update.strftime("%d.%m.%Y %H:%M") if next_update else "Скоро"
    
    case_count = sum(1 for s in slots[:4] if s is not None and isinstance(s, dict))
    item_count = sum(1 for s in slots[4:] if s is not None and isinstance(s, dict))
    
    embed.add_field(name="📊 Статистика магазина", 
                   value=f"📦 Кейсов в наличии: {case_count}/4\n🛒 Предметов в наличии: {item_count}/6\n⏰ Обновление каждые {SHOP_UPDATE_HOURS} часов", 
                   inline=False)
    
    embed.add_field(name="⏰ Время обновления", 
                   value=f"Последнее: {last_update_str}\nСледующее: {next_update_str}", 
                   inline=False)
    
    embed.set_footer(text="💸 Тратьте кг с умом! | 📦 - Кейсы | 🛒 - Предметы")
    
    await ctx.send(embed=embed)

@bot.command(name='купить')
async def buy_command(ctx, slot: int, amount: int = 1):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    
    if slot < 1 or slot > SHOP_SLOTS:
        await ctx.send(f"❌ Слот должен быть от 1 до {SHOP_SLOTS}!")
        return
    
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    
    data = get_user_data(guild_id, user_id, member.name)
    
    last_command_use_time = data.get('last_command_use_time')
    if last_command_use_time and isinstance(last_command_use_time, str):
        try:
            last_command_use_time = datetime.fromisoformat(last_command_use_time)
        except:
            last_command_use_time = None
    
    if data.get('last_command') != "shop" or not last_command_use_time:
        await ctx.send("❌ Сначала используйте `!магазин` для просмотра доступных товаров!")
        return
    
    time_since_shop = datetime.now() - last_command_use_time
    if time_since_shop.total_seconds() > 300:
        await ctx.send("❌ Время ожидания истекло. Используйте `!магазин` заново!")
        update_user_data(guild_id, user_id, last_command=None, last_command_use_time=None)
        return
    
    slots, last_update, next_update = await ensure_shop_updated(guild_id)
    
    if slot - 1 >= len(slots) or slots[slot - 1] is None:
        await ctx.send(f"❌ В слоте {slot} ничего нет!")
        return
    
    item = slots[slot - 1]
    
    if not isinstance(item, dict):
        await ctx.send(f"❌ Ошибка в данных слота {slot}!")
        return
    
    if "amount" not in item or "price" not in item:
        await ctx.send(f"❌ Ошибка в данных слота {slot}!")
        return
    
    if amount > item["amount"]:
        await ctx.send(f"❌ В наличии только {item['amount']} шт!")
        return
    
    total_price = item["price"] * amount
    if data['current_number'] < total_price:
        await ctx.send(f"❌ Недостаточно кг! Нужно: {total_price} кг, у вас: {data['current_number']} кг")
        return
    
    new_number = data['current_number'] - total_price
    item["amount"] -= amount
    
    cases_dict = data.get('cases_dict', {}).copy()
    
    if item.get("type") == "case" or "case_id" in item:
        case_id = item.get("case_id")
        if not case_id:
            await ctx.send(f"❌ Ошибка: не удалось определить тип кейса!")
            return
        
        if case_id not in CASES:
            await ctx.send(f"❌ Ошибка: неизвестный тип кейса {case_id}!")
            return
        
        cases_dict[case_id] = cases_dict.get(case_id, 0) + amount
        purchase_desc = f"{item.get('emoji', '📦')} {item.get('name', 'Кейс')} x{amount}"
        
        update_user_data(
            guild_id, user_id,
            number=new_number,
            cases_dict=cases_dict,
            last_command=None,
            last_command_use_time=None
        )
    else:
        items_dict = get_user_items(data['item_counts'])
        items_dict[item["name"]] = items_dict.get(item["name"], 0) + amount
        purchase_desc = f"{item['name']} x{amount}"
        
        update_user_data(
            guild_id, user_id,
            number=new_number,
            item_counts=save_user_items(items_dict),
            last_command=None,
            last_command_use_time=None
        )
    
    update_shop_data(guild_id, slots, last_update, next_update)
    
    levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, 20)
    
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
        
        new_nick = format_nick_with_prestige(data.get('prestige', 0), new_number, clean_name)
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
    
    embed.add_field(name="📦 Предмет", value=purchase_desc, inline=True)
    embed.add_field(name="💰 Цена", value=f"{total_price} кг", inline=True)
    embed.add_field(name="💸 Осталось", value=f"{new_number} кг", inline=True)
    
    if levels_gained > 0:
        embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", 
                       value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", 
                       inline=False)
    
    await ctx.send(embed=embed)

# ===== АДМИНСКИЕ КОМАНДЫ =====

@bot.command(name='жир_сброс')
async def fat_reset(ctx, member: discord.Member = None):
    if not ctx.author.guild_permissions.administrator and ctx.author != ctx.guild.owner:
        await ctx.send("❌ Эта команда только для администраторов!")
        return
    
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    data = get_user_data(guild_id, user_id, target.name)
    
    update_data = {
        'number': 0,
        'consecutive_plus': 0,
        'consecutive_minus': 0,
        'jackpot_pity': 0,
        'item_counts': '{}'
    }
    
    update_user_data(guild_id, user_id, **update_data)
    
    try:
        new_nick = f"0kg {target.name}"
        await target.edit(nick=new_nick)
        await ctx.send(f"✅ Вес {target.mention} сброшен на 0kg")
    except:
        await ctx.send(f"✅ Вес {target.mention} сброшен на 0kg (ник не изменён)")

@bot.command(name='сброскд')
async def reset_cooldowns(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    
    is_high_tester = has_high_tester_role(member)
    is_regular_tester = has_tester_role(member)
    
    if not is_regular_tester and not is_high_tester:
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}** или **{HIGH_TESTER_ROLE_NAME}**")
        return
    
    if is_high_tester:
        db_path = get_db_path(guild_id)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE user_fat SET fat_cooldown_time = NULL')
        fat_affected = cursor.rowcount
        cursor.execute('UPDATE user_fat SET last_case_time = NULL')
        case_affected = cursor.rowcount
        conn.commit()
        conn.close()
        
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
        db_path = get_db_path(guild_id)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE user_fat SET fat_cooldown_time = NULL')
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🔄 Кулдаун сброшен",
            description=f"**{ctx.author.name}** сбросил кулдаун !жир для всех!",
            color=0x00ff00
        )
        embed.add_field(name="Затронуто пользователей", value=str(affected), inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='сбросвсех')
async def reset_all_users_weight(ctx):
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
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''UPDATE user_fat SET 
        current_number = 0, 
        consecutive_plus = 0, 
        consecutive_minus = 0, 
        jackpot_pity = 0,
        item_counts = "{}"''')
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    
    embed = discord.Embed(
        title="⚖️ Глобальный сброс",
        description=f"**{ctx.author.name}** обнулил всех!",
        color=0xff5500
    )
    embed.add_field(name="Затронуто пользователей", value=str(affected), inline=True)
    await ctx.send(embed=embed)

@bot.command(name='выдатьпредмет')
async def give_shop_item(ctx, amount: int, *, item_name: str):
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
    
    data = get_user_data(guild_id, user_id, user_name)
    item_name = item_name.strip()
    
    for case_id, case in CASES.items():
        if case_id != "daily" and case["name"].lower() == item_name.lower():
            cases_dict = data.get('cases_dict', {}).copy()
            cases_dict[case_id] = cases_dict.get(case_id, 0) + amount
            
            update_user_data(
                guild_id, user_id,
                cases_dict=cases_dict
            )
            
            embed = discord.Embed(
                title="🎁 Выдача кейса",
                description=f"**{member.mention}** выдал себе кейс!",
                color=0xffaa00
            )
            embed.add_field(name="📦 Кейс", value=f"**{case['name']}** x{amount}", inline=True)
            embed.add_field(name="📊 Всего кейсов", value=f"{cases_dict.get(case_id, 0)} шт", inline=True)
            await ctx.send(embed=embed)
            return
    
    found_item = None
    for shop_item in SHOP_ITEMS:
        if shop_item["name"].lower() == item_name.lower():
            found_item = shop_item
            break
    
    if not found_item:
        items_list = "\n".join([f"• {item['name']}" for item in SHOP_ITEMS[:10]])
        if len(SHOP_ITEMS) > 10:
            items_list += f"\n... и ещё {len(SHOP_ITEMS) - 10} предметов"
        await ctx.send(f"❌ Предмет '{item_name}' не найден в магазине!\n\n📦 **Доступные предметы:**\n{items_list}")
        return
    
    items_dict = get_user_items(data['item_counts'])
    items_dict[found_item["name"]] = items_dict.get(found_item["name"], 0) + amount
    
    update_user_data(
        guild_id, user_id,
        item_counts=save_user_items(items_dict)
    )
    
    embed = discord.Embed(
        title="🎁 Выдача предмета",
        description=f"**{member.mention}** выдал себе предмет!",
        color=0xffaa00
    )
    
    embed.add_field(name="📦 Предмет", value=f"**{found_item['name']}** x{amount}", inline=True)
    embed.add_field(name="📝 Описание", value=found_item['description'], inline=False)
    
    items_list = "\n".join([f"• {item}: {count} шт" for item, count in list(items_dict.items())[:8]])
    if len(items_dict) > 8:
        items_list += f"\n... и ещё {len(items_dict) - 8} предметов"
    
    embed.add_field(name="📊 Ваш инвентарь", value=items_list or "Пусто", inline=False)
    embed.set_footer(text="✨ Только для высших тестеров!")
    
    await ctx.send(embed=embed)

@bot.command(name='жир_кулдаун')
async def cooldown_info(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    
    data = get_user_data(guild_id, user_id, member.name)
    
    fat_cd_upgrade = data.get('fat_cd_upgrade', 0)
    cd_reduction_minutes = get_fat_cd_reduction(fat_cd_upgrade)
    actual_fat_cooldown = max(0.1, COOLDOWN_HOURS * 60 - cd_reduction_minutes) / 60
    
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    cd_reduction_minutes_case = get_case_cd_reduction(case_cd_upgrade)
    actual_case_cooldown = max(1, CASE_COOLDOWN_HOURS * 60 - cd_reduction_minutes_case) / 60
    
    items_dict = get_user_items(data['item_counts'])
    for item_name, count in items_dict.items():
        if item_name == "Яблоко":
            actual_fat_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотое Яблоко":
            actual_fat_cooldown *= (1 - count * 0.10)
        elif item_name == "Апельсин":
            actual_case_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотой Апельсин":
            actual_case_cooldown *= (1 - count * 0.10)
    
    actual_fat_cooldown = max(0.1, actual_fat_cooldown)
    actual_case_cooldown = max(0.1, actual_case_cooldown)
    
    fat_can_use, fat_remaining = check_cooldown(data['fat_cooldown_time'], actual_fat_cooldown)
    case_can_use, case_remaining = check_cooldown(data['last_case_time'], actual_case_cooldown)
    
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
    
    embed.add_field(name="!жиркейс", value=f"Кулдаун: {actual_case_cooldown:.1f} ч\nСтатус: {case_status}", inline=True)
    
    embed.add_field(name="Текущий вес", value=f"{data['current_number']}kg", inline=True)
    
    await ctx.send(embed=embed)

# ===== ЗАПУСК БОТА =====
@bot.event
async def on_ready():
    print(f"\n✅ Бот успешно запущен как {bot.user}")
    await migrate_old_burgers_to_prestige()
    await migrate_old_autoburgers_to_auto_fat()
    bot.loop.create_task(auto_fat_loop())
    bot.loop.create_task(passive_income_loop())
    bot.loop.create_task(snatcher_loop())
    bot.loop.create_task(apply_hourly_effects())

if __name__ == "__main__":
    print("🚀 Запуск бота...")
    bot.run(TOKEN)
