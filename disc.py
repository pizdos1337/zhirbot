import discord
from discord.ext import commands
import random
import sqlite3
import os
from datetime import datetime, timedelta
import asyncio
import shutil
import glob
import json

# ============================================
# ========== НАСТРОЙКИ БОТА ==========
# ============================================

# ----- ОСНОВНЫЕ НАСТРОЙКИ -----
TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
PREFIX = "!"
DB_FOLDER = "./data/guild_databases"
COOLDOWN_HOURS = 1
CASE_COOLDOWN_HOURS = 24

# ----- РОЛИ ДЛЯ ТЕСТЕРОВ -----
TESTER_ROLE_NAME = "тестер"
HIGH_TESTER_ROLE_NAME = "Высший тестер"

# ===== НАСТРОЙКИ ВЕРОЯТНОСТЕЙ !жир =====
BASE_MINUS_CHANCE = 0.2
MAX_MINUS_CHANCE = 0.6
PITY_INCREMENT = 0.1
CONSECUTIVE_MINUS_BOOST = 0.2
MAX_CONSECUTIVE_MINUS_BOOST = 0.8

# ===== НАСТРОЙКИ ДЖЕКПОТА =====
BASE_JACKPOT_CHANCE = 0.001
JACKPOT_PITY_INCREMENT = 0.001
MAX_JACKPOT_CHANCE = 0.05
JACKPOT_MIN = 500
JACKPOT_MAX = 1000

# ===== НАСТРОЙКИ АВТО-ЖИРА =====
AUTO_FAT_INTERVALS = {1: 6, 2: 3, 3: 1, 4: 0.5, 5: 0.25, 6: 0.1}
AUTO_FAT_BASE_COST = 500
AUTO_FAT_COST_INCREMENT = 500
AUTO_FAT_MAX_LEVEL = 6

# ===== НАСТРОЙКИ ПРЕСТИЖА =====
PRESTIGE_BONUS_PER_LEVEL = 0.10
PRESTIGE_LUCK_PER_LEVEL = 0.01
PRESTIGE_XP_BONUS_PER_LEVEL = 0.5
PRESTIGE_BASE_COST = 2000
PRESTIGE_COST_INCREMENT = 1000

# ===== НАСТРОЙКИ ПРИБАВКИ =====
INCOME_BONUS_PER_LEVEL = 0.05
INCOME_BASE_COST = 250
INCOME_COST_INCREMENT = 100

# ===== НАСТРОЙКИ УДАЧИ =====
LUCK_CASE_BONUS_PER_LEVEL = 0.25
LUCK_UPGRADE_BONUS_PER_LEVEL = 0.5
LUCK_BASE_COST = 1000
LUCK_COST_INCREMENT = 500

# ===== НАСТРОЙКИ КД !жир =====
FAT_CD_REDUCTION_PER_LEVEL = 5
FAT_CD_BASE_COST = 150
FAT_CD_COST_INCREMENT = 50

# ===== НАСТРОЙКИ КД кейса =====
CASE_CD_REDUCTION_PER_LEVEL = 60
CASE_CD_BASE_COST = 100
CASE_CD_COST_INCREMENT = 100

# ===== НАСТРОЙКИ МАГАЗИНА =====
SHOP_SLOTS = 10
SHOP_UPDATE_HOURS = 12

# ===== НАСТРОЙКИ ОПЫТА =====
XP_PER_FAT = 30
XP_PER_UPGRADE = 50
XP_PER_UPGRADE_KG = 40
XP_PER_CASE = 100
XP_PER_DUEL_WIN = 100
XP_PER_SHOP_BUY = 20
LEVEL_UP_REWARD_PER_LEVEL = 15

# ============================================
# ========== КОНЕЦ НАСТРОЕК ==========
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

total_chance = sum(prize["chance"] for prize in CASE_PRIZES)
for prize in CASE_PRIZES:
    prize["normalized_chance"] = (prize["chance"] / total_chance) * 100

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
    {"name": "Арчжирмезис", "min": 5000, "max": 10000, "emoji": "♛"},
    {"name": "ЖИРНАЯ ТОЛСТАЯ ОГРОМНАЯ СВИНЬЯ", "min": 10001, "max": 99999999, "emoji": "🐖"},
]

print("="*60)
print("🍔 ЖИРНЫЙ БОТ - ЗАПУСК")
print("="*60)

if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найдена переменная окружения DISCORD_BOT_TOKEN!")
    exit(1)

def get_user_items(item_counts_str):
    try:
        return json.loads(item_counts_str) if item_counts_str and item_counts_str != '{}' else {}
    except:
        return {}

def save_user_items(items_dict):
    return json.dumps(items_dict)

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
    prestige = data.get('prestige', 0)
    xp_bonus = 1 + (prestige * 0.5)
    xp_amount = int(xp_amount * xp_bonus)
    new_total_xp = data.get('user_xp', 0) + xp_amount
    new_level, current_xp = get_level_and_xp(new_total_xp)
    total_kg_reward = 0
    for level in range(old_level + 1, new_level + 1):
        total_kg_reward += LEVEL_UP_REWARD_PER_LEVEL * level
    new_weight = data['current_number'] + total_kg_reward
    update_user_data(guild_id, user_id, user_xp=new_total_xp, user_level=new_level, number=new_weight)
    return new_level - old_level, total_kg_reward, new_level

def format_nick_with_prestige(prestige, weight, user_name):
    if prestige > 0:
        return f"{prestige}🌟 {weight}kg {user_name}"
    return f"{weight}kg {user_name}"

def get_prestige_bonus(prestige):
    return 1 + (prestige * PRESTIGE_BONUS_PER_LEVEL)

def get_prestige_luck(prestige):
    return prestige * PRESTIGE_LUCK_PER_LEVEL

def get_income_bonus(income_upgrade):
    return 1 + (income_upgrade * INCOME_BONUS_PER_LEVEL)

def get_fat_cd_reduction(upgrade_count):
    return upgrade_count * FAT_CD_REDUCTION_PER_LEVEL

def get_case_cd_reduction(upgrade_count):
    return upgrade_count * CASE_CD_REDUCTION_PER_LEVEL

def get_auto_fat_interval(auto_fat_level):
    if auto_fat_level <= 0:
        return None
    return AUTO_FAT_INTERVALS.get(auto_fat_level, 1)

def get_upgrade_cost(upgrade_type, current_level):
    if upgrade_type == "fat_cd":
        return FAT_CD_BASE_COST + (current_level * FAT_CD_COST_INCREMENT)
    elif upgrade_type == "case_cd":
        return CASE_CD_BASE_COST + (current_level * CASE_CD_COST_INCREMENT)
    elif upgrade_type == "luck":
        return LUCK_BASE_COST + (current_level * LUCK_COST_INCREMENT)
    elif upgrade_type == "income":
        return INCOME_BASE_COST + (current_level * INCOME_COST_INCREMENT)
    elif upgrade_type == "prestige":
        return PRESTIGE_BASE_COST + (current_level * PRESTIGE_COST_INCREMENT)
    elif upgrade_type == "auto_fat":
        return AUTO_FAT_BASE_COST + (current_level * AUTO_FAT_COST_INCREMENT)
    return 0

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
        'next_auto_fat_time': "TIMESTAMP",
        'animations_enabled': "INTEGER DEFAULT 1",
        'last_passive_income': "TIMESTAMP",
        'last_hourly_income': "TIMESTAMP",
        'daily_last_added': "TIMESTAMP",
        'daily_case_count': "INTEGER DEFAULT 0",
    }
    for col_name, col_type in required_columns.items():
        if col_name not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col_name} {col_type}")
            except:
                pass
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
        cursor.execute('''CREATE TABLE shop (guild_id TEXT PRIMARY KEY, slots TEXT, last_update TIMESTAMP, next_update TIMESTAMP)''')
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
        last_passive_income TIMESTAMP, last_hourly_income TIMESTAMP, daily_last_added TIMESTAMP, daily_case_count INTEGER DEFAULT 0)''')
    for case_id in CASES.keys():
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
    safe_init_guild_database(guild_id, f"Guild_{guild_id}")
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    all_columns = [col[1] for col in cursor.fetchall()]
    select_cols = [col for col in ['user_id', 'user_name', 'current_number', 'last_command_time', 'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'last_case_time', 'item_counts', 'last_command', 'last_command_target', 'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id', 'active_case_channel_id', 'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent', 'duel_amount', 'duel_message_id', 'duel_channel_id', 'duel_initiator', 'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time', 'shadow_upgrade_chance', 'user_xp', 'user_level', 'fat_cd_upgrade', 'case_cd_upgrade', 'luck_upgrade', 'income_upgrade', 'prestige', 'auto_fat_level', 'next_auto_fat_time', 'animations_enabled', 'daily_case_count'] if col in all_columns]
    case_cols = [f"case_{case_id}_count" for case_id in CASES.keys() if case_id != "daily" and f"case_{case_id}_count" in all_columns]
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
            'user_id': str(user_id), 'user_name': user_name or "Unknown", 'current_number': 0, 'last_command_time': None,
            'consecutive_plus': 0, 'consecutive_minus': 0, 'jackpot_pity': 0, 'last_case_time': None, 'item_counts': '{}',
            'last_command': None, 'last_command_target': None, 'last_command_use_time': None, 'fat_cooldown_time': None,
            'active_case_message_id': None, 'active_case_channel_id': None, 'daily_case_last_time': None, 'snatcher_last_time': None,
            'duel_active': 0, 'duel_opponent': None, 'duel_amount': 0, 'duel_message_id': None, 'duel_channel_id': None,
            'duel_initiator': 0, 'last_case_type': None, 'last_case_prize': None, 'upgrade_active': 0, 'upgrade_data': None,
            'duel_start_time': None, 'shadow_upgrade_chance': 0, 'user_xp': 0, 'user_level': 0, 'fat_cd_upgrade': 0,
            'case_cd_upgrade': 0, 'luck_upgrade': 0, 'income_upgrade': 0, 'prestige': 0, 'auto_fat_level': 0,
            'next_auto_fat_time': None, 'animations_enabled': 1, 'daily_case_last_time': None, 'cases_dict': {}
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
    base_fields = ['user_id', 'user_name', 'current_number', 'last_command_time', 'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'last_case_time', 'item_counts', 'last_command', 'last_command_target', 'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id', 'active_case_channel_id', 'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent', 'duel_amount', 'duel_message_id', 'duel_channel_id', 'duel_initiator', 'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time', 'shadow_upgrade_chance', 'user_xp', 'user_level', 'fat_cd_upgrade', 'case_cd_upgrade', 'luck_upgrade', 'income_upgrade', 'prestige', 'auto_fat_level', 'next_auto_fat_time', 'animations_enabled']
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

async def daily_case_accumulation_loop():
    """Фоновая задача: каждую минуту добавляет ежедневные кейсы"""
    await bot.wait_until_ready()
    print("🟢 Запущен цикл накопления ежедневных кейсов")
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
                    
                    # Проверяем колонки
                    cursor.execute("PRAGMA table_info(user_fat)")
                    columns = [col[1] for col in cursor.fetchall()]
                    if 'daily_case_last_time' not in columns:
                        cursor.execute("ALTER TABLE user_fat ADD COLUMN daily_case_last_time TIMESTAMP")
                        conn.commit()
                    if 'daily_case_count' not in columns:
                        cursor.execute("ALTER TABLE user_fat ADD COLUMN daily_case_count INTEGER DEFAULT 0")
                        conn.commit()
                        conn.close()
                        continue
                    
                    cursor.execute("SELECT user_id, user_name, daily_case_last_time, case_cd_upgrade FROM user_fat")
                    users = cursor.fetchall()
                    conn.close()
                    
                    for user_id, user_name, last_time_str, case_cd_upgrade in users:
                        try:
                            cd_reduction = get_case_cd_reduction(case_cd_upgrade or 0)
                            cooldown_minutes = max(1, CASE_COOLDOWN_HOURS * 60 - cd_reduction)
                            
                            last_time = None
                            if last_time_str:
                                if isinstance(last_time_str, str):
                                    last_time = datetime.fromisoformat(last_time_str)
                                else:
                                    last_time = last_time_str
                            
                            if not last_time:
                                # Первый раз: даём 1 кейс
                                update_user_data(guild_id, user_id, daily_case_count=1, daily_case_last_time=current_time)
                                print(f"📦 Первое начисление для {user_name}: +1 daily кейс")
                                continue
                            
                            diff_minutes = (current_time - last_time).total_seconds() / 60
                            
                            if diff_minutes >= cooldown_minutes:
                                intervals = int(diff_minutes // cooldown_minutes)
                                if intervals > 0:
                                    data = get_user_data(guild_id, user_id, user_name)
                                    old_count = data.get('daily_case_count', 0)
                                    new_count = old_count + intervals
                                    new_last_time = last_time + timedelta(minutes=cooldown_minutes * intervals)
                                    update_user_data(guild_id, user_id, daily_case_count=new_count, daily_case_last_time=new_last_time)
                                    print(f"📦 Начисление для {user_name}: +{intervals} daily кейсов (было {old_count}, стало {new_count})")
                        except Exception as e:
                            print(f"❌ Ошибка {user_id}: {e}")
                except Exception as e:
                    print(f"❌ Ошибка сервера {guild_id}: {e}")
            await asyncio.sleep(60)
        except Exception as e:
            print(f"❌ Ошибка цикла: {e}")
            await asyncio.sleep(60)

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

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

def get_user_cases(guild_id, user_id):
    data = get_user_data(guild_id, user_id)
    return data.get('cases_dict', {})

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
    select_cols = ['user_name', 'current_number', 'last_command_time', 'consecutive_plus', 'consecutive_minus', 'jackpot_pity']
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
    return {'total_users': total_users, 'total_weight': total_weight, 'avg_weight': avg_weight, 'positive': positive, 'negative': negative, 'zero': zero}

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

def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, luck_upgrade=0, prestige_bonus=1.0, items_dict=None, current_weight=None):
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
            return change, False, consecutive_plus + 1, 0, 0, True
        else:
            change = random.randint(1, 20) // 3
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, jackpot_pity + 1, False
    elif has_rotten_leg:
        if random.random() < 0.6:
            if current_weight is not None:
                loss = int(current_weight * 0.5)
                change = -loss
            else:
                change = -int(consecutive_plus * 0.5)
            change = int(change * prestige_bonus)
            return change, True, 0, consecutive_minus + 1, jackpot_pity + 1, False
        else:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, 0, True
    else:
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, 0, True
        roll = random.random()
        if roll < minus_chance:
            change = random.randint(-20, -1)
            change = int(change * prestige_bonus)
            return change, True, 0, consecutive_minus + 1, jackpot_pity + 1, False
        else:
            change = random.randint(1, 20)
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, jackpot_pity + 1, False

def open_case(case_id, prestige_luck=0, luck_upgrade=0):
    case = CASES[case_id]
    prizes = case["prizes"]
    total_chance = sum(p["chance"] for p in prizes)
    for prize in prizes:
        prize["normalized_chance"] = (prize["chance"] / total_chance) * 100
    bonus = (prestige_luck * 100) + (luck_upgrade * LUCK_CASE_BONUS_PER_LEVEL)
    modified_prizes = []
    for prize in prizes:
        p = prize.copy()
        if (isinstance(p["value"], int) and p["value"] >= 100) or p["value"] in ["rotten_leg", "water"]:
            p["normalized_chance"] = prize["normalized_chance"] + bonus
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

def are_animations_enabled(user_data):
    return user_data.get('animations_enabled', 1) == 1

async def apply_auto_fat(user_id, guild_id, user_name):
    try:
        data = get_user_data(guild_id, user_id, user_name)
        items_dict = get_user_items(data['item_counts'])
        prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
        
        change, was_minus, new_plus, new_minus, new_pity, was_jackpot = get_change_with_pity_and_jackpot(
            data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
            data.get('luck_upgrade', 0), prestige_bonus, items_dict, data['current_number'])
        
        new_number = data['current_number'] + change
        
        update_data = {
            'number': new_number,
            'user_name': user_name,
            'consecutive_plus': new_plus,
            'consecutive_minus': new_minus,
            'jackpot_pity': new_pity,
            'fat_cooldown_time': datetime.now()
        }
        
        update_user_data(guild_id, user_id, **update_data)
        
        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_FAT)
        
        await update_user_nick(guild_id, user_id, user_name)
        
        print(f"🤖 Авто-жир сработал для {user_name}: {change:+d} кг, опыт +{XP_PER_FAT}")
        
        if levels_gained > 0:
            print(f"🌟 {user_name} повысил уровень до {new_level} (+{kg_reward} кг)")
            
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
                                    await apply_auto_fat(user_id, guild_id, user_name)
                                    interval = get_auto_fat_interval(auto_fat_level)
                                    if interval:
                                        new_next_time = current_time + timedelta(hours=interval)
                                        conn2 = sqlite3.connect(db_path)
                                        c2 = conn2.cursor()
                                        c2.execute('''UPDATE user_fat SET next_auto_fat_time = ? WHERE user_id = ?''', 
                                                  (new_next_time.isoformat(), user_id))
                                        conn2.commit()
                                        conn2.close()
                        except Exception as e:
                            pass
                except Exception as e:
                    pass
        except Exception as e:
            pass
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
                    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, income_upgrade, prestige, last_passive_income FROM user_fat WHERE item_counts != '{}' AND item_counts IS NOT NULL''')
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
                                for item_name, count in items_dict.items():
                                    for shop_item in SHOP_ITEMS:
                                        if shop_item["name"] == item_name:
                                            gain = shop_item.get("gain_per_24h", 0) * count
                                            if gain > 0:
                                                total_gain += gain
                                            break
                                if total_gain > 0:
                                    income_bonus = get_income_bonus(income_upgrade or 0)
                                    prestige_bonus = get_prestige_bonus(prestige or 0)
                                    final_gain = int(total_gain * income_bonus * prestige_bonus)
                                    new_number = current_number + final_gain
                                    conn2 = sqlite3.connect(db_path)
                                    c = conn2.cursor()
                                    c.execute('''UPDATE user_fat SET current_number = ?, last_passive_income = ? WHERE user_id = ?''', (new_number, current_time, user_id))
                                    conn2.commit()
                                    conn2.close()
                                    try:
                                        guild_obj = bot.get_guild(guild_id)
                                        if guild_obj:
                                            member = guild_obj.get_member(int(user_id))
                                            if member:
                                                display_name = member.display_name
                                                clean_name = display_name.split("kg", 1)[-1].strip() if "kg" in display_name else display_name
                                                if not clean_name or len(clean_name) > 30:
                                                    clean_name = user_name
                                                new_nick = format_nick_with_prestige(prestige or 0, new_number, clean_name)
                                                if len(new_nick) > 32:
                                                    new_nick = new_nick[:32]
                                                await member.edit(nick=new_nick)
                                    except:
                                        pass
                            else:
                                conn2 = sqlite3.connect(db_path)
                                c = conn2.cursor()
                                c.execute('''UPDATE user_fat SET last_passive_income = ? WHERE user_id = ? AND last_passive_income IS NULL''', (current_time, user_id))
                                conn2.commit()
                                conn2.close()
                        except Exception as e:
                            pass
                except Exception as e:
                    pass
        except Exception as e:
            pass
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
            if (current_time - last_time).total_seconds() < 6 * 3600:
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
                virtual_slots.append({"name": chosen_item["name"], "amount": amount, "price": chosen_item["price"], "description": chosen_item["description"], "gain_per_24h": chosen_item.get("gain_per_24h", 0)})
            else:
                virtual_slots.append(None)
        chosen_slot = random.randint(0, 9)
        selected_item = virtual_slots[chosen_slot]
        if not selected_item:
            update_user_data(guild_id, user_id, snatcher_last_time=current_time)
            return
        items_dict[selected_item["name"]] = items_dict.get(selected_item["name"], 0) + 1
        update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict), snatcher_last_time=current_time)
        try:
            guild = bot.get_guild(guild_id)
            if guild:
                member = guild.get_member(int(user_id))
                if member:
                    embed = discord.Embed(title="👾 **Снатчер сработал!**", description=f"Ваш **Снатчер** сгенерировал предмет из {chosen_slot + 1} слота!", color=0x9b59b6)
                    embed.add_field(name="📦 Получено", value=f"**+1 {selected_item['name']}**", inline=False)
                    await member.send(embed=embed)
        except:
            pass
    except Exception as e:
        pass

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
                    cursor.execute('''SELECT user_id, user_name FROM user_fat WHERE item_counts LIKE '%"Снатчер"%' ''')
                    users = cursor.fetchall()
                    conn.close()
                    for user_id, user_name in users:
                        try:
                            await apply_snatcher_effect(guild_id, user_id, user_name)
                            await asyncio.sleep(1)
                        except Exception as e:
                            pass
                except Exception as e:
                    pass
        except Exception as e:
            pass
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
                    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, income_upgrade, prestige, last_hourly_income FROM user_fat''')
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
                                for item_name, count in items_dict.items():
                                    if item_name == "Автохолестерол":
                                        gain = random.randint(1, 10) * count
                                        total_gain += gain
                                    elif item_name == "Холестеринимус":
                                        gain = random.randint(1, 5) * count
                                        total_gain += gain
                                if total_gain > 0:
                                    income_bonus = get_income_bonus(income_upgrade or 0)
                                    prestige_bonus = get_prestige_bonus(prestige or 0)
                                    final_gain = int(total_gain * income_bonus * prestige_bonus)
                                    new_number = current_number + final_gain
                                    conn2 = sqlite3.connect(db_path)
                                    c = conn2.cursor()
                                    c.execute('''UPDATE user_fat SET current_number = ?, last_hourly_income = ? WHERE user_id = ?''', (new_number, current_time, user_id))
                                    conn2.commit()
                                    conn2.close()
                            else:
                                conn2 = sqlite3.connect(db_path)
                                c = conn2.cursor()
                                c.execute('''UPDATE user_fat SET last_hourly_income = ? WHERE user_id = ? AND last_hourly_income IS NULL''', (current_time, user_id))
                                conn2.commit()
                                conn2.close()
                        except Exception as e:
                            pass
                except Exception as e:
                    pass
        except Exception as e:
            pass
        await asyncio.sleep(3600)

def can_duel(user_data):
    return not user_data.get('duel_active', 0)

def get_duel_info(user_data):
    return {'active': user_data.get('duel_active', 0), 'opponent': user_data.get('duel_opponent'), 'amount': user_data.get('duel_amount', 0), 'message_id': user_data.get('duel_message_id'), 'channel_id': user_data.get('duel_channel_id'), 'initiator': user_data.get('duel_initiator', 0), 'start_time': user_data.get('duel_start_time')}

async def migrate_old_burgers_to_prestige():
    burger_to_prestige = {0: 1, 1: 2, 2: 3, 3: 4}
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
                        cursor.execute("UPDATE user_fat SET prestige = ?, legendary_burger = -1 WHERE user_id = ?", (prestige_amount, user_id))
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
            columns = [col[1] for col in cursor.fetchall()]
            if 'autoburger_count' in columns and 'auto_fat_level' in columns:
                cursor.execute("SELECT user_id, user_name, autoburger_count FROM user_fat WHERE autoburger_count > 0")
                users = cursor.fetchall()
                for user_id, user_name, autoburger_count in users:
                    if autoburger_count > 0:
                        new_level = min(autoburger_count, AUTO_FAT_MAX_LEVEL)
                        interval = get_auto_fat_interval(new_level)
                        next_time = datetime.now() + timedelta(hours=interval) if interval else None
                        cursor.execute("UPDATE user_fat SET auto_fat_level = ?, next_auto_fat_time = ?, autoburger_count = 0 WHERE user_id = ?", (new_level, next_time, user_id))
                        converted += 1
                conn.commit()
            conn.close()
        except Exception as e:
            pass

async def duel_animation(msg, challenger, opponent):
    c_name = challenger.display_name[:15] + "..." if len(challenger.display_name) > 15 else challenger.display_name
    o_name = opponent.display_name[:15] + "..." if len(opponent.display_name) > 15 else opponent.display_name
    max_len = max(len(c_name), len(o_name))
    c_name = c_name.ljust(max_len)
    o_name = o_name.ljust(max_len)
    duel_emojis = ["⬆️", "⬇️", "⚔️"]
    line = [random.choice(duel_emojis) for _ in range(100)]
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
    anim_embed = discord.Embed(title="⚔️ **ДУЭЛЬ** ⚔️", description="", color=0xff5500)
    animation_frames = [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25), (6, 30), (7, 35), (8, 39), (9, 43), (10, 47), (11, 50), (12, 52), (13, 54), (14, 55), (15, 56), (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)]
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        anim_embed.description = f"**{c_name}**\n**{display_line}**\n**{o_name}**"
        await msg.edit(embed=anim_embed)
        await asyncio.sleep(0.5)
    result_embed = discord.Embed(title="⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️", description=f"**{c_name}**\n**{display_line}**\n**{o_name}**\n\n{result_text}", color=result_color)
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
    all_items = set([item["name"] for item in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
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
        possible_upgrades.append({"name": item_name_check, "price": target_price, "chance": chance, "emoji": ITEM_EMOJIS.get(item_name_check, "🎁")})
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
            possible_upgrades.append({"name": leg_name, "price": leg_price, "chance": chance, "emoji": ITEM_EMOJIS.get(leg_name, "✨")})
            seen_items.add(leg_name)
    possible_upgrades.sort(key=lambda x: x["price"])
    return possible_upgrades

async def upgrade_animation(ctx, member, source_item, target_item, item_count, prestige_luck=0, luck_upgrade=0, animations_enabled=True):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    data = get_user_data(guild_id, user_id, member.name)
    shadow_chance = data.get('shadow_upgrade_chance', 0)
    prestige_bonus = 1 + prestige_luck
    luck_bonus = 1 + (luck_upgrade * LUCK_UPGRADE_BONUS_PER_LEVEL / 100)
    base_chance = target_item['chance']
    real_chance = min(base_chance * prestige_bonus * luck_bonus + shadow_chance / 100, 1.0)
    display_chance = base_chance * prestige_bonus * luck_bonus * 100
    upgrade_emojis = ["🟥", "🟩"]
    line = [random.choice(upgrade_emojis) for _ in range(100)]
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
    anim_embed = discord.Embed(title="🔧 **АПГРЕЙД** 🔧", description=f"**{member.display_name}** улучшает:\n{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\nШанс: **{display_chance:.1f}%**", color=0xff5500)
    upgrade_msg = await ctx.send(embed=anim_embed)
    animation_frames = [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25), (6, 30), (7, 35), (8, 39), (9, 43), (10, 47), (11, 50), (12, 52), (13, 54), (14, 55), (15, 56), (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)]
    
    if animations_enabled:
        for frame_num, center_pos in animation_frames:
            visible = line[center_pos-4:center_pos+5]
            display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
            anim_embed.description = f"**{member.display_name}** улучшает:\n{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n**{display_line}**\n\nШанс: **{display_chance:.1f}%**"
            await upgrade_msg.edit(embed=anim_embed)
            await asyncio.sleep(0.5)
    
    start = 57 - 4
    end = 57 + 5
    visible = line[start:end]
    display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
    
    if not animations_enabled:
        anim_embed.description = f"**{member.display_name}** улучшает:\n{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n**{display_line}**\n\n**РЕЗУЛЬТАТ!**"
        await upgrade_msg.edit(embed=anim_embed)
    
    current_data = get_user_data(guild_id, user_id, member.name)
    items_dict = get_user_items(current_data['item_counts'])
    if success:
        items_dict[target_item['name']] = items_dict.get(target_item['name'], 0) + 1
        update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict), shadow_upgrade_chance=new_shadow, upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
        result_description = f"✅ **Поздравляем!**\n\n{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\nПредмет успешно улучшен!"
        
        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_UPGRADE)
        if levels_gained > 0:
            result_description += f"\n\n⭐ **ПОВЫШЕНИЕ УРОВНЯ!** +{kg_reward} кг! Теперь у вас **{new_level}** уровень!"
        
        await update_user_nick(guild_id, user_id, member.name)
    else:
        update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict), shadow_upgrade_chance=new_shadow, upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
        result_description = f"❌ **Неудача!**\n\n{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** был утерян в процессе улучшения!"
    
    result_embed = discord.Embed(title="🔧 **РЕЗУЛЬТАТ АПГРЕЙДА** 🔧", description=f"**{display_line}**\n\n{result_text}\n\n{result_description}", color=result_color)
    result_embed.set_footer(text=f"Шанс был: {display_chance:.1f}%")
    await upgrade_msg.edit(embed=result_embed)
    
async def upgrade_kg_animation(ctx, member, amount, target_item, prestige_luck=0, luck_upgrade=0, animations_enabled=True):
    guild_id = ctx.guild.id
    user_id = str(member.id)
    data = get_user_data(guild_id, user_id, member.name)
    shadow_chance = data.get('shadow_upgrade_chance', 0)
    prestige_bonus = 1 + prestige_luck
    luck_bonus = 1 + (luck_upgrade * LUCK_UPGRADE_BONUS_PER_LEVEL / 100)
    base_chance = target_item['chance']
    real_chance = min(base_chance * prestige_bonus * luck_bonus + shadow_chance / 100, 1.0)
    display_chance = base_chance * prestige_bonus * luck_bonus * 100
    upgrade_emojis = ["🟥", "🟩"]
    line = [random.choice(upgrade_emojis) for _ in range(100)]
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
    anim_embed = discord.Embed(title="💱 **АПГРЕЙД КГ** 💱", description=f"**{member.display_name}** улучшает {amount} кг в:\n{target_item['emoji']} **{target_item['name']}**\n\nШанс: **{display_chance:.1f}%**", color=0xff5500)
    upgrade_msg = await ctx.send(embed=anim_embed)
    animation_frames = [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25), (6, 30), (7, 35), (8, 39), (9, 43), (10, 47), (11, 50), (12, 52), (13, 54), (14, 55), (15, 56), (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)]
    
    if animations_enabled:
        for frame_num, center_pos in animation_frames:
            visible = line[center_pos-4:center_pos+5]
            display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
            anim_embed.description = f"**{member.display_name}** улучшает {amount} кг в:\n{target_item['emoji']} **{target_item['name']}**\n\n**{display_line}**\n\nШанс: **{display_chance:.1f}%**"
            await upgrade_msg.edit(embed=anim_embed)
            await asyncio.sleep(0.5)
    
    start = 57 - 4
    end = 57 + 5
    visible = line[start:end]
    display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
    
    if not animations_enabled:
        anim_embed.description = f"**{member.display_name}** улучшает {amount} кг в:\n{target_item['emoji']} **{target_item['name']}**\n\n**{display_line}**\n\n**РЕЗУЛЬТАТ!**"
        await upgrade_msg.edit(embed=anim_embed)
    
    current_data = get_user_data(guild_id, user_id, member.name)
    if success:
        if target_item.get("is_case", False):
            cases_dict = current_data.get('cases_dict', {}).copy()
            cases_dict[target_item["case_id"]] = cases_dict.get(target_item["case_id"], 0) + 1
            update_user_data(guild_id, user_id, cases_dict=cases_dict, shadow_upgrade_chance=new_shadow, upgrade_active=0, upgrade_data=None)
            result_description = f"✅ **Поздравляем!**\n\n{amount} кг → {target_item['emoji']} **{target_item['name']}**\n\nПредмет успешно получен!"
        else:
            items_dict = get_user_items(current_data['item_counts'])
            items_dict[target_item["name"]] = items_dict.get(target_item["name"], 0) + 1
            update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict), shadow_upgrade_chance=new_shadow, upgrade_active=0, upgrade_data=None)
            result_description = f"✅ **Поздравляем!**\n\n{amount} кг → {target_item['emoji']} **{target_item['name']}**\n\nПредмет успешно получен!"
        
        levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_UPGRADE_KG)
        if levels_gained > 0:
            result_description += f"\n\n⭐ **ПОВЫШЕНИЕ УРОВНЯ!** +{kg_reward} кг! Теперь у вас **{new_level}** уровень!"
        
        await update_user_nick(guild_id, user_id, member.name)
    else:
        update_user_data(guild_id, user_id, shadow_upgrade_chance=new_shadow, upgrade_active=0, upgrade_data=None)
        result_description = f"❌ **Неудача!**\n\n{amount} кг сгорели в процессе улучшения!"
    
    result_embed = discord.Embed(title="💱 **РЕЗУЛЬТАТ АПГРЕЙДА** 💱", description=f"**{display_line}**\n\n{result_text}\n\n{result_description}", color=result_color)
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
                min_prize = min([p["value"] for p in case["prizes"] if isinstance(p["value"], int)] + [0])
                max_prize = max([p["value"] for p in case["prizes"] if isinstance(p["value"], int)] + [0])
                slots.append({"type": "case", "case_id": chosen_id, "name": case["name"], "amount": amount, "price": case["price"], "description": f"{case['emoji']} Содержит случайные призы!\nОт {min_prize}кг до {max_prize}кг", "emoji": case['emoji']})
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
            slots.append({"type": "item", "name": chosen_item["name"], "amount": amount, "price": chosen_item["price"], "description": chosen_item["description"], "gain_per_24h": chosen_item.get("gain_per_24h", 0), "emoji": ITEM_EMOJIS.get(chosen_item["name"], "📦")})
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
    cursor.execute('''INSERT OR REPLACE INTO shop (guild_id, slots, last_update, next_update) VALUES (?, ?, ?, ?)''', (str(guild_id), slots_json, last_update_str, next_update_str))
    conn.commit()
    conn.close()

async def ensure_shop_updated(guild_id):
    result = get_shop_data(guild_id)
    current_time = datetime.now()
    if result[0] is not None:
        slots, last_update_str, next_update_str = result
        last_update = datetime.fromisoformat(last_update_str) if isinstance(last_update_str, str) else last_update_str if last_update_str else None
        next_update = datetime.fromisoformat(next_update_str) if isinstance(next_update_str, str) else next_update_str if next_update_str else None
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
        display_name = member.display_name
        clean_name = display_name
        if "kg" in display_name:
            parts = display_name.split("kg", 1)
            if len(parts) > 1:
                clean_name = parts[1].strip()
                if not clean_name:
                    clean_name = data.get('user_name', str(user_id))
        else:
            clean_name = display_name
        if not clean_name or len(clean_name) > 30:
            clean_name = data.get('user_name', str(user_id))
        new_nick = format_nick_with_prestige(data.get('prestige', 0), data['current_number'], clean_name)
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        if member.nick != new_nick:
            await member.edit(nick=new_nick)
        return True
    except Exception as e:
        return False

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
        embed = discord.Embed(title="⏳ Подождите!", description=f"{member.mention}, вы уже использовали команду недавно!", color=0xff0000)
        embed.add_field(name="Осталось подождать", value=format_time(remaining), inline=True)
        embed.add_field(name="Кулдаун", value=f"{actual_cooldown*60:.0f} мин", inline=True)
        await ctx.send(embed=embed)
        return
    prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
    change, was_minus, new_plus, new_minus, new_pity, was_jackpot = get_change_with_pity_and_jackpot(
        data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
        data.get('luck_upgrade', 0), prestige_bonus, items_dict, data['current_number'])
    temp_number = data['current_number'] + change
    update_user_data(guild_id, user_id, number=temp_number)
    levels_gained, kg_reward, new_level = add_xp(guild_id, user_id, XP_PER_FAT)
    final_data = get_user_data(guild_id, user_id, user_name)
    final_number = final_data['current_number']
    update_user_data(guild_id, user_id, user_name=user_name, consecutive_plus=new_plus, consecutive_minus=new_minus, jackpot_pity=new_pity, fat_cooldown_time=datetime.now())
    await update_user_nick(guild_id, user_id, user_name)
    rank_name, rank_emoji = get_rank(final_number)
    embed_color = 0xffd700 if was_jackpot else (0xff9933 if final_number >= 0 else 0x66ccff)
    embed_title = "💰 ДЖЕКПОТ! 💰" if was_jackpot else "🍔 Набор массы"
    embed = discord.Embed(title=embed_title, description=f"**{member.mention}** теперь весит **{abs(final_number)}kg** на сервере **{ctx.guild.name}**!", color=embed_color)
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
        embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", inline=False)
    embed.add_field(name="⏰ Следующая команда", value=f"через {actual_cooldown*60:.0f} мин", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='жиркейс')
async def fat_case_command(ctx):
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    # Получаем данные
    data = get_user_data(guild_id, user_id, user_name)
    
    # Проверка активного кейса
    if data.get('active_case_message_id'):
        try:
            channel = bot.get_channel(int(data['active_case_channel_id'])) if data.get('active_case_channel_id') else None
            if channel:
                try:
                    old_msg = await channel.fetch_message(int(data['active_case_message_id']))
                    if old_msg:
                        time_since = datetime.now() - old_msg.created_at.replace(tzinfo=None)
                        if time_since.total_seconds() < 120:
                            embed = discord.Embed(title="⚠️ Кейс уже открыт!", description=f"{member.mention}, у вас уже есть активный кейс!\nСначала завершите или дождитесь таймаута предыдущего.", color=0xffaa00)
                            await ctx.send(embed=embed)
                            return
                except:
                    pass
        except:
            pass
    
    # ===== НАЧИСЛЯЕМ НАКОПЛЕННЫЕ ЕЖЕДНЕВНЫЕ КЕЙСЫ =====
    last_time = data.get('daily_case_last_time')
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    cd_reduction_minutes = get_case_cd_reduction(case_cd_upgrade)
    cooldown_minutes = max(1, CASE_COOLDOWN_HOURS * 60 - cd_reduction_minutes)
    cooldown = timedelta(minutes=cooldown_minutes)
    now = datetime.now()
    
    cases_dict = data.get('cases_dict', {}).copy()
    
    if not last_time:
        # Первый раз - даём 1 кейс
        cases_dict["daily"] = cases_dict.get("daily", 0) + 1
        update_user_data(guild_id, user_id, cases_dict=cases_dict, daily_case_last_time=now)
        print(f"📦 Первое начисление для {user_name}: +1 daily кейс")
    else:
        if isinstance(last_time, str):
            last_time = datetime.fromisoformat(last_time)
        
        diff = now - last_time
        if diff >= cooldown:
            intervals = int(diff.total_seconds() // cooldown.total_seconds())
            if intervals > 0:
                cases_dict["daily"] = cases_dict.get("daily", 0) + intervals
                new_last_time = last_time + cooldown * intervals
                update_user_data(guild_id, user_id, cases_dict=cases_dict, daily_case_last_time=new_last_time)
                print(f"📦 Начисление для {user_name}: +{intervals} daily кейсов (КД {cooldown_minutes} мин)")
    
    # Обновляем данные после начисления
    data = get_user_data(guild_id, user_id, user_name)
    cases_dict = data.get('cases_dict', {}).copy()
    
    # Выбираем кейс для открытия
    case_to_open = None
    case = None
    
    # Сначала проверяем ежедневные кейсы
    if cases_dict.get("daily", 0) > 0:
        case_to_open = "daily"
        case = CASES["daily"]
    else:
        # Потом платные кейсы
        for case_id, count in cases_dict.items():
            if case_id != "daily" and count > 0:
                case_to_open = case_id
                case = CASES[case_id]
                break
    
    if not case_to_open:
        # Если нет кейсов - показываем сообщение
        embed = discord.Embed(title="📭 Нет кейсов!", description=f"{member.mention}, у вас нет кейсов для открытия!\n\nКупить кейсы можно в магазине (`!магазин`)", color=0xff0000)
        await ctx.send(embed=embed)
        return
    
    # Запоминаем, какой кейс собираемся открыть
    update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=case_to_open)
    
    # Собираем эмодзи для анимации
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
        description=f"{member.mention}, у вас есть кейс!\n\n**Нажмите на 🖱️ чтобы открыть**\n**Нажмите на ❌ чтобы отменить**",
        color=0xffaa00
    )
    case_embed.set_footer(text="У вас 30 секунд чтобы открыть кейс!")
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
            cancel_embed = discord.Embed(title="❌ Отмена", description=f"{member.mention}, вы отменили открытие кейса. Кейс сохранён в инвентаре!", color=0xffaa00)
            await case_msg.edit(embed=cancel_embed)
            return
        
        # Списываем кейс из инвентаря
        current_data = get_user_data(guild_id, user_id, user_name)
        current_cases = current_data.get('cases_dict', {}).copy()
        if case_to_open == "daily":
            if current_cases.get("daily", 0) <= 0:
                await ctx.send(f"{member.mention}, у вас нет ежедневных кейсов!")
                await case_msg.delete()
                update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None)
                return
            current_cases["daily"] -= 1
        else:
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
        
        # Получаем приз
        prestige_luck = get_prestige_luck(current_data.get('prestige', 0))
        luck_upgrade = current_data.get('luck_upgrade', 0)
        prize = open_case(case_to_open, prestige_luck, luck_upgrade)
        update_user_data(guild_id, user_id, active_case_message_id=None, active_case_channel_id=None, last_case_type=None, last_case_prize=None)
        
        # Анимация
        line = [random.choice(prize_emojis) for _ in range(100)]
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
        anim_embed = discord.Embed(title=f"🎰 **{case['name']}** 🎰", description="", color=0xffaa00)
        animation_frames = [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25), (6, 30), (7, 35), (8, 39), (9, 43), (10, 47), (11, 50), (12, 52), (13, 54), (14, 55), (15, 55), (16, 55), (17, 56), (18, 56)]
        
        animations_enabled = are_animations_enabled(current_data)
        
        if animations_enabled:
            for frame_num, center_pos in animation_frames:
                visible = line[center_pos-4:center_pos+5]
                display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
                anim_embed.description = f"**{display_line}**"
                await case_msg.edit(embed=anim_embed)
                await asyncio.sleep(0.5)
        else:
            visible = line[52:61]
            display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
            anim_embed.description = f"**{display_line}**\n\n**РЕЗУЛЬТАТ!**"
            await case_msg.edit(embed=anim_embed)
            await asyncio.sleep(1)
        
        # Обработка приза и опыта
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
            if has_water and case_to_open != "daily":
                prize_value = prize_value // 3
            prize_value = int(prize_value * prestige_bonus)
            new_number = after_xp_weight + prize_value
            result_display = f"🎉 **{prize_value:+d} кг** {prize_emoji}"
            result_color = 0xffaa00
        
        update_data = {'number': new_number, 'user_name': user_name, 'item_counts': save_user_items(items_dict)}
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
        if case_to_open != "daily":
            remaining = after_xp_data.get('cases_dict', {}).get(case_to_open, 0)
            if remaining > 0:
                final_embed.add_field(name="📦 Осталось кейсов", value=f"{case['emoji']} {case['name']}: {remaining} шт", inline=False)
        else:
            remaining_daily = after_xp_data.get('cases_dict', {}).get("daily", 0)
            if remaining_daily > 0:
                final_embed.add_field(name="📦 Осталось ежедневных кейсов", value=f"{remaining_daily} шт", inline=False)
        
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
        
        fat_cd_upgrade = data.get('fat_cd_upgrade', 0)
        actual_fat_cooldown = max(0.1, COOLDOWN_HOURS * 60 - get_fat_cd_reduction(fat_cd_upgrade)) / 60
        
        items_dict = get_user_items(data['item_counts'])
        
        for item_name, count in items_dict.items():
            if item_name in ["Яблоко", "Золотое Яблоко"]:
                actual_fat_cooldown *= (1 - count * (0.05 if item_name == "Яблоко" else 0.10))
        
        actual_fat_cooldown = max(0.1, actual_fat_cooldown)
        
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
        
        fat_cd_cost = get_upgrade_cost("fat_cd", fat_cd_level)
        case_cd_cost = get_upgrade_cost("case_cd", case_cd_level)
        luck_cost = get_upgrade_cost("luck", luck_level)
        income_cost = get_upgrade_cost("income", income_level)
        prestige_cost = get_upgrade_cost("prestige", prestige_level)
        auto_fat_cost = get_upgrade_cost("auto_fat", auto_fat_level)
        
        fat_cd_bonus = get_fat_cd_reduction(fat_cd_level)
        case_cd_bonus = get_case_cd_reduction(case_cd_level)
        auto_fat_interval = get_auto_fat_interval(auto_fat_level)
        auto_fat_text = f"{auto_fat_interval} ч" if auto_fat_interval else "Не куплен"
        
        can_use_fat, fat_remaining = check_cooldown(data['fat_cooldown_time'], actual_fat_cooldown)
        
        fat_status = f"✅ Доступен" if can_use_fat else f"⏳ {format_time(fat_remaining)}"
        
        # ===== СТАТУС ЕЖЕДНЕВНЫХ КЕЙСОВ (на основе инвентаря) =====
        daily_stock = data.get('cases_dict', {}).get("daily", 0)
        if daily_stock > 0:
            case_status = f"✅ {daily_stock} шт в инвентаре"
        else:
            last_time = data.get('daily_case_last_time')
            if last_time:
                if isinstance(last_time, str):
                    last_time = datetime.fromisoformat(last_time)
                cd_up = data.get('case_cd_upgrade', 0)
                cd_min = max(1, CASE_COOLDOWN_HOURS * 60 - get_case_cd_reduction(cd_up))
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
        stats_text += f"{fat_cd_color} **⏰ КД !жир** — ур.{fat_cd_level} (-{fat_cd_bonus} мин)\n   Стоимость: `{fat_cd_cost} кг`\n\n"
        
        case_cd_color = "🟢" if data['current_number'] >= case_cd_cost else "🔴"
        stats_text += f"{case_cd_color} **📦 КД кейса** — ур.{case_cd_level} (-{case_cd_bonus} мин)\n   Стоимость: `{case_cd_cost} кг`\n\n"
        
        luck_color = "🟢" if data['current_number'] >= luck_cost else "🔴"
        stats_text += f"{luck_color} **🍀 Удача** — ур.{luck_level} (+{luck_level * LUCK_CASE_BONUS_PER_LEVEL:.2f}% к редким, +{luck_level * LUCK_UPGRADE_BONUS_PER_LEVEL:.2f}% к апгрейдам)\n   Стоимость: `{luck_cost} кг`\n\n"
        
        income_color = "🟢" if data['current_number'] >= income_cost else "🔴"
        stats_text += f"{income_color} **📈 Прибавка** — ур.{income_level} (+{(income_bonus-1)*100:.0f}% к доходу)\n   Стоимость: `{income_cost} кг`\n\n"
        
        prestige_color = "🟢" if data['current_number'] >= prestige_cost else "🔴"
        stats_text += f"{prestige_color} **🌟 Престиж** — ур.{prestige_level}\n   Стоимость: `{prestige_cost} кг`\n\n"
        
        auto_fat_color = "🟢" if data['current_number'] >= auto_fat_cost else "🔴"
        stats_text += f"{auto_fat_color} **🤖 Авто-жир** — ур.{auto_fat_level} (каждые {auto_fat_text})\n   Стоимость: `{auto_fat_cost} кг`\n\n"
        
        embed.add_field(name="⚡ **ПРОКАЧКА**", value=stats_text, inline=False)
        
        pity_emojis = []
        if data['consecutive_plus'] > 0:
            pity_emojis.append(f"🔥{data['consecutive_plus']}")
        if data['consecutive_minus'] > 0:
            pity_emojis.append(f"❄️{data['consecutive_minus']}")
        if data['jackpot_pity'] > 0:
            pity_emojis.append(f"💰{data['jackpot_pity']}")
        if pity_emojis:
            embed.add_field(name="📊 **СЧЁТЧИКИ**", value=" ".join(pity_emojis), inline=True)
        
        cases_dict = data.get('cases_dict', {})
        cases_text = ""
        for case_id, count in cases_dict.items():
            if count > 0 and case_id in CASES:
                cases_text += f"{CASES[case_id]['emoji']} {CASES[case_id]['name']}: {count}\n"
        if cases_text:
            embed.add_field(name="📦 **КЕЙСЫ**", value=cases_text, inline=False)
        
        embed.set_footer(text="🟢 Нажмите на реакцию для улучшения | 🎬 Нажмите 🎬 для переключения анимаций")
        return embed
    
    data = get_user_data(guild_id, user_id, user_name)
    animations_status = data.get('animations_enabled', 1)
    embed = create_profile_embed(data, animations_status)
    msg = await ctx.send(embed=embed)
    
    upgrade_reactions = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣"]
    for reaction in upgrade_reactions:
        await msg.add_reaction(reaction)
    await msg.add_reaction("🎬")
    
    upgrade_map = {"1️⃣": "fat_cd", "2️⃣": "case_cd", "3️⃣": "luck", "4️⃣": "income", "5️⃣": "prestige", "6️⃣": "auto_fat"}
    
    def check(reaction, user):
        return user == ctx.author and reaction.message.id == msg.id and str(reaction.emoji) in upgrade_reactions + ["🎬"]
    
    while True:
        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)
            emoji = str(reaction.emoji)
            
            if emoji == "🎬":
                current_data = get_user_data(guild_id, user_id, user_name)
                current_status = current_data.get('animations_enabled', 1)
                new_status = 0 if current_status == 1 else 1
                update_user_data(guild_id, user_id, animations_enabled=new_status)
                fresh_data = get_user_data(guild_id, user_id, user_name)
                new_embed = create_profile_embed(fresh_data, new_status)
                await msg.edit(embed=new_embed)
                status_text = "включены 🎬" if new_status == 1 else "выключены 🔇"
                temp_msg = await ctx.send(f"Анимации {status_text}!")
                await asyncio.sleep(2)
                await temp_msg.delete()
                try:
                    await msg.remove_reaction(reaction, user)
                except:
                    pass
                continue
            
            if emoji not in upgrade_map:
                continue
            
            upgrade_type = upgrade_map[emoji]
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
                if current_level >= AUTO_FAT_MAX_LEVEL:
                    error_embed = discord.Embed(title="❌ Максимальный уровень!", description=f"Авто-жир уже на максимальном ({AUTO_FAT_MAX_LEVEL}) уровне!", color=0xff0000)
                    temp_msg = await ctx.send(embed=error_embed)
                    await asyncio.sleep(2)
                    await temp_msg.delete()
                    continue
                cost = get_upgrade_cost("auto_fat", current_level)
            
            if current_data['current_number'] < cost:
                error_embed = discord.Embed(title="❌ Недостаточно кг!", description=f"Для улучшения нужно **{cost} кг**, у вас: **{current_data['current_number']} кг**", color=0xff0000)
                temp_msg = await ctx.send(embed=error_embed)
                await asyncio.sleep(2)
                await temp_msg.delete()
                continue
            
            if upgrade_type == "prestige":
                confirm_embed = discord.Embed(
                    title="⚠️ **ПРЕСТИЖ** ⚠️",
                    description=f"{target.mention}, вы уверены, что хотите получить престиж?\n\n"
                               f"**Что произойдёт:**\n"
                               f"• Вес сбросится до 0\n"
                               f"• Все предметы и кейсы исчезнут\n"
                               f"• Опыт и уровень сохранятся\n"
                               f"• Улучшения (КД, удача, прибавка, авто-жир) **СОХРАНЯТСЯ**\n"
                               f"• Престиж увеличится на 1\n\n"
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
                
                current_fat_cd = current_data.get('fat_cd_upgrade', 0)
                current_case_cd = current_data.get('case_cd_upgrade', 0)
                current_luck = current_data.get('luck_upgrade', 0)
                current_income = current_data.get('income_upgrade', 0)
                current_auto_fat = current_data.get('auto_fat_level', 0)
                current_xp = current_data.get('user_xp', 0)
                current_user_level = current_data.get('user_level', 0)
                
                update_user_data(
                    guild_id, user_id,
                    current_number=0,
                    item_counts='{}',
                    cases_dict={},
                    prestige=new_prestige,
                    user_xp=current_xp,
                    user_level=current_user_level,
                    fat_cd_upgrade=current_fat_cd,
                    case_cd_upgrade=current_case_cd,
                    luck_upgrade=current_luck,
                    income_upgrade=current_income,
                    auto_fat_level=current_auto_fat,
                    consecutive_plus=0,
                    consecutive_minus=0,
                    jackpot_pity=0,
                    shadow_upgrade_chance=0
                )
                
                if current_auto_fat > 0:
                    interval = get_auto_fat_interval(current_auto_fat)
                    if interval:
                        next_time = datetime.now() + timedelta(hours=interval)
                        update_user_data(guild_id, user_id, next_auto_fat_time=next_time)
                
                try:
                    new_nick = format_nick_with_prestige(new_prestige, 0, user_name)
                    if len(new_nick) > 32:
                        new_nick = new_nick[:32]
                    await target.edit(nick=new_nick)
                except:
                    pass
                
                fresh_data = get_user_data(guild_id, user_id, user_name)
                current_anim = fresh_data.get('animations_enabled', 1)
                new_embed = create_profile_embed(fresh_data, current_anim)
                await msg.edit(embed=new_embed)
                
                success_embed = discord.Embed(
                    title="🌟 **ПРЕСТИЖ ПОЛУЧЕН!** 🌟",
                    description=f"{target.mention} достиг **{new_prestige}** уровня престижа!\n\n"
                               f"Вес сброшен до 0\n"
                               f"Все предметы удалены\n"
                               f"Улучшения **СОХРАНЕНЫ**!\n"
                               f"Опыт и уровень сохранены!",
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
                update_user_data(guild_id, user_id, number=new_number, auto_fat_level=new_level, next_auto_fat_time=next_time)
            else:
                update_field = {"fat_cd": "fat_cd_upgrade", "case_cd": "case_cd_upgrade", "luck": "luck_upgrade", "income": "income_upgrade"}[upgrade_type]
                update_user_data(guild_id, user_id, number=new_number, **{update_field: new_level})
            
            try:
                new_nick = format_nick_with_prestige(current_data.get('prestige', 0), new_number, user_name)
                if len(new_nick) > 32:
                    new_nick = new_nick[:32]
                await target.edit(nick=new_nick)
            except:
                pass
            
            fresh_data = get_user_data(guild_id, user_id, user_name)
            current_anim = fresh_data.get('animations_enabled', 1)
            new_embed = create_profile_embed(fresh_data, current_anim)
            await msg.edit(embed=new_embed)
            
            if upgrade_type == "fat_cd":
                new_bonus = get_fat_cd_reduction(new_level)
                bonus_text = f"КД !жир уменьшен на {new_bonus} мин"
            elif upgrade_type == "case_cd":
                new_bonus = get_case_cd_reduction(new_level)
                bonus_text = f"КД кейса уменьшен на {new_bonus} мин"
            elif upgrade_type == "luck":
                bonus_text = f"Удача увеличена до +{new_level * LUCK_CASE_BONUS_PER_LEVEL:.2f}% к редким и +{new_level * LUCK_UPGRADE_BONUS_PER_LEVEL:.2f}% к апгрейдам"
            elif upgrade_type == "income":
                new_bonus = get_income_bonus(new_level)
                bonus_text = f"Прибавка увеличена до +{(new_bonus-1)*100:.0f}% к доходу"
            elif upgrade_type == "auto_fat":
                interval = get_auto_fat_interval(new_level)
                bonus_text = f"Авто-жир будет срабатывать каждые {interval} час(ов)"
            
            success_embed = discord.Embed(
                title="✅ **УЛУЧШЕНИЕ ПОЛУЧЕНО!** ✅",
                description=f"{target.mention} улучшил характеристику!\n\n"
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
            try:
                await msg.clear_reactions()
            except:
                pass
            break

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
    
    if amount is None or amount.lower() == "все":
        duel_amount = min(challenger_data['current_number'], opponent_data['current_number'])
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
    
    embed = discord.Embed(
        title="🔫 **ВЫЗОВ НА ДУЭЛЬ!** 🔫",
        description=f"{challenger.mention} вызывает {opponent.mention} на дуэль!\n\n"
                   f"**Ставка:** {amount_text}\n\n"
                   f"Оба должны нажать ✅ чтобы принять\n"
                   f"Если кто-то нажмёт ❌ - дуэль отменяется",
        color=0xff5500
    )
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
                await duel_msg.clear_reactions()
                update_user_data(guild_id, str(challenger.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
                update_user_data(guild_id, str(opponent.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
                decline_embed = discord.Embed(title="❌ Дуэль отклонена", description=f"{user.mention} отказался от дуэли!", color=0xff0000)
                await duel_msg.edit(embed=decline_embed)
                return
            if str(reaction.emoji) == "✅" and user.id not in accepted_users:
                accepted_users.add(user.id)
        
        if duel_cancelled:
            return
        
        await duel_msg.clear_reactions()
        
        if both_animations_on:
            result = await duel_animation(duel_msg, challenger, opponent)
        else:
            result = random.randint(0, 2)
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
            result_embed = discord.Embed(
                title="⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️",
                description=f"**{c_name}** ⚔️ **{o_name}**\n\n{result_text}",
                color=result_color
            )
            await duel_msg.edit(embed=result_embed)
            await asyncio.sleep(1.5)
        
        if result == 0:
            winner, loser = challenger, opponent
            winner_new_weight = challenger_data['current_number'] + duel_amount
            loser_new_weight = opponent_data['current_number'] - duel_amount
            update_user_data(guild_id, str(winner.id), number=winner_new_weight)
            update_user_data(guild_id, str(loser.id), number=loser_new_weight)
            levels_gained, kg_reward, new_level = add_xp(guild_id, str(winner.id), XP_PER_DUEL_WIN)
            result_description = f"**Победитель:** {winner.mention}\n\n📊 **Результаты:**\n{winner.mention}: {challenger_data['current_number']}кг → **{winner_new_weight}кг** (+{duel_amount})\n{loser.mention}: {opponent_data['current_number']}кг → **{loser_new_weight}кг** (-{duel_amount})"
            if levels_gained > 0:
                result_description += f"\n\n⭐ +{kg_reward} кг за повышение уровня! Теперь у вас **{new_level}** уровень!"
        elif result == 1:
            winner, loser = opponent, challenger
            winner_new_weight = opponent_data['current_number'] + duel_amount
            loser_new_weight = challenger_data['current_number'] - duel_amount
            update_user_data(guild_id, str(winner.id), number=winner_new_weight)
            update_user_data(guild_id, str(loser.id), number=loser_new_weight)
            levels_gained, kg_reward, new_level = add_xp(guild_id, str(winner.id), XP_PER_DUEL_WIN)
            result_description = f"**Победитель:** {winner.mention}\n\n📊 **Результаты:**\n{winner.mention}: {opponent_data['current_number']}кг → **{winner_new_weight}кг** (+{duel_amount})\n{loser.mention}: {challenger_data['current_number']}кг → **{loser_new_weight}кг** (-{duel_amount})"
            if levels_gained > 0:
                result_description += f"\n\n⭐ +{kg_reward} кг за повышение уровня! Теперь у вас **{new_level}** уровень!"
        else:
            update_user_data(guild_id, str(challenger.id), number=challenger_data['current_number'])
            update_user_data(guild_id, str(opponent.id), number=opponent_data['current_number'])
            result_description = f"🤝 **НИЧЬЯ!** 🤝\n\n📊 **Результаты:**\n{challenger.mention}: {challenger_data['current_number']}кг → **{challenger_data['current_number']}кг** (без изменений)\n{opponent.mention}: {opponent_data['current_number']}кг → **{opponent_data['current_number']}кг** (без изменений)"
        
        update_user_data(guild_id, str(challenger.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        update_user_data(guild_id, str(opponent.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        
        await update_user_nick(guild_id, str(winner.id) if result != 2 else str(challenger.id))
        await update_user_nick(guild_id, str(loser.id) if result != 2 else str(opponent.id))
        
        result_embed = discord.Embed(
            title="⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️",
            description=result_description,
            color=0xffd700 if result == 0 else (0xc0c0c0 if result == 1 else 0x9b59b6)
        )
        result_embed.set_footer(text="⚔️ Следующая дуэль доступна!")
        await ctx.send(embed=result_embed)
        
    except asyncio.TimeoutError:
        await duel_msg.clear_reactions()
        update_user_data(guild_id, str(challenger.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        update_user_data(guild_id, str(opponent.id), duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_channel_id=None, duel_initiator=0, duel_start_time=None)
        timeout_embed = discord.Embed(title="⏰ Время вышло", description=f"Не все успели принять дуэль. Дуэль отменена.", color=0xffaa00)
        await duel_msg.edit(embed=timeout_embed)

@bot.command(name='отмена')
async def cancel_duel(ctx):
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
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

@bot.command(name='апгрейд')
async def upgrade_command(ctx, choice: str = None):
    guild_id = ctx.guild.id
    member = ctx.author
    data = get_user_data(guild_id, str(member.id), member.name)
    if data.get('upgrade_active', 0) == 1:
        await ctx.send("⚠️ У вас уже есть активный апгрейд! Дождитесь его завершения.")
        return
    items_dict = get_user_items(data['item_counts'])
    available_items = [{"name": item_name, "count": count, "price": get_item_price(item_name), "emoji": ITEM_EMOJIS.get(item_name, "📦")} for item_name, count in items_dict.items() if count > 0 and get_item_price(item_name) > 0]
    available_items.sort(key=lambda x: x["price"])
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
        if item_index < 0 or item_index >= len(available_items):
            await ctx.send(f"❌ Неверный номер! Введите число от 1 до {len(available_items)}")
            return
    except ValueError:
        await ctx.send("❌ Введите корректный номер!")
        return
    selected_item = available_items[item_index]
    items_dict[selected_item["name"]] -= 1
    if items_dict[selected_item["name"]] <= 0:
        del items_dict[selected_item["name"]]
    update_user_data(guild_id, str(member.id), item_counts=save_user_items(items_dict), last_command="upgrade_select", last_command_target=selected_item["name"], last_command_use_time=datetime.now(), upgrade_active=1, upgrade_data=json.dumps({'source_item': selected_item["name"]}))
    possible_upgrades = get_possible_upgrades(selected_item["name"], 1)
    if not possible_upgrades:
        items_dict[selected_item["name"]] = items_dict.get(selected_item["name"], 0) + 1
        update_user_data(guild_id, str(member.id), item_counts=save_user_items(items_dict), upgrade_active=0)
        await ctx.send(f"❌ Для **{selected_item['emoji']} {selected_item['name']}** нет доступных улучшений! Предмет возвращён.")
        return
    max_display = 15
    display_upgrades = possible_upgrades[:max_display]
    remaining_count = len(possible_upgrades) - max_display
    embed = discord.Embed(title="🔧 **ВЫБОР ЦЕЛИ АПГРЕЙДА** 🔧", description=f"{member.mention}, вы выбрали: **{selected_item['emoji']} {selected_item['name']}**\n\nТеперь выберите цель (используйте `!выбрать [номер]`):", color=0x3498db)
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
    if data.get('upgrade_active', 0) == 1:
        await ctx.send("⚠️ У вас уже есть активный апгрейд! Дождитесь его завершения.")
        return
    if data['current_number'] < amount:
        await ctx.send(f"❌ У вас недостаточно кг! Есть: {data['current_number']} кг, нужно: {amount} кг")
        return
    new_number = data['current_number'] - amount
    update_user_data(guild_id, str(member.id), number=new_number, last_command="upgrade_kg_select", last_command_target=str(amount), last_command_use_time=datetime.now(), upgrade_active=1, upgrade_data=json.dumps({'amount': amount}))
    all_items = set([item["name"] for item in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
    possible_upgrades = []
    for item_name in all_items:
        target_price = get_item_price(item_name)
        if target_price == 0 or target_price < amount:
            continue
        chance = amount / target_price
        if chance < 0.01:
            continue
        is_case = any(case.get("name") == item_name for case in CASES.values())
        case_id = next((cid for cid, case in CASES.items() if case.get("name") == item_name), None)
        possible_upgrades.append({"name": item_name, "price": target_price, "chance": chance, "emoji": ITEM_EMOJIS.get(item_name, "🎁"), "is_case": is_case, "case_id": case_id})
    possible_upgrades.sort(key=lambda x: x["price"])
    if not possible_upgrades:
        update_user_data(guild_id, str(member.id), number=data['current_number'], upgrade_active=0)
        await ctx.send(f"❌ На {amount} кг нет доступных улучшений! Кг возвращены.")
        return
    max_display = 15
    display_upgrades = possible_upgrades[:max_display]
    remaining_count = len(possible_upgrades) - max_display
    embed = discord.Embed(title="💱 **АПГРЕЙД КГ В ПРЕДМЕТЫ** 💱", description=f"{member.mention}, вы потратили **{amount} кг**!\n\nВыберите цель (используйте `!выбрать [номер]`):", color=0xffaa00)
    upgrades_text = "\n".join([f"**{i+1}.** {upgrade['emoji']} **{upgrade['name']}** — {upgrade['chance']*100:.1f}% шанс (нужно: {upgrade['price']} кг)" for i, upgrade in enumerate(display_upgrades)])
    if remaining_count > 0:
        upgrades_text += f"\n\n... и ещё {remaining_count} предметов"
    embed.add_field(name="📈 Возможные улучшения", value=upgrades_text, inline=False)
    embed.set_footer(text="Кг уже списаны! Всего доступно: {} предметов".format(len(possible_upgrades)))
    await ctx.send(embed=embed)

@bot.command(name='выбрать')
async def choose_upgrade(ctx, choice: str = None, count: int = 1):
    if not choice:
        await ctx.send("❌ Укажите номер!")
        return
    guild_id = ctx.guild.id
    member = ctx.author
    data = get_user_data(guild_id, str(member.id), member.name)
    if data.get('upgrade_active', 0) != 1:
        await ctx.send("❌ У вас нет активного апгрейда! Сначала используйте `!апгрейд` или `!апгрейдкг`.")
        return
    last_command = data.get('last_command')
    last_use = data.get('last_command_use_time')
    if isinstance(last_use, str):
        last_use = datetime.fromisoformat(last_use) if last_use else None
    if not last_command or not last_use or (datetime.now() - last_use).total_seconds() > 300:
        await ctx.send("❌ Время ожидания истекло. Используйте команду заново!")
        update_user_data(guild_id, str(member.id), upgrade_active=0)
        return
    
    user_data = get_user_data(guild_id, str(member.id), member.name)
    animations_enabled = are_animations_enabled(user_data)
    
    if last_command == "upgrade_kg_select":
        amount = int(data['last_command_target'])
        all_items = set([item["name"] for item in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
        possible_upgrades = []
        for item_name in all_items:
            target_price = get_item_price(item_name)
            if target_price == 0 or target_price < amount:
                continue
            chance = amount / target_price
            if chance < 0.01:
                continue
            is_case = any(case.get("name") == item_name for case in CASES.values())
            case_id = next((cid for cid, case in CASES.items() if case.get("name") == item_name), None)
            possible_upgrades.append({"name": item_name, "price": target_price, "chance": chance, "emoji": ITEM_EMOJIS.get(item_name, "🎁"), "is_case": is_case, "case_id": case_id})
        possible_upgrades.sort(key=lambda x: x["price"])
        try:
            item_index = int(choice) - 1
            if item_index < 0 or item_index >= len(possible_upgrades):
                await ctx.send(f"❌ Неверный номер! Введите число от 1 до {len(possible_upgrades)}")
                update_user_data(guild_id, str(member.id), upgrade_active=0)
                return
        except ValueError:
            await ctx.send("❌ Введите корректный номер!")
            update_user_data(guild_id, str(member.id), upgrade_active=0)
            return
        target_item = possible_upgrades[item_index]
        update_user_data(guild_id, str(member.id), upgrade_active=0)
        await upgrade_kg_animation(ctx, member, amount, target_item, 
                                  get_prestige_luck(user_data.get('prestige', 0)), 
                                  user_data.get('luck_upgrade', 0),
                                  animations_enabled)
    elif last_command == "upgrade_select":
        source_item = data.get('last_command_target')
        if not source_item:
            await ctx.send("❌ Ошибка: не выбран исходный предмет!")
            update_user_data(guild_id, str(member.id), upgrade_active=0)
            return
        possible_upgrades = get_possible_upgrades(source_item, 1)
        if not possible_upgrades:
            await ctx.send("❌ Для этого предмета больше нет доступных улучшений!")
            update_user_data(guild_id, str(member.id), upgrade_active=0)
            return
        try:
            item_index = int(choice) - 1
            if item_index < 0 or item_index >= len(possible_upgrades):
                await ctx.send(f"❌ Неверный номер! Введите число от 1 до {len(possible_upgrades)}")
                update_user_data(guild_id, str(member.id), upgrade_active=0)
                return
        except ValueError:
            await ctx.send("❌ Введите корректный номер!")
            update_user_data(guild_id, str(member.id), upgrade_active=0)
            return
        target_item = possible_upgrades[item_index]
        update_user_data(guild_id, str(member.id), upgrade_active=0)
        await upgrade_animation(ctx, member, source_item, target_item, 1, 
                               get_prestige_luck(user_data.get('prestige', 0)), 
                               user_data.get('luck_upgrade', 0),
                               animations_enabled)
    else:
        await ctx.send("❌ Неизвестный тип апгрейда!")
        update_user_data(guild_id, str(member.id), upgrade_active=0)

@bot.command(name='магазин')
async def shop_command(ctx):
    data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    update_user_data(ctx.guild.id, str(ctx.author.id), last_command="shop", last_command_use_time=datetime.now())
    slots, last_update, next_update = await ensure_shop_updated(ctx.guild.id)
    slots = slots if isinstance(slots, list) else []
    embed = discord.Embed(title="🏪 **МАГАЗИН** 🏪", description="Доступные предметы (используйте `!купить [слот] [количество]`):\n📦 **Слоты 1-4:** Кейсы | 🛒 **Слоты 5-10:** Предметы", color=0xffaa00)
    items_text = ""
    for i in range(1, SHOP_SLOTS + 1):
        slot = slots[i-1] if i-1 < len(slots) else None
        if slot and isinstance(slot, dict):
            prefix = "📦" if i <= 4 else "🛒"
            items_text += f"**{i}.** {prefix} {slot.get('emoji', '📦')} {slot.get('name', 'Неизвестно')} — {slot.get('amount', 0)} шт — **{slot.get('price', 0)} кг/шт**\n   └ {slot.get('description', 'Нет описания')}\n"
        else:
            items_text += f"**{i}.** {'📦🕳️ Пустой слот для кейса' if i <= 4 else '🛒🕳️ Пустой слот для предмета'}\n"
        if len(items_text) > 900:
            items_text += "... и ещё товары"
            break
    embed.add_field(name="📦 Товары", value=items_text, inline=False)
    last_update_str = last_update.strftime("%d.%m.%Y %H:%M") if last_update else "Никогда"
    next_update_str = next_update.strftime("%d.%m.%Y %H:%M") if next_update else "Скоро"
    case_count = sum(1 for s in slots[:4] if s)
    item_count = sum(1 for s in slots[4:] if s)
    embed.add_field(name="📊 Статистика магазина", value=f"📦 Кейсов: {case_count}/4\n🛒 Предметов: {item_count}/6\n⏰ Обновление каждые {SHOP_UPDATE_HOURS} ч", inline=False)
    embed.add_field(name="⏰ Время обновления", value=f"Последнее: {last_update_str}\nСледующее: {next_update_str}", inline=False)
    embed.set_footer(text="💸 Тратьте кг с умом!")
    await ctx.send(embed=embed)

@bot.command(name='купить')
async def buy_command(ctx, slot: int, amount: int = 1):
    if slot < 1 or slot > SHOP_SLOTS:
        await ctx.send(f"❌ Слот должен быть от 1 до {SHOP_SLOTS}!")
        return
    if amount <= 0:
        await ctx.send("❌ Количество должно быть больше 0!")
        return
    data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    last_use = data.get('last_command_use_time')
    if isinstance(last_use, str):
        last_use = datetime.fromisoformat(last_use) if last_use else None
    if data.get('last_command') != "shop" or not last_use or (datetime.now() - last_use).total_seconds() > 300:
        await ctx.send("❌ Сначала используйте `!магазин` для просмотра товаров!")
        return
    slots, last_update, next_update = await ensure_shop_updated(ctx.guild.id)
    if slot - 1 >= len(slots) or not slots[slot - 1]:
        await ctx.send(f"❌ В слоте {slot} ничего нет!")
        return
    item = slots[slot - 1]
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
        if not case_id or case_id not in CASES:
            await ctx.send(f"❌ Ошибка: неизвестный тип кейса!")
            return
        cases_dict[case_id] = cases_dict.get(case_id, 0) + amount
        purchase_desc = f"{item.get('emoji', '📦')} {item.get('name', 'Кейс')} x{amount}"
        update_user_data(ctx.guild.id, str(ctx.author.id), number=new_number, cases_dict=cases_dict, last_command=None, last_command_use_time=None)
    else:
        items_dict = get_user_items(data['item_counts'])
        items_dict[item["name"]] = items_dict.get(item["name"], 0) + amount
        purchase_desc = f"{item['name']} x{amount}"
        update_user_data(ctx.guild.id, str(ctx.author.id), number=new_number, item_counts=save_user_items(items_dict), last_command=None, last_command_use_time=None)
    update_shop_data(ctx.guild.id, slots, last_update, next_update)
    levels_gained, kg_reward, new_level = add_xp(ctx.guild.id, str(ctx.author.id), XP_PER_SHOP_BUY)
    await update_user_nick(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    embed = discord.Embed(title="✅ Покупка совершена!", description=f"**{ctx.author.mention}** приобрёл товары!", color=0x00ff00)
    embed.add_field(name="📦 Предмет", value=purchase_desc, inline=True)
    embed.add_field(name="💰 Цена", value=f"{total_price} кг", inline=True)
    embed.add_field(name="💸 Осталось", value=f"{new_number} кг", inline=True)
    if levels_gained > 0:
        embed.add_field(name="⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐", value=f"+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!", inline=False)
    await ctx.send(embed=embed)

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
    giver_data = get_user_data(ctx.guild.id, str(ctx.author.id), ctx.author.name)
    target_data = get_user_data(ctx.guild.id, str(target.id), target.name)
    if giver_data['current_number'] < amount:
        await ctx.send(f"❌ У вас недостаточно кг! Есть: {giver_data['current_number']} кг, нужно: {amount} кг")
        return
    new_giver_number = giver_data['current_number'] - amount
    new_target_number = target_data['current_number'] + amount
    update_user_data(ctx.guild.id, str(ctx.author.id), number=new_giver_number)
    update_user_data(ctx.guild.id, str(target.id), number=new_target_number)
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
    embed.add_field(name="💰 **ЭКОНОМИКА**", value="`!магазин` - магазин предметов\n`!купить [слот] [кол-во]` - купить предмет\n`!продать [предмет] [кол-во]` - продать предмет\n`!продать всё` - продать всё\n`!датьжир [@user] [кол-во]` - передать кг\n`!датьпредмет [@user] [кол-во] [предмет]` - передать предмет", inline=False)
    embed.add_field(name="⭐ **ХАРАКТЕРИСТИКИ**", value="• **КД !жир** — уменьшает время ожидания\n• **КД кейса** — уменьшает время ожидания кейса\n• **Удача** — повышает шансы в кейсах и апгрейдах\n• **Прибавка** — +5% к доходу от предметов за уровень\n• **Престиж** — +10% ко всем кг, +1% к шансам и +50% к опыту за уровень\n• **Авто-жир** — автоматический !жир", inline=False)
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
    cursor.execute('UPDATE user_fat SET current_number = 0, consecutive_plus = 0, consecutive_minus = 0, jackpot_pity = 0, item_counts = "{}"')
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    for member in ctx.guild.members:
        await update_user_nick(ctx.guild.id, str(member.id), member.name)
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
    if data.get('upgrade_active', 0) == 1:
        last_command = data.get('last_command')
        last_target = data.get('last_command_target')
        if last_command == "upgrade_select":
            items_dict = get_user_items(data['item_counts'])
            items_dict[last_target] = items_dict.get(last_target, 0) + 1
            update_user_data(guild_id, user_id, item_counts=save_user_items(items_dict), upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
            cancelled_items.append(f"🔧 Апгрейд предмета ({last_target} возвращён)")
        elif last_command == "upgrade_kg_select":
            try:
                amount = int(last_target)
                new_number = data['current_number'] + amount
                update_user_data(guild_id, user_id, number=new_number, upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
                cancelled_items.append(f"💱 Апгрейд кг ({amount} кг возвращены)")
            except:
                update_user_data(guild_id, user_id, upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
                cancelled_items.append("💱 Апгрейд кг (отменён)")
        else:
            update_user_data(guild_id, user_id, upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
            cancelled_items.append("🔧 Апгрейд (отменён)")
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
            
            guild_data.append({
                'name': guild.name,
                'members': stats['total_users'],
                'total_weight': stats['total_weight'],
                'avg_weight': stats['avg_weight']
            })
        except Exception as e:
            continue
    
    if not guild_data:
        await ctx.send("📭 Нет данных по серверам!")
        return
    
    guild_data.sort(key=lambda x: x['total_weight'], reverse=True)
    
    embed = discord.Embed(
        title="🌍 **ГЛОБАЛЬНЫЙ РЕЙТИНГ СЕРВЕРОВ** 🌍",
        description="Топ серверов по общей массе жира",
        color=0xffaa00
    )
    
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
        
        if guild['total_weight'] >= 1000:
            weight_display = f"{guild['total_weight']/1000:.1f}т"
        else:
            weight_display = f"{guild['total_weight']}кг"
        
        leaderboard_text += f"{place_icon} **{i}.** {guild['name'][:30]}\n"
        leaderboard_text += f"   📦 **{weight_display}** | 👥 {guild['members']} уч.\n"
        leaderboard_text += f"   📊 Средний вес: {guild['avg_weight']:.0f}кг\n\n"
        
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
    
    embed.add_field(
        name="📊 **ГЛОБАЛЬНАЯ СТАТИСТИКА**",
        value=(
            f"🌍 Серверов: **{total_servers}**\n"
            f"👥 Участников: **{total_global_members}**\n"
            f"⚖️ Всего массы: **{global_display}**"
        ),
        inline=False
    )
    
    embed.set_footer(text="🏆 Топ-10 серверов")
    
    await ctx.send(embed=embed)

@bot.event
async def on_ready():
    print(f"\n✅ Бот успешно запущен как {bot.user}")
    await migrate_old_burgers_to_prestige()
    await migrate_old_autoburgers_to_auto_fat()
    bot.loop.create_task(auto_fat_loop())
    bot.loop.create_task(passive_income_loop())
    bot.loop.create_task(snatcher_loop())
    bot.loop.create_task(apply_hourly_effects())
    bot.loop.create_task(daily_case_accumulation_loop())  # ← ЭТА СТРОКА ДОЛЖНА БЫТЬ

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Не указан обязательный аргумент: {error.param.name}")
    elif isinstance(error, commands.BadArgument):
        await ctx.send(f"❌ Неверный формат аргумента")
    else:
        await ctx.send(f"❌ Произошла ошибка: {str(error)}")
        print(f"Ошибка в команде {ctx.command}: {error}")

if __name__ == "__main__":
    print("🚀 Запуск бота...")
    bot.run(TOKEN)
