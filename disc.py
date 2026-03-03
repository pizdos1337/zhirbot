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

# ===== НАСТРОЙКИ =====
# Токен берётся из переменных окружения!
TOKEN = os.environ.get('DISCORD_BOT_TOKEN')

PREFIX = "!"  # Префикс команд
DB_FOLDER = "guild_databases"  # Папка для хранения баз данных серверов
COOLDOWN_HOURS = 1  # Кулдаун на команду !жир в часах
TESTER_ROLE_NAME = "тестер"  # Название роли для доступа к тестерским командам

# Настройки вероятностей (скрыты от пользователей)
BASE_MINUS_CHANCE = 0.3  # Базовый шанс на минус (30%)
MAX_MINUS_CHANCE = 0.9   # Максимальный шанс на минус (90%)
PITY_INCREMENT = 0.15    # На сколько увеличивается шанс за каждый плюс подряд (15%)

# Настройки накопления на плюс от минусов
CONSECUTIVE_MINUS_BOOST = 0.2  # Каждый минус подряд даёт +20% к шансу на плюс
MAX_CONSECUTIVE_MINUS_BOOST = 0.8  # Максимальный бонус 80%

# Настройки джекпота (скрыты от пользователей)
BASE_JACKPOT_CHANCE = 0.001  # Базовый шанс на джекпот (0.1%)
JACKPOT_PITY_INCREMENT = 0.001  # Увеличение шанса за каждое использование (0.1%)
MAX_JACKPOT_CHANCE = 0.05  # Максимальный шанс на джекпот (5%)
JACKPOT_MIN = 100  # Минимальный джекпот
JACKPOT_MAX = 500  # Максимальный джекпот

# Настройки кейса
CASE_COOLDOWN_HOURS = 24  # Кулдаун на !жиркейс (24 часа)

# Призы в кейсе (weight, chance, description)
CASE_PRIZES = [
    {"value": -20, "chance": 5.0, "emoji": "📉", "name": "-20 кг"},
    {"value": -10, "chance": 15.0, "emoji": "📊", "name": "-10 кг"},
    {"value": 0, "chance": 20.0, "emoji": "🔄", "name": "Ничего"},
    {"value": 10, "chance": 10.0, "emoji": "📈", "name": "+10 кг"},
    {"value": 20, "chance": 7.0, "emoji": "⬆️", "name": "+20 кг"},
    {"value": 50, "chance": 3.0, "emoji": "🚀", "name": "+50 кг"},
    {"value": 100, "chance": 2.0, "emoji": "💫", "name": "+100 кг"},
    {"value": 200, "chance": 1.0, "emoji": "⭐", "name": "+200 кг"},
    {"value": 1000, "chance": 0.5, "emoji": "🌟", "name": "+1000 кг"},
    {"value": 5000, "chance": 0.001, "emoji": "💥", "name": "+5000 кг"},
    {"value": "autoburger", "chance": 0.00001, "emoji": "🍔✨", "name": "АВТОБУРГЕР"},
]

# Нормализуем шансы, чтобы сумма была 100%
total_chance = sum(prize["chance"] for prize in CASE_PRIZES)
for prize in CASE_PRIZES:
    prize["normalized_chance"] = (prize["chance"] / total_chance) * 100

# Настройки Автобургера
AUTOBURGER_INTERVALS = [6, 4, 2, 1]  # Интервалы в часах для 1,2,3,4+ автобургеров
AUTOBURGER_MAX_BONUS = 0.6      # Максимальный бонус к плюсу (60%)
AUTOBURGER_GROWTH_RATE = 0.03   # Скорость роста (0.03 даёт 60% при 100 бургерах)
# =====================

# Проверяем, что токен найден
if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найдена переменная окружения DISCORD_BOT_TOKEN!")
    print("📌 Убедитесь, что на хостинге установлена переменная окружения с токеном бота")
    print("🚫 Бот не может быть запущен без токена")
    exit(1)

# ===== ЗАЩИТА БАЗЫ ДАННЫХ =====
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
# ==============================

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
    """
    Определяет звание по весу
    Возвращает (название_звания, эмодзи)
    """
    for rank in RANKS:
        if rank["min"] <= weight <= rank["max"]:
            return rank["name"], rank["emoji"]
    if weight > 99999999:
        return "🌀 Бесконечность", "🌀"
    if weight < -999:
        return "Черная дыра", "💀"
    return "❓ Неопределённый", "❓"

# Создаём папку для баз данных, если её нет
if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)
    print(f"📁 Создана папка для баз данных: {DB_FOLDER}")

# Настройка бота
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# ===== РАБОТА С БАЗОЙ ДАННЫХ (для конкретного сервера) =====
def get_db_path(guild_id):
    """Возвращает путь к файлу базы данных для конкретного сервера"""
    return os.path.join(DB_FOLDER, f"guild_{guild_id}.db")

def init_guild_database(guild_id):
    """Создаёт таблицы в базе данных для конкретного сервера, если их нет"""
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
            next_autoburger_time TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"✅ База данных инициализирована для сервера {guild_id}")

def get_user_data(guild_id, user_id, user_name=None):
    """
    Получает данные пользователя из БД конкретного сервера
    """
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('SELECT current_number, last_command_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time FROM user_fat WHERE user_id = ?', (str(user_id),))
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
    else:
        number = 0
        last_time = None
        consecutive_plus = 0
        consecutive_minus = 0
        jackpot_pity = 0
        autoburger_count = 0
        last_case_time = None
        next_autoburger_time = None
        cursor.execute('''
            INSERT INTO user_fat (user_id, user_name, current_number, last_command_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time))
        conn.commit()
    
    conn.close()
    return number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time

def update_user_data(guild_id, user_id, new_number, user_name=None, consecutive_plus=None, consecutive_minus=None, jackpot_pity=None, autoburger_count=None, last_case_time=None, next_autoburger_time=None):
    """Обновляет данные пользователя в БД"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    current_time = datetime.now()
    
    updates = ["current_number = ?", "user_name = ?", "last_command_time = ?"]
    values = [new_number, user_name or "Unknown", current_time]
    
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
    
    values.append(str(user_id))
    
    query = f"UPDATE user_fat SET {', '.join(updates)} WHERE user_id = ?"
    cursor.execute(query, values)
    
    if cursor.rowcount == 0:
        cursor.execute('''
            INSERT INTO user_fat (user_id, user_name, current_number, last_command_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", new_number, current_time, consecutive_plus or 0, consecutive_minus or 0, jackpot_pity or 0, autoburger_count or 0, last_case_time, next_autoburger_time))
    
    conn.commit()
    conn.close()
    
    # Бекап
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
    
    cursor.execute('UPDATE user_fat SET last_command_time = NULL')
    affected_rows = cursor.rowcount
    conn.commit()
    conn.close()
    
    return affected_rows

def reset_all_weights(guild_id):
    """Сбрасывает вес для всех пользователей на 0"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('UPDATE user_fat SET current_number = 0, consecutive_plus = 0, consecutive_minus = 0, jackpot_pity = 0, autoburger_count = 0')
    affected_rows = cursor.rowcount
    conn.commit()
    conn.close()
    
    return affected_rows

def get_all_users_sorted(guild_id):
    """
    Получает всех пользователей конкретного сервера отсортированных по числу (убывание)
    """
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT user_name, current_number, last_command_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count
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
    
    return {
        'total_users': total_users,
        'total_weight': total_weight,
        'avg_weight': avg_weight,
        'positive': positive,
        'negative': negative,
        'zero': zero,
        'total_autoburgers': total_autoburgers
    }

def check_cooldown(last_command_time, cooldown_hours):
    """Проверяет, прошёл ли кулдаун"""
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
    """Форматирует секунды в читаемый вид"""
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

def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count=0):
    """
    Определяет изменение веса с учётом всех механик
    Автобургеры: экспоненциальный рост с насыщением
    """
    # Экспоненциальный бонус от автобургеров
    if autoburger_count > 0:
        # bonus = MAX_BONUS * (1 - e^(-rate * count))
        autoburger_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count))
    else:
        autoburger_boost = 0
    
    # Бонус от минусов подряд
    minus_boost = min(consecutive_minus * CONSECUTIVE_MINUS_BOOST, MAX_CONSECUTIVE_MINUS_BOOST)
    
    # Рассчитываем текущий шанс на минус
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - autoburger_boost - minus_boost
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))
    
    # Рассчитываем текущий шанс на джекпот
    jackpot_chance = BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT)
    jackpot_chance = min(jackpot_chance, MAX_JACKPOT_CHANCE)
    
    # Сначала проверяем джекпот
    jackpot_roll = random.random()
    if jackpot_roll < jackpot_chance:
        change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
        new_consecutive_plus = consecutive_plus + 1
        new_consecutive_minus = 0
        new_jackpot_pity = 0
        was_minus = False
        was_jackpot = True
        return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    # Проверяем минус/плюс
    roll = random.random()
    
    if roll < minus_chance:
        # Выпал минус
        change = random.randint(-20, -1)
        new_consecutive_plus = 0
        new_consecutive_minus = consecutive_minus + 1
        new_jackpot_pity = jackpot_pity + 1
        was_minus = True
        was_jackpot = False
    else:
        # Выпал плюс
        change = random.randint(1, 20)
        new_consecutive_plus = consecutive_plus + 1
        new_consecutive_minus = 0
        new_jackpot_pity = jackpot_pity + 1
        was_minus = False
        was_jackpot = False
    
    return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot

def get_case_prize():
    """Определяет приз из кейса"""
    roll = random.random() * 100
    
    cumulative = 0
    for prize in CASE_PRIZES:
        cumulative += prize["normalized_chance"]
        if roll < cumulative:
            return prize
    
    return CASE_PRIZES[-1]

def get_autoburger_interval(autoburger_count):
    """Возвращает интервал в часах для автобургера"""
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
        current_number, _, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, _, _ = get_user_data(guild_id, user_id, user_name)
        
        change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
            consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count
        )
        
        new_number = current_number + change
        
        update_user_data(
            guild_id, user_id, new_number, user_name,
            new_consecutive_plus, new_consecutive_minus, new_jackpot_pity,
            autoburger_count
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
                
                new_nick = f"{new_number}kg {clean_name}"
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

# ===== КОМАНДЫ БОТА =====

@bot.event
async def on_ready():
    print(f"\n{'='*50}")
    print(f"✅ Бот успешно запущен как {bot.user}")
    print(f"📊 ID бота: {bot.user.id}")
    print(f"⏰ Кулдаун на !жир: {COOLDOWN_HOURS} часов")
    print(f"📦 Кулдаун на !жиркейс: {CASE_COOLDOWN_HOURS} часов")
    print(f"🎭 Роль для тестерских команд: {TESTER_ROLE_NAME}")
    print(f"📁 Базы данных хранятся в папке: {DB_FOLDER}")
    print(f"🍔 Система автобургеров активна")
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n📋 Серверы, на которых присутствует бот:")
    for guild in bot.guilds:
        print(f"  - {guild.name} (ID: {guild.id}, участников: {guild.member_count})")
        init_guild_database(guild.id)
    
    print(f"{'='*50}\n")
    
    bot.loop.create_task(autoburger_loop())

@bot.event
async def on_guild_join(guild):
    print(f"✅ Бот добавлен на новый сервер: {guild.name} (ID: {guild.id})")
    init_guild_database(guild.id)
    
    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            embed = discord.Embed(
                title="🍔 Жирный бот прибыл!",
                description="Привет! Я бот для жирных преобразований!\n\n"
                           f"**Команды:**\n"
                           f"`!жир` - изменить свой вес (кулдаун {COOLDOWN_HOURS} ч)\n"
                           f"`!жиркейс` - открыть кейс с призами (кулдаун 24 ч)\n"
                           f"`!жиротрясы` - таблица рекордов со званиями\n"
                           f"`!жир_инфо` - информация о весе\n"
                           f"`!жир_кулдаун` - статус кулдауна",
                color=0xffaa00
            )
            await channel.send(embed=embed)
            break

@bot.command(name='жир')
async def fat_command(ctx):
    """
    Меняет Display Name пользователя на "{число}kg {оригинальный ник}"
    """
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, _, _ = get_user_data(guild_id, user_id, user_name)
    
    can_use, remaining = check_cooldown(last_time, COOLDOWN_HOURS)
    
    if not can_use:
        embed = discord.Embed(
            title="⏳ Подождите!",
            description=f"{member.mention}, вы уже использовали команду недавно!",
            color=0xff0000
        )
        embed.add_field(name="Осталось подождать", value=format_time(remaining), inline=True)
        embed.add_field(name="Кулдаун", value=f"{COOLDOWN_HOURS} часов", inline=True)
        embed.set_footer(text="Приходите взвешиваться позже!")
        
        await ctx.send(embed=embed)
        return
    
    change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
        consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count
    )
    new_number = current_number + change
    
    update_user_data(
        guild_id, user_id, new_number, user_name,
        new_consecutive_plus, new_consecutive_minus, new_jackpot_pity,
        autoburger_count
    )
    
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
    
    new_nick = f"{new_number}kg {clean_name}"
    
    if len(new_nick) > 32:
        new_nick = new_nick[:32]
    
    try:
        await member.edit(nick=new_nick)
        
        rank_name, rank_emoji = get_rank(new_number)
        
        if was_jackpot:
            embed_color = 0xffd700
            embed_title = "💰 ДЖЕКПОТ! 💰"
        else:
            embed_color = 0xff9933 if new_number >= 0 else 0x66ccff
            embed_title = "🍔 Жирная трансформация"
        
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
        
        embed.add_field(name="⏰ Следующая команда", value=f"через {COOLDOWN_HOURS} часов", inline=True)
        embed.set_footer(text=f"Новый ник: {new_nick}")
        
        await ctx.send(embed=embed)
        
    except discord.Forbidden:
        await ctx.send(f"❌ У меня нет прав менять никнейм для {member.mention} на этом сервере!")
    except discord.HTTPException as e:
        await ctx.send(f"❌ Ошибка при смене ника: {e}")

@bot.command(name='жиркейс')
async def fat_case(ctx):
    """Открывает кейс с призами"""
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, user_name)
    
    can_use, remaining = check_cooldown(last_case_time, CASE_COOLDOWN_HOURS)
    
    if not can_use:
        embed = discord.Embed(
            title="⏳ Подождите!",
            description=f"{member.mention}, вы уже открывали кейс недавно!",
            color=0xff0000
        )
        embed.add_field(name="Осталось подождать", value=format_time(remaining), inline=True)
        embed.add_field(name="Кулдаун кейса", value=f"{CASE_COOLDOWN_HOURS} часов", inline=True)
        
        await ctx.send(embed=embed)
        return
    
    prize = get_case_prize()
    
    new_autoburger_count = autoburger_count
    new_number = current_number
    new_next_autoburger_time = next_autoburger_time
    prize_description = ""
    
    if prize["value"] == "autoburger":
        new_autoburger_count = autoburger_count + 1
        
        interval = get_autoburger_interval(new_autoburger_count)
        if interval:
            new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
        
        prize_description = f"**+1 {prize['emoji']}**"
    else:
        new_number = current_number + prize["value"]
        prize_description = f"**{prize['value']:+d} кг** {prize['emoji']}"
    
    current_time = datetime.now()
    update_user_data(
        guild_id, user_id, new_number, user_name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        new_autoburger_count, current_time, new_next_autoburger_time
    )
    
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
            
            new_nick = f"{new_number}kg {clean_name}"
            if len(new_nick) > 32:
                new_nick = new_nick[:32]
            
            await member.edit(nick=new_nick)
        except:
            pass
    
    rank_name, rank_emoji = get_rank(new_number)
    
    if prize["value"] == "autoburger":
        embed = discord.Embed(
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
        embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/1085819476236259459.png")
        embed.set_footer(text="✨ Удачи в наборе массы! ✨")
    else:
        embed = discord.Embed(
            title="📦 Открытие кейса",
            description=f"**{member.mention}** открыл кейс и получил:",
            color=0xffaa00
        )
        
        embed.add_field(name="🎁 Приз", value=prize_description, inline=False)
        embed.add_field(name="🍖 Новый вес", value=f"{new_number}kg", inline=True)
        embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
        
        if new_autoburger_count > autoburger_count:
            embed.add_field(name="🍔 Автобургеры", value=f"+1! Теперь: {new_autoburger_count}", inline=True)
    
    embed.add_field(name="⏰ Следующий кейс", value=f"через {CASE_COOLDOWN_HOURS} часов", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='жиркейс_шансы')
async def fat_case_chances(ctx):
    """Показывает шансы в кейсе"""
    embed = discord.Embed(
        title="📊 Шансы в кейсе !жиркейс",
        description="Вероятность выпадения каждого приза:",
        color=0xffaa00
    )
    
    chances_text = ""
    for prize in CASE_PRIZES:
        if prize["value"] == "autoburger":
            chances_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']:.5f}%\n"
        else:
            chances_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']}%\n"
    
    embed.add_field(name="Призы", value=chances_text, inline=False)
    embed.add_field(name="⏰ Кулдаун", value=f"{CASE_COOLDOWN_HOURS} часов", inline=True)
    embed.add_field(name="📦 Команда", value="!жиркейс", inline=True)
    
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
    for i, (user_name, number, last_update, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count) in enumerate(users, 1):
        if i == 1:
            place_icon = "🥇"
        elif i == 2:
            place_icon = "🥈"
        elif i == 3:
            place_icon = "🥉"
        else:
            place_icon = "🔹"
        
        rank_name, rank_emoji = get_rank(number)
        
        try:
            if last_update:
                last_update_dt = datetime.fromisoformat(str(last_update))
                date_str = last_update_dt.strftime("%d.%m.%Y %H:%M")
            else:
                date_str = "доступно сейчас"
        except:
            date_str = "неизвестно"
        
        pity_emojis = []
        if consecutive_plus and consecutive_plus > 0:
            pity_emojis.append("🔥")
        if consecutive_minus and consecutive_minus > 0:
            pity_emojis.append("❄️")
        if jackpot_pity and jackpot_pity > 0:
            pity_emojis.append("💰")
        if autoburger_count and autoburger_count > 0:
            pity_emojis.append(f"🍔{autoburger_count}")
        
        pity_str = f" {' '.join(pity_emojis)}" if pity_emojis else ""
        
        leaderboard_text += f"{place_icon} **{i}.** {user_name} — **{number}kg** {rank_emoji} *{rank_name}*{pity_str}\n"
        
        if len(leaderboard_text) > 900:
            leaderboard_text += "... и ещё несколько участников"
            break
    
    embed.description = leaderboard_text
    
    stats = get_guild_stats(guild_id)
    embed.add_field(name="📊 Статистика сервера", 
                   value=f"Участников: {stats['total_users']}\n"
                         f"Суммарный вес: {stats['total_weight']}kg\n"
                         f"Средний вес: {stats['avg_weight']:.1f}kg\n"
                         f"🔼 Толстых: {stats['positive']} | 🔽 Худых: {stats['negative']} | ⚖️ Нулевых: {stats['zero']}\n"
                         f"🍔 Всего автобургеров: {stats['total_autoburgers']}", 
                   inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='жир_инфо')
async def fat_info(ctx, member: discord.Member = None):
    """Информация о пользователе"""
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, target.name)
    rank_name, rank_emoji = get_rank(number)
    
    embed = discord.Embed(
        title=f"🍔 Информация о {target.display_name} на сервере {ctx.guild.name}",
        color=0x00ff00
    )
    
    embed.add_field(name="Текущий вес", value=f"{number}kg", inline=True)
    embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
    
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
                       value=f"{autoburger_count} шт (каждые {interval} ч)\n⚡ Бонус: +{current_boost:.1f}%", 
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
    
    can_use, remaining = check_cooldown(last_time, COOLDOWN_HOURS)
    if can_use:
        cooldown_status = "✅ !жир доступен"
    else:
        cooldown_status = f"⏳ !жир через {format_time(remaining)}"
    
    can_use_case, case_remaining = check_cooldown(last_case_time, CASE_COOLDOWN_HOURS)
    if can_use_case:
        case_status = "✅ !жиркейс доступен"
    else:
        case_status = f"⏳ !жиркейс через {format_time(case_remaining)}"
    
    embed.add_field(name="Команды", value=f"{cooldown_status}\n{case_status}", inline=False)
    
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
    
    update_user_data(guild_id, user_id, 0, target.name, 0, 0, 0, 0)
    
    try:
        new_nick = f"0kg {target.name}"
        await target.edit(nick=new_nick)
        await ctx.send(f"✅ Вес {target.mention} сброшен на 0kg")
    except:
        await ctx.send(f"✅ Вес {target.mention} сброшен на 0kg (ник не изменён)")

@bot.command(name='сброскд')
async def reset_cooldowns(ctx):
    """Сброс кулдаунов (только тестеры)"""
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    affected = reset_all_cooldowns(guild_id)
    
    embed = discord.Embed(
        title="🔄 Кулдаун сброшен",
        description=f"**{ctx.author.name}** сбросил кулдаун для всех!",
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
    
    number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, member.name)
    
    fat_can_use, fat_remaining = check_cooldown(last_time, COOLDOWN_HOURS)
    case_can_use, case_remaining = check_cooldown(last_case_time, CASE_COOLDOWN_HOURS)
    
    embed = discord.Embed(
        title=f"⏰ Кулдауны на сервере {ctx.guild.name}",
        description=f"Для {member.mention}",
        color=0x3498db
    )
    
    if fat_can_use:
        fat_status = "✅ Доступна"
    else:
        fat_status = f"⏳ {format_time(fat_remaining)}"
    
    embed.add_field(name="!жир", value=f"Кулдаун: {COOLDOWN_HOURS} ч\nСтатус: {fat_status}", inline=True)
    
    if case_can_use:
        case_status = "✅ Доступен"
    else:
        case_status = f"⏳ {format_time(case_remaining)}"
    
    embed.add_field(name="!жиркейс", value=f"Кулдаун: 24 ч\nСтатус: {case_status}", inline=True)
    
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
        embed.add_field(
            name=guild.name,
            value=f"Участников в игре: {stats['total_users']}\n"
                  f"Суммарный вес: {stats['total_weight']}kg\n"
                  f"Средний вес: {stats['avg_weight']:.1f}kg\n"
                  f"🍔 Автобургеров: {stats['total_autoburgers']}",
            inline=False
        )
    
    await ctx.send(embed=embed)

# ===== КОМАНДЫ ДЛЯ ТЕСТЕРОВ (АВТОБУРГЕРЫ) =====

@bot.command(name='автобургер')
async def give_autoburger(ctx, количество: int = 1):
    """
    Выдаёт автобургеры (только для тестеров)
    Использование: !автобургер [количество]
    Пример: !автобургер 5 - выдаст 5 автобургеров
    """
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
    
    current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, user_name)
    
    new_autoburger_count = autoburger_count + количество
    
    interval = get_autoburger_interval(new_autoburger_count)
    if interval:
        new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
    else:
        new_next_autoburger_time = None
    
    update_user_data(
        guild_id, user_id, current_number, user_name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        new_autoburger_count, last_case_time, new_next_autoburger_time
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
    """
    Сбрасывает количество автобургеров у пользователя (только для тестеров)
    Использование: !автобургер_сброс [@пользователь]
    """
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, target.name)
    
    if autoburger_count == 0:
        await ctx.send(f"ℹ️ У {target.mention} нет автобургеров!")
        return
    
    update_user_data(
        guild_id, user_id, current_number, target.name,
        consecutive_plus, consecutive_minus, jackpot_pity,
        0, last_case_time, None
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
    """
    Показывает информацию об автобургерах пользователя (только для тестеров)
    """
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    current_number, last_time, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, target.name)
    
    embed = discord.Embed(
        title=f"🍔 Информация об автобургерах",
        description=f"Для {target.mention}",
        color=0x3498db
    )
    
    embed.add_field(name="Количество", value=f"{autoburger_count} 🍔", inline=True)
    
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
    
    await ctx.send(embed=embed)
    
@bot.command(name='жирхелп')
async def fat_help(ctx):
    """
    Показывает список всех команд бота
    """
    embed = discord.Embed(
        title="🍔 **ЖИРНЫЙ БОТ - ПОМОЩЬ** 🍔",
        description="Все команды для набора массы и жирных преобразований!",
        color=0xffaa00
    )
    
    # Основные команды
    embed.add_field(
        name="🎮 **ОСНОВНЫЕ КОМАНДЫ**",
        value="""
        `!жир` - изменить свой вес (кулдаун 1ч)
        `!жиркейс` - открыть кейс с призами (кулдаун 24ч)
        `!жиркейс_шансы` - показать шансы в кейсе
        `!жиротрясы` - таблица рекордов
        `!жир_инфо [@user]` - информация о весе
        `!жир_звания` - список всех званий
        `!жир_кулдаун` - статус кулдаунов
        """,
        inline=False
    )
    
    # Команды для тестеров
    embed.add_field(
        name="🛠️ **КОМАНДЫ ДЛЯ ТЕСТЕРОВ** (роль 'тестер')",
        value="""
        `!автобургер [кол-во]` - выдать себе автобургеры
        `!автобургер_сброс [@user]` - сбросить автобургеры
        `!автобургер_инфо [@user]` - инфо об автобургерах
        `!сброскд` - сбросить кулдаун для всех
        `!сбросвсех` - сбросить вес ВСЕХ на 0
        """,
        inline=False
    )
    
    # Команды для админов
    embed.add_field(
        name="👑 **КОМАНДЫ ДЛЯ АДМИНОВ**",
        value="""
        `!жир_сброс [@user]` - сбросить вес пользователя
        `!жир_серверы` - статистика по серверам
        """,
        inline=False
    )
    
    # Легенда
    embed.add_field(
        name="📊 **ЛЕГЕНДА**",
        value="""
        `🔥` - серия плюсов подряд
        `❄️` - серия минусов подряд
        `💰` - накопление на джекпот
        `🍔X` - количество автобургеров
        """,
        inline=True
    )
    
    # Статистика
    embed.add_field(
        name="⚡ **БОНУСЫ**",
        value="""
        **Автобургеры:** +бонус к плюсу
        **Минусы подряд:** +бонус к плюсу
        **Плюсы подряд:** +шанс на минус
        """,
        inline=True
    )
    
    embed.set_footer(text="🔥❄️💰🍔 - следите за своими показателями!")
    
    await ctx.send(embed=embed)
    
# ===== ЗАПУСК БОТА =====
if __name__ == "__main__":
    print("🚀 Запуск бота...")
    bot.run(TOKEN)

