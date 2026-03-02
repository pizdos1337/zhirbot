import discord
from discord.ext import commands
import random
import sqlite3
import os
from datetime import datetime, timedelta
import time
import math
import asyncio

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
AUTOBURGER_BOOST_PER_BURGER = 0.1  # Каждый автобургер даёт +10% к шансу на плюс
# =====================

# Проверяем, что токен найден
if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найдена переменная окружения DISCORD_BOT_TOKEN!")
    print("📌 Убедитесь, что на хостинге установлена переменная окружения с токеном бота")
    print("🚫 Бот не может быть запущен без токена")
    exit(1)

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
    # Если вес выходит за пределы 
    if weight > 99999999:
        return "🌀 Бесконечность", "🌀"
    # Если вес меньше -999
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
            jackpot_pity INTEGER DEFAULT 0,
            autoburger_count INTEGER DEFAULT 0,  -- Количество Автобургеров
            last_case_time TIMESTAMP,             -- Последнее открытие кейса
            next_autoburger_time TIMESTAMP        -- Следующее автоприменение !жир
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"✅ База данных инициализирована для сервера {guild_id}")

def get_user_data(guild_id, user_id, user_name=None):
    """
    Получает данные пользователя из БД конкретного сервера
    Если пользователя нет, создаёт запись
    """
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('SELECT current_number, last_command_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time FROM user_fat WHERE user_id = ?', (str(user_id),))
    result = cursor.fetchone()
    
    if result:
        number = result[0]
        last_time = result[1]
        consecutive_plus = result[2] or 0
        jackpot_pity = result[3] or 0
        autoburger_count = result[4] or 0
        last_case_time = result[5]
        next_autoburger_time = result[6]
    else:
        number = 0
        last_time = None
        consecutive_plus = 0
        jackpot_pity = 0
        autoburger_count = 0
        last_case_time = None
        next_autoburger_time = None
        cursor.execute('''
            INSERT INTO user_fat (user_id, user_name, current_number, last_command_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", number, last_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time))
        conn.commit()
    
    conn.close()
    return number, last_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time

def update_user_data(guild_id, user_id, new_number, user_name=None, consecutive_plus=None, jackpot_pity=None, autoburger_count=None, last_case_time=None, next_autoburger_time=None):
    """Обновляет число пользователя в БД конкретного сервера"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    current_time = datetime.now()
    
    # Строим запрос динамически
    updates = ["current_number = ?", "user_name = ?", "last_command_time = ?"]
    values = [new_number, user_name or "Unknown", current_time]
    
    if consecutive_plus is not None:
        updates.append("consecutive_plus = ?")
        values.append(consecutive_plus)
    
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
        # Вставка новой записи
        cursor.execute('''
            INSERT INTO user_fat (user_id, user_name, current_number, last_command_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", new_number, current_time, consecutive_plus or 0, jackpot_pity or 0, autoburger_count or 0, last_case_time, next_autoburger_time))
    
    conn.commit()
    conn.close()
    return current_time

def reset_all_cooldowns(guild_id):
    """Сбрасывает кулдаун для всех пользователей на конкретном сервере"""
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
    """Сбрасывает вес для всех пользователей на конкретном сервере на 0"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('UPDATE user_fat SET current_number = 0, consecutive_plus = 0, jackpot_pity = 0, autoburger_count = 0')
    affected_rows = cursor.rowcount
    conn.commit()
    conn.close()
    
    return affected_rows

def get_all_users_sorted(guild_id):
    """
    Получает всех пользователей конкретного сервера отсортированных по числу (возрастание)
    """
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT user_name, current_number, last_command_time, consecutive_plus, jackpot_pity, autoburger_count
        FROM user_fat 
        ORDER BY current_number ASC
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
    
    total_autoburgers = sum(u[5] for u in users)
    
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

def get_change_with_pity_and_jackpot(consecutive_plus, jackpot_pity, autoburger_count=0):
    """
    Определяет изменение веса с учётом "удачи" и джекпота и бонуса от автобургеров
    Возвращает (изменение, был_ли_минус, новый_счётчик_плюсов, новый_счётчик_джекпота, был_ли_джекпот)
    """
    # Бонус от автобургеров увеличивает шанс на плюс
    autoburger_boost = autoburger_count * AUTOBURGER_BOOST_PER_BURGER
    
    # Рассчитываем текущий шанс на минус (уменьшаем на бонус от автобургеров)
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - autoburger_boost
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))  # Не меньше 10% и не больше 90%
    
    # Рассчитываем текущий шанс на джекпот
    jackpot_chance = BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT)
    jackpot_chance = min(jackpot_chance, MAX_JACKPOT_CHANCE)
    
    # Сначала проверяем джекпот (самый редкий)
    jackpot_roll = random.random()
    if jackpot_roll < jackpot_chance:
        # ДЖЕКПОТ!
        change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
        new_consecutive_plus = consecutive_plus + 1  # Джекпот считается как плюс
        new_jackpot_pity = 0  # Сбрасываем счётчик джекпота
        was_minus = False
        was_jackpot = True
        return change, was_minus, new_consecutive_plus, new_jackpot_pity, was_jackpot
    
    # Если не джекпот, проверяем минус/плюс
    roll = random.random()
    
    if roll < minus_chance:
        # Выпал минус
        change = random.randint(-20, -1)
        new_consecutive_plus = 0  # Сбрасываем счётчик плюсов
        new_jackpot_pity = jackpot_pity + 1  # Увеличиваем счётчик джекпота
        was_minus = True
        was_jackpot = False
    else:
        # Выпал плюс
        change = random.randint(1, 20)
        new_consecutive_plus = consecutive_plus + 1  # Увеличиваем счётчик плюсов
        new_jackpot_pity = jackpot_pity + 1  # Увеличиваем счётчик джекпота
        was_minus = False
        was_jackpot = False
    
    return change, was_minus, new_consecutive_plus, new_jackpot_pity, was_jackpot

def get_case_prize():
    """
    Определяет приз из кейса на основе шансов
    """
    roll = random.random() * 100  # от 0 до 100
    
    cumulative = 0
    for prize in CASE_PRIZES:
        cumulative += prize["normalized_chance"]
        if roll < cumulative:
            return prize
    
    # На всякий случай (если сумма меньше 100)
    return CASE_PRIZES[-1]

def get_autoburger_interval(autoburger_count):
    """
    Возвращает интервал в часах для автобургера
    """
    if autoburger_count <= 0:
        return None
    elif autoburger_count == 1:
        return AUTOBURGER_INTERVALS[0]
    elif autoburger_count == 2:
        return AUTOBURGER_INTERVALS[1]
    elif autoburger_count == 3:
        return AUTOBURGER_INTERVALS[2]
    else:
        return AUTOBURGER_INTERVALS[3]  # 4+ автобургера

async def apply_autoburger(user_id, guild_id, user_name):
    """
    Фоновое применение команды !жир для автобургера
    """
    try:
        # Получаем текущие данные
        current_number, _, consecutive_plus, jackpot_pity, autoburger_count, _, _ = get_user_data(guild_id, user_id, user_name)
        
        # Применяем !жир с бонусом от автобургеров
        change, was_minus, new_consecutive_plus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
            consecutive_plus, jackpot_pity, autoburger_count
        )
        
        new_number = current_number + change
        
        # Обновляем данные
        update_user_data(
            guild_id, user_id, new_number, user_name,
            new_consecutive_plus, new_jackpot_pity,
            autoburger_count  # не меняем количество автобургеров
        )
        
        # Обновляем ник (опционально)
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
                    pass  # Игнорируем ошибки смены ника в фоне
        
        print(f"🤖 Автобургер сработал для {user_name}: {change:+d} кг")
        
    except Exception as e:
        print(f"❌ Ошибка в автобургере: {e}")

async def autoburger_loop():
    """
    Фоновый цикл для проверки и запуска автобургеров
    """
    await bot.wait_until_ready()
    
    while not bot.is_closed():
        try:
            current_time = datetime.now()
            
            # Проверяем все серверы
            for guild in bot.guilds:
                guild_id = guild.id
                
                # Получаем всех пользователей с автобургерами
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
                                # Пора применять автобургер
                                await apply_autoburger(user_id, guild_id, user_name)
                                
                                # Обновляем следующее время
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
        
        # Проверяем каждую минуту
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
    
    # Запускаем фоновый цикл автобургеров
    bot.loop.create_task(autoburger_loop())

@bot.event
async def on_guild_join(guild):
    """Событие при добавлении бота на новый сервер"""
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
    Число меняется случайно от -20 до +20 с системой "удачи" и шансом на джекпот
    """
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    # Получаем данные пользователя
    current_number, last_time, consecutive_plus, jackpot_pity, autoburger_count, _, _ = get_user_data(guild_id, user_id, user_name)
    
    # Проверяем кулдаун
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
    
    # Получаем изменение с учётом "удачи" и джекпота и бонуса от автобургеров
    change, was_minus, new_consecutive_plus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
        consecutive_plus, jackpot_pity, autoburger_count
    )
    new_number = current_number + change
    
    # Обновляем данные пользователя
    update_user_data(
        guild_id, user_id, new_number, user_name,
        new_consecutive_plus, new_jackpot_pity,
        autoburger_count  # не меняем количество автобургеров
    )
    
    # Получаем чистое имя для ника
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
        
        # Получаем звание
        rank_name, rank_emoji = get_rank(new_number)
        
        # Создаём embed
        if was_jackpot:
            embed_color = 0xffd700  # Золотой для джекпота
            embed_title = "💰 ДЖЕКПОТ! 💰"
        else:
            embed_color = 0xff9933 if new_number >= 0 else 0x66ccff
            embed_title = "🍔 Жирная трансформация"
        
        embed = discord.Embed(
            title=embed_title,
            description=f"**{member.mention}** теперь весит **{abs(new_number)}kg** на сервере **{ctx.guild.name}**!",
            color=embed_color
        )
        
        # Информация об изменении
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
        
        # Информация о накоплениях
        if was_jackpot:
            embed.add_field(name="💰 Джекпот", value="✨ Счётчик сброшен!", inline=False)
        elif was_minus and consecutive_plus > 0:
            embed.add_field(name="❌ Сброс", value=f"Серия из {consecutive_plus} прервана!", inline=False)
        
        # Информация об автобургерах
        if autoburger_count > 0:
            interval = get_autoburger_interval(autoburger_count)
            embed.add_field(name="🍔 Автобургеры", value=f"Количество: {autoburger_count} (каждые {interval} ч)", inline=True)
        
        embed.add_field(name="⏰ Следующая команда", value=f"через {COOLDOWN_HOURS} часов", inline=True)
        embed.set_footer(text=f"Новый ник: {new_nick}")
        
        await ctx.send(embed=embed)
        
    except discord.Forbidden:
        await ctx.send(f"❌ У меня нет прав менять никнейм для {member.mention} на этом сервере!")
    except discord.HTTPException as e:
        await ctx.send(f"❌ Ошибка при смене ника: {e}")

@bot.command(name='жиркейс')
async def fat_case(ctx):
    """
    Открывает кейс с различными призами
    Кулдаун: 24 часа
    """
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    # Получаем данные пользователя
    current_number, last_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, user_name)
    
    # Проверяем кулдаун на кейс
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
    
    # Получаем приз
    prize = get_case_prize()
    
    new_autoburger_count = autoburger_count
    new_number = current_number
    new_next_autoburger_time = next_autoburger_time
    prize_description = ""
    
    # Обрабатываем приз
    if prize["value"] == "autoburger":
        # Автобургер!
        new_autoburger_count = autoburger_count + 1
        
        # Обновляем следующее время автобургера
        interval = get_autoburger_interval(new_autoburger_count)
        if interval:
            new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
        
        # ИСПРАВЛЕНО: добавлена закрывающая кавычка и скобка
        prize_description = f"**+1 {prize['emoji']}**"
    else:
        # Просто кг
        new_number = current_number + prize["value"]
        prize_description = f"**{prize['value']:+d} кг** {prize['emoji']}"
    
    # Обновляем данные пользователя
    current_time = datetime.now()
    update_user_data(
        guild_id, user_id, new_number, user_name,
        consecutive_plus, jackpot_pity,
        new_autoburger_count, current_time, new_next_autoburger_time
    )
    
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
            
            new_nick = f"{new_number}kg {clean_name}"
            if len(new_nick) > 32:
                new_nick = new_nick[:32]
            
            await member.edit(nick=new_nick)
        except:
            pass  # Игнорируем ошибки смены ника
    
    # Получаем звание для нового веса
    rank_name, rank_emoji = get_rank(new_number)
    
    # Создаём красивое сообщение
    if prize["value"] == "autoburger":
        # Особое оформление для Автобургера
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
- Бонус к шансу плюса: **+{new_autoburger_count * 10}%**

*Автобургеры складываются, увеличивая бонус и уменьшая интервал!*
            """,
            color=0xffaa00
        )
        embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/1085819476236259459.png")  # Бургер эмодзи
        embed.set_footer(text="✨ Удачи в наборе массы! ✨")
    else:
        # Обычное оформление
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
    """
    Показывает шансы выпадения призов в кейсе
    """
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
    """
    Показывает таблицу рекордов всех пользователей на этом сервере со званиями
    """
    guild_id = ctx.guild.id
    guild_name = ctx.guild.name
    
    users = get_all_users_sorted(guild_id)
    
    if not users:
        await ctx.send(f"📭 На сервере **{guild_name}** пока никто не участвовал в жирных преобразованиях!")
        return
    
    embed = discord.Embed(
        title=f"🏆 Таблица жиротрясов - {guild_name}",
        description="Рейтинг пользователей по весу (от самых лёгких до самых тяжёлых)",
        color=0xffaa00
    )
    
    leaderboard_text = ""
    for i, (user_name, number, last_update, consecutive_plus, jackpot_pity, autoburger_count) in enumerate(users, 1):
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
        
        # Добавляем информацию о накоплениях
        pity_emojis = []
        if consecutive_plus and consecutive_plus > 0:
            pity_emojis.append("🔥")
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
    """
    Показывает информацию о текущем весе пользователя на этом сервере
    """
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    number, last_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, target.name)
    rank_name, rank_emoji = get_rank(number)
    
    embed = discord.Embed(
        title=f"🍔 Информация о {target.display_name} на сервере {ctx.guild.name}",
        color=0x00ff00
    )
    
    embed.add_field(name="Текущий вес", value=f"{number}kg", inline=True)
    embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
    
    # Показываем эмодзи
    pity_emojis = []
    if consecutive_plus > 0:
        pity_emojis.append("🔥")
    if jackpot_pity > 0:
        pity_emojis.append("💰")
    
    if pity_emojis:
        embed.add_field(name="📊 Статус", value=" ".join(pity_emojis), inline=True)
    
    if autoburger_count > 0:
        interval = get_autoburger_interval(autoburger_count)
        embed.add_field(name="🍔 Автобургеры", value=f"{autoburger_count} шт (каждые {interval} ч)", inline=True)
        
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
    """
    Показывает все доступные звания
    """
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
    """
    Сбрасывает число пользователя на 0 на этом сервере (только для админов)
    """
    if not ctx.author.guild_permissions.administrator and ctx.author != ctx.guild.owner:
        await ctx.send("❌ Эта команда только для администраторов!")
        return
    
    guild_id = ctx.guild.id
    target = member or ctx.author
    user_id = str(target.id)
    
    # Сбрасываем вес и накопления
    update_user_data(guild_id, user_id, 0, target.name, 0, 0, 0)
    
    try:
        new_nick = f"0kg {target.name}"
        await target.edit(nick=new_nick)
        await ctx.send(f"✅ Вес пользователя {target.mention} на сервере **{ctx.guild.name}** сброшен на 0kg и ник обновлён")
    except:
        await ctx.send(f"✅ Вес пользователя {target.mention} на сервере **{ctx.guild.name}** сброшен на 0kg (ник не изменён)")

@bot.command(name='сброскд')
async def reset_cooldowns(ctx):
    """
    Сбрасывает кулдаун для всех пользователей на этом сервере
    Доступно только для роли "тестер"
    """
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав на использование этой команды! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    affected = reset_all_cooldowns(guild_id)
    
    embed = discord.Embed(
        title="🔄 Кулдаун сброшен",
        description=f"**{ctx.author.name}** сбросил кулдаун для всех пользователей на сервере **{ctx.guild.name}**!",
        color=0x00ff00
    )
    embed.add_field(name="Затронуто пользователей", value=str(affected), inline=True)
    embed.add_field(name="Новый статус", value="✅ Все могут использовать !жир", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='сбросвсех')
async def reset_all_users_weight(ctx):
    """
    Сбрасывает вес для ВСЕХ пользователей на этом сервере на 0
    Доступно только для роли "тестер"
    """
    if not has_tester_role(ctx.author):
        await ctx.send(f"❌ У вас нет прав на использование этой команды! Нужна роль **{TESTER_ROLE_NAME}**")
        return
    
    guild_id = ctx.guild.id
    
    confirmation = await ctx.send(f"⚠️ **Внимание!** Вы действительно хотите сбросить вес **ВСЕХ** пользователей на сервере **{ctx.guild.name}** на 0? Это действие нельзя отменить!\n\nНапишите `да` в течение 30 секунд для подтверждения.")
    
    def check(msg):
        return msg.author == ctx.author and msg.content.lower() == "да"
    
    try:
        await bot.wait_for('message', timeout=30.0, check=check)
    except:
        await ctx.send("❌ Отмена сброса (таймаут или неверный ответ)")
        return
    
    affected = reset_all_weights(guild_id)
    
    embed = discord.Embed(
        title="⚖️ Глобальный сброс веса",
        description=f"**{ctx.author.name}** выполнил глобальный сброс веса для всех пользователей на сервере **{ctx.guild.name}**!",
        color=0xff5500
    )
    embed.add_field(name="Затронуто пользователей", value=str(affected), inline=True)
    embed.add_field(name="Новый вес", value="0kg для всех", inline=True)
    embed.set_footer(text="Таблица жиротрясов полностью обновлена")
    
    await ctx.send(embed=embed)

@bot.command(name='жир_кулдаун')
async def cooldown_info(ctx):
    """
    Показывает информацию о текущем кулдауне для пользователя на этом сервере
    """
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    
    number, last_time, consecutive_plus, jackpot_pity, autoburger_count, last_case_time, next_autoburger_time = get_user_data(guild_id, user_id, member.name)
    
    # Проверяем кулдауны
    fat_can_use, fat_remaining = check_cooldown(last_time, COOLDOWN_HOURS)
    case_can_use, case_remaining = check_cooldown(last_case_time, CASE_COOLDOWN_HOURS)
    
    embed = discord.Embed(
        title=f"⏰ Кулдауны на сервере {ctx.guild.name}",
        description=f"Для {member.mention}",
        color=0x3498db
    )
    
    # !жир
    if fat_can_use:
        fat_status = "✅ Доступна"
    else:
        fat_status = f"⏳ {format_time(fat_remaining)}"
    
    embed.add_field(name="!жир", value=f"Кулдаун: {COOLDOWN_HOURS} ч\nСтатус: {fat_status}", inline=True)
    
    # !жиркейс
    if case_can_use:
        case_status = "✅ Доступен"
    else:
        case_status = f"⏳ {format_time(case_remaining)}"
    
    embed.add_field(name="!жиркейс", value=f"Кулдаун: 24 ч\nСтатус: {case_status}", inline=True)
    
    # Текущий вес
    embed.add_field(name="Текущий вес", value=f"{number}kg", inline=True)
    
    # Эмодзи статуса
    pity_emojis = []
    if consecutive_plus > 0:
        pity_emojis.append("🔥")
    if jackpot_pity > 0:
        pity_emojis.append("💰")
    if autoburger_count > 0:
        pity_emojis.append(f"🍔{autoburger_count}")
    
    if pity_emojis:
        embed.add_field(name="Статус", value=" ".join(pity_emojis), inline=True)
    
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
    """
    Показывает список серверов, где есть бот, и статистику по ним (только для админов)
    """
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

# ===== ЗАПУСК БОТА =====
if __name__ == "__main__":
    print("🚀 Запуск бота...")
    bot.run(TOKEN)
