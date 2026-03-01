import discord
from discord.ext import commands
import random
import sqlite3
import os
from datetime import datetime, timedelta
import time

# ===== НАСТРОЙКИ =====
# Токен берётся из переменных окружения!
# На хостинге нужно установить переменную DISCORD_BOT_TOKEN
TOKEN = os.environ.get('DISCORD_BOT_TOKEN')

PREFIX = "!"  # Префикс команд
DB_FOLDER = "guild_databases"  # Папка для хранения баз данных серверов
COOLDOWN_HOURS = 6  # Кулдаун на команду !жир в часах
TESTER_ROLE_NAME = "тестер"  # Название роли для доступа к тестерским командам
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
            last_command_time TIMESTAMP
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
    
    cursor.execute('SELECT current_number, last_command_time FROM user_fat WHERE user_id = ?', (str(user_id),))
    result = cursor.fetchone()
    
    if result:
        number = result[0]
        last_time = result[1]
    else:
        number = 0
        last_time = None
        cursor.execute('''
            INSERT INTO user_fat (user_id, user_name, current_number, last_command_time)
            VALUES (?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", number, last_time))
        conn.commit()
    
    conn.close()
    return number, last_time

def update_user_data(guild_id, user_id, new_number, user_name=None):
    """Обновляет число пользователя в БД конкретного сервера"""
    init_guild_database(guild_id)
    
    db_path = get_db_path(guild_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    current_time = datetime.now()
    
    cursor.execute('''
        UPDATE user_fat 
        SET current_number = ?, user_name = ?, last_command_time = ?
        WHERE user_id = ?
    ''', (new_number, user_name or "Unknown", current_time, str(user_id)))
    
    if cursor.rowcount == 0:
        cursor.execute('''
            INSERT INTO user_fat (user_id, user_name, current_number, last_command_time)
            VALUES (?, ?, ?, ?)
        ''', (str(user_id), user_name or "Unknown", new_number, current_time))
    
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
    
    cursor.execute('UPDATE user_fat SET current_number = 0')
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
        SELECT user_name, current_number, last_command_time 
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
    
    return {
        'total_users': total_users,
        'total_weight': total_weight,
        'avg_weight': avg_weight,
        'positive': positive,
        'negative': negative,
        'zero': zero
    }

def check_cooldown(last_command_time):
    """Проверяет, прошёл ли кулдаун"""
    if last_command_time is None:
        return True, 0
    
    try:
        if isinstance(last_command_time, str):
            last_time = datetime.fromisoformat(last_command_time)
        else:
            last_time = last_command_time
        
        time_diff = datetime.now() - last_time
        cooldown_seconds = COOLDOWN_HOURS * 3600
        
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
    
    if hours > 0:
        return f"{hours} ч {minutes} мин"
    else:
        return f"{minutes} мин"

def has_tester_role(member):
    """Проверяет, есть ли у участника роль "тестер" """
    if not member:
        return False
    
    for role in member.roles:
        if role.name.lower() == TESTER_ROLE_NAME.lower():
            return True
    
    return False

# ===== КОМАНДЫ БОТА =====

@bot.event
async def on_ready():
    print(f"\n{'='*50}")
    print(f"✅ Бот успешно запущен как {bot.user}")
    print(f"📊 ID бота: {bot.user.id}")
    print(f"⏰ Кулдаун на !жир: {COOLDOWN_HOURS} часов")
    print(f"🎭 Роль для тестерских команд: {TESTER_ROLE_NAME}")
    print(f"📁 Базы данных хранятся в папке: {DB_FOLDER}")
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n📋 Серверы, на которых присутствует бот:")
    for guild in bot.guilds:
        print(f"  - {guild.name} (ID: {guild.id}, участников: {guild.member_count})")
        init_guild_database(guild.id)
    
    print(f"{'='*50}\n")

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
    Число меняется случайно от -20 до +20 относительно текущего значения
    """
    guild_id = ctx.guild.id
    member = ctx.author
    user_id = str(member.id)
    user_name = member.name
    
    current_number, last_time = get_user_data(guild_id, user_id, user_name)
    
    can_use, remaining = check_cooldown(last_time)
    
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
    
    change = random.randint(-20, 20)
    new_number = current_number + change
    
    update_user_data(guild_id, user_id, new_number, user_name)
    
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
        
        embed = discord.Embed(
            title="🍔 Жирная трансформация",
            description=f"**{member.mention}** теперь весит **{abs(new_number)}kg** на сервере **{ctx.guild.name}**!",
            color=0xff9933 if new_number >= 0 else 0x66ccff
        )
        
        if change > 0:
            embed.add_field(name="📈 Изменение", value=f"+{change} кг (поправился)", inline=True)
        elif change < 0:
            embed.add_field(name="📉 Изменение", value=f"{change} кг (похудел)", inline=True)
        else:
            embed.add_field(name="⚖️ Изменение", value="0 кг (без изменений)", inline=True)
        
        embed.add_field(name="🍖 Текущий вес", value=f"{new_number}kg", inline=True)
        embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
        embed.add_field(name="⏰ Следующая команда", value=f"через {COOLDOWN_HOURS} часов", inline=True)
        embed.set_footer(text=f"Новый ник: {new_nick}")
        
        await ctx.send(embed=embed)
        
    except discord.Forbidden:
        await ctx.send(f"❌ У меня нет прав менять никнейм для {member.mention} на этом сервере!")
    except discord.HTTPException as e:
        await ctx.send(f"❌ Ошибка при смене ника: {e}")

@bot.command(name='жиротрясы')
async def fat_leaderboard(ctx):
    """
    Показывает таблицу рекордов всех пользователей на этом сервере со званиями
    Сортировка по возрастанию (от самых худых до самых толстых)
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
    for i, (user_name, number, last_update) in enumerate(users, 1):
        # Определяем иконку для топ-3
        if i == 1:
            place_icon = "🥇"
        elif i == 2:
            place_icon = "🥈"
        elif i == 3:
            place_icon = "🥉"
        else:
            place_icon = "🔹"
        
        # Получаем звание для текущего веса
        rank_name, rank_emoji = get_rank(number)
        
        # Форматируем дату последнего обновления
        try:
            if last_update:
                last_update_dt = datetime.fromisoformat(str(last_update))
                date_str = last_update_dt.strftime("%d.%m.%Y %H:%M")
            else:
                date_str = "доступно сейчас"
        except:
            date_str = "неизвестно"
        
        # Добавляем в список с званием
        leaderboard_text += f"{place_icon} **{i}.** {user_name} — **{number}kg** {rank_emoji} *{rank_name}*\n"
        
        if len(leaderboard_text) > 900:
            leaderboard_text += "... и ещё несколько участников"
            break
    
    embed.description = leaderboard_text
    
    # Статистика по серверу
    stats = get_guild_stats(guild_id)
    
    embed.add_field(name="📊 Статистика сервера", 
                   value=f"Участников: {stats['total_users']}\n"
                         f"Суммарный вес: {stats['total_weight']}kg\n"
                         f"Средний вес: {stats['avg_weight']:.1f}kg\n"
                         f"🔼 Толстых: {stats['positive']} | 🔽 Худых: {stats['negative']} | ⚖️ Нулевых: {stats['zero']}", 
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
    
    number, last_time = get_user_data(guild_id, user_id, target.name)
    rank_name, rank_emoji = get_rank(number)
    
    embed = discord.Embed(
        title=f"🍔 Информация о {target.display_name} на сервере {ctx.guild.name}",
        color=0x00ff00
    )
    
    embed.add_field(name="Текущий вес", value=f"{number}kg", inline=True)
    embed.add_field(name="🎖️ Звание", value=f"{rank_emoji} {rank_name}", inline=True)
    
    can_use, remaining = check_cooldown(last_time)
    if can_use:
        cooldown_status = "✅ Можно использовать !жир"
    else:
        cooldown_status = f"⏳ Будет доступно через {format_time(remaining)}"
    
    embed.add_field(name="Статус команды", value=cooldown_status, inline=False)
    
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
    
    # Добавляем особые звания
    ranks_text += "🌀 **Бесконечность** — 1000+ kg\n"
    ranks_text += "💀 **Абсолютный ноль** — -1000- kg"
    
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
    
    update_user_data(guild_id, user_id, 0, target.name)
    
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
    
    number, last_time = get_user_data(guild_id, user_id, member.name)
    can_use, remaining = check_cooldown(last_time)
    
    embed = discord.Embed(
        title=f"⏰ Информация о кулдауне на сервере {ctx.guild.name}",
        description=f"Для {member.mention}",
        color=0x3498db
    )
    
    if can_use:
        embed.add_field(name="Статус", value="✅ Команда !жир доступна!", inline=False)
    else:
        embed.add_field(name="Статус", value=f"⏳ Команда будет доступна через **{format_time(remaining)}**", inline=False)
    
    embed.add_field(name="Кулдаун", value=f"{COOLDOWN_HOURS} часов", inline=True)
    embed.add_field(name="Текущий вес", value=f"{number}kg", inline=True)
    
    if has_tester_role(ctx.author):
        embed.add_field(name="Особые права", value="🎭 Есть роль тестер", inline=True)
    
    if last_time:
        try:
            if isinstance(last_time, str):
                last_time_dt = datetime.fromisoformat(last_time)
            else:
                last_time_dt = last_time
            embed.add_field(name="Последнее использование", 
                           value=last_time_dt.strftime("%d.%m.%Y %H:%M"), 
                           inline=True)
        except:
            embed.add_field(name="Последнее использование", value="неизвестно", inline=True)
    
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
                  f"Средний вес: {stats['avg_weight']:.1f}kg",
            inline=False
        )
    
    await ctx.send(embed=embed)

# ===== ЗАПУСК БОТА =====
if __name__ == "__main__":
    print("🚀 Запуск бота...")
    bot.run(TOKEN)