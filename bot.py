import asyncio
import logging
from telethon import TelegramClient
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.errors import SessionPasswordNeededError, UserAlreadyParticipantError, InviteHashExpiredError
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ========================
# НАСТРОЙКИ
# ========================
API_ID = 28687552
API_HASH = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN = "8559985318:AAHJdshGOYv1hQMEM6kpOFFJzL1lX9OnCGw"
ADMIN_ID = 174415647
# ========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

user_clients: dict[int, TelegramClient] = {}


# ===================== STATES =====================
class Auth(StatesGroup):
    phone = State()
    code = State()
    password = State()


# ===================== HELPERS =====================
async def get_group_members(client: TelegramClient, group_link: str, status_msg=None):
    """Быстрый парсинг — параллельные запросы + история сообщений"""
    try:
        entity, just_joined = await resolve_entity(client, group_link)
        members_dict = {}
        lock = asyncio.Lock()

        async def add_user(user):
            if user and not user.bot:
                async with lock:
                    if user.id not in members_dict:
                        members_dict[user.id] = {
                            "id": user.id,
                            "username": f"@{user.username}" if user.username else "нет username",
                            "name": f"{user.first_name or ''} {user.last_name or ''}".strip()
                        }

        # ── Метод 1: стандартный список ──
        try:
            offset = 0
            while True:
                result = await client(GetParticipantsRequest(
                    channel=entity,
                    filter=ChannelParticipantsSearch(""),
                    offset=offset, limit=200, hash=0
                ))
                if not result.users:
                    break
                for user in result.users:
                    await add_user(user)
                offset += len(result.users)
                if offset >= result.count:
                    break
        except Exception:
            pass

        if status_msg:
            try:
                await status_msg.edit_text(
                    f"⚡️ Метод 1 готов\n👥 Найдено: {len(members_dict)}\n🔍 Запускаю перебор символов...")
            except Exception:
                pass

        # ── Метод 2: параллельный перебор символов ──
        chars = list("abcdefghijklmnopqrstuvwxyz0123456789_")
        semaphore = asyncio.Semaphore(5)

        async def fetch_by_char(char):
            async with semaphore:
                try:
                    result = await client(GetParticipantsRequest(
                        channel=entity,
                        filter=ChannelParticipantsSearch(char),
                        offset=0, limit=200, hash=0
                    ))
                    for user in result.users:
                        await add_user(user)
                    if len(result.users) == 200:
                        offset = 200
                        while True:
                            r2 = await client(GetParticipantsRequest(
                                channel=entity,
                                filter=ChannelParticipantsSearch(char),
                                offset=offset, limit=200, hash=0
                            ))
                            if not r2.users:
                                break
                            for user in r2.users:
                                await add_user(user)
                            offset += len(r2.users)
                            if offset >= r2.count:
                                break
                            await asyncio.sleep(0.1)
                    await asyncio.sleep(0.1)
                except Exception:
                    await asyncio.sleep(0.5)

        batch_size = 5
        for i in range(0, len(chars), batch_size):
            batch = chars[i:i + batch_size]
            await asyncio.gather(*[fetch_by_char(c) for c in batch])
            if status_msg and i % 10 == 0:
                try:
                    await status_msg.edit_text(
                        f"⚡️ Перебор символов: {i}/{len(chars)}\n"
                        f"👥 Найдено: {len(members_dict)}"
                    )
                except Exception:
                    pass

        if status_msg:
            try:
                await status_msg.edit_text(
                    f"⚡️ Символы готовы\n👥 Найдено: {len(members_dict)}\n📜 Читаю историю сообщений...")
            except Exception:
                pass

        # ── Метод 3: история сообщений ──
        try:
            msg_count = 0
            user_ids_to_fetch = set()

            async for msg in client.iter_messages(entity, limit=5000):
                if msg.sender_id and msg.sender_id not in members_dict:
                    user_ids_to_fetch.add(msg.sender_id)
                msg_count += 1
                if msg_count % 1000 == 0 and status_msg:
                    try:
                        await status_msg.edit_text(
                            f"📜 Читаю историю: {msg_count} сообщений\n"
                            f"👥 Найдено: {len(members_dict)}"
                        )
                    except Exception:
                        pass

            uid_list = list(user_ids_to_fetch)
            fetch_sem = asyncio.Semaphore(10)

            async def fetch_user(uid):
                async with fetch_sem:
                    try:
                        user = await client.get_entity(uid)
                        await add_user(user)
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass

            for i in range(0, len(uid_list), 20):
                batch = uid_list[i:i + 20]
                await asyncio.gather(*[fetch_user(uid) for uid in batch])

        except Exception as e:
            logger.warning(f"История недоступна: {e}")

        if status_msg:
            try:
                await status_msg.edit_text(
                    f"✅ Парсинг завершён!\n👥 Итого: {len(members_dict)}"
                )
            except Exception:
                pass

        return list(members_dict.values()), entity.title

    except Exception as e:
        return None, str(e)


async def resolve_entity(client: TelegramClient, group_link: str):
    """Получить entity группы — поддержка публичных и приватных ссылок"""
    link = group_link.strip()

    invite_hash = None
    if "+joinchat/" in link:
        invite_hash = link.split("+joinchat/")[-1].rstrip("/")
    elif "t.me/+" in link:
        invite_hash = link.split("t.me/+")[-1].rstrip("/")
    elif "t.me/joinchat/" in link:
        invite_hash = link.split("t.me/joinchat/")[-1].rstrip("/")

    if invite_hash:
        try:
            result = await client(ImportChatInviteRequest(invite_hash))
            if hasattr(result, 'chats') and result.chats:
                return result.chats[0], True
        except UserAlreadyParticipantError:
            try:
                check = await client(CheckChatInviteRequest(invite_hash))
                if hasattr(check, 'chat'):
                    return check.chat, False
            except Exception:
                pass
        except InviteHashExpiredError:
            raise Exception("❌ Ссылка-приглашение устарела или недействительна")
        except Exception as e:
            raise Exception(f"❌ Не удалось вступить по ссылке: {e}")

    # Публичная группа
    if "t.me/" in link:
        group_name = link.split("t.me/")[-1].rstrip("/")
    else:
        group_name = link

    return await client.get_entity(group_name), False


async def get_or_create_client(uid: int) -> TelegramClient:
    """Получить существующий клиент или создать новый. Переподключает если отключился."""
    client = user_clients.get(uid)

    if client is not None:
        # Если клиент есть, но отключился — переподключаем
        if not client.is_connected():
            try:
                await client.connect()
                logger.info(f"Клиент {uid} переподключён")
            except Exception as e:
                logger.warning(f"Не удалось переподключить клиент {uid}: {e}")
                # Создаём заново если переподключение не вышло
                client = TelegramClient(f"session_{uid}", API_ID, API_HASH)
                await client.connect()
                user_clients[uid] = client
        return client

    # Создаём новый клиент
    client = TelegramClient(f"session_{uid}", API_ID, API_HASH)
    await client.connect()
    user_clients[uid] = client
    return client


# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У тебя нет доступа к этому боту")
        return

    await state.clear()

    client = await get_or_create_client(message.from_user.id)
    if await client.is_user_authorized():
        me = await client.get_me()
        await message.answer(
            f"👋 Привет! Я бот для парсинга участников групп.\n\n"
            f"✅ Авторизован как: {me.first_name} (@{me.username})\n\n"
            f"Отправь ссылку на группу (например <code>t.me/groupname</code>) "
            f"и я спаршу всех участников!",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "👋 Привет! Для начала нужно авторизоваться.\n\n"
            "📱 Введи свой номер телефона в формате: <code>+79001234567</code>",
            parse_mode="HTML"
        )
        await state.set_state(Auth.phone)


@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer(
        "📱 Введи номер телефона в формате: <code>+79001234567</code>",
        parse_mode="HTML"
    )
    await state.set_state(Auth.phone)


@dp.message(Command("logout"))
async def cmd_logout(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    uid = message.from_user.id
    if uid in user_clients:
        await user_clients[uid].log_out()
        del user_clients[uid]
    await state.clear()
    await message.answer("✅ Сессия завершена. Используй /auth для повторной авторизации.")


# --- AUTH FLOW ---
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Номер должен начинаться с +\nПример: <code>+79001234567</code>", parse_mode="HTML")
        return

    client = await get_or_create_client(message.from_user.id)
    try:
        result = await client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer(
            "📨 Код отправлен в Telegram!\n\n"
            "Введи код который пришёл в приложение Telegram.\n"
            "Пиши код слитно: <code>12345</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке кода: {str(e)}")
        logger.error(f"Ошибка send_code: {e}")


@dp.message(Auth.code)
async def auth_code(message: Message, state: FSMContext):
    code = message.text.strip().replace(" ", "")
    data = await state.get_data()
    phone = data.get("phone")
    phone_code_hash = data.get("phone_code_hash")

    client = user_clients.get(message.from_user.id)
    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново: /auth")
        await state.clear()
        return

    # Переподключаем если отключился
    if not client.is_connected():
        try:
            await client.connect()
        except Exception as e:
            await message.answer(f"❌ Не удалось переподключиться: {str(e)}\n\nНачни заново: /auth")
            await state.clear()
            return

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        me = await client.get_me()
        await state.clear()
        await message.answer(
            f"✅ Авторизация успешна!\n\n"
            f"Вошёл как: <b>{me.first_name}</b> (@{me.username})\n\n"
            f"Теперь отправь ссылку на группу для парсинга.",
            parse_mode="HTML"
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("🔐 Включена двухфакторная аутентификация.\nВведи пароль:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}\n\nПопробуй снова: /auth")
        await state.clear()
        logger.error(f"Ошибка sign_in: {e}")


@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    password = message.text.strip()
    client = user_clients.get(message.from_user.id)
    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново: /auth")
        await state.clear()
        return

    # Переподключаем если отключился
    if not client.is_connected():
        try:
            await client.connect()
        except Exception as e:
            await message.answer(f"❌ Не удалось переподключиться: {str(e)}\n\nНачни заново: /auth")
            await state.clear()
            return

    try:
        await client.sign_in(password=password)
        me = await client.get_me()
        await state.clear()
        await message.answer(
            f"✅ Авторизация успешна!\n\n"
            f"Вошёл как: <b>{me.first_name}</b> (@{me.username})\n\n"
            f"Теперь отправь ссылку на группу для парсинга.",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: {str(e)}")
        logger.error(f"Ошибка 2FA: {e}")


# --- PARSING ---
@dp.message(F.text)
async def handle_group_link(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У тебя нет доступа")
        return

    current_state = await state.get_state()
    if current_state is not None:
        return

    group_link = message.text.strip()
    if not group_link or group_link.startswith("/"):
        return

    client = user_clients.get(message.from_user.id)
    if not client or not await client.is_user_authorized():
        await message.answer(
            "❌ Сначала авторизуйся!\n\nИспользуй /auth для входа в аккаунт."
        )
        return

    await message.answer(
        f"⏳ Подключаюсь к группе: <code>{group_link}</code>...",
        parse_mode="HTML"
    )

    try:
        entity, just_joined = await resolve_entity(client, group_link)
        if just_joined:
            await message.answer(f"✅ Вступил в группу: <b>{entity.title}</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ {str(e)}")
        return

    await message.answer(
        f"🔍 Перебираю участников...\n⏱ Подожди 2-3 минуты.",
        parse_mode="HTML"
    )
    status_msg = await message.answer("⏳ Парсинг запущен...\n👥 Найдено: 0")

    members, group_title = await get_group_members(client, group_link, status_msg)

    if members is None:
        await message.answer(f"❌ Ошибка: {group_title}")
        return

    await message.answer(
        f"✅ Найдено <b>{len(members)}</b> участников в группе <b>'{group_title}'</b>\n\n"
        f"Начинаю отправлять...",
        parse_mode="HTML"
    )

    chunk_size = 50
    for i in range(0, len(members), chunk_size):
        chunk = members[i:i + chunk_size]
        lines = []
        for j, user in enumerate(chunk, i + 1):
            name = f" | {user['name']}" if user['name'] else ""
            lines.append(f"#{j} {user['username']}{name} | ID: {user['id']}")
        await message.answer("\n".join(lines))
        await asyncio.sleep(0.3)

    await message.answer(f"✅ Готово! Спаршено <b>{len(members)}</b> участников", parse_mode="HTML")


# ===================== MAIN =====================
async def main():
    logger.info("🤖 Парсер запущен...")
    try:
        await dp.start_polling(bot)
    finally:
        for client in user_clients.values():
            await client.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
