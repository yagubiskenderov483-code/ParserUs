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
API_ID = 28687552
API_HASH = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN = "8247363509:AAGF6ak5b7fMmI6Pqs91lGia9yxrXMiqrUg"
ADMIN_ID = 7602363090
# ========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

user_clients: dict[int, TelegramClient] = {}


class Auth(StatesGroup):
    phone = State()
    code = State()
    password = State()


# ===================== HELPERS =====================

def extract_username(user) -> str:
    """
    Надёжно достаёт username.
    Новый API: user.usernames — список объектов с .username
    Старый API: user.username — строка
    """
    usernames = getattr(user, 'usernames', None)
    if usernames:
        for u in usernames:
            val = getattr(u, 'username', None)
            if val:
                return f"@{val}"

    uname = getattr(user, 'username', None)
    if uname:
        return f"@{uname}"

    return "нет username"


async def ensure_connected(client: TelegramClient):
    """Гарантирует что клиент подключён перед любым запросом."""
    if not client.is_connected():
        await client.connect()
        await asyncio.sleep(0.5)


async def resolve_entity(client: TelegramClient, group_link: str):
    """Получить entity группы — публичные и приватные ссылки."""
    await ensure_connected(client)

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
            # Уже участник — пробуем получить через get_entity
            try:
                entity = await client.get_entity(f"t.me/+{invite_hash}")
                return entity, False
            except Exception:
                pass
            try:
                check = await client(CheckChatInviteRequest(invite_hash))
                if hasattr(check, 'chat'):
                    return check.chat, False
            except Exception as e:
                raise Exception(f"❌ Уже участник, но не смог получить группу: {e}")
        except InviteHashExpiredError:
            raise Exception("❌ Ссылка-приглашение устарела или недействительна")
        except Exception as e:
            raise Exception(f"❌ Не удалось вступить по ссылке: {e}")

    if "t.me/" in link:
        group_name = link.split("t.me/")[-1].rstrip("/")
    else:
        group_name = link

    await ensure_connected(client)
    return await client.get_entity(group_name), False


async def get_group_members(client: TelegramClient, group_link: str, status_msg=None):
    """Парсинг: стандартный список + символы + история сообщений."""
    try:
        await ensure_connected(client)
        entity, just_joined = await resolve_entity(client, group_link)
        members_dict = {}
        lock = asyncio.Lock()

        async def add_user(user):
            if user and not getattr(user, 'bot', False):
                async with lock:
                    if user.id not in members_dict:
                        members_dict[user.id] = {
                            "id": user.id,
                            "username": extract_username(user),
                            "name": f"{user.first_name or ''} {user.last_name or ''}".strip()
                        }

        # ── Метод 1: стандартный список ──
        try:
            offset = 0
            while True:
                await ensure_connected(client)
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
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning(f"Метод 1 ошибка: {e}")

        if status_msg:
            try:
                await status_msg.edit_text(
                    f"⚡️ Метод 1 готов\n👥 Найдено: {len(members_dict)}\n🔍 Запускаю перебор символов...")
            except Exception:
                pass

        # ── Метод 2: перебор символов ──
        chars = list("abcdefghijklmnopqrstuvwxyz0123456789_")
        semaphore = asyncio.Semaphore(3)

        async def fetch_by_char(char):
            async with semaphore:
                try:
                    await ensure_connected(client)
                    result = await client(GetParticipantsRequest(
                        channel=entity,
                        filter=ChannelParticipantsSearch(char),
                        offset=0, limit=200, hash=0
                    ))
                    for user in result.users:
                        await add_user(user)
                    if len(result.users) == 200:
                        off = 200
                        while True:
                            await ensure_connected(client)
                            r2 = await client(GetParticipantsRequest(
                                channel=entity,
                                filter=ChannelParticipantsSearch(char),
                                offset=off, limit=200, hash=0
                            ))
                            if not r2.users:
                                break
                            for user in r2.users:
                                await add_user(user)
                            off += len(r2.users)
                            if off >= r2.count:
                                break
                            await asyncio.sleep(0.15)
                    await asyncio.sleep(0.15)
                except Exception as e:
                    logger.warning(f"fetch_by_char '{char}': {e}")
                    await asyncio.sleep(1)

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
            await ensure_connected(client)
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
            fetch_sem = asyncio.Semaphore(5)

            async def fetch_user(uid):
                async with fetch_sem:
                    try:
                        await ensure_connected(client)
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
                await status_msg.edit_text(f"✅ Парсинг завершён!\n👥 Итого: {len(members_dict)}")
            except Exception:
                pass

        return list(members_dict.values()), entity.title

    except Exception as e:
        logger.error(f"get_group_members error: {e}")
        return None, str(e)


async def get_or_create_client(uid: int) -> TelegramClient:
    client = user_clients.get(uid)
    if client is not None:
        if not client.is_connected():
            try:
                await client.connect()
            except Exception:
                client = TelegramClient(f"session_{uid}", API_ID, API_HASH)
                await client.connect()
                user_clients[uid] = client
        return client

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
            f"Отправь ссылку на группу (например <code>t.me/groupname</code>) и я спаршу всех участников!",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "👋 Привет! Для начала нужно авторизоваться.\n\n"
            "📱 Введи свой номер телефона: <code>+79001234567</code>",
            parse_mode="HTML"
        )
        await state.set_state(Auth.phone)


@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("📱 Введи номер телефона: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)


@dp.message(Command("logout"))
async def cmd_logout(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    uid = message.from_user.id
    if uid in user_clients:
        try:
            await user_clients[uid].log_out()
        except Exception:
            pass
        del user_clients[uid]
    await state.clear()
    await message.answer("✅ Сессия завершена. Используй /auth для повторной авторизации.")


@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Номер должен начинаться с +\nПример: <code>+79001234567</code>", parse_mode="HTML")
        return

    await message.answer("⏳ Отправляю код...")
    client = await get_or_create_client(message.from_user.id)

    try:
        await ensure_connected(client)
        result = await client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer(
            "📨 Код отправлен в Telegram!\n\nВведи код слитно: <code>12345</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке кода: <code>{e}</code>", parse_mode="HTML")
        logger.error(f"send_code error: {e}")


@dp.message(Auth.code)
async def auth_code(message: Message, state: FSMContext):
    code = message.text.strip().replace(" ", "")
    data = await state.get_data()

    client = user_clients.get(message.from_user.id)
    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново: /auth")
        await state.clear()
        return

    try:
        await ensure_connected(client)
        await client.sign_in(phone=data["phone"], code=code, phone_code_hash=data["phone_code_hash"])
        me = await client.get_me()
        await state.clear()
        await message.answer(
            f"✅ Авторизация успешна!\n\nВошёл как: <b>{me.first_name}</b> (@{me.username})\n\nТеперь отправь ссылку на группу.",
            parse_mode="HTML"
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("🔐 Включена двухфакторная аутентификация.\nВведи пароль:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>\n\nПопробуй снова: /auth", parse_mode="HTML")
        await state.clear()
        logger.error(f"sign_in error: {e}")


@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    client = user_clients.get(message.from_user.id)
    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново: /auth")
        await state.clear()
        return

    try:
        await ensure_connected(client)
        await client.sign_in(password=message.text.strip())
        me = await client.get_me()
        await state.clear()
        await message.answer(
            f"✅ Авторизация успешна!\n\nВошёл как: <b>{me.first_name}</b> (@{me.username})\n\nТеперь отправь ссылку на группу.",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: <code>{e}</code>", parse_mode="HTML")
        logger.error(f"2FA error: {e}")


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
        await message.answer("❌ Сначала авторизуйся!\n\nИспользуй /auth для входа в аккаунт.")
        return

    await message.answer(f"⏳ Подключаюсь к группе: <code>{group_link}</code>...", parse_mode="HTML")

    try:
        await ensure_connected(client)
        entity, just_joined = await resolve_entity(client, group_link)
        if just_joined:
            await message.answer(f"✅ Вступил в группу: <b>{entity.title}</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ {e}")
        return

    status_msg = await message.answer("🔍 Парсинг запущен...\n⏱ Подожди несколько минут.\n👥 Найдено: 0")

    members, group_title = await get_group_members(client, group_link, status_msg)

    if members is None:
        await message.answer(f"❌ Ошибка: {group_title}")
        return

    await message.answer(
        f"✅ Найдено <b>{len(members)}</b> участников в <b>'{group_title}'</b>\n\nНачинаю отправлять...",
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
            try:
                await client.disconnect()
            except Exception:
                pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
