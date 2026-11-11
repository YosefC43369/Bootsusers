# auto_react.py

import asyncio
import csv
import random
from dataclasses import dataclass, field
from typing import Literal

from telethon import TelegramClient, events
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_ID   = 34674448
API_HASH = "7098ee8e75f3c435f808ee9410a2f05b"

REACTION_EMOJI = "👍"           # emoji to send as reaction
REACTION_MODE: Literal["simultaneous", "staggered"] = "staggered"

# used only when mode = "staggered"
STAGGER_MIN_SEC = 1.0           # minimum delay between each account's reaction
STAGGER_MAX_SEC = 5.0           # maximum delay (randomised per account)

ACCOUNTS_CSV = "accounts.csv"   # columns: phone, session_name
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class Account:
    phone: str
    session: str
    client: TelegramClient = field(default=None, repr=False)


def load_accounts(path: str) -> list[Account]:
    accounts = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            accounts.append(Account(
                phone=row["phone"].strip(),
                session=row["session_name"].strip(),
            ))
    return accounts


async def connect_all(accounts: list[Account]) -> None:
    for acc in accounts:
        acc.client = TelegramClient(acc.session, API_ID, API_HASH)
        await acc.client.start(phone=acc.phone)
        print(f"[+] connected: {acc.phone}")


async def send_reaction(client: TelegramClient, chat_id: int, msg_id: int) -> None:
    await client(SendReactionRequest(
        peer=chat_id,
        msg_id=msg_id,
        reaction=[ReactionEmoji(emoticon=REACTION_EMOJI)],
    ))


async def react_all(accounts: list[Account], chat_id: int, msg_id: int) -> None:
    if REACTION_MODE == "simultaneous":
        await asyncio.gather(*[
            send_reaction(acc.client, chat_id, msg_id)
            for acc in accounts
        ])
    else:  # staggered
        for acc in accounts:
            await send_reaction(acc.client, chat_id, msg_id)
            delay = random.uniform(STAGGER_MIN_SEC, STAGGER_MAX_SEC)
            print(f"    {acc.phone} reacted — waiting {delay:.1f}s")
            await asyncio.sleep(delay)


async def main() -> None:
    accounts = load_accounts(ACCOUNTS_CSV)
    await connect_all(accounts)

    # the FIRST account is the "sender" — listen for its outgoing messages
    sender = accounts[0]
    reactors = accounts[1:]          # remaining accounts will react
    # if you want all 10 to react, set reactors = accounts

    print(f"\n[*] listening for outgoing messages on {sender.phone} ...")

    @sender.client.on(events.NewMessage(outgoing=True))
    async def on_send(event):
        chat_id = event.chat_id
        msg_id  = event.message.id
        print(f"\n[>] sent msg {msg_id} in {chat_id} — triggering reactions")
        await react_all(reactors, chat_id, msg_id)

    await sender.client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())