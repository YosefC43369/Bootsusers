# auto_join.py

import asyncio
import csv
import random
from dataclasses import dataclass, field

from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import UserAlreadyParticipantError, InviteHashExpiredError

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_ID   = 34674448
API_HASH = "7098ee8e75f3c435f808ee9410a2f05b"

# use either a public username/link  e.g. "t.me/example"  or  "example"
# for private invite links use the hash only: "AbCdEfGhIj..."
TARGET_CHANNEL = "https://t.me/+gupMRZzM0RZkZDll"

JOIN_DELAY_MIN = 2.0   # seconds between each join (avoid flood wait)
JOIN_DELAY_MAX = 8.0

ACCOUNTS_CSV = "Telegram_70_000_000_[YOSCRYPT].csv"   # columns: phone, session_name
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


def is_invite_hash(target: str) -> bool:
    # private invite links look like a base64 token with no slashes
    cleaned = target.replace("https://t.me/+", "").replace("https://t.me/joinchat/", "")
    return cleaned == target.split("/")[-1] and not target.startswith("@") and "." not in cleaned


async def join_channel(client: TelegramClient, target: str, phone: str) -> None:
    try:
        if is_invite_hash(target):
            await client(ImportChatInviteRequest(target))
        else:
            await client(JoinChannelRequest(target))
        print(f"[+] {phone} joined {target}")
    except UserAlreadyParticipantError:
        print(f"[~] {phone} already in {target}")
    except InviteHashExpiredError:
        print(f"[!] invite link expired — check TARGET_CHANNEL")
    except Exception as e:
        print(f"[!] {phone} error: {e}")


async def main() -> None:
    accounts = load_accounts(ACCOUNTS_CSV)

    for acc in accounts:
        acc.client = TelegramClient(acc.session, API_ID, API_HASH)
        await acc.client.start(phone=acc.phone)
        await join_channel(acc.client, TARGET_CHANNEL, acc.phone)
        await acc.client.disconnect()

        delay = random.uniform(JOIN_DELAY_MIN, JOIN_DELAY_MAX)
        print(f"    waiting {delay:.1f}s before next account ...\n")
        await asyncio.sleep(delay)

    print("[*] all accounts processed.")


if __name__ == "__main__":
    asyncio.run(main())