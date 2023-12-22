import asyncio
import os

from dataclasses import dataclass
from dotenv import load_dotenv

from solana.rpc.async_api import AsyncClient

from anchorpy import Wallet

from driftpy.drift_client import DriftClient
from driftpy.constants.config import DriftEnv
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.slot.slot_subscriber import SlotSubscriber
from driftpy.user_map.user_map_config import WebsocketConfig, UserMapConfig
from driftpy.user_map.user_map import UserMap
from driftpy.dlob.dlob_client import DLOBClient
from driftpy.keypair import load_keypair


@dataclass
class JitMakerConfig:
    dry_run: bool
    drift_env: DriftEnv
    target_leverage: int
    perp_market_index: int
    sub_account_id: int


class JitMaker:
    def __init__(
        self,
        drift_client: DriftClient,
        slot_subscriber: SlotSubscriber,
        usermap: UserMap,
        dlob_client: DLOBClient,
        config: JitMakerConfig,
    ):
        self.drift_client = drift_client
        self.slot_subscriber = slot_subscriber
        self.usermap = usermap
        self.dlob_client = dlob_client
        self.perp_market_account = None
        self.dry_run = config.dry_run
        self.target_leverage = config.target_leverage
        self.perp_market_index = config.perp_market_index
        self.sub_account_id = config.sub_account_id

    async def subscribe(self):
        await self.drift_client.subscribe()
        await self.drift_client.fetch_market_lookup_table()

        print("Subscribed to Drift Client")

        await self.slot_subscriber.subscribe()
        self.slot_subscriber.event_emitter.on_slot_change += self.on_slot_update_sync
        print("Subscribed to Slot Subscriber")

        await self.usermap.subscribe()

        print("Subscribed to User Map")

    def on_slot_update_sync(self, slot):
        asyncio.create_task(self.on_slot_update(slot))

    async def on_slot_update(self, slot):
        pass


async def main():
    load_dotenv()
    secret = os.getenv("PRIVATE_KEY")
    rpc = os.getenv("RPC_URL")
    dry_run = os.getenv("DRY_RUN")
    drift_env = os.getenv("DRIFT_ENV")
    target_leverage = os.getenv("TARGET_LEVERAGE")
    perp_market_index = os.getenv("PERP_MARKET_INDEX")
    sub_account_id = os.getenv("SUB_ACCOUNT_ID")

    config = JitMakerConfig(
        dry_run if dry_run is not None else False,
        drift_env if drift_env is not None else "mainnet",
        target_leverage if target_leverage is not None else 1,
        perp_market_index if perp_market_index is not None else 1,
        sub_account_id if sub_account_id is not None else 0,
    )

    kp = load_keypair(secret)
    wallet = Wallet(kp)

    connection = AsyncClient(rpc)
    drift_client = DriftClient(
        connection,
        wallet,
        "mainnet",
        account_subscription=AccountSubscriptionConfig("websocket"),
    )

    slot_subscriber = SlotSubscriber(drift_client)

    ws_config = WebsocketConfig()

    usermap_config = UserMapConfig(drift_client, ws_config)

    usermap = UserMap(usermap_config)

    dlob_client = DLOBClient("https://dlob.drift.trade")

    jit_maker = JitMaker(drift_client, slot_subscriber, usermap, dlob_client, config)

    await jit_maker.subscribe()

    print("Subscribed to JIT Maker")

    # quick and dirty way to keep event loop open
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
