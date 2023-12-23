import asyncio
import os

from typing import Union
from dotenv import load_dotenv

from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

from anchorpy import Wallet

from driftpy.slot.slot_subscriber import SlotSubscriber
from driftpy.drift_client import DriftClient
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.auction_subscriber.auction_subscriber import AuctionSubscriber
from driftpy.auction_subscriber.types import AuctionSubscriberConfig
from driftpy.user_map.user_map import UserMap
from driftpy.user_map.user_map_config import UserMapConfig, WebsocketConfig
from driftpy.dlob.dlob_client import DLOBClient
from driftpy.dlob.client_types import DLOBClientConfig
from driftpy.constants.config import DriftEnv
from driftpy.keypair import load_keypair

from jit_proxy.jitter.jitter_shotgun import JitterShotgun  # type: ignore
from jit_proxy.jitter.jitter_sniper import JitterSniper  # type: ignore
from jit_proxy.jitter.base_jitter import JitParams  # type: ignore
from jit_proxy.jit_proxy_client import JitProxyClient, PriceType  # type: ignore

from keepyr_types import Bot, JitMakerConfig


class JitMaker(Bot):
    def __init__(
        self,
        config: JitMakerConfig,
        drift_client: DriftClient,
        usermap: UserMap,
        jitter: Union[JitterSniper, JitterShotgun],
        drift_env: DriftEnv,
    ):
        self.lookup_tables = None

        self.name = config.bot_id
        self.dry_run = config.dry_run
        self.sub_accounts = config.sub_accounts
        self.perp_market_indexes = config.perp_market_indexes

        self.drift_client = drift_client
        self.usermap = usermap
        self.jitter = jitter

        self.slot_subscriber = SlotSubscriber(self.drift_client)

        dlob_config = DLOBClientConfig(
            self.drift_client, self.usermap, self.slot_subscriber, 30
        )

        self.dlob_client = DLOBClient("https://dlob.drift.trade", dlob_config)

    async def init(self):
        print(f"Initializing {self.name}")

        await self.slot_subscriber.subscribe()
        await self.dlob_client.subscribe()

        self.lookup_tables = await self.drift_client.fetch_market_lookup_table()

        print(f"Initialized {self.name}")


async def main():
    load_dotenv()
    secret = os.getenv("PRIVATE_KEY")
    url = os.getenv("RPC_URL")

    kp = load_keypair(secret)
    wallet = Wallet(kp)

    connection = AsyncClient(url)

    drift_client = DriftClient(
        connection,
        wallet,
        "mainnet",
        account_subscription=AccountSubscriptionConfig("websocket"),
    )

    usermap_config = UserMapConfig(drift_client, WebsocketConfig())
    usermap = UserMap(usermap_config)

    auction_subscriber = AuctionSubscriber(AuctionSubscriberConfig(drift_client))

    jit_proxy_client = JitProxyClient(
        drift_client,
        Pubkey.from_string("J1TnP8zvVxbtF5KFp5xRmWuvG9McnhzmBd9XGfCyuxFP"),
    )

    jitter = JitterShotgun(drift_client, auction_subscriber, jit_proxy_client)

    jit_params = JitParams(
        bid=-1_000_000,
        ask=1_010_000,
        min_position=0,
        max_position=2,
        price_type=PriceType.Oracle(),
        sub_account_id=None,
    )

    jitter.update_perp_params(0, jit_params)
    jitter.update_spot_params(0, jit_params)

    # quick & dirty way to keep event loop open
    # try:
    #     while True:
    #         await asyncio.sleep(3600)
    # except asyncio.CancelledError:
    #     pass

    print("Hello world")


if __name__ == "__main__":
    asyncio.run(main())
