import argparse
import yaml  # type: ignore
import asyncio

from anchorpy import Wallet

from solders.pubkey import Pubkey  # type: ignore


from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Commitment
from solana.rpc.types import TxOpts

from driftpy.drift_client import DriftClient
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.auction_subscriber.auction_subscriber import AuctionSubscriber
from driftpy.auction_subscriber.types import AuctionSubscriberConfig
from driftpy.slot.slot_subscriber import SlotSubscriber
from driftpy.user_map.user_map import UserMap
from driftpy.user_map.user_map_config import UserMapConfig, WebsocketConfig
from driftpy.keypair import load_keypair
from driftpy.tx.fast_tx_sender import FastTxSender  # type: ignore
from driftpy.types import TxParams

from jit_proxy.jitter.jitter_shotgun import JitterShotgun  # type: ignore
from jit_proxy.jitter.jitter_sniper import JitterSniper  # type: ignore
from jit_proxy.jit_proxy_client import JitProxyClient  # type: ignore

from keepyr_types import JitMakerConfig, PerpFillerConfig
from keepyr_utils import str_to_market_type, start_server

from jit_maker.src.jit_maker import JitMaker
from perp_filler.src.perp_filler import PerpFiller

JIT_PROGRAM_ID = Pubkey.from_string("J1TnP8zvVxbtF5KFp5xRmWuvG9McnhzmBd9XGfCyuxFP")


def load_config(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


async def run_forever(bot):
    await bot.init()
    await bot.start_interval_loop()
    asyncio.create_task(start_server(bot))
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


async def main():
    parser = argparse.ArgumentParser(
        description="load bot configs from a *.config.yaml"
    )
    parser.add_argument("config_path", type=str, help="path to config.yaml file")

    args = parser.parse_args()
    config = load_config(args.config_path)

    private_key = config["private_key"]
    url = config["rpc_url"]
    kp = load_keypair(private_key)
    wallet = Wallet(kp)

    connection = AsyncClient(url)

    commitment = Commitment("processed")
    tx_opts = TxOpts(skip_confirmation=False, preflight_commitment=commitment)
    fast_tx_sender = FastTxSender(connection, tx_opts, 3)

    drift_client = DriftClient(
        connection,
        wallet,
        "mainnet",
        account_subscription=AccountSubscriptionConfig(
            "websocket", commitment=commitment
        ),
        tx_params=TxParams(1_400_000, 20_000),  # crank priority fees way up
        opts=tx_opts,
        tx_sender=fast_tx_sender,
    )

    usermap_config = UserMapConfig(drift_client, WebsocketConfig())
    usermap = UserMap(usermap_config)

    await usermap.subscribe()

    bot_names = config["bots"]
    bot_configs = config["bot_configs"]

    bots = []
    for idx, bot in enumerate(bot_names):
        bot_config = bot_configs[idx]
        if bot == "jit_maker":
            jit_proxy_client = JitProxyClient(
                drift_client,
                JIT_PROGRAM_ID,
            )
            auction_subscriber = AuctionSubscriber(
                AuctionSubscriberConfig(drift_client, commitment)
            )
            jitter_type = bot_config["jitter_type"]
            if jitter_type.lower() == "shotgun":
                jitter = JitterShotgun(
                    drift_client,
                    auction_subscriber,
                    jit_proxy_client,
                    bot_config["verbose"],
                )
            elif jitter_type.lower() == "sniper":
                slot_subscriber = SlotSubscriber(drift_client)
                jitter = JitterSniper(
                    drift_client,
                    slot_subscriber,
                    auction_subscriber,
                    jit_proxy_client,
                    bot_config["verbose"],
                )
            else:
                raise ValueError(
                    f"expected jitter type shotgun or sniper, got {jitter_type}"
                )

            jit_maker_config = JitMakerConfig(
                bot_config["bot_id"],
                bot_config["market_indexes"],
                bot_config["sub_accounts"],
                str_to_market_type(bot_config["market_type"]),
                drift_client,
                usermap,
                jitter,
                config["drift_env"],
                bot_config["target_leverage"],
                bot_config["spread"],
            )

            jit_maker = JitMaker(jit_maker_config)

            bots.append(jit_maker)
        elif bot == "perp_filler":
            if not bot_config["filler_polling_interval"]:
                filler_polling_interval = None
            else:
                filler_polling_interval = bot_config["filler_polling_interval"]

            perp_filler_config = PerpFillerConfig(
                bot_config["bot_id"],
                drift_client,
                usermap,
                filler_polling_interval,
                bot_config["revert_on_failure"],
                bot_config["simulate_tx_for_cu_estimate"],
                bot_config["use_burst_cu_limit"],
            )

            perp_filler = PerpFiller(perp_filler_config)

            bots.append(perp_filler)
        # load other bots from configs

    tasks = [run_forever(bot) for bot in bots]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
