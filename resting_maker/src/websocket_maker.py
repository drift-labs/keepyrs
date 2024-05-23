# import ccxt.async_support as ccxt
import asyncio
import websockets
import websocket
import json
import httpx 
import time
import ssl

import os
import json
import copy

import sys
from dotenv import load_dotenv
from driftpy.drift_client import DriftClient, DEFAULT_TX_OPTIONS
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.auction_subscriber.auction_subscriber import AuctionSubscriber
from driftpy.auction_subscriber.types import AuctionSubscriberConfig
from anchorpy import Wallet
from anchorpy import Provider
from solders.keypair import Keypair
from solana.rpc.async_api import AsyncClient
from driftpy.keypair import load_keypair
from driftpy.constants.perp_markets import devnet_perp_market_configs, mainnet_perp_market_configs, PerpMarketConfig
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed
from solana.rpc.types import TxOpts
from solders.pubkey import Pubkey
from driftpy.constants.config import configs
from driftpy.types import *
#MarketType, OrderType, OrderParams, PositionDirection, OrderTriggerCondition
from driftpy.tx.fast_tx_sender import FastTxSender  # type: ignore
# from driftpy.tx.standard_tx_sender import StandardTxSender  # type: ignore

from driftpy.drift_client import DriftClient
from driftpy.constants.numeric_constants import BASE_PRECISION, PRICE_PRECISION
from borsh_construct.enum import _rust_enum
import os

MAX_TS_BUFFER = 15 # five seconds
PRICE_CHANGE_THRESHOLD = .00025
TARGET_MAX_POSITION_NOTIONAL = 1000 # not strict but target

def check_position(drift_acct: DriftClient, market_index):
    print(str(drift_acct.authority))
    user = drift_acct.get_user()
    bb = user.get_perp_position(market_index)
    if bb is not None:
        bb = bb.base_asset_amount / 1e9
    else:
        bb = 0

    return bb


def calculate_mark_premium(perp_market: PerpMarketAccount):
    prem1 = perp_market.amm.last_mark_price_twap5min/1e6 - perp_market.amm.historical_oracle_data.last_oracle_price_twap5min/1e6
    prem2 = perp_market.amm.last_mark_price_twap/1e6 - perp_market.amm.historical_oracle_data.last_oracle_price_twap/1e6
    return min(prem1, prem2)


def calculate_reservation_offset(inventory, max_inventory, ref_price, mark_premium):
    return (-inventory/max(abs(inventory), max_inventory)) * .0001 * ref_price + mark_premium

def do_trade(drift_acct: DriftClient, do_trade_params
             ):
    
    sub_account_id = drift_acct.active_sub_account_id
    orders = []
    cancel_market_indexes = []
    for (market_index, 
             binance_bid, binance_ask, drift_bid, drift_ask) in do_trade_params:
        cancel_market_indexes.append(market_index)
        # sub_account_id = drift_acct.active_sub_account_id
        current_position = check_position(drift_acct, market_index)
        perp_market = drift_acct.get_perp_market_account(market_index)
        max_base_position = TARGET_MAX_POSITION_NOTIONAL / max(drift_ask, binance_ask)
        print(f'max_base_position:{max_base_position}')

        if drift_bid > binance_ask and drift_bid > drift_ask:
            print(f'CROSSING DRIFT BID: {drift_bid:.3f}/{drift_ask:.3f}')
            drift_bid = drift_ask*(1-.00025)
        
        if drift_ask < binance_bid and drift_bid > drift_ask:
            print(f'CROSSING DRIFT ASK: {drift_bid:.3f}/{drift_ask:.3f}')
            drift_ask = drift_bid*(1.00025)

        rev_offset_twap = calculate_reservation_offset(current_position, 
                                                max_base_position, 
                                                (binance_bid+binance_ask)/2, 
                                                calculate_mark_premium(perp_market)
                                                )
        rev_offset_ask_live = drift_ask - binance_ask
        rev_offset_bid_live = drift_bid - binance_bid

        rev_offset_bid = rev_offset_twap
        rev_offset_ask = rev_offset_twap
        if current_position > 0:
            rev_offset_ask = max(min(rev_offset_twap, rev_offset_ask_live), 0)
        elif current_position < 0:
            rev_offset_bid = max(min(rev_offset_twap, rev_offset_bid_live), 0)

        bbid = binance_bid + rev_offset_bid
        bask = binance_ask + rev_offset_ask

        ask = max(drift_bid*1.00025, max(bask, drift_ask*(1-.00025)))
        bid = min(drift_ask*(1-.00025), min(bbid, drift_bid*(1+.00025)))
        print(f'Quoting Price: {bid:.3f}/{ask:.3f}')

        assert(bid<=ask)

        max_ts = 0 #int(time.time()+120) # (await drift_acct.program.provider.connection.get_block_time(last_slot)).value + MAX_TS_BUFFER
        default_order_params = OrderParams(
                order_type=OrderType.Limit(),
                market_type=MarketType.Perp(),
                direction=PositionDirection.Long(),
                user_order_id=0,
                base_asset_amount=int(max_base_position/5 * BASE_PRECISION),
                price=0,
                market_index=market_index,
                reduce_only=False,
                post_only=PostOnlyParams.NONE(),
                immediate_or_cancel=False,
                trigger_price=0,
                trigger_condition=OrderTriggerCondition.Above(),
                oracle_price_offset=0,
                auction_duration=None,
                max_ts=max_ts,
                auction_start_price=None,
                auction_end_price=None,
            )
        bid_order_params = copy.deepcopy(default_order_params)
        bid_order_params.direction = PositionDirection.Long()
        bid_order_params.price = int(bid * PRICE_PRECISION - 1)

        bid_order_params2 = copy.deepcopy(default_order_params)
        bid_order_params2.direction = PositionDirection.Long()
        bid_order_params2.price = int(bid * (PRICE_PRECISION*.9999) - 1)


        bid_order_params3 = copy.deepcopy(default_order_params)
        bid_order_params3.direction = PositionDirection.Long()
        bid_order_params3.price = int(bid * (PRICE_PRECISION*.9995) - 1)
        bid_order_params3.base_asset_amount *= 3

        ask_order_params = copy.deepcopy(default_order_params)
        ask_order_params.direction = PositionDirection.Short()
        ask_order_params.price = int(ask * (PRICE_PRECISION*1.00002) + 1)

        ask_order_params2 = copy.deepcopy(default_order_params)
        ask_order_params2.direction = PositionDirection.Short()
        ask_order_params2.price = int(ask * (PRICE_PRECISION*1.0001) + 1)

        ask_order_params3 = copy.deepcopy(default_order_params)
        ask_order_params3.direction = PositionDirection.Short()
        ask_order_params3.base_asset_amount *= 3
        ask_order_params3.price = int(ask * (PRICE_PRECISION*1.0005) + 1)
        # maker_info = MakerInfo(maker=Pubkey('TODO'), order=None)
        # print('POSTIION =', current_position)
        if current_position > -max_base_position/2:
            orders.append(ask_order_params)
        # if current_position > -max_base_position:
        #     orders.append(ask_order_params2)
            # orders.append(ask_order_params3)        
        if current_position < max_base_position/2:
            orders.append(bid_order_params)
        # if current_position < max_base_position:
            # orders.append(bid_order_params2)   
            # orders.append(bid_order_params3)        
     
    # print(orders)
    instr = drift_acct.get_place_orders_ix(orders)
    # instrs = [drift_acct.get_cancel_orders_ix(sub_account_id=0, 
    #                                             market_index=market_index,
    #                                             market_type=MarketType.Perp(),
    #                                             )
    #         for market_index in cancel_market_indexes]  +
    
    instrs = [drift_acct.get_cancel_orders_ix(market_type=MarketType.Perp())] + [instr]
   
    asyncio.create_task(
    drift_acct.send_ixs(instrs, sequencer_subaccount=sub_account_id)
        )
    print('sented tx')

def calculate_weighted_average_price(bids, asks):
    total_bid_price = 0
    total_bid_quantity = 0
    total_ask_price = 0
    total_ask_quantity = 0

    for bid in bids:
        price, quantity = map(float, bid)
        total_bid_price += price * quantity
        total_bid_quantity += quantity

    for ask in asks:
        price, quantity = map(float, ask)
        total_ask_price += price * quantity
        total_ask_quantity += quantity

    if total_bid_quantity == 0 or total_ask_quantity == 0:
        return None, None  # Avoid division by zero

    weighted_average_bid_price = total_bid_price / total_bid_quantity
    weighted_average_ask_price = total_ask_price / total_ask_quantity

    return weighted_average_bid_price, weighted_average_ask_price


cache = {0: {
         'binance':[None, None], 
         'drift': [None, None]
         },
         1: {
         'binance':[None, None], 
         'drift': [None, None]
         },
         2: {
         'binance':[None, None], 
         'drift': [None, None]
         },
}
cache_lock = asyncio.Lock()

async def binance_task(symbol, levels, update_speed, market_index):
    # Subscribe to the bid/ask depth updates
    depth_channel = f'{symbol.lower()}@depth{levels}@{update_speed}'
    url = f'wss://stream.binance.com:9443/ws/{depth_channel}'
    print('Starting...')
    
    async with websockets.connect(url) as websocket1:
        while True:
            message = await websocket1.recv()
            data = json.loads(message)
            # print("Received data from Binance:", data)
            bids = data['bids'][:6]  # Top bids
            asks = data['asks'][:6]  # Top asks
            weighted_average_bid_price, weighted_average_ask_price = calculate_weighted_average_price(bids, asks)
            async with cache_lock:
                cache[market_index]['binance'] = [weighted_average_bid_price, weighted_average_ask_price]
                cache[market_index]['binance_ts'] = time.time()

async def drift_task(market_index):
    url = 'wss://dlob.drift.trade/ws'
    print('Starting...')
    market_name = mainnet_perp_market_configs[market_index].symbol
    
    async with websockets.connect(url) as websocket1:
        await websocket1.send(json.dumps({'type': 'subscribe',
                                     'marketType': 'perp', 
                                     'channel': 'orderbook',
                                     'market': market_name}))

        while True:
            message = await websocket1.recv()
            data = json.loads(message)
            data1 = data.get('data')
            if data1 is not None:
                data1 = json.loads(data1)
                #['bids', 'asks', 'marketName', 'marketType', 'marketIndex', 
                # 'ts', 'slot', 'oracle', 'oracleData', 'marketSlot'])
                asks = float(data1['asks'][0]['price'])/1e6  # Top asks
                bids = float(data1['bids'][0]['price'])/1e6  # Top bids
                async with cache_lock:
                    cache[market_index]['drift'] = [bids, asks]
                    cache[market_index]['drift_ts'] = time.time()
    # Subscribe to orderbook data

async def handle_binance_depth_feed(keypath, url, symbols, levels, update_speed, market_indexes, subaccount_ids):
    load_dotenv()
    secret = os.getenv("PRIVATE_KEY")
    url = os.getenv("RPC_URL")

    kp = load_keypair(secret)
    wallet = Wallet(kp)
    for symbol, market_index in zip(symbols, market_indexes):
        binance_task1 = asyncio.create_task(binance_task(symbol, levels, update_speed, market_index))
        dlob_task1 = asyncio.create_task(drift_task(market_index))

    print('ready to init drift client')
    connection = AsyncClient(url)

    commitment = Processed
    tx_opts = TxOpts(skip_confirmation=True, preflight_commitment=commitment)
    fast_tx_sender = FastTxSender(connection, tx_opts, 3)

    subaccount_id = subaccount_ids[0] # todo
    drift_acct = DriftClient(
        connection,
        wallet,
        "mainnet",
        account_subscription=AccountSubscriptionConfig(
            "websocket", commitment=commitment
        ),
        tx_params=TxParams(320_000, 500),  # crank priority fees way up
        opts=tx_opts,
        tx_sender=fast_tx_sender,
        enforce_tx_sequencing=True,
        active_sub_account_id=subaccount_id,
    )
    await drift_acct.subscribe()
    drift_acct.switch_active_user(subaccount_id)
    await drift_acct.init_sequence(subaccount_id)

    while True:
        cur_ts = time.time()
        exec_trade_params = False
        do_trade_params = []
        for market_index in market_indexes:
            async with cache_lock:
                binance_ts = cache[market_index].get('binance_ts', 0)
                drift_ts = cache[market_index].get('binance_ts', 0)
                binance_bid, binance_ask = cache[market_index]['binance']
                last_binance_bid, last_binance_ask = cache[market_index].get('last_binance', (1e-12, 1e-12))
                drift_bid, drift_ask = cache[market_index]['drift']
                last_drift_bid, last_drift_ask = cache[market_index].get('last_drift', (1e-12, 1e-12))

            pct_price_delta_array = [
                (drift_bid - last_drift_bid)/last_drift_bid,
                (drift_ask - last_drift_ask)/last_drift_ask,
                (binance_bid - last_binance_bid)/last_binance_bid,
                (binance_ask - last_binance_ask)/last_binance_ask,
            ]
            do_trade_params.append((market_index, 
                     binance_bid, binance_ask,
                     drift_bid, drift_ask))
            
            if any([abs(x) > PRICE_CHANGE_THRESHOLD for x in pct_price_delta_array]):                
                print(f'BINAN Price: {binance_bid:.3f}/{binance_ask:.3f}, ({cur_ts-binance_ts} seconds)')
                print(f'DRIFT Price: {drift_bid:.3f}/{drift_ask:.3f}, ({cur_ts-drift_ts} seconds)')
                exec_trade_params = True
                async with cache_lock:
                    cache[market_index]['last_binance'] = binance_bid, binance_ask
                    cache[market_index]['last_drift'] = drift_bid, drift_ask
            else:
                pass
        
        if exec_trade_params and len(do_trade_params):
            do_trade(drift_acct, do_trade_params
                        )
        await asyncio.sleep(.010)  # Introduce a small delay


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--keypath', type=str, required=False, default=os.environ.get('ANCHOR_WALLET'))
    parser.add_argument('--env', type=str, default='devnet')
    parser.add_argument('--url', type=str,)
    parser.add_argument('--markets', type=str, required=False, default='SOLUSDT,BTCUSDT,ETHUSDT') # todo
    parser.add_argument('--subaccounts', type=str, required=False, default='0,0,0')
    parser.add_argument('--market-indexes', type=str, required=False, default='0,1,2')
    parser.add_argument('--authority', type=str, required=False, default=None) # todo
    args = parser.parse_args()

    # assert(args.spread > 0, 'spread must be > $0')
    # assert(args.spread+args.offset < 2000, 'Invalid offset + spread (> $2000)')

    if args.keypath is None:
        if os.environ['ANCHOR_WALLET'] is None:
            raise NotImplementedError("need to provide keypath or set ANCHOR_WALLET")
        else:
            args.keypath = os.environ['ANCHOR_WALLET']

    if args.env == 'devnet':
        url = 'https://api.devnet.solana.com'
    elif args.env == 'mainnet':
        url = 'https://api.mainnet-beta.solana.com'
    else:
        raise NotImplementedError('only devnet/mainnet env supported')

    if args.url:
        url = args.url

    print(args.env, url)

    keypath = args.keypath
    levels = 5  # You can change this to 5, 10, or 20
    update_speed = '100ms'  # You can change this to '1000ms' or '100ms'
    symbols = args.markets.split(',')
    market_indexes = [int(x) for x in args.market_indexes.split(',')]
    subaccount_ids = [int(x) for x in args.subaccounts.split(',')]
    assert(len(market_indexes) == len(subaccount_ids))
    assert(len(market_indexes) == len(symbols))
    asyncio.get_event_loop().run_until_complete(
    asyncio.gather(
        handle_binance_depth_feed(keypath, url, symbols, levels, update_speed, market_indexes, subaccount_ids),
    )
)