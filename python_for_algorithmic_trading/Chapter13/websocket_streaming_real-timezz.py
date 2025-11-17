import asyncio
import websockets
import os
import json
import traceback

# target
TARGET_CONTRACTS = [
    {
        "root": "QQQ",
        "expiration": 20250428,
        "strike": 462000,
        "right": "P"
    },
    {
        "root": "TSLA",
        "expiration": 20250502,
        "strike": 40000,
        "right": "C"
    }
]

# helper method
def match_contract(msg_contract, target):
    return (
        msg_contract.get("root") == target["root"] and
        msg_contract.get("expiration") == target["expiration"] and
        msg_contract.get("strike") == target["strike"] and
        msg_contract.get("right") == target["right"]
    )

# Handler
async def process_spxw_4800c(msg):
    print("[QQQ 4620P]", msg)

async def process_tsla_400c(msg):
    print("[TSLA 400C]", msg)

CONTRACT_HANDLERS = {
    ("QQQ", 20250428, 462000, "P"): process_spxw_4800c,
    ("TSLA", 20250502, 40000, "C"): process_tsla_400c,
}

# Websocket reader
async def websocket_reader(uri, outgoing_queue):
    old_http_proxy = os.environ.get('http_proxy')
    old_https_proxy = os.environ.get('https_proxy')
    
    try:
        if 'http_proxy' in os.environ:
            del os.environ['http_proxy']
        if 'https_proxy' in os.environ:
            del os.environ['https_proxy']
            
        while True:
            try:
                async with websockets.connect(uri) as ws:
                    print("[Connected to ThetaData]")
                    
                    req = {
                        "msg_type": "STREAM",
                        "sec_type": "OPTION",
                        "req_type": "TRADE",
                        "add": True,
                        "id": 1
                    }
                    await ws.send(json.dumps(req))
                    
                    async for message in ws:
                        await outgoing_queue.put(message)
                    
            except Exception as e:
                print("Reader errror", e)
                traceback.print_exc()
            
            print("Retrying connection in 3s...")
            await asyncio.sleep(3)
            
    finally:
        if old_http_proxy:
            os.environ['http_proxy'] = old_http_proxy
        if old_https_proxy:
            os.environ['https_proxy'] = old_https_proxy

# Dispatcher
async def dispatcher(incoming_queue):
    while True:
        raw = await incoming_queue.get()
        try:
            msg = json.loads(raw)
            c = msg.get("contract", None)
            if not c:
                continue
            
            # match_contract(c, TARGET_CONTRACTS)
            
            key = (c.get("root"), c.get("expiration"), c.get("strike"), c.get("right"))
            # print(f'Debug key: {key}')
            # Debug key: ('TSLA', 20250620, 210000, 'P')
            # Debug key: ('TSLA', 20250516, 360000, 'C')
            handler = CONTRACT_HANDLERS.get(key)
            
            if handler:
                asyncio.create_task(handler(msg))
            
        except Exception as e:
            print("Dispatcher error:", e)
            traceback.print_exc()

async def main():
    queue = asyncio.Queue()
    
    uri = "ws://127.0.0.1:25520/v1/events"
    
    await asyncio.gather(
        websocket_reader(uri, queue),
        dispatcher(queue)
    )

# -----------------------
async def stream_trades():
    old_http_proxy = os.environ.get('http_proxy')
    old_https_proxy = os.environ.get('https_proxy')
    
    try:
        if 'http_proxy' in os.environ:
            del os.environ['http_proxy']
        if 'https_proxy' in os.environ:
            del os.environ['https_proxy']
            
        print("Attempting to connect to WebSocket...")
        
        async with websockets.connect('ws://127.0.0.1:25520/v1/events') as websocket:
            req = {}
            req['msg_type'] = 'STREAM'
            # req['msg_type'] = 'STREAM_BULK' # Whole market data
            req['sec_type'] = 'OPTION'
            req['req_type'] = 'QUOTE'
            req['add'] = False
            # req['add'] = True
            req['id'] = 0
            req['contract'] = {}
            req['contract']['root'] = "QQQ"
            req['contract']['expiration'] = "20250428"
            req['contract']['strike'] = "462000"
            req['contract']['right'] = "P"
            print(f'Debug req:{req}')
            # await websocket.send(json.dumps(req))
            await websocket.send(req.__str__())
            while True:
                response = await websocket.recv()
                print(response)
                    
    except Exception as e:
        print(f"Connection error: {e}")
    finally:
        if old_http_proxy:
            os.environ['http_proxy'] = old_http_proxy
        if old_https_proxy:
            os.environ['https_proxy'] = old_https_proxy


if __name__ == "__main__":
    # Change the req['add'] and run for subscribe and unsubsribe.
    asyncio.get_event_loop().run_until_complete(stream_trades())
    
    # Recieve and print
    # asyncio.run(main())