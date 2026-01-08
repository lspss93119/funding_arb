import asyncio
import aiohttp
import json
import logging
import time
from typing import Dict, Any, List, Callable, Optional

logger = logging.getLogger("trading_bot.core.websocket")

class BackpackWebSocket:
    URL = "wss://ws.backpack.exchange"

    def __init__(self, api_key: str, api_secret: str, callback: Callable[[Dict], Any] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.callback = callback
        self.session = None
        self.ws = None
        self._running = False
        self._subscriptions = set()

    async def connect(self):
        self._running = True
        self.session = aiohttp.ClientSession()
        logger.info(f"Connecting to {self.URL}...")
        
        try:
            async with self.session.ws_connect(self.URL) as ws:
                self.ws = ws
                logger.info("WebSocket Connected.")
                
                # Resubscribe if we have pending subscriptions
                if self._subscriptions:
                    await self._subscribe_all(list(self._subscriptions))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if self.callback:
                            await self.callback(data)
                        else:
                            print(f"WS Msg: {data}")
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error('ws connection closed with exception %s', ws.exception())
                        break
        except Exception as e:
            logger.error(f"WebSocket Error: {e}")
        finally:
            logger.info("WebSocket disconnected.")
            self.ws = None

    async def close(self):
        self._running = False
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()

    def _generate_signature(self, instruction: str, timestamp: int, window: int) -> list:
        try:
            from trading_bot.utils.signing import get_signature
            # Backpack WS Signature format: ["key", "sig", "ts", "window"]
            # Signature payload: instruction=subscribe&timestamp=...&window=...
            
            # Note: params for get_signature usually expects a dict, but for this specific string format
            # we might need to manually construct the string or adjust get_signature.
            # However, looking at docs: "instruction=subscribe&timestamp=...&window=..."
            # This is exactly what get_signature produces if we pass empty params but correct instruction/ts/window?
            # Let's check signing.py later. For now assuming we construct it manually.
            
            from nacl.signing import SigningKey
            from nacl.encoding import Base64Encoder
            
            payload = f"instruction={instruction}&timestamp={timestamp}&window={window}"
            
            signer = SigningKey(base64.b64decode(self.api_secret))
            signed = signer.sign(payload.encode("utf-8"))
            signature = base64.b64encode(signed.signature).decode("utf-8")
            
            return [self.api_key, signature, str(timestamp), str(window)]
            
        except Exception as e:
            logger.error(f"WS Signing failed: {e}")
            return []
            
    import base64 # re-import for safety inside method if needed (moved to top in real code)

    async def subscribe(self, streams: List[str]):
        if not self.ws:
            self._subscriptions.update(streams)
            return

        payload = {
            "method": "SUBSCRIBE",
            "params": streams
        }
        await self.ws.send_json(payload)
        self._subscriptions.update(streams)
        logger.info(f"Subscribed to {streams}")

    async def subscribe_private(self, streams: List[str]):
        # Private streams need signature
        timestamp = int(time.time() * 1000)
        window = 5000
        signature_data = self._generate_signature("subscribe", timestamp, window)
        
        if not signature_data:
            logger.error("Could not generate signature for private subscription")
            return

        payload = {
            "method": "SUBSCRIBE",
            "params": streams,
            "signature": signature_data
        }
        
        if self.ws:
            await self.ws.send_json(payload)
            self._subscriptions.update(streams)
            logger.info(f"Subscribed to private channels: {streams}")

    async def _subscribe_all(self, streams):
        # Determine which are private
        private_streams = [s for s in streams if s.startswith("account.")]
        public_streams = [s for s in streams if not s.startswith("account.")]
        
        if public_streams:
            await self.subscribe(public_streams)
        if private_streams:
            await self.subscribe_private(private_streams)

