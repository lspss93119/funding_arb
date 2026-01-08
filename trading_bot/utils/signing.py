import base64
import time
import nacl.signing
import nacl.encoding
from urllib.parse import urlencode

def get_signature(instruction: str, params: dict, api_secret: str, timestamp: int = None, window: int = 5000) -> str:
    """
    Generates syntax matching the "With Instruction" variation.
    Message: instruction=balanceQuery&param=val&timestamp=...&window=...
    """
    if timestamp is None:
        timestamp = int(time.time() * 1000)

    # Sort keys for consistent ordering
    sorted_params = dict(sorted(params.items()))
    query_string = urlencode(sorted_params)
    
    # Standard Pattern matching the SUCCESS case
    # instruction=VALUE & query & timestamp...
    
    parts = []
    if instruction:
        parts.append(f"instruction={instruction}")
    
    if query_string:
        parts.append(query_string)
        
    parts.append(f"timestamp={timestamp}")
    parts.append(f"window={window}")
    
    message = "&".join(parts)
    
    # print(f"[DEBUG] Signing Message: {message}")
    
    try:
        private_key_bytes = base64.b64decode(api_secret)
        signing_key = nacl.signing.SigningKey(private_key_bytes)
        signed = signing_key.sign(message.encode('utf-8'))
        return base64.b64encode(signed.signature).decode('utf-8')
    except Exception as e:
        print(f"Signing error: {e}")
        return ""
