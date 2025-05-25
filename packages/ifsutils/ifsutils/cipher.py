from Crypto.Cipher import AES
from Crypto.Hash import HMAC, SHA256
import os
import sys
import base64

def str_to_bytes_b64(input: str) -> bytes:
    return base64.urlsafe_b64decode(input)
    
def bytes_to_str_b64(input: bytes) -> str:
    return base64.urlsafe_b64encode(input).decode('utf-8')

class AES_CTRKey:
    signing_key: bytes
    encryption_key: bytes

    def __init__(self, b_in: bytes):
        self.signing_key = b_in[:16]
        self.encryption_key = b_in[16:]
    
    def get_key(self) -> bytes:
        return self.signing_key + self.encryption_key
    
class AES_CTRCryptoPacket:
    nonce: bytes
    tag: bytes
    payload: bytes

    def __init__(self, b_in: bytes):
        self.nonce = b_in[:8]
        self.tag = b_in[8:40]
        self.payload = b_in[40:]
    
    def get_packet(self) -> bytes:
        return self.nonce + self.tag + self.payload
    
    def get_nonced_payload(self) -> bytes:
        return self.nonce + self.payload
    

class CipherManager:

    keys: AES_CTRKey

    def __init__(self, key_in: 'bytes | str'):
        b_key: bytes
        if isinstance(key_in, str):
            b_key = str_to_bytes_b64(key_in)
        else:
            b_key = key_in
        
        if len(b_key) != 32:
            raise ValueError('Key must be 32 bytes in length!')
        else:
            self.keys = AES_CTRKey(b_key)

    def symmetric_encrypt(self, input: str) -> str:
        cipher_engine = AES.new(self.keys.encryption_key, AES.MODE_CTR)
        ciphertext: bytes = cipher_engine.encrypt(input)
        hmac = HMAC.new(self.keys.signing_key, digestmod=SHA256)
        tag = hmac.update(cipher_engine.nonce + ciphertext).digest()
        packet: AES_CTRCryptoPacket = AES_CTRCryptoPacket(cipher_engine.nonce + tag + ciphertext)
        return bytes_to_str_b64(packet.get_packet())

    def symmetric_decrypt(self, input: str) -> str:
        b_in: bytes = str_to_bytes_b64(input)
        packet: AES_CTRCryptoPacket = AES_CTRCryptoPacket(b_in)

        hmac = HMAC.new(self.keys.signing_key, digestmod=SHA256)
        
        try:
            tag = hmac.update(packet.get_nonced_payload()).verify(packet.tag)
        except ValueError:
            print('The payload has been modified since production and is now invalid!')
            sys.exit(1)
        
        cipher_engine = AES.new(self.keys.encryption_key, AES.MODE_CTR, nonce=packet.nonce)
        b_out: bytes = cipher_engine.decrypt(packet.payload)

        return b_out.decode('utf-8')

    @staticmethod
    def generate_key() -> str:
        r_bytes: bytes = os.urandom(32)
        return bytes_to_str_b64(r_bytes)




    
    