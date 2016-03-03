#!/usr/bin/python
# -*- coding: utf-8 -*-

import random, base64
from hashlib import sha1

#RC4加密算法 
def crypt(data, key):
    x = 0
    box = range(256)
    for i in range(256):
        x = (x + box[i] + ord(key[i % len(key)])) % 256
        box[i], box[x] = box[x], box[i]
    x = y = 0
    out = []
    for char in data:
        x = (x + 1) % 256
        y = (y + box[x]) % 256
        box[x], box[y] = box[y], box[x]
        out.append(chr(ord(char) ^ box[(box[x] + box[y]) % 256 ]))
    return ''.join(out)

# 使用RC4算法加密编码后的数据，data为加密数据，key为密钥
def m_encode(data, key, encode=base64.b64encode, salt_length=16):
    """ RC4 encryption with random salt and final encoding"""
    salt = ''
    for n in range(salt_length):
        salt += chr(random.randrange(256))
    data = salt + crypt(data, sha1(key + salt).digest())
    if encode:
        data = encode(data)

    return data

# 使用RC4算法解密编码后的数据，data为加密数据，key为密钥    
def m_decode(data, key, decode=base64.b64decode, salt_length=16):
    if decode:
        data = decode(data)
    salt = data[:salt_length]
    return crypt(data[salt_length:], sha1(key + salt).digest())
        
