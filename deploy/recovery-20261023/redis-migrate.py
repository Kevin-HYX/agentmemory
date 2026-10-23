"""离线格式迁移；生产读写仍由 iii-engine 的 Redis 适配器处理。"""
import hashlib
import json
import pathlib
import socket
import subprocess
import sys
import time

root = pathlib.Path(sys.argv[1])
mode = sys.argv[2]
source = pathlib.Path(sys.argv[3])
password = next(line.split(' ', 1)[1] for line in (root / 'redis.conf').read_text().splitlines() if line.startswith('requirepass '))
info = json.loads(subprocess.check_output(['docker', 'inspect', 'agentmemory-recovery-redis']))[0]
host = info['NetworkSettings']['Networks']['agentmemory_recovery']['IPAddress']
connection = socket.create_connection((host, 6379), timeout=120)
reader = connection.makefile('rb')

def encode(*args):
    parts = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
    return b'*' + str(len(parts)).encode() + b'\r\n' + b''.join(b'$' + str(len(p)).encode() + b'\r\n' + p + b'\r\n' for p in parts)

def reply():
    prefix = reader.read(1)
    line = reader.readline()
    if not line.endswith(b'\r\n'):
        raise RuntimeError('Redis 响应被截断')
    value = line[:-2]
    if prefix == b'-':
        raise RuntimeError('Redis 迁移操作失败：' + value.decode())
    if prefix == b'+': return value
    if prefix == b':': return int(value)
    if prefix == b'$':
        size = int(value)
        if size == -1: return None
        data = reader.read(size)
        if len(data) != size or reader.read(2) != b'\r\n': raise RuntimeError('Redis 数据被截断')
        return data
    raise RuntimeError('非预期的 Redis 响应')

connection.sendall(encode('AUTH', password)); assert reply() == b'OK'
started = time.time()
count = 0
digest = hashlib.sha256()
batch = []

def flush():
    global count
    if not batch: return
    commands = []
    for scope, key, value in batch:
        if mode == 'load': commands.append(encode('HSET', 'state:' + scope, key, json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()))
        elif mode == 'verify': commands.append(encode('HGET', 'state:' + scope, key))
        else: raise RuntimeError('未知操作')
    connection.sendall(b''.join(commands))
    for scope, key, value in batch:
        result = reply()
        if mode == 'load': assert result in (0, 1)
        else:
            if result is None or json.loads(result) != value: raise RuntimeError('复读校验失败：' + scope + '/' + key)
        digest.update(json.dumps([scope, key, value], ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode())
        count += 1
    batch.clear()

with source.open() as stream:
    for line in stream:
        batch.append(json.loads(line))
        if len(batch) >= 100: flush()
flush()
result = {'mode': mode, 'file': source.name, 'records': count, 'digest': digest.hexdigest(), 'seconds': round(time.time() - started, 2)}
(root / (source.name + '.' + mode + '.report.json')).write_text(json.dumps(result, indent=2))
print(json.dumps(result), flush=True)
connection.close()
