import concurrent.futures
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(sys.argv[1])
PHASE = sys.argv[2]
APP = 'agentmemory-recovery-candidate'
REDIS = 'agentmemory-recovery-redis'
env = dict(line.split('=', 1) for line in (ROOT / 'candidate.env').read_text().splitlines())
base = 'http://127.0.0.1:3311/agentmemory/'
report_path = ROOT / 'acceptance.json'
report = json.loads(report_path.read_text()) if report_path.exists() else {}

def request(path, body=None, timeout=90, auth=True):
    headers = {'Content-Type': 'application/json'}
    if auth: headers['Authorization'] = 'Bearer ' + env['AGENTMEMORY_SECRET']
    req = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        if path == 'health' and error.code == 503: return json.loads(error.read())
        raise

def ready(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            health = request('health', timeout=5)['health']
            if health['kvConnectivity']['status'] == 'ok' and health['connectionState'] == 'connected': return
        except (OSError, KeyError, ValueError): pass
        time.sleep(2)
    raise RuntimeError('服务未在期限内恢复')

def save():
    report_path.write_text(json.dumps(report, indent=2))

def index_check():
    status = request('graph/index')
    assert status['ready'] and status['coverage'] == 'complete'
    assert (status['nodes'], status['edges']) == (81956, 157842)
    return status

def canary_check():
    for _ in range(30):
        result = request('search', {'query': 'amrecovery20261023proof', 'limit': 5, 'targetLayer': 'memory'})
        if report['canaryId'] in json.dumps(result): return True
        time.sleep(2)
    raise RuntimeError('重启后未检索到验证记录')

def inspect(name):
    return json.loads(subprocess.check_output(['docker', 'inspect', name]))[0]

def memory(name):
    pid = inspect(name)['State']['Pid']
    group = next(line.split('::', 1)[1] for line in Path(f'/proc/{pid}/cgroup').read_text().splitlines() if line.startswith('0::'))
    return int((Path('/sys/fs/cgroup') / group.lstrip('/') / 'memory.current').read_text())

ready()
if PHASE == 'baseline':
    try: request('sessions?limit=2', auth=False)
    except urllib.error.HTTPError as error: assert error.code == 401
    else: raise RuntimeError('未授权请求没有被拒绝')
    report['graphIndex'] = index_check()
    sessions = request('sessions?limit=2')['sessions']
    assert len(sessions) == 2
    if 'canaryId' not in report:
        created = request('remember', {'content': 'amrecovery20261023proof memory restart persistence verification', 'type': 'fact', 'ttlDays': 1})
        assert created['success']
        report['canaryId'] = created['memory']['id']; save()
    assert canary_check()
    restarts_before = inspect(APP)['RestartCount']
    results = []
    samples = []
    stop = threading.Event()
    def sample():
        while not stop.is_set():
            samples.append({'time': time.time(), 'app': memory(APP), 'redis': memory(REDIS)})
            stop.wait(3)
    sampler = threading.Thread(target=sample); sampler.start()
    def probe(index):
        started = time.time()
        path, body = ('search', {'query': ['AgentMemory', '内存', 'Dify', '部署'][index % 4], 'limit': 5}) if index % 2 == 0 else ('graph/query', {'query': 'AgentMemory', 'limit': 5})
        result = request(path, body)
        assert result.get('success', True) is not False
        return {'endpoint': path, 'seconds': round(time.time() - started, 3)}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool: results.extend(pool.map(probe, range(40)))
        for _ in range(3):
            started = time.time(); result = request('insights?limit=5')
            assert result['success'] and len(result['insights']) == 5
            results.append({'endpoint': 'insights', 'seconds': round(time.time() - started, 3)})
        time.sleep(15)
    finally: stop.set(); sampler.join()
    assert inspect(APP)['RestartCount'] == restarts_before, '正常负载期间发生意外重启'
    report['baseline'] = {'requests': results, 'samples': samples, 'peakAppBytes': max(x['app'] for x in samples), 'peakRedisBytes': max(x['redis'] for x in samples), 'lastSample': samples[-1], 'unexpectedRestarts': 0}
    save(); print(json.dumps({k: v for k, v in report['baseline'].items() if k not in ('requests', 'samples')}), flush=True)
elif PHASE in ('engine-kill', 'worker-stop', 'redis-kill'):
    before = inspect(APP)['RestartCount']
    started = time.time()
    if PHASE == 'redis-kill':
        redis_pid = inspect(REDIS)['State']['Pid']
        assert redis_pid > 1
        os.kill(redis_pid, signal.SIGKILL)
    else:
        top = subprocess.check_output(['docker', 'top', APP, '-eo', 'pid,args'], text=True)
        needle = '/usr/local/bin/iii' if PHASE == 'engine-kill' else '/app/dist/index.mjs'
        candidates = [int(line.split()[0]) for line in top.splitlines()[1:] if needle in line]
        assert len(candidates) == 1
        os.kill(candidates[0], signal.SIGKILL if PHASE == 'engine-kill' else signal.SIGSTOP)
    time.sleep(3)
    deadline = started + 150
    if PHASE != 'redis-kill':
        while inspect(APP)['RestartCount'] <= before and time.time() < deadline: time.sleep(2)
        assert inspect(APP)['RestartCount'] > before
    ready(max(1, deadline-time.time()))
    assert canary_check(); index_check()
    report[PHASE] = {'recoveredSeconds': round(time.time()-started, 2), 'appRestartsBefore': before, 'appRestartsAfter': inspect(APP)['RestartCount'], 'canaryPersisted': True, 'graphIndexComplete': True}
    save(); print(json.dumps(report[PHASE]), flush=True)
elif PHASE == 'cleanup':
    result = request('forget', {'memoryId': report['canaryId']})
    assert result['success']; report['canaryRemoved'] = True; save(); print('canary removed')
else: raise RuntimeError('未知验收阶段')
