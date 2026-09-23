"""把本机微信数据同步到 aizee 服务端（账号/消息/联系人/群/群成员/会话 + 图片）。

两个入口：

    py sync_to_server.py init          # 首次初始化：全量上传
    py sync_to_server.py incremental    # 增量：定时 job 就跑这个

增量入口自己管节奏，每台机器只需要加一个定时任务（建议 5~15 分钟）：
账号自身信息、消息和图片每轮都同步；四类实体超过 24 小时才重新整表覆盖；
超过 7 天做一次全量兜底（微信换设备登录会补写低于水位线的历史消息，按水位线
收窄的扫描看不见它们）。节奏状态存在 _sync_state.json，不需要配多个 job。

手动入口（排障用，跳过节奏判断）：

    py sync_to_server.py entities | messages | media | sweep

token 从环境变量/.env 读取，不写入仓库：

- WECHAT_SYNC_TOKEN：同步服务的凭据（必填）。归档接口和图库都在同一个服务上、
  用同一个凭据，所以只配这一个就够了。
- AIZEE_TOKEN：图库 token。留空时自动复用 WECHAT_SYNC_TOKEN，只有图库凭据与
  同步凭据不同的部署才需要单独配。

    WECHAT_SYNC_TOKEN=xxx py sync_to_server.py incremental

要连别的服务（例如本地自建的 prime-contact）时才用这几个开关：

- WECHAT_SYNC_BASE=http://127.0.0.1:9001/api/wechat：换服务地址。
- WECHAT_SYNC_AUTH=exchange：只有老的 aizee-crm 网关才需要（它要把 api token
  换成短期 JWT）；prime 这套默认 direct，token 直接当 Bearer 用。
- WECHAT_SYNC_HEALTH_PATH=/actuator/health：换健康探针路径。
- WECHAT_SYNC_ENV_FILE=.env.xxx：换读另一份配置，不动 .env。
- SEND_SENDER_USERNAME=1：服务端 wechat_messages 有 sender_username 列时才开。

实现要点（均已实测验证）：
- 服务端接口不带版本前缀（/messages 而不是 /v1/messages）。SYNC_BASE 已含
  服务前缀 /api/wechat，再拼版本段会被网关判为未声明路由，直接 403。
- account 只是各表的关联键，本身不含身份信息，所以每轮单独上报一次账号自身
  的昵称/微信号/头像（取自本地 contact 表里 username == wxid 的那行）。
- 消息以 (account, chat, sort_seq, local_id) 幂等，可重复执行、可中断续传。
  sort_seq 是秒级时间戳，同秒多条消息必然撞号，故 local_id 必须在唯一键内。
- 同一 chat 按 sort_seq 升序发送，前一批 200 后再发下一批，整会话确认后才
  推进水位线；崩在 POST 与 PUT 之间时从旧水位线重放，靠幂等键去重。
- 增量扫描用 sort_seq >= watermark（不是 >）：水位线落在某一秒中间时，>
  会永久丢掉那一秒剩下的消息；>= 重读边界那一秒，重复的记 skipped。
- 同一会话的消息分片在多个本地库中，local_id 仅在分片内唯一；因此媒体不通过
  (chat, local_id) 二次查库，而是用批量扫描得到的 md5 直接定位 .dat 解密。
- 图库只接受 png/jpg/jpeg/webp/gif；wxgf(HEVC) 原图经 ffmpeg 转 jpg，缺
  ffmpeg 时回退缩略图。画质变体编进图库名，日后补原图不会撞重名。
- 同一账号同时只允许一个同步进程（锁文件），否则会并行推进同一 chat 的水位线。
- 响应外壳两种宿主都兼容：aizee-crm 顶层直接给业务字段，prime-contact 用
  BaseResponse 且业务失败也回 HTTP 200（code=400/500）。因此只判 HTTP 状态码不够，
  必须同时要求 code==200，否则失败会被当成功、水位线会被推进到没落库的位置。
"""

import json
import os
import re
import socket
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from wechatauto.db import MSG_TYPE_NAMES, WeChatDB, list_accounts
from wechatauto.media import MediaDownloader

_HERE = os.path.dirname(os.path.abspath(__file__))


def load_dotenv(path=None):
    """把同目录 .env 读进 os.environ。标准库实现，不依赖 python-dotenv。

    已存在的真实环境变量优先（与 dotenv 惯例一致），便于临时覆盖：
        AIZEE_TOKEN=xxx py sync_to_server.py ...
    支持 `KEY=VALUE`、`export KEY=VALUE`、# 注释、空行、值两侧的引号。

    默认读同目录 .env；联调其它宿主时用 WECHAT_SYNC_ENV_FILE 指向别的配置文件，
    这样不用为了连本地服务去改生产 .env。
    """
    path = path or os.environ.get("WECHAT_SYNC_ENV_FILE") \
        or os.path.join(_HERE, ".env")
    try:
        raw = open(path, "rb").read()
    except OSError:
        return {}
    for enc in ("utf-8-sig", "utf-8", "gbk", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        return {}
    found = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, val = line.split("=", 1)
        key, val = key.strip(), val.strip()
        if not key:
            continue
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        found[key] = val
        os.environ.setdefault(key, val)      # 真实环境变量优先
    return found


DOTENV = load_dotenv()


def _env_int(key, default):
    try:
        return int(os.environ.get(key, "") or default)
    except ValueError:
        return default


# 现在归档接口和图库接口都在同一个服务上（prime 那套），只是路径不同：
#   /api/wechat        传消息/联系人/会话/水位线
#   /api/file-gallery  传图片二进制
# 所以两个地址默认同主机。换服务时才需要改。
SYNC_BASE = os.environ.get("WECHAT_SYNC_BASE",
                           "https://primeapi.aizee.cc/api/wechat").rstrip("/")
GALLERY_BASE = os.environ.get("AIZEE_GALLERY_BASE",
                              "https://primeapi.aizee.cc").rstrip("/")

# 同步 token。同一个服务既用它鉴权归档接口、也用它传图库，所以只配这一个就够了：
# AIZEE_TOKEN 留空时自动复用同步 token，只有图库 token 与同步 token 不同的部署才需要另配。
SYNC_TOKEN = os.environ.get("WECHAT_SYNC_TOKEN", "")
TOKEN = os.environ.get("AIZEE_TOKEN", "") or SYNC_TOKEN

# 认证方式。两种宿主的鉴权入口不同：
#   direct   （默认）—— prime-contact 等宿主：JWT 与 user_api_token 明文 token 都直接
#                      当 Bearer 用，没有 /user/auth/exchange 这个端点；
#   exchange         —— 老的 aizee-crm 网关：api token 先换 5 分钟 JWT 再带 Bearer。
SYNC_AUTH_MODE = os.environ.get("WECHAT_SYNC_AUTH", "direct").strip().lower()

# 健康检查路径。prime 这套没有 /health，用归档接口自身当探针最可靠：
# 一次请求同时验证「服务可达 + token 有效 + 归档模块已加载 + 数据库通」。
HEALTH_PATH = os.environ.get("WECHAT_SYNC_HEALTH_PATH", "/watermark/__health__")

# 抓图片 AES 密钥时等待「用户点开一张图」的秒数。微信只在点开图片查看后才把密钥放进
# 内存，所以这段等待是留给人去看图的。定时任务里置 0，避免每轮白等两分钟。
IMAGE_KEY_WAIT = _env_int("IMAGE_KEY_WAIT_SEC", 120)

BATCH = _env_int("SYNC_BATCH", 500)                  # 单批条数，服务端上限 1000
MAX_BODY = _env_int("SYNC_MAX_BODY", 24 * 1024 * 1024)   # 请求体上限，服务端 32MiB
WM_BATCH = _env_int("SYNC_WM_BATCH", 500)            # 单次 PUT 的 chat 数，上限 1000
# md5 -> {name, url}。按内容哈希去重，多账号共享：同一张图不同账号发过也只上传一次
CACHE_PATH = os.path.join(_HERE, "_media_url_cache.json")
STATE_PATH = os.path.join(_HERE, "_sync_state.json")        # 按账号分区的节奏状态


def _lock_path(account):
    """锁按账号隔离：不同账号在服务端互不影响，可各自独立同步。"""
    safe = re.sub(r"[^0-9A-Za-z_.-]", "_", account)
    return os.path.join(_HERE, "_sync_%s.lock" % safe)
# 服务端加了 sender_username 列后保持 1；未加时置 0，否则整批 400 unknown field
# 服务端 wechat_messages 有 sender_username 列时才发这个字段（发多了整批 400 unknown field）
# prime 那套归档表没有这一列，所以默认不发
SEND_SENDER_USERNAME = os.environ.get("SEND_SENDER_USERNAME", "0") != "0"
ENTITY_INTERVAL = _env_int("ENTITY_INTERVAL_SEC", 24 * 3600)
SWEEP_INTERVAL = _env_int("SWEEP_INTERVAL_SEC", 7 * 24 * 3600)

# ----------------------------------------------------------------------
# CRM 网关鉴权：api token(usr_*) → 短期 JWT(默认 5 分钟) → 请求头 Bearer。
# 认证端点挂在网关 /api 根下，与同步服务的 /api/wechat 前缀不同级。
# ----------------------------------------------------------------------
_AUTH = {"jwt": "", "exp": 0.0, "refresh": ""}


def _gateway_api_base():
    """SYNC_BASE 所在网关的 /api 根（scheme://host/api），认证端点在这里。"""
    parts = urllib.parse.urlsplit(SYNC_BASE)
    return "%s://%s/api" % (parts.scheme, parts.netloc)


def _auth_exchange(body, path):
    data = json.dumps(body).encode("utf-8")
    status, resp = _retry("POST", _gateway_api_base() + path, data,
                          {"Content-Type": "application/json"})
    payload = json.loads(resp.decode("utf-8", "replace"))
    d = payload.get("data") if isinstance(payload, dict) else None
    if status != 200 or not (d or {}).get("token"):
        raise RuntimeError("CRM 认证失败: POST %s -> %d %s"
                           % (path, status, resp[:300].decode("utf-8", "replace")))
    _AUTH["jwt"] = d["token"]
    _AUTH["exp"] = time.time() + int(d.get("expiresIn") or 300) - 60
    if d.get("refreshToken"):
        _AUTH["refresh"] = d["refreshToken"]


def sync_auth_headers():
    """返回同步服务请求头。direct 模式直接用 token；否则换发(或复用)JWT。"""
    if not SYNC_TOKEN:
        return {}
    if SYNC_AUTH_MODE == "direct":
        return {"Authorization": "Bearer " + SYNC_TOKEN}
    if not _AUTH["jwt"] or time.time() >= _AUTH["exp"]:
        if _AUTH["refresh"]:
            try:
                _auth_exchange({"refreshToken": _AUTH["refresh"]},
                               "/user/auth/refresh")
            except Exception:
                _auth_exchange({"apiToken": SYNC_TOKEN}, "/user/auth/exchange")
        else:
            _auth_exchange({"apiToken": SYNC_TOKEN}, "/user/auth/exchange")
    return {"Authorization": "Bearer " + _AUTH["jwt"]}

# ----------------------------------------------------------------------
# 单进程锁：同一账号并行推进同一 chat 的水位线会破坏「按序确认」语义
# ----------------------------------------------------------------------
def _alive(pid):
    try:
        import psutil
        return psutil.pid_exists(pid)
    except Exception:
        return True          # 判不了就当活着，宁可不抢锁


def acquire_lock(account):
    """原子创建锁文件；持有者进程已死则接管。返回 True 表示拿到锁。"""
    LOCK_PATH = _lock_path(account)
    for _ in range(2):
        try:
            fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w") as f:
                f.write(str(os.getpid()))
            return True
        except FileExistsError:
            try:
                with open(LOCK_PATH) as f:
                    pid = int((f.read() or "0").strip() or 0)
            except (OSError, ValueError):
                pid = 0
            if pid and pid != os.getpid() and _alive(pid):
                print("  已有同步进程在处理该账号（pid=%d），跳过" % pid)
                return False
            print("  清理过期锁（pid=%s 已不存在）" % pid)
            try:
                os.unlink(LOCK_PATH)
            except OSError:
                return False
    return False


def release_lock(account):
    try:
        os.unlink(_lock_path(account))
    except OSError:
        pass


def _acct_state(state, account):
    """取某账号的节奏状态分区。旧的扁平格式按缺失处理（会多跑一次实体+兜底）。"""
    return state.setdefault("accounts", {}).setdefault(account, {})


def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def _save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


# ----------------------------------------------------------------------
# HTTP：标准库实现，5xx 与网络错误指数退避重试（投递语义为「至少一次」）
# ----------------------------------------------------------------------
def _req(method, url, body=None, headers=None, timeout=120):
    """返回 (status, body_bytes)。HTTP 错误不抛异常，网络错误抛 URLError。"""
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _retry(method, url, body=None, headers=None, timeout=120, attempts=5):
    """对 5xx 和网络异常重试；4xx 直接返回（请求本身有问题，重试无意义）。"""
    delay = 1.0
    last = None
    for i in range(attempts):
        try:
            status, data = _req(method, url, body, headers, timeout)
            if status < 500:
                return status, data
            last = "HTTP %d: %s" % (status, data[:200].decode("utf-8", "replace"))
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = "%s: %s" % (type(e).__name__, e)
        if i < attempts - 1:
            print("      重试 %d/%d（%s）" % (i + 1, attempts - 1, last))
            time.sleep(delay)
            delay = min(delay * 2, 16)
    raise RuntimeError("重试 %d 次仍失败: %s" % (attempts, last))


def _response(label, status, raw):
    """校验响应并解出业务负载；不是 2xx 一律抛错，返回 dict。

    两种宿主的响应外壳不同，这里都兼容：
    - aizee-crm：顶层就是业务字段（total/inserted/... 或 watermark）；
    - prime-contact：BaseResponse `{code,message,data}`，且**业务失败也用 HTTP 200
      承载**（校验错误 code=400、数据库失败 code=500）。所以只判 HTTP 状态码会把
      失败当成功，进而把水位线推进到没真正落库的位置 —— 必须同时检查 code。
    """
    text = raw.decode("utf-8", "replace")
    if status != 200:
        raise RuntimeError("%s -> HTTP %d %s" % (label, status, text[:300]))
    try:
        payload = json.loads(text)
    except ValueError:
        return {}
    if not isinstance(payload, dict):
        return {}
    if "code" in payload:
        if payload.get("code") != 200:
            raise RuntimeError("%s -> code=%s %s" % (
                label, payload.get("code"), str(payload.get("message"))[:200]))
        data = payload.get("data")
        return data if isinstance(data, dict) else {}
    return payload


def sync_post(path, payload):
    """向同步服务 POST JSON，失败视为致命错误（避免带着坏数据继续推进）。"""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = sync_auth_headers()
    headers["Content-Type"] = "application/json"
    status, data = _retry("POST", SYNC_BASE + path, body, headers)
    return _response("POST " + path, status, data)


def sync_put_watermark(account, marks):
    """PUT 水位线；服务端按 chat 取 GREATEST，较小值是成功的空操作。"""
    items = list(marks.items())
    for i in range(0, len(items), WM_BATCH):
        chunk = dict(items[i:i + WM_BATCH])
        body = json.dumps({"watermark": chunk}).encode("utf-8")
        headers = sync_auth_headers()
        headers["Content-Type"] = "application/json"
        status, data = _retry("PUT", "%s/watermark/%s"
                              % (SYNC_BASE, urllib.parse.quote(account)),
                              body, headers)
        _response("PUT watermark", status, data)


def gallery_get_by_name(name):
    """图库按名查已上传文件，返回 URL；404 返回 None。"""
    url = "%s/api/file-gallery/images/by-name?%s" % (
        GALLERY_BASE, urllib.parse.urlencode({"name": name}))
    status, data = _retry("GET", url, None, {"Authorization": "Bearer " + TOKEN})
    if status == 404:
        return None
    if status != 200:
        raise RuntimeError("by-name -> %d %s" % (status, data[:200].decode("utf-8", "replace")))
    return (json.loads(data).get("data") or {}).get("url")


def gallery_upload(name, filename, content, mime):
    """multipart 上传图片，返回 URL。重名（400 文件名称已存在）时按名取回。"""
    boundary = "----wxsync%d" % int(time.time() * 1000)
    out = []
    for field, value in (("name", name),):
        out.append(("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n"
                    % (boundary, field, value)).encode("utf-8"))
    out.append(("--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\n"
                "Content-Type: %s\r\n\r\n" % (boundary, filename, mime)).encode("utf-8"))
    out.append(content)
    out.append(("\r\n--%s--\r\n" % boundary).encode("utf-8"))
    body = b"".join(out)
    status, data = _retry("POST", GALLERY_BASE + "/api/file-gallery/images", body, {
        "Authorization": "Bearer " + TOKEN,
        "Content-Type": "multipart/form-data; boundary=" + boundary,
    })
    text = data.decode("utf-8", "replace")
    if status == 200:
        return (json.loads(text).get("data") or {}).get("url")
    if status == 400 and "已存在" in text:
        return gallery_get_by_name(name)
    raise RuntimeError("upload %s -> %d %s" % (name, status, text[:300]))


# ----------------------------------------------------------------------
# 字段规整：严格贴合服务端 DDL 长度/类型/非负约束，避免整批 400
# ----------------------------------------------------------------------
INT32_MAX = 2147483647


def _s(v, limit):
    """非空字符串字段：None 转 ""，按字符数截断。"""
    if v is None:
        return ""
    s = v if isinstance(v, str) else str(v)
    return s[:limit]


def _text(v, max_bytes):
    """可空 TEXT 字段：按 UTF-8 字节截断，保证不截出半个字符。"""
    if v is None:
        return None
    s = v if isinstance(v, str) else str(v)
    b = s.encode("utf-8")
    if len(b) <= max_bytes:
        return s
    return b[:max_bytes].decode("utf-8", "ignore")


def _nn(v):
    """非负整数字段：None/负数/非数字统一归零。"""
    try:
        n = int(v)
    except (TypeError, ValueError):
        return 0
    return n if n >= 0 else 0


def _int32(v):
    """type_code 落在 INT 范围；微信包装类型超界时取低字节的真实类型。"""
    n = _nn(v)
    return n if n <= INT32_MAX else (n & 0xFF)


def _opt_int(v):
    """可空 BIGINT：None 或非法值为 NULL。"""
    if v is None:
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def send_batches(path, account, key, items):
    """按条数和请求体大小双重限制分批 POST，累计统计。"""
    totals = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0}
    buf, buf_bytes = [], 0
    def flush():
        nonlocal buf, buf_bytes
        if not buf:
            return
        r = sync_post(path, {"account": account, key: buf})
        for k in totals:
            totals[k] += r.get(k, 0)
        buf, buf_bytes = [], 0
    for it in items:
        n = len(json.dumps(it, ensure_ascii=False).encode("utf-8")) + 2
        if buf and (len(buf) >= BATCH or buf_bytes + n > MAX_BODY):
            flush()
        buf.append(it)
        buf_bytes += n
    flush()
    return totals


# ----------------------------------------------------------------------
# 账号自身信息：account 是各表的关联键，本身不含身份信息，单独上报一行
# ----------------------------------------------------------------------
def collect_self(db, account):
    """采集账号自己的昵称/微信号/头像。

    微信把自己也存在 contact 表里（username == wxid），alias 是用户自设的微信号，
    big_head_url 是公网 CDN 地址（不经图库）。查不到那行时仍上报一行，让服务端
    至少有这个 account 的存在记录和活跃时间。
    """
    row = None
    conn = db._contact_conn()
    if conn:
        try:
            row = conn.execute(
                "SELECT alias, nick_name, remark, big_head_url FROM contact "
                "WHERE username=? LIMIT 1", (account,)).fetchone()
        except sqlite3.DatabaseError:
            row = None
        finally:
            conn.close()
    return {
        "alias": _s(row["alias"], 255) if row else "",
        "nick_name": _s(row["nick_name"], 255) if row else "",
        "remark": _s(row["remark"], 255) if row else "",
        "avatar_url": _s(row["big_head_url"], 1024) if row else "",
        "collector_host": _s(socket.gethostname(), 255),
        "last_sync_at": int(time.time()),
    }


def sync_account(db, account):
    """上报账号自身信息，每轮一行。失败只告警，绝不打断本轮同步。

    last_sync_at 每轮都变，所以这行必然记 updated —— 这是有意的：其余实体无变化
    时不写库、updated_at 不动，没有它就分不清「账号信息没变」和「采集机挂了」。

    这一行是运维元数据，价值远低于消息，所以任何失败都只打日志：服务端未部署
    /accounts 时网关返回 500（未声明路由不是 404），而 500 同时也是「数据库写入
    失败、应退避重试」的正常信号，无法靠状态码区分两者。与其猜，不如让消息同步
    照常进行，把失败原因打出来由运维判断。
    """
    item = collect_self(db, account)
    body = json.dumps({"account": account, "accounts": [item]},
                      ensure_ascii=False).encode("utf-8")
    try:
        headers = sync_auth_headers()
        headers["Content-Type"] = "application/json"
        status, data = _retry("POST", SYNC_BASE + "/accounts", body, headers)
        _response("POST /accounts", status, data)
        print("账号信息: %s（%s）" % (item["nick_name"] or "昵称未取到",
                                     item["alias"] or "无微信号"))
        return item
    except Exception as exc:
        print("账号信息上报失败，跳过本轮: %s: %s" % (type(exc).__name__, exc))
    return None


# ----------------------------------------------------------------------
# 四类 upsert 实体
# ----------------------------------------------------------------------
def collect_contacts(db):
    """全部 contact（含好友、群、公众号、自己），取 username/nick_name/remark/alias。"""
    conn = db._contact_conn()
    if not conn:
        return []
    try:
        rows = conn.execute(
            "SELECT username, nick_name, remark, alias FROM contact").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        if not (r["username"] or "").strip():
            continue
        out.append({
            "username": _s(r["username"], 255),
            "nick_name": _s(r["nick_name"], 255),
            "remark": _s(r["remark"], 255),
            "alias": _s(r["alias"], 255),
        })
    return out


def collect_sessions(db):
    """会话列表：本地把 last_msg_sender 与显示名合并过，这里直接查表分开取两列。"""
    out = []
    for rel, path, _ in db._db_files:
        if os.path.basename(path) != "session.db":
            continue
        conn = db._open(rel)
        try:
            rows = conn.execute(
                "SELECT username, unread_count, summary, last_timestamp, "
                "last_msg_sender, last_sender_display_name "
                "FROM SessionTable WHERE is_hidden=0").fetchall()
        finally:
            conn.close()
        for r in rows:
            if not (r["username"] or "").strip():
                continue
            out.append({
                "username": _s(r["username"], 255),
                "unread_count": _nn(r["unread_count"]),
                "summary": _text(r["summary"], 65535),
                "last_timestamp": _nn(r["last_timestamp"]),
                "last_msg_sender": _s(r["last_msg_sender"], 255),
                "last_sender_name": _s(r["last_sender_display_name"], 255),
            })
        break
    return out


def sync_entities(db, account):
    """联系人 → 群 → 群成员 → 会话。源端为空时不发请求，保留服务端已有记录。"""
    contacts = collect_contacts(db)
    print("联系人 %d 条" % len(contacts))
    if contacts:
        print("   ", send_batches("/contacts", account, "contacts", contacts))

    groups = db.get_groups()
    rooms, members = [], []
    for g in groups:
        if not (g["username"] or "").strip():
            continue
        rooms.append({
            "username": _s(g["username"], 255),
            "name": _s(g["name"], 255),
            "owner": _s(g["owner"], 255),
            "member_count": _nn(g["member_count"]),
        })
        for m in g.get("members") or []:
            if not (m["username"] or "").strip():
                continue
            members.append({
                "room_username": _s(g["username"], 255),
                "member_username": _s(m["username"], 255),
                "nick_name": _s(m["nick_name"], 255),
                "remark": _s(m["remark"], 255),
                "is_owner": 1 if m["is_owner"] else 0,
            })
    print("群 %d 个" % len(rooms))
    if rooms:
        print("   ", send_batches("/chatrooms", account, "chatrooms", rooms))
    print("群成员 %d 条" % len(members))
    if members:
        print("   ", send_batches("/chatroom_members", account,
                                  "chatroom_members", members))

    sessions = collect_sessions(db)
    print("会话 %d 条" % len(sessions))
    if sessions:
        print("   ", send_batches("/sessions", account, "sessions", sessions))


# ----------------------------------------------------------------------
# 消息：跨分库合并同一会话，按 sort_seq 升序，(account, chat, sort_seq) 幂等
# ----------------------------------------------------------------------
MSG_SELECT = ("SELECT local_id, local_type, server_id, real_sender_id, create_time, "
              "message_content, packed_info_data, sort_seq FROM %s")


def _name2id_index(conn):
    """分片库的 Name2Id(rowid → user_name)。real_sender_id 就是这张表的 rowid。

    必须在分片内解析：这是个**按分片的 id 空间**，同一个数字在 message_0.db 和
    message_1.db 里通常是不同的人（实测 shard0 的 5 是 zhaojuan7390，
    shard1 的 5 是 wxid_2sbtuurztov522），合成一张全局表会张冠李戴。
    """
    idx = {}
    try:
        for rid, name in conn.execute("SELECT rowid, user_name FROM Name2Id"):
            if name:
                idx[int(rid)] = name
    except sqlite3.DatabaseError:
        pass
    return idx


def _sender_username(sid, chat, senders, account):
    """发送者 wxid：可 join contacts.username。sender_id 是本机数字 id，跨机无意义。

    自己 → account；其余查该分片的 Name2Id；单聊非自己的一方必然是对端，由 chat 推出。
    """
    u = _lookup_sender(senders, sid)
    if u:
        return u
    if sid in (2, "2"):
        return account
    if not chat.endswith("@chatroom"):
        return chat
    return ""


def _lookup_sender(senders, sid):
    """在 {rowid: user_name} 里查发送者；sender_id 可能是 int 或 str。"""
    return WeChatDB._lookup_sender_id(senders, sid)


def scan_messages(db, watermark=None):
    """返回 {chat: [消息 dict 升序]} 与统计。同完整键重复时保留首条并计数。

    watermark 为 {chat: 已同步的最大 sort_seq} 时只扫该会话 sort_seq >= 水位线
    的消息（增量）。用 >= 而非 >：sort_seq 是秒级，水位线落在某一秒中间时 >
    会永久丢掉那一秒剩下的消息；>= 重读边界那一秒，重复的由服务端记 skipped。
    """
    idx = db._build_md5_index()
    nicks = db._nickname_index()
    account = db.wxid
    self_nick = db.get_self_info().get("nick_name", "我")

    raw = {}
    for rel in db._message_dbs():
        conn = db._open(rel)
        try:
            n2i = _name2id_index(conn)     # 该分片的 rowid → user_name
            tabs = [t[0] for t in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'Msg_%'")]
            for t in tabs:
                chat = idx.get(t[4:], t[4:])
                since = (watermark or {}).get(chat)
                try:
                    if since:
                        rows = conn.execute(
                            MSG_SELECT % t + " WHERE sort_seq >= ?",
                            (int(since),)).fetchall()
                    else:
                        rows = conn.execute(MSG_SELECT % t).fetchall()
                except sqlite3.DatabaseError:
                    continue
                # 记下每行的来源分片映射：id 是分片内的，过后合并就分不清了
                raw.setdefault(chat, []).extend((r, n2i) for r in rows)
        finally:
            conn.close()

    out, dup, unresolved = {}, 0, 0
    for chat, rows in raw.items():
        if len(chat) == 32 and re.fullmatch(r"[0-9a-f]{32}", chat):
            unresolved += 1          # contact/session 里已无此会话，只剩表名 md5
        rows.sort(key=lambda x: (_nn(x[0]["sort_seq"]), _nn(x[0]["local_id"])))
        seen, msgs = set(), []
        for r, n2i in rows:
            seq = _nn(r["sort_seq"])
            # 去重键必须与服务端唯一键一致：(chat, sort_seq, local_id)。
            # sort_seq 是秒级时间戳，同秒多条消息必然撞号，只按它去重会真丢数据。
            key = (seq, _nn(r["local_id"]))
            if key in seen:
                dup += 1
                continue
            seen.add(key)
            e = db._export_row(r, MSG_TYPE_NAMES)
            item = {
                "chat": _s(chat, 255),
                "chat_name": _s(nicks.get(chat, chat), 255),
                "local_id": _nn(e["local_id"]),
                "sort_seq": seq,
                "type": _s(e["type"], 32),
                "type_code": _int32(e["type_code"]),
                "sender_id": _s(e["sender_id"], 64),
                "sender_name": _s(db._resolve_sender(
                    r["real_sender_id"], n2i, nicks, self_nick, chat,
                    self_username=account), 255),
                "create_time": _nn(e["create_time"]),
                "content": _text(e["content"], 16777215),
                "server_id": _opt_int(e["server_id"]),
                "md5": _s(e["md5"], 32) or None,
                "media_url": None,
            }
            if SEND_SENDER_USERNAME:
                item["sender_username"] = _s(_sender_username(
                    r["real_sender_id"], chat, n2i, account), 255)
            msgs.append(item)
        if msgs:
            out[chat] = msgs
    return out, {"dup_key": dup, "unresolved_chats": unresolved}


def sync_messages(db, account, incremental=False):
    """同 chat 串行确认：前一批 200 后才发下一批，整会话确认后才推进水位线。

    incremental=True 时按服务端水位线收窄扫描范围；返回 (totals, by_chat)，
    by_chat 供媒体步骤复用，避免重复扫库。
    """
    status, data = _retry("GET", "%s/watermark/%s"
                          % (SYNC_BASE, urllib.parse.quote(account)),
                          None, sync_auth_headers())
    # prime-contact 把水位线放在 data.watermark 里，aizee-crm 直接放顶层，_response 已统一
    known = _response("GET watermark", status, data).get("watermark", {}) or {}
    print("服务端已有水位线 %d 个会话" % len(known))

    by_chat, stat = scan_messages(db, known if incremental else None)
    total = sum(len(v) for v in by_chat.values())
    print("待发消息 %d 条 / %d 会话（%s；完整键重复跳过 %d，会话名未解析 %d）"
          % (total, len(by_chat), "增量" if incremental else "全量",
             stat["dup_key"], stat["unresolved_chats"]))

    totals = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0}
    pending, done, sent = {}, 0, 0
    for chat in sorted(by_chat):
        msgs = by_chat[chat]
        for i in range(0, len(msgs), BATCH):
            r = send_batches("/messages", account, "messages", msgs[i:i + BATCH])
            for k in totals:
                totals[k] += r[k]
        # 整个会话已逐批确认落库，才把最大 sort_seq 作为水位线前缀
        pending[chat] = max(m["sort_seq"] for m in msgs)
        done += 1
        sent += len(msgs)
        if len(pending) >= WM_BATCH:
            sync_put_watermark(account, pending)
            pending = {}
        if done % 100 == 0 or done == len(by_chat):
            print("   会话 %d/%d，消息 %d/%d" % (done, len(by_chat), sent, total))
    if pending:
        sync_put_watermark(account, pending)
    print("消息合计:", totals)
    return totals, by_chat


# ----------------------------------------------------------------------
# 媒体：图片上传 + media_url 回填（服务端仅接受 png/jpg/jpeg/webp/gif）
# ----------------------------------------------------------------------
def _img_ext(data):
    """按魔数判断可上传格式；wxgf(HEVC) 与未知格式返回 None。"""
    if data[:3] == b"\xff\xd8\xff":
        return "jpg", "image/jpeg"
    if data[:4] == b"\x89PNG":
        return "png", "image/png"
    if data[:3] == b"GIF":
        return "gif", "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp", "image/webp"
    return None, None


def _decode(dl, path):
    """解密 .dat 为可上传图片。wxgf 在有 ffmpeg 时转 jpg，否则视为不支持。

    返回 ((bytes, ext, mime), None) 或 (None, 失败原因)。
    """
    try:
        data = dl.decrypt_image(path)
    except Exception:
        return None, "解密失败"
    ext, mime = _img_ext(data)
    if ext:
        return (data, ext, mime), None
    if data[:4] == b"wxgf":
        jpg = dl._wxgf_to_jpg(data)      # 需要 ffmpeg，缺失时返回 None
        if jpg:
            return (jpg, "jpg", "image/jpeg"), None
    return None, "格式不支持"


def _load_cache():
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_cache(cache):
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f)
    os.replace(tmp, CACHE_PATH)


def scan_images(db, by_chat=None):
    """图片消息 → {md5: [消息 dict]}。不走 get_message_row，规避分片 local_id 歧义。

    by_chat 传入 sync_messages 的扫描结果时直接复用，不重复扫库。
    """
    if by_chat is None:
        by_chat, _ = scan_messages(db)
    out = {}
    for chat, msgs in by_chat.items():
        for m in msgs:
            if m["type_code"] == 3 and m["md5"]:
                out.setdefault(m["md5"], []).append(m)
    return out


def sync_media(db, account, by_chat=None):
    """原图优先、wxgf 回退缩略图；md5 作图库唯一名，本地缓存 + by-name 双重去重。

    上轮已上传但回填未确认的消息记在 _sync_state.json 的 pending 里，本轮一并重发：
    增量扫描窗口会随水位线前移，不重试就会永久漏掉这些图。
    """
    if not TOKEN:
        raise SystemExit("缺少 AIZEE_TOKEN 环境变量")
    by_md5 = scan_images(db, by_chat)
    print("图片消息 %d 条，唯一 md5 %d 个"
          % (sum(len(v) for v in by_md5.values()), len(by_md5)))
    if not by_md5:
        print("本轮没有图片消息，跳过图片上传")
        return

    dl = MediaDownloader(db)
    if not dl.detect_image_key(monitor_timeout=IMAGE_KEY_WAIT):
        # 图片是附加信息，不该拖垮整轮：这里只跳过图片，消息和实体照常。
        # 用 SystemExit 会绕过 main() 里按账号隔离的 except Exception，导致本轮
        # 后续账号完全不处理（而且退出码语义也对不上「图片这步跳过」）。
        print("取不到图片解密密钥（确认微信已登录并点开过一张图）；"
              "本轮跳过图片，消息同步不受影响")
        return

    attach = {}
    for root, _, files in os.walk(os.path.join(db.account_dir, "msg", "attach")):
        for f in files:
            attach.setdefault(f, os.path.join(root, f))

    cache = _load_cache()
    stat = {"缓存命中": 0, "图库已存在": 0, "新上传": 0,
            "磁盘缺失": 0, "格式不支持": 0, "解密失败": 0}
    backfill = []
    for n, (md5, msgs) in enumerate(sorted(by_md5.items()), 1):
        # 原图优先、回退缩略图；画质变体编进图库名，日后补原图不会撞重名
        got, why = None, "磁盘缺失"
        for suffix, tag in ((".dat", ""), ("_t.dat", "_t")):
            p = attach.get(md5 + suffix)
            if not p:
                continue
            got, why = _decode(dl, p)
            if got:
                got = got + (tag,)
                break
        if not got:
            stat[why] += 1
            continue
        data, ext, mime, tag = got
        name = "wx_%s%s.%s" % (md5, tag, ext)

        hit = cache.get(md5)
        if isinstance(hit, dict) and hit.get("name") == name:
            url = hit["url"]                     # 同名同画质，已传过
            stat["缓存命中"] += 1
        else:
            url = gallery_get_by_name(name)
            if url:
                stat["图库已存在"] += 1
            else:
                url = gallery_upload(name, name, data, mime)
                stat["新上传"] += 1
            if not url:
                continue
            cache[md5] = {"name": name, "url": url}
            if len(cache) % 50 == 0:
                _save_cache(cache)
        for m in msgs:
            backfill.append(dict(m, media_url=url))
        if n % 200 == 0 or n == len(by_md5):
            print("   进度 %d/%d %s" % (n, len(by_md5), stat))
    _save_cache(cache)

    # 并入上轮已上传但回填未确认的消息：增量窗口随水位线前移，不重试会永久漏掉
    state = _load_json(STATE_PATH, {})
    pend = _acct_state(state, account).get("pending_backfill") or []
    if pend:
        have = {(m["chat"], m["sort_seq"], m["local_id"]) for m in backfill}
        add = [m for m in pend
               if (m.get("chat"), m.get("sort_seq"), m.get("local_id")) not in have]
        backfill.extend(add)
        print("并入上轮未确认的回填 %d 条" % len(add))

    print("回填 %d 条消息 media_url" % len(backfill))
    if backfill:
        # 先落盘 pending 再发送：发送中崩溃时下一轮仍会重试
        _acct_state(state, account)["pending_backfill"] = backfill[:5000]
        _save_json(STATE_PATH, state)
        print("   ", send_batches("/messages", account, "messages", backfill))
        state = _load_json(STATE_PATH, {})
        _acct_state(state, account)["pending_backfill"] = []     # 已确认落库
        _save_json(STATE_PATH, state)
    print("媒体统计:", stat)


def _run(cmd, db, account):
    full = _load_json(STATE_PATH, {})
    state = _acct_state(full, account)
    now = int(time.time())

    # 一行数据，不进 24 小时节流：否则 last_sync_at 反映不了采集活跃度
    sync_account(db, account)

    if cmd == "init":
        sync_entities(db, account)
        _, by_chat = sync_messages(db, account, incremental=False)
        state["last_entities"] = state["last_sweep"] = now
        if TOKEN:
            sync_media(db, account, by_chat)
        else:
            print("未设置 AIZEE_TOKEN，跳过图片上传（之后可单独跑 media 补上）")
    elif cmd == "entities":
        sync_entities(db, account)
        state["last_entities"] = now
    elif cmd in ("messages", "sweep"):
        sync_messages(db, account, incremental=(cmd == "messages"))
        if cmd == "sweep":
            state["last_sweep"] = now
    elif cmd == "media":
        sync_media(db, account)
    else:                                     # incremental：定时 job 的入口
        if now - int(state.get("last_entities", 0)) >= ENTITY_INTERVAL:
            sync_entities(db, account)
            state["last_entities"] = now
        else:
            print("四类实体未到重跑间隔，跳过")
        # 微信换设备登录会补写低于水位线的历史消息，按水位线收窄的扫描看不见它们，
        # 所以周期性做一次全量兜底（全部幂等 skipped，只补真正缺的）
        sweep = now - int(state.get("last_sweep", 0)) >= SWEEP_INTERVAL
        print("本轮消息范围:", "全量兜底" if sweep else "增量")
        _, by_chat = sync_messages(db, account, incremental=not sweep)
        if sweep:
            state["last_sweep"] = now
        if TOKEN:
            sync_media(db, account, by_chat)
        else:
            print("未设置 AIZEE_TOKEN，跳过图片上传")
    # sync_media 可能已重写过状态文件，重新读取后只更新本账号分区
    latest = _load_json(STATE_PATH, {})
    _acct_state(latest, account).update(
        {k: v for k, v in state.items() if k != "pending_backfill"})
    _save_json(STATE_PATH, latest)


def _pick_accounts(want):
    """默认返回本机全部已登录账号；want 可以是 wxid 或带哈希后缀的目录名。"""
    accounts = list_accounts()
    if not accounts:
        raise SystemExit("未找到任何已登录的微信账号")
    if not want:
        return accounts
    hit = [a for a in accounts if want in (a["wxid"], a["account"])]
    if not hit:
        raise SystemExit("未找到账号 %s；本机可用:\n  %s" % (
            want, "\n  ".join("%s (%s)" % (a["wxid"], a["account"]) for a in accounts)))
    return hit


def main():
    argv = [a for a in sys.argv[1:] if a != "--"]
    want = None
    if "--account" in argv:
        i = argv.index("--account")
        try:
            want = argv[i + 1]
        except IndexError:
            raise SystemExit("--account 后面要跟 wxid 或账号目录名")
        del argv[i:i + 2]
    cmd = argv[0] if argv else "incremental"
    if cmd not in ("init", "incremental", "entities", "messages", "media", "sweep"):
        raise SystemExit(__doc__)

    status, body = _retry("GET", SYNC_BASE + HEALTH_PATH, None,
                          sync_auth_headers(), attempts=2)
    try:
        health = _response("GET " + HEALTH_PATH, status, body)
    except RuntimeError as exc:
        raise SystemExit("同步服务不可用: %s %s" % (SYNC_BASE, exc))
    # prime-contact 的 actuator 健康端点返回 {"status":"UP"}，DOWN 要拦住
    if health.get("status") not in (None, "UP"):
        raise SystemExit("同步服务不健康: %s status=%s"
                         % (SYNC_BASE, health.get("status")))

    targets = _pick_accounts(want)
    print("[%s] %s，本轮 %d 个账号 → %s"
          % (time.strftime("%Y-%m-%d %H:%M:%S"), cmd, len(targets), SYNC_BASE))
    t_all = time.time()
    failed = []
    for a in targets:
        # 每个账号独立上锁；同一账号绝不并行，不同账号互不阻塞
        if not acquire_lock(a["account"]):
            continue
        try:
            t0 = time.time()
            db = WeChatDB(account=a["account"])
            account = db.wxid
            if not account:
                raise RuntimeError("取不到 wxid")
            print("\n=== %s（%s）==="
                  % (account, db.get_self_info().get("nick_name", "") or a["account"]))
            _run(cmd, db, account)
            print("--- %s 完成，耗时 %.1fs" % (account, time.time() - t0))
        except Exception as exc:
            # 一个账号失败不影响其他账号，末尾统一报告并以非零码退出
            failed.append((a["wxid"], "%s: %s" % (type(exc).__name__, exc)))
            print("--- %s 失败: %s: %s" % (a["wxid"], type(exc).__name__, exc))
        finally:
            release_lock(a["account"])

    print("\n全部完成，总耗时 %.1fs" % (time.time() - t_all))
    if failed:
        print("失败 %d 个账号:" % len(failed))
        for wxid, why in failed:
            print("  %s  %s" % (wxid, why))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
