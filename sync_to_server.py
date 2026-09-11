"""把本机微信数据同步到 aizee 服务端（消息/联系人/群/群成员/会话 + 图片）。

两个入口：

    py sync_to_server.py init          # 首次初始化：全量上传
    py sync_to_server.py incremental    # 增量：定时 job 就跑这个

增量入口自己管节奏，每台机器只需要加一个定时任务（建议 5~15 分钟）：
消息和图片每轮都同步；四类实体超过 24 小时才重新整表覆盖；超过 7 天做一次
全量兜底（微信换设备登录会补写低于水位线的历史消息，按水位线收窄的扫描
看不见它们）。节奏状态存在 _sync_state.json，不需要配多个 job。

手动入口（排障用，跳过节奏判断）：

    py sync_to_server.py entities | messages | media | sweep

token 从环境变量读取，不写入仓库；不传则只能跑不涉及图片上传的步骤：

    AIZEE_TOKEN=xxx py sync_to_server.py incremental

实现要点（均已实测验证）：
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
"""

import json
import os
import re
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
    """
    path = path or os.path.join(_HERE, ".env")
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


SYNC_BASE = os.environ.get("WECHAT_SYNC_BASE", "http://127.0.0.1:9005").rstrip("/")
GALLERY_BASE = os.environ.get("AIZEE_GALLERY_BASE", "https://primeapi.aizee.cc").rstrip("/")
TOKEN = os.environ.get("AIZEE_TOKEN", "")

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
SEND_SENDER_USERNAME = os.environ.get("SEND_SENDER_USERNAME", "1") != "0"
ENTITY_INTERVAL = _env_int("ENTITY_INTERVAL_SEC", 24 * 3600)
SWEEP_INTERVAL = _env_int("SWEEP_INTERVAL_SEC", 7 * 24 * 3600)

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


def sync_post(path, payload):
    """向同步服务 POST JSON，非 200 视为致命错误（避免带着坏数据继续推进）。"""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    status, data = _retry("POST", SYNC_BASE + path, body,
                          {"Content-Type": "application/json"})
    text = data.decode("utf-8", "replace")
    if status != 200:
        raise RuntimeError("POST %s -> %d %s" % (path, status, text[:500]))
    return json.loads(text)


def sync_put_watermark(account, marks):
    """PUT 水位线；服务端按 chat 取 GREATEST，较小值是成功的空操作。"""
    items = list(marks.items())
    for i in range(0, len(items), WM_BATCH):
        chunk = dict(items[i:i + WM_BATCH])
        body = json.dumps({"watermark": chunk}).encode("utf-8")
        status, data = _retry("PUT", "%s/v1/watermark/%s"
                              % (SYNC_BASE, urllib.parse.quote(account)),
                              body, {"Content-Type": "application/json"})
        if status != 200:
            raise RuntimeError("PUT watermark -> %d %s"
                               % (status, data[:300].decode("utf-8", "replace")))


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
        print("   ", send_batches("/v1/contacts", account, "contacts", contacts))

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
        print("   ", send_batches("/v1/chatrooms", account, "chatrooms", rooms))
    print("群成员 %d 条" % len(members))
    if members:
        print("   ", send_batches("/v1/chatroom_members", account,
                                  "chatroom_members", members))

    sessions = collect_sessions(db)
    print("会话 %d 条" % len(sessions))
    if sessions:
        print("   ", send_batches("/v1/sessions", account, "sessions", sessions))


# ----------------------------------------------------------------------
# 消息：跨分库合并同一会话，按 sort_seq 升序，(account, chat, sort_seq) 幂等
# ----------------------------------------------------------------------
MSG_SELECT = ("SELECT local_id, local_type, server_id, real_sender_id, create_time, "
              "message_content, packed_info_data, sort_seq FROM %s")


def _sender_username(sid, chat, senders, account):
    """发送者 wxid：可 join contacts.username。sender_id 是本机数字 id，跨机无意义。

    自己 → account；群聊查 SenderName2Id；单聊非自己的一方必然是对端，由 chat 推出。
    """
    if sid in (2, "2"):
        return account
    if isinstance(sid, int):
        u = senders.get(sid)
        if u:
            return u
    if not chat.endswith("@chatroom"):
        return chat
    return ""


def scan_messages(db, watermark=None):
    """返回 {chat: [消息 dict 升序]} 与统计。同完整键重复时保留首条并计数。

    watermark 为 {chat: 已同步的最大 sort_seq} 时只扫该会话 sort_seq >= 水位线
    的消息（增量）。用 >= 而非 >：sort_seq 是秒级，水位线落在某一秒中间时 >
    会永久丢掉那一秒剩下的消息；>= 重读边界那一秒，重复的由服务端记 skipped。
    """
    idx = db._build_md5_index()
    nicks = db._nickname_index()
    senders = db._sender_id_index()
    account = db.wxid
    self_nick = db.get_self_info().get("nick_name", "我")

    raw = {}
    for rel in db._message_dbs():
        conn = db._open(rel)
        try:
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
                raw.setdefault(chat, []).extend(rows)
        finally:
            conn.close()

    out, dup, unresolved = {}, 0, 0
    for chat, rows in raw.items():
        if len(chat) == 32 and re.fullmatch(r"[0-9a-f]{32}", chat):
            unresolved += 1          # contact/session 里已无此会话，只剩表名 md5
        rows.sort(key=lambda r: (_nn(r["sort_seq"]), _nn(r["local_id"])))
        seen, msgs = set(), []
        for r in rows:
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
                    r["real_sender_id"], senders, nicks, self_nick), 255),
                "create_time": _nn(e["create_time"]),
                "content": _text(e["content"], 16777215),
                "server_id": _opt_int(e["server_id"]),
                "md5": _s(e["md5"], 32) or None,
                "media_url": None,
            }
            if SEND_SENDER_USERNAME:
                item["sender_username"] = _s(_sender_username(
                    r["real_sender_id"], chat, senders, account), 255)
            msgs.append(item)
        if msgs:
            out[chat] = msgs
    return out, {"dup_key": dup, "unresolved_chats": unresolved}


def sync_messages(db, account, incremental=False):
    """同 chat 串行确认：前一批 200 后才发下一批，整会话确认后才推进水位线。

    incremental=True 时按服务端水位线收窄扫描范围；返回 (totals, by_chat)，
    by_chat 供媒体步骤复用，避免重复扫库。
    """
    status, data = _retry("GET", "%s/v1/watermark/%s"
                          % (SYNC_BASE, urllib.parse.quote(account)))
    known = json.loads(data).get("watermark", {}) if status == 200 else {}
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
            r = send_batches("/v1/messages", account, "messages", msgs[i:i + BATCH])
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

    dl = MediaDownloader(db)
    if not dl.detect_image_key():
        raise SystemExit("取不到图片解密密钥（确认微信已登录并在运行）")

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
        print("   ", send_batches("/v1/messages", account, "messages", backfill))
        state = _load_json(STATE_PATH, {})
        _acct_state(state, account)["pending_backfill"] = []     # 已确认落库
        _save_json(STATE_PATH, state)
    print("媒体统计:", stat)


def _run(cmd, db, account):
    full = _load_json(STATE_PATH, {})
    state = _acct_state(full, account)
    now = int(time.time())

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

    status, _ = _retry("GET", SYNC_BASE + "/v1/health", attempts=2)
    if status != 200:
        raise SystemExit("同步服务不可用: %s %s" % (SYNC_BASE, status))

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
