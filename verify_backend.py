"""验证后端改动是否生效。用 test_ 前缀账号，不污染真实账号数据。

检查项：
  1. sender_username 是否被接受（未加列时校验层会报 unknown field）
  2. 唯一键是否已含 local_id（同 chat+sort_seq、不同 local_id 应两条都插入）
  3. 新键上的幂等（原批重放应全部 skipped）
  4. sender_username 是否可回填（决定它是不是一次性写定）
"""
import json

import sync_to_server as S

ACCOUNT = "test_wxsync_probe"
CHAT = "test_probe@chatroom"
SEQ = 1700000000000


def post(payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = S.sync_auth_headers()
    headers["Content-Type"] = "application/json"
    status, data = S._retry("POST", S.SYNC_BASE + "/messages", body, headers)
    text = data.decode("utf-8", "replace")
    try:
        return status, json.loads(text)
    except ValueError:
        return status, text


def msg(local_id, sender_username, media_url=None):
    return {
        "chat": CHAT, "chat_name": "探测", "local_id": local_id, "sort_seq": SEQ,
        "type": "文本", "type_code": 1, "sender_id": "999",
        "sender_username": sender_username, "sender_name": "探测",
        "create_time": 1700000000, "content": "backend verify probe",
        "server_id": None, "md5": None, "media_url": media_url,
    }


status, health = S._retry("GET", S.SYNC_BASE + "/health", None,
                          S.sync_auth_headers(), attempts=2)
print("health:", status, health.decode()[:80])

print("\n[1+2] 同 sort_seq、不同 local_id，带 sender_username")
st, r = post({"account": ACCOUNT, "messages": [msg(1, "probe_a"), msg(2, "probe_b")]})
print("     ", st, r)
if st == 400:
    m = (r.get("error", {}) or {}).get("message", "") if isinstance(r, dict) else str(r)
    if "unknown field" in m:
        raise SystemExit("FAIL: sender_username 未被接受 —— EntitySpec 的列清单还没加")
    raise SystemExit("FAIL: 校验拒绝 -> %s" % m)
if st != 200:
    raise SystemExit("FAIL: HTTP %s" % st)
if r.get("inserted") == 2:
    print("      PASS: sender_username 已接受；唯一键已含 local_id（两条都插入）")
elif r.get("inserted", 0) + r.get("skipped", 0) == 2 and r.get("inserted") == 1:
    raise SystemExit("FAIL: 唯一键仍是 (account, chat, sort_seq) —— 第二条被跳过，"
                     "灌全量会丢 1566 条")
else:
    print("      注意: 计数异常（可能此前已探测过），继续后续检查")

print("\n[3] 原批重放，应全部 skipped")
st, r = post({"account": ACCOUNT, "messages": [msg(1, "probe_a"), msg(2, "probe_b")]})
print("     ", st, r)
print("      PASS: 新键幂等" if r.get("skipped") == 2
      else "      注意: 重放未全部 skipped，检查 upsert 的变更判定")

print("\n[4] 只改 sender_username，看是否可回填")
st, r = post({"account": ACCOUNT, "messages": [msg(1, "probe_changed")]})
print("     ", st, r)
if r.get("updated") == 1:
    print("      sender_username 可回填（先灌空值以后能补）")
elif r.get("skipped") == 1:
    print("      sender_username 不可回填 —— 首次写入即定终身，"
          "务必在正式灌入前确认取值正确")

print("\n探测数据留在 account='%s'，如需清理：" % ACCOUNT)
print("  DELETE FROM wechat_messages WHERE account = '%s';" % ACCOUNT)
