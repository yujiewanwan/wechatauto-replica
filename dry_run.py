"""干跑：只在本地组装 payload 并按服务端 DDL 约束自检，不发任何写请求。"""
import json
import sync_to_server as S
from wechatauto.db import WeChatDB

db = WeChatDB()
account = db.wxid
print("account:", account, "len:", len(account))

errs = []


def chk(entity, item, rules):
    for field, kind, limit in rules:
        v = item.get(field, "__MISSING__")
        if v == "__MISSING__":
            errs.append("%s 缺字段 %s" % (entity, field))
        elif kind == "s":
            if not isinstance(v, str):
                errs.append("%s.%s 应为 str，实际 %s" % (entity, field, type(v).__name__))
            elif len(v) > limit:
                errs.append("%s.%s 超长 %d>%d" % (entity, field, len(v), limit))
        elif kind == "i":
            if not isinstance(v, int) or isinstance(v, bool):
                errs.append("%s.%s 应为 int，实际 %s" % (entity, field, type(v).__name__))
            elif v < 0 or v > limit:
                errs.append("%s.%s 越界 %d" % (entity, field, v))
        elif kind == "oi" and v is not None and not isinstance(v, int):
            errs.append("%s.%s 应为 int/None" % (entity, field))
        elif kind == "ot" and v is not None:
            if not isinstance(v, str):
                errs.append("%s.%s 应为 str/None" % (entity, field))
            elif len(v.encode()) > limit:
                errs.append("%s.%s 超字节 %d>%d" % (entity, field, len(v.encode()), limit))


contacts = S.collect_contacts(db)
for c in contacts:
    chk("contact", c, [("username", "s", 255), ("nick_name", "s", 255),
                       ("remark", "s", 255), ("alias", "s", 255)])
print("contacts:", len(contacts), "样本:", json.dumps(contacts[0], ensure_ascii=False))

sessions = S.collect_sessions(db)
for s in sessions:
    chk("session", s, [("username", "s", 255), ("unread_count", "i", 2**31 - 1),
                       ("summary", "ot", 65535), ("last_timestamp", "i", 2**63 - 1),
                       ("last_msg_sender", "s", 255), ("last_sender_name", "s", 255)])
print("sessions:", len(sessions))

by_chat, stat = S.scan_messages(db)
total = sum(len(v) for v in by_chat.values())
print("messages:", total, "chats:", len(by_chat), stat)
mrules = [("chat", "s", 255), ("chat_name", "s", 255), ("local_id", "i", 2**63 - 1),
          ("sort_seq", "i", 2**63 - 1), ("type", "s", 32), ("type_code", "i", 2**31 - 1),
          ("sender_id", "s", 64), ("sender_name", "s", 255),
          ("create_time", "i", 2**63 - 1), ("content", "ot", 16777215),
          ("server_id", "oi", 0), ("md5", "ot", 32), ("media_url", "ot", 65535)]
for chat, msgs in by_chat.items():
    seqs = [m["sort_seq"] for m in msgs]
    if seqs != sorted(seqs) or len(set(seqs)) != len(seqs):
        errs.append("%s: sort_seq 未严格升序去重" % chat)
    for m in msgs:
        chk("message", m, mrules)

one = next(iter(by_chat.values()))[0]
print("消息样本:", json.dumps(one, ensure_ascii=False)[:400])
print("单条最大字节:", max(
    len(json.dumps(m, ensure_ascii=False).encode()) for v in by_chat.values() for m in v))

print("\n校验错误 %d 条" % len(errs))
for e in errs[:20]:
    print("  ", e)
