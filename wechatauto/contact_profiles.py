"""微信 4.x 朋友资料解析。未知普通字段跳过，已知字段损坏时拒绝整轮同步。"""


def _varint(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(data):
            raise ValueError("截断的 protobuf varint")
        byte = data[offset]
        offset += 1
        if shift == 63 and byte > 1:
            raise ValueError("protobuf varint 溢出")
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
    raise ValueError("无效的 protobuf varint")


def _fields(data):
    offset = 0
    while offset < len(data):
        key, offset = _varint(data, offset)
        field, wire = key >> 3, key & 7
        if field == 0:
            raise ValueError("无效的 protobuf 字段编号")
        if wire == 0:
            value, offset = _varint(data, offset)
        elif wire in (1, 2, 5):
            if wire == 2:
                size, offset = _varint(data, offset)
            else:
                size = 8 if wire == 1 else 4
            value = data[offset:offset + size]
            if len(value) != size:
                raise ValueError("截断的 protobuf 字段")
            offset += size
        else:
            raise ValueError("不支持的 protobuf wire type")
        yield field, wire, value


def parse_extra_buffer(data):
    """已验证：电话 14→2→1，标签 30（逗号分隔的十进制 ID）。"""
    phones, label_ids = [], []
    for field, wire, value in _fields(data or b""):
        if field not in (14, 30):
            continue
        if wire != 2:
            raise ValueError("朋友资料字段类型发生变化")
        if field == 30:
            raw = value.decode("utf-8")
            if raw:
                parts = raw.split(",")
                if any(not part.isascii() or not part.isdecimal() for part in parts):
                    raise ValueError("标签 ID 格式异常")
                label_ids.extend(int(part) for part in parts)
        else:
            for number, kind, nested in _fields(value):
                if number != 2:
                    continue
                if kind != 2:
                    raise ValueError("电话列表结构发生变化")
                for number2, kind2, phone in _fields(nested):
                    if number2 == 1:
                        if kind2 != 2:
                            raise ValueError("电话字段结构发生变化")
                        text = phone.decode("utf-8")
                        if not text.strip():
                            raise ValueError("空电话字段")
                        phones.append(text)
    return list(dict.fromkeys(phones)), list(dict.fromkeys(label_ids))


def collect_contact_profiles(db):
    """从刷新后的数据库读取一致快照；不以空列表掩盖缺库、解析或关联错误。"""
    conn = db._contact_conn()
    if conn is None:
        raise RuntimeError("联系人数据库不可读，拒绝朋友资料全量同步")
    try:
        conn.execute("BEGIN")
        labels = [{"id": row[0], "name": row[1]} for row in conn.execute(
            "SELECT label_id_,label_name_ FROM contact_label ORDER BY label_id_")]
        rows = conn.execute(
            "SELECT username,nick_name,alias,remark,extra_buffer FROM contact").fetchall()
    finally:
        conn.close()
    if not any(row["username"] == db.wxid for row in rows):
        raise RuntimeError("联系人快照缺少当前账号，拒绝同步")
    known = {label["id"] for label in labels}
    contacts = []
    for row in rows:
        if not row["username"]:
            raise ValueError("联系人 username 为空")
        phones, ids = parse_extra_buffer(row["extra_buffer"])
        if not set(ids).issubset(known):
            raise ValueError("联系人引用未知标签，拒绝同步，等待微信数据同步完成")
        contacts.append({
            "username": row["username"], "nick_name": row["nick_name"] or "",
            "alias": row["alias"] or "", "remark": row["remark"] or "",
            "phones": phones, "label_ids": ids,
        })
    return {"account": db.wxid, "complete": True, "labels": labels, "contacts": contacts}
