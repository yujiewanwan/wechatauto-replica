# One-off phase 2: locate raw db keys in Weixin.exe memory, then mine the
# surrounding key-storage structure for contact.db's key.
# The hex-literal scan (phase 1) proved contact.db's key is not stored as hex
# text anywhere; WCDB likely keeps keys as raw 32-byte values. We search for
# the raw bytes of a KNOWN key, then verify 32-byte windows around every hit
# against contact.db page1 (SQLCipher4 HMAC, with explicit-salt variants).
import ctypes, json, os, sys, time
from wechatauto import db as dbm

REL = "contact\\contact.db"
ACCOUNT = "wxid_kr1hz9owplpn22_fdc5"

db = dbm.WeChatDB(account=ACCOUNT)
if REL not in db.unkeyed:
    print("already keyed:", REL); sys.exit(0)

saved = json.load(open(db.keys_file, encoding="utf-8"))
known = {rel: bytes.fromhex(h)[:32] for rel, h in saved.items()}
page1 = open(db._db_path(REL), "rb").read(dbm.PAGE_SZ)
k32 = dbm._k32

RADIUS = 0x40000  # +-256KB around each hit


def verify(cand):
    if not dbm.WeChatDB._probable_key(cand):
        return False
    if dbm._verify_enc_key(cand, page1):
        return cand
    return None


hits = 0
t0 = time.time()
found = None
for pid in db._find_weixin_pids():
    h = k32.OpenProcess(0x0010 | 0x0400, False, pid)
    if not h:
        continue
    pid_hits = 0
    try:
        addr = 0
        while True:
            mbi = dbm._MBI()
            r = k32.VirtualQueryEx(h, ctypes.c_void_p(addr), ctypes.byref(mbi), ctypes.sizeof(mbi))
            if r == 0:
                break
            if (mbi.State == 0x1000 and (mbi.Protect & 0xFF) & 0xE6
                    and not (mbi.Protect & 0x100) and 0 < mbi.RegionSize < 0x80000000):
                size = mbi.RegionSize
                base = mbi.BaseAddress or 0
                CH = 0x4000000  # read in 64MB chunks: no region-size cap
                data = bytearray()
                for off in range(0, size, CH):
                    n = min(CH, size - off)
                    buf = ctypes.create_string_buffer(n)
                    br = ctypes.c_size_t(0)
                    if k32.ReadProcessMemory(h, ctypes.c_void_p(base + off), buf, n, ctypes.byref(br)) and br.value:
                        data += buf.raw[: br.value]
                    else:
                        break
                data = bytes(data)
                if data:
                    for rel, key in known.items():
                        start = 0
                        while True:
                            i = data.find(key, start)
                            if i < 0:
                                break
                            hits += 1; pid_hits += 1
                            lo = max(0, i - RADIUS)
                            hi = min(len(data), i + RADIUS)
                            block = data[lo:hi]
                            tested = set()
                            for off in range(0, len(block) - 31):
                                cand = block[off:off + 32]
                                if cand in tested:
                                    continue
                                tested.add(cand)
                                got = verify(cand)
                                if got:
                                    found = got
                                    raise StopIteration
                            start = i + 1
            addr = (mbi.BaseAddress or 0) + mbi.RegionSize
    except StopIteration:
        k32.CloseHandle(h)
        print("CONTACT KEY FOUND in pid %d (%ds)" % (pid, time.time() - t0), flush=True)
        break
    k32.CloseHandle(h)
    if pid_hits:
        print("pid %d: %d raw-key hits (%ds)" % (pid, pid_hits, time.time() - t0), flush=True)

if found:
    db._keys[REL] = found
    db._save_keys()
    print("SAVED: contact.db key recovered from neighboring key structure")
else:
    print("no luck: %d raw-key hits scanned, contact.db key not in +-256KB neighborhoods (%ds)"
          % (hits, time.time() - t0))
