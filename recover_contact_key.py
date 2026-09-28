# One-off: brute-force scan Weixin.exe process memory for contact.db's key.
# The stock Config.Cipher heuristic found 20/21 db keys for this account but
# missed contact\contact.db; this sweeps ALL readable regions for hex-literal
# key candidates and verifies each against contact.db page1 (SQLCipher4 HMAC).
import ctypes, os, re, sys, time
from wechatauto import db as dbm

REL = "contact\\contact.db"
ACCOUNT = "wxid_kr1hz9owplpn22_fdc5"

db = dbm.WeChatDB(account=ACCOUNT)
if REL not in db.unkeyed:
    print("already keyed:", REL)
    sys.exit(0)

page1 = open(db._db_path(REL), "rb").read(dbm.PAGE_SZ)
HEXRUN = re.compile(rb"[0-9a-fA-F]{64,}")
k32 = dbm._k32

pids = db._find_weixin_pids()
print("scanning %d pids for %s" % (len(pids), REL), flush=True)

found = None
tested = set()
t0 = time.time()
for pid in pids:
    h = k32.OpenProcess(0x0010 | 0x0400, False, pid)
    if not h:
        continue
    n_cand = 0
    try:
        addr = 0
        while True:
            mbi = dbm._MBI()
            r = k32.VirtualQueryEx(h, ctypes.c_void_p(addr), ctypes.byref(mbi), ctypes.sizeof(mbi))
            if r == 0:
                break
            if (mbi.State == 0x1000 and (mbi.Protect & 0xFF) & 0xE6
                    and not (mbi.Protect & 0x100) and 0 < mbi.RegionSize < 0x10000000):
                buf = ctypes.create_string_buffer(mbi.RegionSize)
                br = ctypes.c_size_t(0)
                ok = k32.ReadProcessMemory(h, ctypes.c_void_p(mbi.BaseAddress or 0),
                                           buf, mbi.RegionSize, ctypes.byref(br))
                data = buf.raw[: br.value] if ok and br.value else b""
                if data:
                    for m in HEXRUN.finditer(data):
                        run = m.group(0)
                        starts = [0]
                        if len(run) > 96:
                            starts += list(range(0, len(run) - 63, 32))
                            starts.append(len(run) - 64)
                        for s in dict.fromkeys(starts):
                            if s + 64 > len(run):
                                continue
                            cand = bytes.fromhex(run[s:s + 64].decode())
                            if cand in tested or not dbm.WeChatDB._probable_key(cand):
                                continue
                            tested.add(cand)
                            n_cand += 1
                            explicit = None
                            if s + 96 <= len(run):
                                explicit = bytes.fromhex(run[s + 64:s + 96].decode())
                            for salt_opt in ([None, explicit] if explicit else [None]):
                                if dbm._verify_enc_key(cand, page1, salt=salt_opt):
                                    found = cand + (salt_opt or b"")
                                    raise StopIteration
            addr = (mbi.BaseAddress or 0) + mbi.RegionSize
    except StopIteration:
        k32.CloseHandle(h)
        print("FOUND in pid %d after %d candidates (%.0fs)" % (pid, n_cand, time.time() - t0), flush=True)
        break
    k32.CloseHandle(h)
    print("pid %d done, %d candidates so far (%.0fs)" % (pid, len(tested), time.time() - t0), flush=True)

if found:
    db._keys[REL] = found
    db._save_keys()
    print("SAVED to keys.json; contact.db now keyed")
else:
    print("NOT FOUND in any pid; key not present in scanned memory")
