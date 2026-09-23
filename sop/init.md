# 新机器初始化

在这台新电脑上，让微信聊天数据自动上传。照着抄命令就行，约 15 分钟。

---

## 一、你只需要准备一个 token

服务地址项目里已经有默认值，不用管：

| 用途 | 默认地址 |
|---|---|
| 归档接口（消息 / 联系人 / 会话） | `https://primeapi.aizee.cc/api/wechat` |
| 图库（图片二进制） | `https://primeapi.aizee.cc` |

**归档和图片都在同一个服务上，用同一个 token。** 所以你要提供的就一样：

- **token** —— 找服务端负责人要

**一个前提**：这台机器的微信要**一直登录着**（不是登录过就行）。
采集要从运行中的微信里读数据，微信没登录，什么都传不上去。

> 只有要把数据传到**别的**服务（比如本机自建的 prime-contact）时，才需要另外改地址。
> 那种情况 `.env` 里加一行 `WECHAT_SYNC_BASE=http://127.0.0.1:9001/api/wechat`。

---

## 二、装环境

```bat
:: 1. 装 Windows 版微信并登录

:: 2. 装 Python 3.9+（安装时勾选 Add python.exe to PATH），然后：
py -V

:: 3. 进项目目录，装依赖
cd /d <项目路径>
py -m pip install -e .
```

---

## 三、写配置

```bat
cd /d <项目路径>
copy .env.example .env
notepad .env
```

只填 token 就行（地址和其余开关都有默认值）：

```ini
WECHAT_SYNC_TOKEN=<token>
```

图片不用额外配：图库和归档是同一个服务，会自动复用这个 token。

其余保持默认即可。

---

## 四、先手动跑一次（不能跳）

```bat
cd /d <项目路径>
sync_wechat.bat
```

第一次会自动做全量，单账号 8000 条消息约 5~6 分钟，**中途别关**。中断了直接重跑，不会重复传。

跑成功的标志（日志 `logs\sync_incremental.log`）：

```
消息合计: {'total': 8493, 'inserted': 8493, 'updated': 0, 'skipped': 0}
--- wxid_xxxxxxxx 完成，耗时 327.5s
```

---

## 五、让它按固定频率自动跑 ← 就这一步是"自动"

用 Windows 自带的「任务计划程序」，一条命令建任务：

```bat
schtasks /create /tn "WeChatSync" /tr "<项目路径>\sync_wechat.bat" /sc minute /mo 10 /f
```

`/mo 10` 就是**每 10 分钟跑一次**，无限期重复。改数字就换频率。

### 频率填多少

| 机器上采集几个微信账号 | 建议 `/mo` | 原因 |
|---|---|---|
| 1 个 | `5` | 一轮约 2 分钟，5 分钟足够 |
| 2 个以上 | `10` | 一轮耗时按账号累加，间隔太短会一直在跑 |

**别再短了。** 轮询任务本身有保护：上一轮还没跑完时，新的触发会被直接跳过
（`IgnoreNew`），所以设短了不会出错，只是没意义。

### 建完必须再做一步：关掉电池限制

`schtasks` 建出来的任务默认「只在用交流电时启动」「一拔电源就停」，
采集机是笔记本时，拔了电源就**静默停止上传**，界面上看不出任何异常：

```powershell
$t = Get-ScheduledTask -TaskName "WeChatSync"
$t.Settings.DisallowStartIfOnBatteries = $false
$t.Settings.StopIfGoingOnBatteries = $false
Set-ScheduledTask -TaskName "WeChatSync" -Settings $t.Settings
```

### 验收 / 日常管理

```bat
schtasks /run    /tn "WeChatSync"              :: 立刻手动跑一次
schtasks /query  /tn "WeChatSync" /fo LIST /v  :: 看状态、间隔、上次结果
schtasks /change /tn "WeChatSync" /ri 20       :: 把频率改成 20 分钟
schtasks /change /tn "WeChatSync" /disable     :: 临时停用（/enable 恢复）
schtasks /delete /tn "WeChatSync" /f           :: 彻底删掉
type logs\sync_incremental.log                 :: 看日志
```

建完后建议：`schtasks /run /tn "WeChatSync"` 手动跑一次，隔 20 分钟回来看日志有没有多出两轮，
确认真的在自动跑。

### 两条注意事项

- **机器必须处于登录状态**：注销、重启后没人登录，任务完全不跑。锁屏没关系。
  无人值守的采集机请开自动登录。
- **换机器前先停掉旧机器**（`schtasks /delete`）：同一个微信账号只允许一个采集进程。

---

## 六、出问题看哪里

日志在 `logs\sync_incremental.log`。最常见的两种：

| 日志现象 | 原因 | 怎么办 |
|---|---|---|
| `数据库无可用密钥` | 这个账号的微信没登录 | 登录该微信账号后重跑 |
| `HTTP 401` | token 失效 | 找负责人换一个 |

其余故障、图片上传、多账号细节见 `docs/微信同步运维.md`。
