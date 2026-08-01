# Cluster-Chat-Server

> Migration status: Phase 0 through Phase 3 add an independent C++ AI API gateway while the legacy
> chat server remains available as a rollback target. See
> [`ai-api-gateway-migration-spec.md`](ai-api-gateway-migration-spec.md).

## AI Gateway (Phase 3)

`AiGateway` exposes `GET /healthz`, `GET /readyz`, `GET /v1/models`, and streaming or non-streaming
`POST /v1/responses`. It uses asynchronous Boost.Beast/Asio for ingress and one process-wide
libcurl multi transport for upstream requests. SSE responses are incrementally validated and
relayed with bounded buffering, high/low-water backpressure, upstream cancellation on client
disconnect, and separate first-event, idle, and maximum-duration timeouts.

Phase 3 stores Tenant identity, HMAC-only API Keys, reusable Access Policies, Provider resources,
and Tenant-scoped Logical Models in MySQL. Blocking SQL is isolated on a fixed worker pool. A
configuration-version poll invalidates cached Auth Snapshots; database or schema failures make
`/readyz` and new authorization fail closed while `/healthz` remains available. Provider secrets
are resolved from `env:NAME` or a non-symlink `file:basename` below `AI_GATEWAY_SECRET_DIR`; plaintext
Gateway or Provider keys are never stored in MySQL. Redis quotas, Usage, retries, routing, and
failover remain out of scope.

Configure the Gateway DB and HMAC pepper values in `.env`, then build:

```shell
cmake -S . -B build/gateway \
  -DBUILD_CHAT_SERVER=OFF \
  -DBUILD_CHAT_CLIENT=OFF
cmake --build build/gateway
```

Create the database schema, import resources, and issue a Tenant Key. The complete Key is printed
to stdout only for that issuance command:

```shell
build/gateway/bin/AiGatewayAdmin migrate --dir migrations/gateway
build/gateway/bin/AiGatewayAdmin apply-config --file config/ai-gateway.example.json
AI_GATEWAY_API_KEY="$(build/gateway/bin/AiGatewayAdmin issue-key \
  --tenant example --policy default --name local-client)"
BUILD_DIR=build/gateway ./scripts/run-gateway.sh
```

Use `AiGatewayAdmin set-key-status --key-id <public-id> --status disabled` to revoke a Key, and
`AiGatewayAdmin bump-version` after rotating a `file:` Secret. Environment Secret rotation requires
a Gateway restart. `apply-config` upserts supplied resources without deleting omitted resources;
an effective change increments the configuration version once.

Example non-streaming request:

```shell
curl http://127.0.0.1:8080/v1/responses \
  -H "Authorization: Bearer ${AI_GATEWAY_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"model":"your-logical-model","input":"hello","stream":false}'
```

Example streaming request:

```shell
curl -N http://127.0.0.1:8080/v1/responses \
  -H "Authorization: Bearer ${AI_GATEWAY_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"model":"your-logical-model","input":"hello","stream":true}'
```

The runtime identity controls use these defaults:

| Variable | Default | Meaning |
|---|---:|---|
| `AI_GATEWAY_DB_POOL_SIZE` | `4` | Maximum concurrent Gateway DB connections |
| `AI_GATEWAY_DB_WORKERS` | `4` | Fixed blocking SQL worker count |
| `AI_GATEWAY_DB_QUEUE_SIZE` | `1024` | Maximum queued DB operations |
| `AI_GATEWAY_AUTH_CACHE_TTL_SECONDS` | `30` | Auth Snapshot cache lifetime |
| `AI_GATEWAY_AUTH_CACHE_MAX_ENTRIES` | `10000` | Auth Snapshot LRU capacity |
| `AI_GATEWAY_CONFIG_POLL_INTERVAL_MS` | `1000` | Configuration-version poll interval |

The stream controls use these defaults:

| Variable | Default | Meaning |
|---|---:|---|
| `AI_GATEWAY_STREAM_PREFETCH_BYTES` | `65536` | Maximum bytes before the first valid business event |
| `AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES` | `262144` | Pause upstream reads at this queued-byte watermark |
| `AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES` | `65536` | Resume upstream reads after draining to this watermark |
| `AI_GATEWAY_STREAM_IDLE_TIMEOUT_MS` | `60000` | Maximum unpaused interval without upstream bytes |
| `AI_GATEWAY_STREAM_MAX_DURATION_MS` | `900000` | Total stream duration including paused time |

The low watermark must be below the high watermark, and prefetch must not exceed the high
watermark. The Nginx HTTP/1.1 reverse-proxy example for SSE is
[`deploy/nginx/ai-gateway.conf.example`](deploy/nginx/ai-gateway.conf.example).

Run the localhost integration tests, which start a temporary MariaDB and Mock Provider, with:

```shell
ctest --test-dir build/gateway --output-on-failure
```

The captured Codex client requests `stream=true`; its request and event contract is covered by the
localhost Provider fixture. Architectural decisions and the captured contract are in
`docs/adr/` and `docs/compatibility/`.

### Lingsuan Provider

[`config/ai-gateway.lingsuan.json`](config/ai-gateway.lingsuan.json) contains the models returned
by `GET https://lingsuan.top/v1/models` on 2026-08-01. It stores only the
`env:LINGSUAN_API_KEY` Secret reference and maps the logical model names to
`https://lingsuan.top/v1/responses`. Refresh and review the list before applying it when the
upstream catalog changes. The Provider model catalog does not itself guarantee that every listed
model accepts the Responses protocol; validate models used by production policies. Lingsuan
currently terminates successful streams with `response.completed` and no `[DONE]` sentinel; the
Gateway accepts and forwards that valid terminal form.

Apply the configuration and issue a Tenant Key with the existing Gateway DB environment loaded:

```shell
build/gateway/bin/AiGatewayAdmin apply-config --file config/ai-gateway.lingsuan.json
AI_GATEWAY_API_KEY="$(build/gateway/bin/AiGatewayAdmin issue-key \
  --tenant lingsuan --policy default --name local-client)"
```

After starting `AiGateway` with `LINGSUAN_API_KEY` and Nginx with the provided HTTP proxy example,
run the explicit live check below. It makes billable non-streaming and streaming requests through
Nginx; `gpt-5.6-terra` is the default and can be overridden with
`AI_GATEWAY_LIVE_TEST_MODEL`.

```shell
AI_GATEWAY_API_KEY="${AI_GATEWAY_API_KEY}" \
  python3 scripts/test-lingsuan-nginx.py
```

在 Linux 环境下基于 muduo 开发的集群聊天服务器。实现新用户注册、用户登录、添加好友、添加群组、好友通信、群组聊天、保持离线消息等功能。

## 项目特点

- 基于 muduo 网络库开发网络核心模块，实现高效通信
- 使用第三方 JSON 库实现通信数据的序列化和反序列化
- 使用 Nginx 的 TCP 负载均衡功能，将客户端请求分派到多个服务器上，以提高并发处理能力
- 基于发布-订阅的服务器中间件redis消息队列，解决跨服务器通信难题
- 封装 MySQL 接口，将用户数据储存到磁盘中，实现数据持久化
- 基于 CMake 构建项目

## 在 WSL2 上运行

WSL2 运行的是完整 Linux 内核，本项目不需要改成 Windows 程序。请在 WSL 的
Ubuntu 终端内编译和运行，并把仓库放在 Linux 文件系统（例如
`~/Cluster-Chat-Server`）中；不要放在 `/mnt/c`，否则 CMake 编译和文件访问会明显变慢。

当前适配并验证的环境为 Ubuntu 24.04。基础运行需要 Muduo、MariaDB/MySQL、
Redis、hiredis、Boost 和 libcurl；Nginx 只在学习多服务器负载均衡章节时才需要。

### 1. 初始化环境

如果课程中的 Muduo 已安装到 `/usr/local`，执行：

```shell
./scripts/setup-wsl.sh
```

脚本会安装 Ubuntu 依赖、启动 MariaDB/Redis、创建本地数据库账号、导入
`chat.sql`，最后编译项目。首次运行前必须通过环境变量或 `.env` 设置
`CHAT_DB_PASSWORD` 和 `AI_GATEWAY_DB_PASSWORD`（交互终端也可分别输入）；脚本不会创建
固定的教学密码，也不会修改数据库 root 密码。运行 Gateway 前还必须设置至少 32 字节的
`AI_GATEWAY_API_KEY_HMAC_PEPPER`。
若脚本提示找不到 Muduo，请先按课程步骤安装 Muduo，并确认存在：

```text
/usr/local/include/muduo/net/TcpServer.h
/usr/local/lib/libmuduo_net.a
/usr/local/lib/libmuduo_base.a
```

WSL 如果没有启用 systemd，应编辑 WSL 内的 `/etc/wsl.conf`：

```ini
[boot]
systemd=true
```

然后在 Windows PowerShell 执行 `wsl --shutdown`，重新打开 Ubuntu。项目脚本也保留了
传统 `service` 命令的兼容路径。

### 2. 配置与编译

连接参数位于项目根目录 `.env`。初始化脚本会由 `.env.example` 自动生成它，但不会
把交互输入的密码写回磁盘；运行服务前应显式填写密钥。也可手动执行：

```shell
cp .env.example .env
./build.sh
```

可用配置项：`CHAT_DB_HOST`、`CHAT_DB_PORT`、`CHAT_DB_USER`、
`CHAT_DB_PASSWORD`、`CHAT_DB_NAME`、`CHAT_REDIS_HOST`、`CHAT_REDIS_PORT`、
`CHAT_SERVER_IP` 和 `CHAT_SERVER_PORT`。构建产物位于 `build/wsl/bin`；仓库原有
`bin` 目录中的文件是旧版 Ubuntu 编译产物，不应在 WSL 中直接使用。

### 3. 启动服务端和客户端

打开一个 WSL 终端启动服务端：

```shell
./scripts/run-server.sh
```

再打开一个或多个 WSL 终端启动客户端：

```shell
./scripts/run-client.sh
```

客户端脚本也接受显式地址和端口，例如 `./scripts/run-client.sh 127.0.0.1 6000`。
默认服务端监听 `0.0.0.0:6000`，WSL 内客户端使用 `127.0.0.1:6000`。在常规 WSL2
配置中，Windows 侧也可以通过 `localhost:6000` 访问；从局域网其他机器访问时，还需
配置 Windows 防火墙与端口转发。

仅需重新编译客户端、不准备数据库等服务时，可以使用：

```shell
cmake -S . -B build/client -DBUILD_CHAT_SERVER=OFF
cmake --build build/client
```

![1663830571(1).png](https://syz-picture.oss-cn-shenzhen.aliyuncs.com/D:%5CPrograme%20Files(x86)%5CPicGo1663830578650-52d58f18-370f-426a-b8fe-f07dfd06b116.png)

# 项目讲解

## 数据库表设计

**User表** 

| 字段名称 | 字段类型                  | 字段说明     | 约束                        |
| -------- | ------------------------- | ------------ | --------------------------- |
| id       | INT                       | 用户id       | PRIMARY KEY、AUTO_INCREMENT |
| name     | VARCHAR(50)               | 用户名       | NOT NULL, UNIQUE            |
| password | VARCHAR(50)               | 用户密码     | NOT NULL                    |
| state    | ENUM('online', 'offline') | 当前登录状态 | DEFAULT 'offline'           |

**Friend表**

| 字段名称 | 字段类型 | 字段说明 | 约束               |
| -------- | -------- | -------- | ------------------ |
| userid   | INT      | 用户id   | NOT NULL、联合主键 |
| friendid | INT      | 好友id   | NOT NULL、联合主键 |

**AllGroup表**

| 字段名称  | 字段类型     | 字段说明   | 约束                        |
| --------- | ------------ | ---------- | --------------------------- |
| id        | INT          | 组id       | PRIMARY KEY、AUTO_INCREMENT |
| groupname | VARCHAR(50)  | 组名称     | NOT NULL, UNIQUE            |
| groupdesc | VARCHAR(200) | 组功能描述 | DEFAULT ''                  |

**GroupUser表**

| 字段名称  | 字段类型                  | 字段说明 | 约束               |
| --------- | ------------------------- | -------- | ------------------ |
| groupid   | INT                       | 组id     | NOT NULL、联合主键 |
| userid    | INT                       | 组员id   | NOT NULL、联合主键 |
| grouprole | ENUM('creator', 'normal') | 组内角色 | DEFAULT 'normal'   |

**OfflineMessage表**

| 字段名称 | 字段类型    | 字段说明                   | 约束     |
| -------- | ----------- | -------------------------- | -------- |
| userid   | INT         | 用户id                     | NOT NULL |
| message  | VARCHAR(50) | 离线消息（存储Json字符串） | NOT NULL |

## 网络模块设计

我们会使用 muduo 完成网络模块的代码，在这之前我们需要了解 muduo 的基本使用。

muduo 的线程模型为「one loop per thread + threadPool」模型。一个线程对应一个事件循环（EventLoop），也对应着一个 Reactor 模型。EventLoop 负责 IO 和定时器事件的分派。

muduo 是主从 Reactor 模型，有 `mainReactor` 和 `subReactor`。`mainReactor`通过 `Acceptor` 接收新连接，然后将新连接派发到 `subReactor` 上进行连接的维护。这样 `mainReactor` 可以只专注于监听新连接的到来，而从维护旧连接的业务中得到解放。同时多个 `Reactor` 可以并行运行在多核 CPU 中，增加服务效率。因此我们可以通过 muduo 快速完成网络模块。

![](https://cdn.nlark.com/yuque/0/2022/png/26752078/1663324955126-3a8078fe-f271-4a1b-82c7-b75edff3cda8.png?x-oss-process=image%2Fresize%2Cw_720%2Climit_0#crop=0&crop=0&crop=1&crop=1&from=url&height=345&id=Jzfh0&margin=%5Bobject%20Object%5D&originHeight=435&originWidth=720&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=&width=571)

使用 muduo 注册消息事件到来的回调函数，并根据得到的 `MSGID` 定位到不同的处理函数中。以此实现业务模块和网络模块的解耦。

```cpp
// 上报读写事件相关信息的回调函数
void ChatServer::onMessage(const TcpConnectionPtr &conn,
                           Buffer *buffer,
                           Timestamp time)
{
    string buf = buffer->retrieveAllAsString();
    json js = json::parse(buf);
    
	// 业务模块和网络模块解耦
    auto msgHandler = ChatService::instance()->getHandler(js["msgid"].get<int>());
    // 回调消息绑定好的事件处理器，来执行相应的业务处理
    msgHandler(conn, js, time);
}
```

## 业务模块设计

### 注册模块

我们从网络模块接收数据，根据 `MSGID` 定位到注册模块。从传递过来的 `json` 对象中获取用户 ID 和用户密码。并以此生成 `User` 对象，调用 model 层方法将新生成的 `User` 插入到数据库中。

### 登录模块

从 `json` 对象中获取用户ID和密码，并在数据库中查询获取用户信息是否匹配。如果用户已经登录过，即 `state == "online"`，则返回错误信息。登录成功后需要在改服务端的用户表中记录登录用户，并显示该用户的好友列表和收到的离线消息。

### 客户端异常退出模块

如果客户端异常退出了，我们会从服务端记录用户连接的表中找到该用户，如果它断连了就从此表中删除，并设置其状态为 `offline`。

### 服务端异常退出模块

如果服务端异常退出，它会将所有在线的客户的状态都设置为 `offline`。即，让所有用户都下线。异常退出一般是 `CTRL + C` 时，我们需要捕捉信号。这里使用了 Linux 的信号处理函数，我们向信号注册回调函数，然后在函数内将所有用户置为下线状态。

### 点对点聊天模块

通过传递的 `json` 查找对话用户 ID：

- 用户处于登录状态：直接向该用户发送信息
- 用户处于离线状态：需存储离线消息

### 添加好友模块

从 `json` 对象中获取添加登录用户 ID 和其想添加的好友的 ID，调用 model 层代码在 friend 表中插入好友信息。

### 群组模块

创建群组需要描述群组名称，群组的描述，然后调用 model 层方法在数据库中记录新群组信息。
加入群组需要给出用户 ID 和想要加入群组的 ID，其中会显示该用户是群组的普通成员还是创建者。
群组聊天给出群组 ID 和聊天信息，群内成员在线会直接接收到。

## 使用Nginx负载均衡模块

### 负载均衡是什么

假设一台机器支持两万的并发量，现在我们需要保证八万的并发量。首先想到的是升级服务器的配置，比如提高 CPU 执行频率，加大内存等提高机器的物理性能来解决此问题。但是单台机器的性能毕竟是有限的，而且也有着摩尔定律也日已失效。

这个时候我们就可以增加服务器的数量，将用户请求分发到不同的服务器上分担压力，这就是负载均衡。那我们就需要有一个第三方组件充当负载均衡器，由它负责将不同的请求分发到不同的服务器上。而本项目，我们选择 `Nginx` 的负载均衡功能。

![](https://cdn.nlark.com/yuque/0/2022/png/26752078/1663746624651-351f9bcb-4ed5-40cd-9f2f-1b72c9964316.png#crop=0&crop=0&crop=1&crop=1&from=url&id=i9BRn&margin=%5Bobject%20Object%5D&originHeight=494&originWidth=967&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)

选择 `Nginx` 的 `tcp` 负载均衡模块的原因：

1. 把`client`的请求按照负载算法分发到具体的业务服务器`ChatServer`上
2. 能够`ChantServer`保持心跳机制，检测`ChatServer`故障
3. 能够发现新添加的`ChatServer`设备，方便扩展服务器数量

### 配置负载均衡

![](https://cdn.nlark.com/yuque/0/2022/png/26752078/1663732379258-4c925576-3374-4f0d-8274-6031a8366536.png#crop=0&crop=0&crop=1&crop=1&from=url&id=ARrJw&margin=%5Bobject%20Object%5D&originHeight=402&originWidth=810&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)

配置好后，重新加载配置文件启动。Ubuntu 24.04/WSL 的软件包安装方式使用：

```shell
sudo systemctl reload nginx
```

## redis发布-订阅功能解决跨服务器通信问题

### 如何保证支持跨服务器通信

我们之前的`ChatServer`是维护了一个连接的用户表，每次向别的用户发消息都会从用户表中查看对端用户是否在线。然后再判断是直接发送，还是转为离线消息。

但是现在我们是集群服务器，有多个服务器维护用户。我们的`ChatServerA`要聊天的对象在`ChatServerB`，`ChatServerA`在自己服务器的用户表中找不到。那么可能对端用户在线，它却给对端用户发送了离线消息。因此，我们需要保证跨服务器间的通信！那我们如何实现，非常直观的想法，我们可以让后端的服务器之间互相连接。

![](https://cdn.nlark.com/yuque/0/2022/png/26752078/1663747492823-3d6b305d-0008-4fce-a1fa-4683f1800adb.png#crop=0&crop=0&crop=1&crop=1&from=url&id=RU1j6&margin=%5Bobject%20Object%5D&originHeight=554&originWidth=698&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)

上面的设计，让各个ChatServer服务器互相之间直接建立TCP连接进行通信，相当于在服务器网络之间进行广播。这样的设计使得各个服务器之间耦合度太高，不利于系统扩展，并且会占用系统大量的socket资源，各服务器之间的带宽压力很大，不能够节省资源给更多的客户端提供服务，因此绝对不是一个好的设计。

集群部署的服务器之间进行通信，最好的方式就是引入中间件消息队列，解耦各个服务器，使整个系统松耦合，提高服务器的响应能力，节省服务器的带宽资源，如下图所示：

![](https://cdn.nlark.com/yuque/0/2022/png/26752078/1663747534358-10e307b4-95c8-43f3-8dc2-5deed9893f1c.png#crop=0&crop=0&crop=1&crop=1&from=url&id=QproC&margin=%5Bobject%20Object%5D&originHeight=505&originWidth=619&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)

# 详细记录
