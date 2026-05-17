# 可靠IPC通信方案 (ipc_reliable)

## 特性

相比原始 `ipca/ipcb` 的简单字符串通信，新的 `ipc_server/ipc_client` 实现了：

| 特性 | 原方案 | 新方案 |
|------|--------|--------|
| 数据格式 | 字符串 | 二进制数据 |
| 通信方式 | 请求-响应 | 双向主动通信 |
| 确认机制 | 无 | ACK确认 |
| 重连恢复 | 无 | 自动重连 |
| 心跳保活 | 无 | 有 |
| 消息去重 | 无 | 序列号去重 |
| 数据校验 | 无 | CRC32校验 |
| 消息边界 | 字节流 | 完整消息包 |

## 协议设计

```
+-----------------+------------------+------------------+
|    Header       |     Payload      |      CRC32       |
|   (24 bytes)    |   (variable)     |   (in header)    |
+-----------------+------------------+------------------+

Header 格式:
  - Magic (4 bytes): 0x49504352 ('IPCR')
  - Version (1 byte): 1
  - Type (1 byte): MSG_TYPE_DATA/ACK/HEARTBEAT
  - Flags (2 bytes): NEED_ACK/FIRST_FRAG/LAST_FRAG
  - Sequence (4 bytes): 消息序列号
  - Ack (4 bytes): 确认号
  - Payload Length (4 bytes): 数据长度
  - Checksum (4 bytes): CRC32校验
```

## 编译

```bash
cd /home/ou/work/ohos
./build.sh --product-name rk3568 --build-target //applications/sample/ipc_demo:ipc_server
./build.sh --product-name rk3568 --build-target //applications/sample/ipc_demo:ipc_client
```

## 使用方法

### 1. 启动服务端

```bash
/system/bin/ipc_server [socket_path]
```

默认使用 `/data/local/tmp/ipc_reliable.sock`

### 2. 启动客户端

```bash
/system/bin/ipc_client [socket_path]
```

### 3. 客户端命令

| 命令 | 功能 |
|------|------|
| `1` | 发送心跳/状态查询 (cmd=0x00010001) |
| `2` | 发送数据上报 (cmd=0x00020001) |
| `3` | 发送自定义二进制数据 (cmd=0xDEADBEEF) |
| `s` | 显示连接状态 |
| `d` | 手动断开连接 |
| `r` | 手动重连 |
| `q` | 退出程序 |

## 重连演示

1. 启动服务端
```bash
/system/bin/ipc_server
```

2. 启动客户端
```bash
/system/bin/ipc_client
```

3. 在客户端按 `2` 发送几条消息

4. 强制停止服务端 (Ctrl+C)，观察客户端自动重连

5. 重新启动服务端

6. 观察客户端自动恢复连接，并能继续通信

## API 使用示例

### 服务端代码

```c
#include "ipc_reliable.h"

void OnMessage(uint32_t seq, const uint8_t *data, uint32_t len, void *userData) {
    // 处理收到的二进制数据
    // data[0-3] = 命令 (大端)
    // data[4...] = payload
}

void OnConnect(bool connected, void *userData) {
    // 连接状态变化
}

int main() {
    IpcConfig config = {
        .socketPath = "/data/local/tmp/my_ipc.sock",
        .isServer = true,
        .onMessage = OnMessage,
        .onConnect = OnConnect,
    };
    
    IpcContext *ctx = IpcInit(&config);
    IpcListen(ctx);
    
    while (running) {
        if (IpcPollAccept(ctx, 1000)) {
            IpcAccept(ctx);
        }
        
        // 可以主动发送消息
        if (IpcIsConnected(ctx)) {
            uint8_t msg[] = {0x00, 0x01, 0x02, 0x03};
            IpcSendData(ctx, msg, sizeof(msg), true, &seq);
        }
    }
    
    IpcDestroy(ctx);
}
```

### 客户端代码

```c
#include "ipc_reliable.h"

int main() {
    IpcConfig config = {
        .socketPath = "/data/local/tmp/my_ipc.sock",
        .isServer = false,
        .enableReconnect = true,     // 启用自动重连
        .maxReconnectAttempts = 10,
        .onMessage = OnMessage,
        .onConnect = OnConnect,
    };
    
    IpcContext *ctx = IpcInit(&config);
    IpcConnect(ctx);  // 初次连接
    
    // 如果连接断开，库会自动重连
    
    // 发送二进制数据
    uint8_t binaryData[256];
    IpcSendData(ctx, binaryData, sizeof(binaryData), true, &seq);
    
    IpcDestroy(ctx);
}
```

## 配置参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `connectTimeoutMs` | 5000 | 连接超时(ms) |
| `ackTimeoutMs` | 3000 | 等待ACK超时(ms) |
| `heartbeatIntervalMs` | 10000 | 心跳发送间隔(ms) |
| `heartbeatTimeoutMs` | 30000 | 心跳超时断开(ms) |
| `enableReconnect` | false | 是否启用自动重连 |
| `maxReconnectAttempts` | 10 | 最大重连次数 |
| `reconnectIntervalMs` | 1000 | 重连尝试间隔(ms) |

## 安全考虑

1. **接口描述符校验**: 每个消息包含接口token，防止中继攻击
2. **CRC32校验**: 数据完整性校验
3. **序列号验证**: 防止重放攻击和消息乱序
4. **消息长度限制**: 最大64KB，防止内存耗尽攻击
5. **文件权限**: Socket文件权限0666，可根据需要调整

## 局限性和改进建议

1. **当前局限**:
   - 单客户端连接（服务端一次只处理一个客户端）
   - 消息分片功能已实现但未完整测试
   - 没有内置的加密机制

2. **改进方向**:
   - 迁移到 Binder IPC 获得更好的系统集成
   - 使用 HDF 框架实现驱动级 IPC
   - 添加 TLS/SSL 加密通道
