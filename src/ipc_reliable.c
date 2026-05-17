/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * 可靠IPC通信协议实现 - 纯C实现
 */

#include "ipc_reliable.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <sys/un.h>
#include <unistd.h>
#include <sys/syscall.h>

/* ==================== 内部常量 ==================== */

#define IPC_LOG_TAG "IpcReliable"
#define IPC_LOG_LEVEL_DEBUG 0
#define IPC_LOG_LEVEL_INFO  1
#define IPC_LOG_LEVEL_WARN  2
#define IPC_LOG_LEVEL_ERROR 3

/* ==================== CRC32表 ==================== */

static const uint32_t crc32Table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x1c26c936, 0x6f91c099, 0xf6166e88, 0x81111d1e, 0x1f5da5bd, 0x685a5a2b,
    0xf1a44f91, 0x86a34707, 0x35404e3d, 0x4247d4ab, 0xdaf0d611, 0xadf74587,
    0x3391dc24, 0x4496bcb2, 0xdd2f1d08, 0xaa27619e, 0x35c20b6b, 0x42c50bfd,
    0xdb7c6c47, 0xac7bbfd1, 0x32d18d72, 0x45d6d6e4, 0xdc31b45e, 0xab36c8c8,
    0x53c02f9d, 0x24c70f0b, 0xbdd1f111, 0xca36f287, 0x5450cf24, 0x2357c9b2,
    0xbaee408, 0xcd493998, 0x52aeff2d, 0x25a9f6bb, 0xbc10401, 0xcb46f697,
    0x4eeb3d34, 0x39ec65a2, 0xa00e418, 0xd70a2f8e, 0x494e8c6d, 0x3e49fb9b,
    0xa7905e21, 0xd09661b7, 0x4fc10cb5, 0x38c60c23, 0xa180dc99, 0xd66fb0f,
    0x5065710c, 0x2762419a, 0xbedb0920, 0xc9df1fb6, 0x46cae243, 0x31cdd543,
    0xa834b57f, 0xdf3393e9, 0x4175d54a, 0x3672dcd, 0xacdb3e77, 0xdbdc2fe1,
    0x43f66e59, 0x34f1cdcf, 0xad48546c, 0xda4ff4fa, 0x43f86c40, 0x34ff9ed6,
    0x80151d63, 0xf70df5f5, 0x6e4a4ff, 0x196d4999, 0x870b8a3a, 0xf00d8cac,
    0x69b4416, 0x19b380, 0x8015c080, 0xf7036f16, 0x6e0b8eac, 0x190c8a3a,
    0x9e1d4999, 0xe904f6ff, 0x720dd545, 0x0500a8d3, 0xb64c2b40, 0xec63f226,
    0x756aa39c, 0x26d930a, 0x9c0906a9, 0xeb0e363f, 0x72076785, 0x5000573,
    0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 0x92d28e9b, 0xe5d5be0d,
    0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e,
    0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777, 0x88085ae6, 0xff0f6a70,
    0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
    0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2, 0xa7672661, 0xd06016f7,
    0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0,
    0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9, 0xbdbdf21c, 0xcabac28a,
    0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
    0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94, 0xb40bbe37, 0xc30c8ea1,
    0x5a05df1b, 0x2d02ef8d
};

/* ==================== 日志系统 ==================== */

static IpcLogCallback g_logCallback = NULL;

static void IpcLog(int level, const char *tag, const char *fmt, ...)
{
    if (g_logCallback) {
        char buf[512];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);
        g_logCallback(level, tag, buf);
    } else {
        va_list args;
        va_start(args, fmt);
        const char *levelStr = "?";
        switch (level) {
            case IPC_LOG_LEVEL_DEBUG: levelStr = "D"; break;
            case IPC_LOG_LEVEL_INFO:  levelStr = "I"; break;
            case IPC_LOG_LEVEL_WARN:  levelStr = "W"; break;
            case IPC_LOG_LEVEL_ERROR: levelStr = "E"; break;
        }
        fprintf(stderr, "[%s][%s] ", levelStr, tag);
        vfprintf(stderr, fmt, args);
        fprintf(stderr, "\n");
        va_end(args);
    }
}

#define IPC_DEBUG(fmt, ...) IpcLog(IPC_LOG_LEVEL_DEBUG, IPC_LOG_TAG, fmt, ##__VA_ARGS__)
#define IPC_INFO(fmt, ...)  IpcLog(IPC_LOG_LEVEL_INFO, IPC_LOG_TAG, fmt, ##__VA_ARGS__)
#define IPC_WARN(fmt, ...)  IpcLog(IPC_LOG_LEVEL_WARN, IPC_LOG_TAG, fmt, ##__VA_ARGS__)
#define IPC_ERROR(fmt, ...) IpcLog(IPC_LOG_LEVEL_ERROR, IPC_LOG_TAG, fmt, ##__VA_ARGS__)

/* ==================== 内部数据结构 ==================== */

typedef enum {
    STATE_IDLE = 0,
    STATE_LISTENING,
    STATE_CONNECTING,
    STATE_CONNECTED,
    STATE_RECONNECTING,
    STATE_DISCONNECTED,
    STATE_ERROR
} IpcState;

typedef struct {
    uint32_t seq;
    uint8_t *data;
    uint32_t len;
    uint64_t timestamp;
    bool acked;
} IpcPendingMessage;

typedef struct IpcContext {
    /* 配置 */
    IpcConfig config;
    char socketPath[256];
    bool isServer;
    
    /* 状态 */
    volatile IpcState state;
    volatile bool running;
    
    /* Socket */
    int listenFd;
    int connFd;
    
    /* 线程 */
    pthread_t recvThread;
    pthread_t heartbeatThread;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    
    /* 序列号管理 */
    uint32_t nextSeq;
    uint32_t expectedSeq;
    
    /* 消息窗口 */
    IpcPendingMessage sendWindow[IPC_WINDOW_SIZE];
    pthread_mutex_t windowLock;
    
    /* 统计 */
    uint64_t txBytes;
    uint64_t rxBytes;
    uint64_t txMessages;
    uint64_t rxMessages;
    uint64_t reconnectCount;
    
    /* 保活 */
    uint64_t lastRecvTime;
    uint64_t lastSendTime;
} IpcContextImpl;

/* ==================== 工具函数 ==================== */

static uint64_t GetTickMs(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    }
    return 0;
}

static uint32_t CalculateCrc32(const uint8_t *data, uint32_t len)
{
    uint32_t crc = 0xffffffff;
    for (uint32_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32Table[(crc ^ data[i]) & 0xff];
    }
    return crc ^ 0xffffffff;
}

/* ==================== 消息序列化 ==================== */

static void SerializeHeader(uint8_t *buf, const IpcMessageHeader *hdr)
{
    buf[0] = (hdr->magic >> 24) & 0xff;
    buf[1] = (hdr->magic >> 16) & 0xff;
    buf[2] = (hdr->magic >> 8) & 0xff;
    buf[3] = hdr->magic & 0xff;
    buf[4] = hdr->version;
    buf[5] = hdr->type;
    buf[6] = (hdr->flags >> 8) & 0xff;
    buf[7] = hdr->flags & 0xff;
    buf[8] = (hdr->seq >> 24) & 0xff;
    buf[9] = (hdr->seq >> 16) & 0xff;
    buf[10] = (hdr->seq >> 8) & 0xff;
    buf[11] = hdr->seq & 0xff;
    buf[12] = (hdr->ack >> 24) & 0xff;
    buf[13] = (hdr->ack >> 16) & 0xff;
    buf[14] = (hdr->ack >> 8) & 0xff;
    buf[15] = hdr->ack & 0xff;
    buf[16] = (hdr->payloadLen >> 24) & 0xff;
    buf[17] = (hdr->payloadLen >> 16) & 0xff;
    buf[18] = (hdr->payloadLen >> 8) & 0xff;
    buf[19] = hdr->payloadLen & 0xff;
    buf[20] = (hdr->checksum >> 24) & 0xff;
    buf[21] = (hdr->checksum >> 16) & 0xff;
    buf[22] = (hdr->checksum >> 8) & 0xff;
    buf[23] = hdr->checksum & 0xff;
}

static bool DeserializeHeader(const uint8_t *buf, IpcMessageHeader *hdr)
{
    hdr->magic = ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
                 ((uint32_t)buf[2] << 8) | buf[3];
    hdr->version = buf[4];
    hdr->type = buf[5];
    hdr->flags = ((uint16_t)buf[6] << 8) | buf[7];
    hdr->seq = ((uint32_t)buf[8] << 24) | ((uint32_t)buf[9] << 16) |
               ((uint32_t)buf[10] << 8) | buf[11];
    hdr->ack = ((uint32_t)buf[12] << 24) | ((uint32_t)buf[13] << 16) |
               ((uint32_t)buf[14] << 8) | buf[15];
    hdr->payloadLen = ((uint32_t)buf[16] << 24) | ((uint32_t)buf[17] << 16) |
                      ((uint32_t)buf[18] << 8) | buf[19];
    hdr->checksum = ((uint32_t)buf[20] << 24) | ((uint32_t)buf[21] << 16) |
                    ((uint32_t)buf[22] << 8) | buf[23];
    
    if (hdr->magic != IPC_MAGIC) {
        IPC_ERROR("Invalid magic: 0x%08X", hdr->magic);
        return false;
    }
    if (hdr->version != IPC_VERSION) {
        IPC_ERROR("Unsupported version: %d", hdr->version);
        return false;
    }
    if (hdr->payloadLen > IPC_MAX_PAYLOAD_SIZE) {
        IPC_ERROR("Payload too large: %u", hdr->payloadLen);
        return false;
    }
    return true;
}

/* ==================== Socket操作 ==================== */

static int CreateSocket(void)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        IPC_ERROR("socket() failed: %s", strerror(errno));
        return -1;
    }
    
    /* 设置非阻塞模式 */
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    
    /* 设置 reuseaddr */
    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    return fd;
}

static bool SetSocketBlocking(int fd, bool blocking)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    
    flags = blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK);
    return fcntl(fd, F_SETFL, flags) == 0;
}

static int BindAndListen(IpcContextImpl *ctx)
{
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, ctx->socketPath, sizeof(addr.sun_path) - 1);
    
    unlink(ctx->socketPath);
    
    if (bind(ctx->listenFd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        IPC_ERROR("bind() failed: %s", strerror(errno));
        return IPC_ERR_SOCKET_FAILED;
    }
    
    chmod(ctx->socketPath, 0666);
    
    if (listen(ctx->listenFd, IPC_SOCKET_BACKLOG) < 0) {
        IPC_ERROR("listen() failed: %s", strerror(errno));
        return IPC_ERR_SOCKET_FAILED;
    }
    
    ctx->state = STATE_LISTENING;
    IPC_INFO("Server listening on %s", ctx->socketPath);
    return IPC_OK;
}

static int DoConnect(IpcContextImpl *ctx)
{
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, ctx->socketPath, sizeof(addr.sun_path) - 1);
    
    ctx->state = STATE_CONNECTING;
    
    /* 临时设置为阻塞模式 */
    SetSocketBlocking(ctx->connFd, true);
    
    if (connect(ctx->connFd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
            IPC_ERROR("connect() failed: %s", strerror(errno));
            ctx->state = STATE_DISCONNECTED;
            return IPC_ERR_CONNECT_FAILED;
        }
    }
    
    /* 恢复非阻塞模式 */
    SetSocketBlocking(ctx->connFd, false);
    
    ctx->state = STATE_CONNECTED;
    ctx->lastRecvTime = GetTickMs();
    ctx->lastSendTime = GetTickMs();
    
    IPC_INFO("Connected to %s", ctx->socketPath);
    
    if (ctx->config.onConnect) {
        ctx->config.onConnect(true, ctx->config.userData);
    }
    
    return IPC_OK;
}

/* ==================== 消息发送 ==================== */

static int SendAll(int fd, const uint8_t *data, uint32_t len)
{
    uint32_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd = { .fd = fd, .events = POLLOUT };
                if (poll(&pfd, 1, 100) <= 0) {
                    continue;
                }
                continue;
            }
            return IPC_ERR_SEND_FAILED;
        }
        sent += n;
    }
    return IPC_OK;
}

static int SendMessageInternal(IpcContextImpl *ctx, IpcMessageType type,
                                uint16_t flags, uint32_t seq, uint32_t ack,
                                const uint8_t *payload, uint32_t payloadLen)
{
    if (ctx->connFd < 0 || ctx->state != STATE_CONNECTED) {
        return IPC_ERR_NOT_CONNECTED;
    }
    
    if (payloadLen > IPC_MAX_PAYLOAD_SIZE) {
        return IPC_ERR_MSG_TOO_LARGE;
    }
    
    /* 构建消息 */
    IpcMessageHeader hdr;
    hdr.magic = IPC_MAGIC;
    hdr.version = IPC_VERSION;
    hdr.type = (uint8_t)type;
    hdr.flags = flags;
    hdr.seq = seq;
    hdr.ack = ack;
    hdr.payloadLen = payloadLen;
    
    /* 计算校验和（payload部分） */
    if (payload && payloadLen > 0) {
        hdr.checksum = CalculateCrc32(payload, payloadLen);
    } else {
        hdr.checksum = 0;
    }
    
    /* 序列化头部 */
    uint8_t headerBuf[24];
    SerializeHeader(headerBuf, &hdr);
    
    pthread_mutex_lock(&ctx->lock);
    
    /* 发送头部 */
    int ret = SendAll(ctx->connFd, headerBuf, sizeof(headerBuf));
    if (ret != IPC_OK) {
        pthread_mutex_unlock(&ctx->lock);
        return ret;
    }
    
    /* 发送payload */
    if (payload && payloadLen > 0) {
        ret = SendAll(ctx->connFd, payload, payloadLen);
        if (ret != IPC_OK) {
            pthread_mutex_unlock(&ctx->lock);
            return ret;
        }
    }
    
    ctx->lastSendTime = GetTickMs();
    ctx->txBytes += sizeof(headerBuf) + payloadLen;
    ctx->txMessages++;
    
    pthread_mutex_unlock(&ctx->lock);
    
    return IPC_OK;
}

/* ==================== 消息接收 ==================== */

static int RecvAll(int fd, uint8_t *buf, uint32_t len, uint32_t timeoutMs)
{
    uint32_t received = 0;
    uint64_t startTime = GetTickMs();
    
    while (received < len) {
        ssize_t n = recv(fd, buf + received, len - received, 0);
        if (n > 0) {
            received += n;
        } else if (n == 0) {
            return IPC_ERR_PEER_CLOSED;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                uint64_t elapsed = GetTickMs() - startTime;
                if (elapsed >= timeoutMs) {
                    return IPC_ERR_TIMEOUT;
                }
                struct pollfd pfd = { .fd = fd, .events = POLLIN };
                int pollRet = poll(&pfd, 1, (int)(timeoutMs - elapsed));
                if (pollRet < 0) {
                    return IPC_ERR_RECV_FAILED;
                }
                if (pollRet == 0) {
                    return IPC_ERR_TIMEOUT;
                }
                continue;
            }
            return IPC_ERR_RECV_FAILED;
        }
    }
    return IPC_OK;
}

static void HandleMessage(IpcContextImpl *ctx, const IpcMessageHeader *hdr,
                          const uint8_t *payload)
{
    ctx->lastRecvTime = GetTickMs();
    ctx->rxBytes += sizeof(IpcMessageHeader) + hdr->payloadLen;
    ctx->rxMessages++;
    
    switch (hdr->type) {
        case MSG_TYPE_DATA:
            IPC_DEBUG("Received DATA seq=%u, len=%u", hdr->seq, hdr->payloadLen);
            
            /* 检查序列号 */
            if (hdr->seq < ctx->expectedSeq) {
                IPC_WARN("Duplicate message seq=%u, expected=%u", 
                         hdr->seq, ctx->expectedSeq);
            } else {
                ctx->expectedSeq = hdr->seq + 1;
                
                /* 回调用户 */
                if (ctx->config.onMessage && hdr->payloadLen > 0) {
                    ctx->config.onMessage(hdr->seq, payload, hdr->payloadLen, 
                                          ctx->config.userData);
                }
            }
            
            /* 发送ACK */
            if (hdr->flags & IPC_FLAG_NEED_ACK) {
                SendMessageInternal(ctx, MSG_TYPE_ACK, IPC_FLAG_IS_ACK, 
                                    0, hdr->seq, NULL, 0);
            }
            break;
            
        case MSG_TYPE_ACK:
            IPC_DEBUG("Received ACK for seq=%u", hdr->ack);
            pthread_mutex_lock(&ctx->windowLock);
            for (int i = 0; i < IPC_WINDOW_SIZE; i++) {
                if (ctx->sendWindow[i].seq == hdr->ack) {
                    ctx->sendWindow[i].acked = true;
                    if (ctx->sendWindow[i].data) {
                        free(ctx->sendWindow[i].data);
                        ctx->sendWindow[i].data = NULL;
                    }
                    break;
                }
            }
            pthread_mutex_unlock(&ctx->windowLock);
            pthread_cond_broadcast(&ctx->cond);
            break;
            
        case MSG_TYPE_HEARTBEAT:
            IPC_DEBUG("Received HEARTBEAT");
            SendMessageInternal(ctx, MSG_TYPE_HEARTBEAT_ACK, 0, 0, 0, NULL, 0);
            break;
            
        case MSG_TYPE_HEARTBEAT_ACK:
            IPC_DEBUG("Received HEARTBEAT_ACK");
            break;
            
        case MSG_TYPE_CONNECT_REQ:
            IPC_INFO("Received CONNECT_REQ");
            SendMessageInternal(ctx, MSG_TYPE_CONNECT_RESP, 0, 0, 0, NULL, 0);
            break;
            
        case MSG_TYPE_CONNECT_RESP:
            IPC_INFO("Received CONNECT_RESP");
            break;
            
        case MSG_TYPE_DISCONNECT:
            IPC_INFO("Received DISCONNECT");
            ctx->state = STATE_DISCONNECTED;
            break;
    }
}

static void *ReceiveThread(void *arg)
{
    IpcContextImpl *ctx = (IpcContextImpl *)arg;
    uint8_t headerBuf[24];
    uint8_t *payloadBuf = NULL;
    
    IPC_INFO("Receive thread started");
    
    while (ctx->running) {
        if (ctx->state != STATE_CONNECTED || ctx->connFd < 0) {
            usleep(10000);  /* 10ms */
            continue;
        }
        
        /* 接收头部 */
        int ret = RecvAll(ctx->connFd, headerBuf, sizeof(headerBuf), 100);
        if (ret == IPC_ERR_TIMEOUT) {
            continue;
        }
        if (ret != IPC_OK) {
            IPC_ERROR("Failed to receive header: %d", ret);
            goto disconnect;
        }
        
        /* 解析头部 */
        IpcMessageHeader hdr;
        if (!DeserializeHeader(headerBuf, &hdr)) {
            IPC_ERROR("Failed to deserialize header");
            goto disconnect;
        }
        
        /* 接收payload */
        if (hdr.payloadLen > 0) {
            payloadBuf = (uint8_t *)realloc(payloadBuf, hdr.payloadLen);
            if (!payloadBuf) {
                IPC_ERROR("Failed to allocate payload buffer");
                goto disconnect;
            }
            
            ret = RecvAll(ctx->connFd, payloadBuf, hdr.payloadLen, 5000);
            if (ret != IPC_OK) {
                IPC_ERROR("Failed to receive payload: %d", ret);
                goto disconnect;
            }
            
            /* 验证校验和 */
            uint32_t calcChecksum = CalculateCrc32(payloadBuf, hdr.payloadLen);
            if (calcChecksum != hdr.checksum) {
                IPC_ERROR("Checksum mismatch: calc=%08X, recv=%08X",
                          calcChecksum, hdr.checksum);
                continue;
            }
        }
        
        HandleMessage(ctx, &hdr, payloadBuf);
        continue;
        
    disconnect:
        pthread_mutex_lock(&ctx->lock);
        if (ctx->connFd >= 0) {
            close(ctx->connFd);
            ctx->connFd = -1;
        }
        ctx->state = STATE_DISCONNECTED;
        pthread_mutex_unlock(&ctx->lock);
        
        if (ctx->config.onConnect) {
            ctx->config.onConnect(false, ctx->config.userData);
        }
        
        /* 触发重连 */
        if (ctx->config.enableReconnect) {
            IPC_INFO("Triggering reconnect...");
            IpcReconnect((IpcContext *)ctx);
        }
    }
    
    free(payloadBuf);
    IPC_INFO("Receive thread stopped");
    return NULL;
}

/* ==================== 心跳线程 ==================== */

static void *HeartbeatThread(void *arg)
{
    IpcContextImpl *ctx = (IpcContextImpl *)arg;
    uint32_t interval = ctx->config.heartbeatIntervalMs;
    uint32_t timeout = ctx->config.heartbeatTimeoutMs;
    
    IPC_INFO("Heartbeat thread started, interval=%ums, timeout=%ums", 
             interval, timeout);
    
    while (ctx->running) {
        usleep(interval * 1000);
        
        if (ctx->state != STATE_CONNECTED) {
            continue;
        }
        
        uint64_t now = GetTickMs();
        
        /* 检查是否超时未收到消息 */
        if (now - ctx->lastRecvTime > timeout) {
            IPC_WARN("Heartbeat timeout, last recv was %llums ago",
                     (unsigned long long)(now - ctx->lastRecvTime));
            
            pthread_mutex_lock(&ctx->lock);
            if (ctx->connFd >= 0) {
                close(ctx->connFd);
                ctx->connFd = -1;
            }
            ctx->state = STATE_DISCONNECTED;
            pthread_mutex_unlock(&ctx->lock);
            
            if (ctx->config.onConnect) {
                ctx->config.onConnect(false, ctx->config.userData);
            }
            
            if (ctx->config.enableReconnect) {
                IpcReconnect((IpcContext *)ctx);
            }
            continue;
        }
        
        /* 发送心跳 */
        if (now - ctx->lastSendTime > interval) {
            SendMessageInternal(ctx, MSG_TYPE_HEARTBEAT, 0, 0, 0, NULL, 0);
        }
    }
    
    IPC_INFO("Heartbeat thread stopped");
    return NULL;
}

/* ==================== 公共API实现 ==================== */

IpcContext* IpcInit(const IpcConfig *config)
{
    if (!config) {
        return NULL;
    }
    
    IpcContextImpl *ctx = (IpcContextImpl *)calloc(1, sizeof(IpcContextImpl));
    if (!ctx) {
        return NULL;
    }
    
    /* 复制配置 */
    ctx->config = *config;
    ctx->isServer = config->isServer;
    
    /* 设置默认值 */
    if (config->socketPath) {
        strncpy(ctx->socketPath, config->socketPath, sizeof(ctx->socketPath) - 1);
    } else {
        strcpy(ctx->socketPath, IPC_DEFAULT_SOCKET_PATH);
    }
    
    if (ctx->config.connectTimeoutMs == 0) {
        ctx->config.connectTimeoutMs = IPC_CONNECT_TIMEOUT_MS;
    }
    if (ctx->config.ackTimeoutMs == 0) {
        ctx->config.ackTimeoutMs = IPC_ACK_TIMEOUT_MS;
    }
    if (ctx->config.heartbeatIntervalMs == 0) {
        ctx->config.heartbeatIntervalMs = IPC_HEARTBEAT_INTERVAL_MS;
    }
    if (ctx->config.heartbeatTimeoutMs == 0) {
        ctx->config.heartbeatTimeoutMs = IPC_HEARTBEAT_TIMEOUT_MS;
    }
    if (ctx->config.maxReconnectAttempts == 0) {
        ctx->config.maxReconnectAttempts = IPC_MAX_RECONNECT_ATTEMPTS;
    }
    if (ctx->config.reconnectIntervalMs == 0) {
        ctx->config.reconnectIntervalMs = IPC_RECONNECT_INTERVAL_MS;
    }
    
    /* 初始化状态 */
    ctx->state = STATE_IDLE;
    ctx->running = true;
    ctx->connFd = -1;
    ctx->listenFd = -1;
    ctx->nextSeq = 1;
    ctx->expectedSeq = 1;
    
    /* 初始化锁和条件变量 */
    pthread_mutex_init(&ctx->lock, NULL);
    pthread_mutex_init(&ctx->windowLock, NULL);
    pthread_cond_init(&ctx->cond, NULL);
    
    IPC_INFO("IPC initialized, mode=%s, path=%s",
             ctx->isServer ? "server" : "client", ctx->socketPath);
    
    return (IpcContext *)ctx;
}

void IpcDestroy(IpcContext *context)
{
    if (!context) return;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    ctx->running = false;
    
    /* 关闭连接 */
    IpcDisconnect(context);
    
    /* 等待线程结束 */
    if (ctx->recvThread) {
        pthread_join(ctx->recvThread, NULL);
    }
    if (ctx->heartbeatThread) {
        pthread_join(ctx->heartbeatThread, NULL);
    }
    
    /* 关闭监听socket */
    if (ctx->listenFd >= 0) {
        close(ctx->listenFd);
        unlink(ctx->socketPath);
    }
    
    /* 释放窗口资源 */
    for (int i = 0; i < IPC_WINDOW_SIZE; i++) {
        if (ctx->sendWindow[i].data) {
            free(ctx->sendWindow[i].data);
        }
    }
    
    pthread_mutex_destroy(&ctx->lock);
    pthread_mutex_destroy(&ctx->windowLock);
    pthread_cond_destroy(&ctx->cond);
    
    free(ctx);
    IPC_INFO("IPC destroyed");
}

int IpcListen(IpcContext *context)
{
    if (!context) return IPC_ERR_INVALID_PARAM;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    if (!ctx->isServer) {
        return IPC_ERR_INVALID_PARAM;
    }
    
    ctx->listenFd = CreateSocket();
    if (ctx->listenFd < 0) {
        return IPC_ERR_SOCKET_FAILED;
    }
    
    int ret = BindAndListen(ctx);
    if (ret != IPC_OK) {
        close(ctx->listenFd);
        ctx->listenFd = -1;
        return ret;
    }
    
    /* 启动工作线程 */
    pthread_create(&ctx->recvThread, NULL, ReceiveThread, ctx);
    pthread_create(&ctx->heartbeatThread, NULL, HeartbeatThread, ctx);
    
    return IPC_OK;
}

int IpcAccept(IpcContext *context)
{
    if (!context) return IPC_ERR_INVALID_PARAM;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    if (!ctx->isServer || ctx->listenFd < 0) {
        return IPC_ERR_INVALID_PARAM;
    }
    
    struct sockaddr_un clientAddr;
    socklen_t addrLen = sizeof(clientAddr);
    
    int newFd = accept(ctx->listenFd, (struct sockaddr *)&clientAddr, &addrLen);
    if (newFd < 0) {
        IPC_ERROR("accept() failed: %s", strerror(errno));
        return IPC_ERR_SOCKET_FAILED;
    }
    
    /* 关闭旧的连接（如果存在） */
    pthread_mutex_lock(&ctx->lock);
    if (ctx->connFd >= 0) {
        close(ctx->connFd);
    }
    ctx->connFd = newFd;
    ctx->state = STATE_CONNECTED;
    ctx->lastRecvTime = GetTickMs();
    ctx->lastSendTime = GetTickMs();
    ctx->nextSeq = 1;
    ctx->expectedSeq = 1;
    pthread_mutex_unlock(&ctx->lock);
    
    /* 发送连接响应 */
    SendMessageInternal(ctx, MSG_TYPE_CONNECT_RESP, 0, 0, 0, NULL, 0);
    
    IPC_INFO("New client connected");
    
    if (ctx->config.onConnect) {
        ctx->config.onConnect(true, ctx->config.userData);
    }
    
    return IPC_OK;
}

bool IpcPollAccept(IpcContext *context, uint32_t timeoutMs)
{
    if (!context) return false;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    if (!ctx->isServer || ctx->listenFd < 0) {
        return false;
    }
    
    struct pollfd pfd = { .fd = ctx->listenFd, .events = POLLIN };
    return poll(&pfd, 1, timeoutMs) > 0;
}

int IpcConnect(IpcContext *context)
{
    if (!context) return IPC_ERR_INVALID_PARAM;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    if (ctx->isServer) {
        return IPC_ERR_INVALID_PARAM;
    }
    
    ctx->connFd = CreateSocket();
    if (ctx->connFd < 0) {
        return IPC_ERR_SOCKET_FAILED;
    }
    
    int ret = DoConnect(ctx);
    if (ret != IPC_OK) {
        close(ctx->connFd);
        ctx->connFd = -1;
        return ret;
    }
    
    /* 发送连接请求 */
    SendMessageInternal(ctx, MSG_TYPE_CONNECT_REQ, 0, 0, 0, NULL, 0);
    
    /* 启动工作线程 */
    pthread_create(&ctx->recvThread, NULL, ReceiveThread, ctx);
    pthread_create(&ctx->heartbeatThread, NULL, HeartbeatThread, ctx);
    
    return IPC_OK;
}

int IpcReconnect(IpcContext *context)
{
    if (!context) return IPC_ERR_INVALID_PARAM;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    if (ctx->isServer) {
        return IPC_ERR_INVALID_PARAM;  /* 服务端不支持重连 */
    }
    
    ctx->state = STATE_RECONNECTING;
    ctx->reconnectCount = 0;
    
    while (ctx->running && 
           ctx->reconnectCount < ctx->config.maxReconnectAttempts) {
        
        ctx->reconnectCount++;
        IPC_INFO("Reconnect attempt %u/%u", 
                 (uint32_t)ctx->reconnectCount, ctx->config.maxReconnectAttempts);
        
        /* 关闭旧连接 */
        if (ctx->connFd >= 0) {
            close(ctx->connFd);
            ctx->connFd = -1;
        }
        
        /* 创建新socket */
        ctx->connFd = CreateSocket();
        if (ctx->connFd < 0) {
            usleep(ctx->config.reconnectIntervalMs * 1000);
            continue;
        }
        
        /* 尝试连接 */
        if (DoConnect(ctx) == IPC_OK) {
            /* 发送连接请求 */
            SendMessageInternal(ctx, MSG_TYPE_CONNECT_REQ, 0, 0, 0, NULL, 0);
            
            ctx->reconnectCount = 0;
            IPC_INFO("Reconnected successfully");
            return IPC_OK;
        }
        
        close(ctx->connFd);
        ctx->connFd = -1;
        
        usleep(ctx->config.reconnectIntervalMs * 1000);
    }
    
    ctx->state = STATE_ERROR;
    IPC_ERROR("Reconnect failed after %u attempts", ctx->config.maxReconnectAttempts);
    
    if (ctx->config.onError) {
        ctx->config.onError(IPC_ERR_CONNECT_FAILED, "Reconnect failed", 
                           ctx->config.userData);
    }
    
    return IPC_ERR_CONNECT_FAILED;
}

int IpcSendData(IpcContext *context, const uint8_t *data, uint32_t len,
                bool needAck, uint32_t *seq)
{
    if (!context || !data) return IPC_ERR_INVALID_PARAM;
    if (len == 0 || len > IPC_MAX_PAYLOAD_SIZE) return IPC_ERR_MSG_TOO_LARGE;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    if (ctx->state != STATE_CONNECTED) {
        return IPC_ERR_NOT_CONNECTED;
    }
    
    pthread_mutex_lock(&ctx->lock);
    uint32_t currentSeq = ctx->nextSeq++;
    pthread_mutex_unlock(&ctx->lock);
    
    if (seq) {
        *seq = currentSeq;
    }
    
    uint16_t flags = needAck ? IPC_FLAG_NEED_ACK : 0;
    
    /* 如果需要ACK，加入发送窗口 */
    if (needAck) {
        pthread_mutex_lock(&ctx->windowLock);
        int slot = currentSeq % IPC_WINDOW_SIZE;
        
        /* 清理旧数据 */
        if (ctx->sendWindow[slot].data) {
            free(ctx->sendWindow[slot].data);
        }
        
        ctx->sendWindow[slot].seq = currentSeq;
        ctx->sendWindow[slot].data = (uint8_t *)malloc(len);
        if (ctx->sendWindow[slot].data) {
            memcpy(ctx->sendWindow[slot].data, data, len);
        }
        ctx->sendWindow[slot].len = len;
        ctx->sendWindow[slot].timestamp = GetTickMs();
        ctx->sendWindow[slot].acked = false;
        
        pthread_mutex_unlock(&ctx->windowLock);
    }
    
    /* 发送消息 */
    int ret = SendMessageInternal(ctx, MSG_TYPE_DATA, flags, currentSeq, 0, data, len);
    if (ret != IPC_OK && needAck) {
        /* 发送失败，从窗口移除 */
        pthread_mutex_lock(&ctx->windowLock);
        int slot = currentSeq % IPC_WINDOW_SIZE;
        if (ctx->sendWindow[slot].data) {
            free(ctx->sendWindow[slot].data);
            ctx->sendWindow[slot].data = NULL;
        }
        pthread_mutex_unlock(&ctx->windowLock);
        return ret;
    }
    
    /* 等待ACK */
    if (needAck && ret == IPC_OK) {
        pthread_mutex_lock(&ctx->windowLock);
        
        int slot = currentSeq % IPC_WINDOW_SIZE;
        uint64_t startTime = GetTickMs();
        
        while (!ctx->sendWindow[slot].acked && ctx->running) {
            uint64_t elapsed = GetTickMs() - startTime;
            if (elapsed >= ctx->config.ackTimeoutMs) {
                pthread_mutex_unlock(&ctx->windowLock);
                IPC_WARN("ACK timeout for seq=%u", currentSeq);
                
                /* 清理 */
                if (ctx->sendWindow[slot].data) {
                    free(ctx->sendWindow[slot].data);
                    ctx->sendWindow[slot].data = NULL;
                }
                return IPC_ERR_ACK_FAILED;
            }
            
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 10000000;  /* 10ms */
            pthread_cond_timedwait(&ctx->cond, &ctx->windowLock, &ts);
        }
        
        bool acked = ctx->sendWindow[slot].acked;
        pthread_mutex_unlock(&ctx->windowLock);
        
        if (!acked) {
            return IPC_ERR_ACK_FAILED;
        }
    }
    
    return IPC_OK;
}

int IpcSendAck(IpcContext *context, uint32_t seq)
{
    if (!context) return IPC_ERR_INVALID_PARAM;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    return SendMessageInternal(ctx, MSG_TYPE_ACK, IPC_FLAG_IS_ACK, 0, seq, NULL, 0);
}

bool IpcIsConnected(const IpcContext *context)
{
    if (!context) return false;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    return ctx->state == STATE_CONNECTED;
}

void IpcDisconnect(IpcContext *context)
{
    if (!context) return;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    if (ctx->connFd >= 0 && ctx->state == STATE_CONNECTED) {
        /* 发送断开通知 */
        SendMessageInternal(ctx, MSG_TYPE_DISCONNECT, 0, 0, 0, NULL, 0);
    }
    
    pthread_mutex_lock(&ctx->lock);
    if (ctx->connFd >= 0) {
        close(ctx->connFd);
        ctx->connFd = -1;
    }
    ctx->state = STATE_DISCONNECTED;
    pthread_mutex_unlock(&ctx->lock);
    
    if (ctx->config.onConnect) {
        ctx->config.onConnect(false, ctx->config.userData);
    }
}

int IpcWaitConnected(IpcContext *context, uint32_t timeoutMs)
{
    if (!context) return IPC_ERR_INVALID_PARAM;
    
    IpcContextImpl *ctx = (IpcContextImpl *)context;
    
    uint64_t startTime = GetTickMs();
    while (GetTickMs() - startTime < timeoutMs) {
        if (ctx->state == STATE_CONNECTED) {
            return IPC_OK;
        }
        usleep(10000);  /* 10ms */
    }
    
    return IPC_ERR_TIMEOUT;
}

void IpcSetLogCallback(IpcLogCallback callback)
{
    g_logCallback = callback;
}

const char* IpcGetErrorString(int errorCode)
{
    switch (errorCode) {
        case IPC_OK: return "OK";
        case IPC_ERR_INVALID_PARAM: return "Invalid parameter";
        case IPC_ERR_NO_MEMORY: return "Out of memory";
        case IPC_ERR_SOCKET_FAILED: return "Socket operation failed";
        case IPC_ERR_CONNECT_FAILED: return "Connection failed";
        case IPC_ERR_SEND_FAILED: return "Send failed";
        case IPC_ERR_RECV_FAILED: return "Receive failed";
        case IPC_ERR_TIMEOUT: return "Operation timed out";
        case IPC_ERR_CHECKSUM: return "Checksum error";
        case IPC_ERR_PEER_CLOSED: return "Peer closed connection";
        case IPC_ERR_MSG_TOO_LARGE: return "Message too large";
        case IPC_ERR_NOT_CONNECTED: return "Not connected";
        case IPC_ERR_ACK_FAILED: return "ACK failed";
        default: return "Unknown error";
    }
}
