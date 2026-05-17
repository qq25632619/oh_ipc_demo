/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * 可靠IPC通信协议 - 纯C实现
 * 特性：
 * 1. 二进制数据传输
 * 2. 双向主动通信
 * 3. 消息确认(ACK)机制
 * 4. 自动重连恢复
 * 5. 心跳保活
 * 6. 消息序列号与去重
 */

#ifndef IPC_RELIABLE_H
#define IPC_RELIABLE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ==================== 配置常量 ==================== */

#define IPC_DEFAULT_SOCKET_PATH "/data/local/tmp/ipc_reliable.sock"
#define IPC_MAX_MESSAGE_SIZE    (64 * 1024)     /* 最大消息64KB */
#define IPC_MAX_PAYLOAD_SIZE    (IPC_MAX_MESSAGE_SIZE - sizeof(IpcMessageHeader))
#define IPC_SOCKET_BACKLOG      8
#define IPC_CONNECT_TIMEOUT_MS  5000            /* 连接超时5秒 */
#define IPC_ACK_TIMEOUT_MS      3000            /* ACK超时3秒 */
#define IPC_HEARTBEAT_INTERVAL_MS 10000         /* 心跳间隔10秒 */
#define IPC_HEARTBEAT_TIMEOUT_MS  30000         /* 心跳超时30秒 */
#define IPC_MAX_RECONNECT_ATTEMPTS 10           /* 最大重连次数 */
#define IPC_RECONNECT_INTERVAL_MS  1000         /* 重连间隔1秒 */
#define IPC_WINDOW_SIZE         16              /* 滑动窗口大小 */

/* ==================== 消息类型 ==================== */

typedef enum {
    MSG_TYPE_DATA = 0x01,       /* 普通数据消息 */
    MSG_TYPE_ACK  = 0x02,       /* 确认消息 */
    MSG_TYPE_HEARTBEAT = 0x03,  /* 心跳消息 */
    MSG_TYPE_HEARTBEAT_ACK = 0x04, /* 心跳确认 */
    MSG_TYPE_CONNECT_REQ = 0x05,   /* 连接请求 */
    MSG_TYPE_CONNECT_RESP = 0x06,  /* 连接响应 */
    MSG_TYPE_DISCONNECT = 0x07,    /* 断开连接 */
} IpcMessageType;

/* ==================== 错误码 ==================== */

typedef enum {
    IPC_OK = 0,
    IPC_ERR_INVALID_PARAM = -1,
    IPC_ERR_NO_MEMORY = -2,
    IPC_ERR_SOCKET_FAILED = -3,
    IPC_ERR_CONNECT_FAILED = -4,
    IPC_ERR_SEND_FAILED = -5,
    IPC_ERR_RECV_FAILED = -6,
    IPC_ERR_TIMEOUT = -7,
    IPC_ERR_CHECKSUM = -8,
    IPC_ERR_PEER_CLOSED = -9,
    IPC_ERR_MSG_TOO_LARGE = -10,
    IPC_ERR_NOT_CONNECTED = -11,
    IPC_ERR_ACK_FAILED = -12,
} IpcErrorCode;

/* ==================== 协议头定义 ==================== */

/* 消息头 - 固定16字节 */
typedef struct __attribute__((packed)) {
    uint32_t magic;             /* 魔术字: 0x49504352 ('IPCR') */
    uint8_t  version;           /* 协议版本: 1 */
    uint8_t  type;              /* 消息类型: IpcMessageType */
    uint16_t flags;             /* 标志位 */
    uint32_t seq;               /* 序列号 */
    uint32_t ack;               /* 确认号 */
    uint32_t payloadLen;        /*  payload长度 */
    uint32_t checksum;          /* CRC32校验和 */
} IpcMessageHeader;

#define IPC_MAGIC       0x49504352  /* 'IPCR' */
#define IPC_VERSION     1

/* 标志位定义 */
#define IPC_FLAG_NEED_ACK   0x0001  /* 需要ACK确认 */
#define IPC_FLAG_IS_ACK     0x0002  /* 这是一个ACK */
#define IPC_FLAG_FRAGMENT   0x0004  /* 分片消息 */
#define IPC_FLAG_FIRST_FRAG 0x0008  /* 第一个分片 */
#define IPC_FLAG_LAST_FRAG  0x0010  /* 最后一个分片 */

/* ==================== 回调函数类型 ==================== */

/* 消息接收回调 */
typedef void (*IpcOnMessageCallback)(uint32_t seq, const uint8_t *data, 
                                      uint32_t len, void *userData);
/* 连接状态变化回调 */
typedef void (*IpcOnConnectCallback)(bool connected, void *userData);
/* 错误回调 */
typedef void (*IpcOnErrorCallback)(int errorCode, const char *errorMsg, void *userData);
/* 日志回调 */
typedef void (*IpcLogCallback)(int level, const char *tag, const char *msg);

/* ==================== 上下文结构体 ==================== */

typedef struct IpcContext IpcContext;

/* ==================== 初始化配置 ==================== */

typedef struct {
    const char *socketPath;         /* Socket路径，NULL使用默认 */
    bool isServer;                  /* 是否为服务端 */
    
    /* 超时配置 */
    uint32_t connectTimeoutMs;      /* 连接超时 */
    uint32_t ackTimeoutMs;          /* ACK超时 */
    uint32_t heartbeatIntervalMs;   /* 心跳间隔 */
    uint32_t heartbeatTimeoutMs;    /* 心跳超时 */
    
    /* 重连配置 */
    bool enableReconnect;           /* 启用自动重连 */
    uint32_t maxReconnectAttempts;  /* 最大重连次数 */
    uint32_t reconnectIntervalMs;   /* 重连间隔 */
    
    /* 回调函数 */
    IpcOnMessageCallback onMessage; /* 收到消息回调 */
    IpcOnConnectCallback onConnect; /* 连接状态回调 */
    IpcOnErrorCallback onError;     /* 错误回调 */
    void *userData;                 /* 用户数据 */
} IpcConfig;

/* ==================== API接口 ==================== */

/**
 * @brief 初始化IPC上下文（服务端或客户端）
 * @param config 配置参数
 * @return 成功返回上下文指针，失败返回NULL
 */
IpcContext* IpcInit(const IpcConfig *config);

/**
 * @brief 销毁IPC上下文，释放资源
 * @param ctx IPC上下文
 */
void IpcDestroy(IpcContext *ctx);

/**
 * @brief 服务端开始监听（仅服务端需要调用）
 * @param ctx IPC上下文
 * @return 成功返回IPC_OK
 */
int IpcListen(IpcContext *ctx);

/**
 * @brief 客户端连接到服务端（仅客户端需要调用）
 * @param ctx IPC上下文
 * @return 成功返回IPC_OK
 */
int IpcConnect(IpcContext *ctx);

/**
 * @brief 发送二进制数据（带确认机制）
 * @param ctx IPC上下文
 * @param data 数据指针
 * @param len 数据长度
 * @param needAck 是否需要确认
 * @param seq 输出序列号
 * @return 成功返回IPC_OK
 */
int IpcSendData(IpcContext *ctx, const uint8_t *data, uint32_t len, 
                bool needAck, uint32_t *seq);

/**
 * @brief 发送ACK确认
 * @param ctx IPC上下文
 * @param seq 要确认的序列号
 * @return 成功返回IPC_OK
 */
int IpcSendAck(IpcContext *ctx, uint32_t seq);

/**
 * @brief 检查连接状态
 * @param ctx IPC上下文
 * @return 已连接返回true
 */
bool IpcIsConnected(const IpcContext *ctx);

/**
 * @brief 断开连接
 * @param ctx IPC上下文
 */
void IpcDisconnect(IpcContext *ctx);

/**
 * @brief 手动触发重连（通常在连接断开后调用）
 * @param ctx IPC上下文
 * @return 成功返回IPC_OK
 */
int IpcReconnect(IpcContext *ctx);

/**
 * @brief 设置全局日志回调
 * @param callback 日志回调函数
 */
void IpcSetLogCallback(IpcLogCallback callback);

/**
 * @brief 获取错误描述
 * @param errorCode 错误码
 * @return 错误描述字符串
 */
const char* IpcGetErrorString(int errorCode);

/* ==================== 服务端专用API ==================== */

/**
 * @brief 服务端接受新连接（阻塞模式）
 * @param ctx IPC上下文
 * @return 成功返回IPC_OK
 */
int IpcAccept(IpcContext *ctx);

/**
 * @brief 服务端非阻塞检查新连接
 * @param ctx IPC上下文
 * @param timeoutMs 超时时间（毫秒）
 * @return 有新连接返回true
 */
bool IpcPollAccept(IpcContext *ctx, uint32_t timeoutMs);

/* ==================== 客户端专用API ==================== */

/**
 * @brief 等待连接建立（阻塞模式）
 * @param ctx IPC上下文
 * @param timeoutMs 超时时间
 * @return 成功返回IPC_OK
 */
int IpcWaitConnected(IpcContext *ctx, uint32_t timeoutMs);

#ifdef __cplusplus
}
#endif

#endif /* IPC_RELIABLE_H */
