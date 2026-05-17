/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * OpenHarmony Binder IPC 纯C实现 - 完整版
 * 
 * 方案说明：
 * 1. 使用官方 IPC C API (libipc_capi.so) 进行 Binder 通信
 * 2. 使用 HDF Service 机制进行服务注册/发现（纯C）
 * 3. 支持双向通信、自动重连、心跳保活
 */

#ifndef OH_IPC_BINDER_H
#define OH_IPC_BINDER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ==================== 配置常量 ==================== */

#define OHIPC_DEFAULT_SERVICE_NAME "ohos.ipc.binary_service"
#define OHIPC_MAX_MESSAGE_SIZE     (200 * 1024)  /* Binder限制约200KB */
#define OHIPC_MAX_PAYLOAD_SIZE     (OHIPC_MAX_MESSAGE_SIZE - 128) /* 预留头部 */

/* ==================== 错误码 ==================== */

typedef enum {
    OHIPC_OK = 0,
    OHIPC_ERR_INVALID_PARAM = -1,
    OHIPC_ERR_NO_MEMORY = -2,
    OHIPC_ERR_BINDER_FAILED = -3,
    OHIPC_ERR_SERVICE_EXISTS = -4,
    OHIPC_ERR_SERVICE_NOT_FOUND = -5,
    OHIPC_ERR_SEND_FAILED = -6,
    OHIPC_ERR_RECV_FAILED = -7,
    OHIPC_ERR_TIMEOUT = -8,
    OHIPC_ERR_REMOTE_DEAD = -9,
    OHIPC_ERR_MSG_TOO_LARGE = -10,
    OHIPC_ERR_NOT_CONNECTED = -11,
    OHIPC_ERR_HDF_FAILED = -12,
} OhIpcErrorCode;

/* ==================== 消息类型 ==================== */

typedef enum {
    OHIPC_MSG_DATA = 1,         /* 普通数据消息 */
    OHIPC_MSG_ACK = 2,          /* 确认消息（可选） */
    OHIPC_MSG_HEARTBEAT = 3,    /* 心跳消息 */
    OHIPC_MSG_HEARTBEAT_ACK = 4, /* 心跳响应 */
} OhIpcMessageType;

/* ==================== 回调函数类型 ==================== */

/**
 * @brief 收到消息回调
 * @param type 消息类型
 * @param cmdCode 命令码（用户自定义）
 * @param data 二进制数据（可能为NULL）
 * @param len 数据长度
 * @param userData 用户数据
 */
typedef void (*OhIpcOnMessageCallback)(uint32_t type, uint32_t cmdCode,
                                        const uint8_t *data, uint32_t len,
                                        void *userData);

/**
 * @brief 连接状态变化回调
 * @param connected 是否已连接
 * @param userData 用户数据
 */
typedef void (*OhIpcOnConnectCallback)(bool connected, void *userData);

/**
 * @brief 远程对象死亡回调
 * @param userData 用户数据
 */
typedef void (*OhIpcOnDeathCallback)(void *userData);

/**
 * @brief 错误回调
 * @param errorCode 错误码
 * @param errorMsg 错误描述
 * @param userData 用户数据
 */
typedef void (*OhIpcOnErrorCallback)(int errorCode, const char *errorMsg, void *userData);

/* ==================== 配置结构体 ==================== */

typedef struct {
    const char *serviceName;    /* 服务名称（必需） */
    bool isServer;              /* 是否为服务端 */
    
    /* 超时配置（毫秒） */
    uint32_t connectTimeoutMs;    /* 连接超时（默认5000） */
    uint32_t ackTimeoutMs;      /* ACK超时（默认3000） */
    uint32_t heartbeatIntervalMs; /* 心跳间隔（默认10000） */
    uint32_t heartbeatTimeoutMs;  /* 心跳超时（默认30000） */
    
    /* 重连配置 */
    bool enableReconnect;       /* 启用自动重连（仅客户端） */
    uint32_t maxReconnectAttempts; /* 最大重连次数（默认10） */
    uint32_t reconnectIntervalMs;  /* 重连间隔（默认1000） */
    
    /* 回调函数 */
    OhIpcOnMessageCallback onMessage;   /* 收到消息回调（必需） */
    OhIpcOnConnectCallback onConnect; /* 连接状态回调（可选） */
    OhIpcOnDeathCallback onDeath;       /* 远程死亡回调（可选） */
    OhIpcOnErrorCallback onError;     /* 错误回调（可选） */
    void *userData;                     /* 用户数据 */
} OhIpcConfig;

/* ==================== 不透明上下文 ==================== */

typedef struct OhIpcContext OhIpcContext;

/* ==================== 核心API ==================== */

/**
 * @brief 初始化 Binder IPC 上下文
 * @param config 配置参数（必需：serviceName, onMessage）
 * @return 成功返回上下文指针，失败返回NULL
 */
OhIpcContext* OhIpcInit(const OhIpcConfig *config);

/**
 * @brief 销毁 Binder IPC 上下文
 * @param ctx IPC上下文
 */
void OhIpcDestroy(OhIpcContext *ctx);

/* ==================== 服务端API ==================== */

/**
 * @brief 启动服务端（注册到HDF并监听）
 * @param ctx IPC上下文
 * @return 成功返回OHIPC_OK
 * @note 会阻塞直到服务注册成功或超时
 */
int OhIpcServerStart(OhIpcContext *ctx);

/**
 * @brief 停止服务端
 * @param ctx IPC上下文
 */
void OhIpcServerStop(OhIpcContext *ctx);

/**
 * @brief 向客户端发送消息（服务端主动发送）
 * @param ctx IPC上下文
 * @param cmdCode 命令码
 * @param data 二进制数据
 * @param len 数据长度
 * @return 成功返回OHIPC_OK
 * @note 需要客户端先连接并建立双向通道
 */
int OhIpcServerSendToClient(OhIpcContext *ctx, uint32_t cmdCode,
                            const uint8_t *data, uint32_t len);

/* ==================== 客户端API ==================== */

/**
 * @brief 连接到服务端
 * @param ctx IPC上下文
 * @param timeoutMs 超时时间（毫秒）
 * @return 成功返回OHIPC_OK
 */
int OhIpcClientConnect(OhIpcContext *ctx, uint32_t timeoutMs);

/**
 * @brief 断开连接
 * @param ctx IPC上下文
 */
void OhIpcClientDisconnect(OhIpcContext *ctx);

/**
 * @brief 检查连接状态
 * @param ctx IPC上下文
 * @return 已连接返回true
 */
bool OhIpcIsConnected(const OhIpcContext *ctx);

/**
 * @brief 手动触发重连
 * @param ctx IPC上下文
 * @return 成功返回OHIPC_OK
 */
int OhIpcClientReconnect(OhIpcContext *ctx);

/**
 * @brief 向服务端发送消息
 * @param ctx IPC上下文
 * @param cmdCode 命令码
 * @param data 二进制数据
 * @param len 数据长度
 * @param needAck 是否需要确认
 * @return 成功返回OHIPC_OK
 */
int OhIpcClientSend(OhIpcContext *ctx, uint32_t cmdCode,
                     const uint8_t *data, uint32_t len, bool needAck);

/* ==================== 通用API ==================== */

/**
 * @brief 获取错误描述字符串
 * @param errorCode 错误码
 * @return 错误描述
 */
const char* OhIpcGetErrorString(int errorCode);

/**
 * @brief 检查 Binder 驱动是否可用
 * @return 可用返回true
 */
bool OhIpcIsBinderAvailable(void);

/**
 * @brief 获取当前时间戳（毫秒）
 * @return 时间戳
 */
uint64_t OhIpcGetTimestampMs(void);

/* 接口描述符前缀（用于创建 Stub） */
#define OHIPC_DESCRIPTOR_PREFIX "ohos.ipc.binder."

#ifdef __cplusplus
}
#endif

#endif /* OH_IPC_BINDER_H */
