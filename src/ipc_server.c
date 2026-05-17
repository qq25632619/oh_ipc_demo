/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * 可靠IPC服务端示例程序
 */

#include "ipc_reliable.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static volatile int g_running = 1;

static void SignalHandler(int sig)
{
    (void)sig;
    g_running = 0;
}

/* 打印二进制数据为十六进制 */
static void PrintHex(const char *prefix, const uint8_t *data, uint32_t len)
{
    printf("%s", prefix);
    for (uint32_t i = 0; i < len && i < 32; i++) {
        printf("%02X ", data[i]);
    }
    if (len > 32) {
        printf("... (%u bytes total)", len);
    }
    printf("\n");
}

/* 收到消息回调 */
static void OnMessage(uint32_t seq, const uint8_t *data, 
                      uint32_t len, void *userData)
{
    (void)userData;
    
    printf("\n[SERVER] Received message seq=%u, len=%u\n", seq, len);
    PrintHex("  Data: ", data, len);
    
    /* 解析消息（示例协议：前4字节是命令，后面是payload） */
    if (len >= 4) {
        uint32_t cmd = ((uint32_t)data[0] << 24) |
                       ((uint32_t)data[1] << 16) |
                       ((uint32_t)data[2] << 8) |
                       data[3];
        printf("  Command: 0x%08X\n", cmd);
        
        /* 根据命令处理 */
        switch (cmd) {
            case 0x00010001:  /* 心跳或状态查询 */
                printf("  -> Status request\n");
                break;
            case 0x00020001:  /* 数据上报 */
                printf("  -> Data report\n");
                if (len > 4) {
                    printf("  Payload: %.*s\n", (int)(len - 4), data + 4);
                }
                break;
            default:
                printf("  -> Unknown command\n");
                break;
        }
    }
    
    /* 发送ACK携带处理结果 */
    printf("[SERVER] ACK sent for seq=%u\n", seq);
}

/* 连接状态变化回调 */
static void OnConnect(bool connected, void *userData)
{
    (void)userData;
    if (connected) {
        printf("\n[SERVER] Client connected\n");
    } else {
        printf("\n[SERVER] Client disconnected\n");
    }
}

/* 错误回调 */
static void OnError(int errorCode, const char *errorMsg, void *userData)
{
    (void)userData;
    fprintf(stderr, "[SERVER] Error %d: %s\n", errorCode, errorMsg);
}

int main(int argc, char *argv[])
{
    const char *socketPath = IPC_DEFAULT_SOCKET_PATH;
    
    if (argc > 1) {
        socketPath = argv[1];
    }
    
    printf("IPC Reliable Server Demo\n");
    printf("Socket path: %s\n\n", socketPath);
    
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    
    /* 配置 */
    IpcConfig config = {
        .socketPath = socketPath,
        .isServer = true,
        .connectTimeoutMs = 5000,
        .ackTimeoutMs = 3000,
        .heartbeatIntervalMs = 10000,
        .heartbeatTimeoutMs = 30000,
        .enableReconnect = false,  /* 服务端不需要重连 */
        .maxReconnectAttempts = 0,
        .reconnectIntervalMs = 0,
        .onMessage = OnMessage,
        .onConnect = OnConnect,
        .onError = OnError,
        .userData = NULL
    };
    
    /* 初始化 */
    IpcContext *ctx = IpcInit(&config);
    if (!ctx) {
        fprintf(stderr, "Failed to initialize IPC\n");
        return 1;
    }
    
    /* 开始监听 */
    if (IpcListen(ctx) != IPC_OK) {
        fprintf(stderr, "Failed to listen\n");
        IpcDestroy(ctx);
        return 1;
    }
    
    printf("Waiting for client connection...\n");
    
    /* 主循环：接受连接和处理 */
    while (g_running) {
        /* 非阻塞检查新连接 */
        if (IpcPollAccept(ctx, 1000)) {
            printf("New connection incoming...\n");
            
            if (IpcAccept(ctx) != IPC_OK) {
                fprintf(stderr, "Failed to accept connection\n");
                continue;
            }
            
            printf("Client accepted. Waiting for messages...\n");
            printf("Press Ctrl+C to exit\n\n");
        }
        
        /* 如果已连接，可以主动向客户端发送消息 */
        if (IpcIsConnected(ctx)) {
            static int msgCount = 0;
            msgCount++;
            
            /* 每10秒发送一次主动消息 */
            if (msgCount % 10 == 0) {
                /* 构建二进制消息 */
                uint8_t msg[16];
                msg[0] = 0x00; msg[1] = 0x03;  /* Command: 0x0003 */
                msg[2] = 0x00; msg[3] = 0x01;
                msg[4] = (msgCount >> 24) & 0xff;  /* 序列号 */
                msg[5] = (msgCount >> 16) & 0xff;
                msg[6] = (msgCount >> 8) & 0xff;
                msg[7] = msgCount & 0xff;
                msg[8] = 0x48; msg[9] = 0x65;  /* "Hello" */
                msg[10] = 0x6C; msg[11] = 0x6C;
                msg[12] = 0x6F; msg[13] = 0x00;
                
                uint32_t seq = 0;
                int ret = IpcSendData(ctx, msg, 14, true, &seq);
                if (ret == IPC_OK) {
                    printf("[SERVER] Sent proactive message seq=%u\n", seq);
                } else {
                    printf("[SERVER] Failed to send: %s\n", IpcGetErrorString(ret));
                }
            }
        }
    }
    
    printf("\nShutting down...\n");
    IpcDestroy(ctx);
    printf("Server exited\n");
    
    return 0;
}
