/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * 可靠IPC客户端示例程序
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
    
    printf("\n[CLIENT] Received message seq=%u, len=%u\n", seq, len);
    PrintHex("  Data: ", data, len);
}

/* 连接状态变化回调 */
static void OnConnect(bool connected, void *userData)
{
    (void)userData;
    if (connected) {
        printf("\n[CLIENT] Connected to server\n");
    } else {
        printf("\n[CLIENT] Disconnected from server\n");
        printf("[CLIENT] Auto-reconnect will be attempted...\n");
    }
}

/* 错误回调 */
static void OnError(int errorCode, const char *errorMsg, void *userData)
{
    (void)userData;
    fprintf(stderr, "[CLIENT] Error %d: %s\n", errorCode, errorMsg);
}

/* 发送二进制消息 */
static int SendBinaryMessage(IpcContext *ctx, uint32_t cmd, 
                              const uint8_t *payload, uint32_t payloadLen)
{
    uint8_t msg[IPC_MAX_MESSAGE_SIZE];
    
    if (payloadLen + 4 > IPC_MAX_PAYLOAD_SIZE) {
        fprintf(stderr, "Message too large\n");
        return -1;
    }
    
    /* 命令（4字节大端） */
    msg[0] = (cmd >> 24) & 0xff;
    msg[1] = (cmd >> 16) & 0xff;
    msg[2] = (cmd >> 8) & 0xff;
    msg[3] = cmd & 0xff;
    
    /* Payload */
    if (payload && payloadLen > 0) {
        memcpy(msg + 4, payload, payloadLen);
    }
    
    uint32_t seq = 0;
    int ret = IpcSendData(ctx, msg, 4 + payloadLen, true, &seq);
    
    if (ret == IPC_OK) {
        printf("[CLIENT] Message sent seq=%u, cmd=0x%08X, len=%u\n", 
               seq, cmd, 4 + payloadLen);
    } else {
        printf("[CLIENT] Failed to send: %s\n", IpcGetErrorString(ret));
    }
    
    return ret;
}

int main(int argc, char *argv[])
{
    const char *socketPath = IPC_DEFAULT_SOCKET_PATH;
    
    if (argc > 1) {
        socketPath = argv[1];
    }
    
    printf("IPC Reliable Client Demo\n");
    printf("Socket path: %s\n\n", socketPath);
    
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    
    /* 配置 - 启用自动重连 */
    IpcConfig config = {
        .socketPath = socketPath,
        .isServer = false,
        .connectTimeoutMs = 5000,
        .ackTimeoutMs = 3000,
        .heartbeatIntervalMs = 10000,
        .heartbeatTimeoutMs = 30000,
        .enableReconnect = true,         /* 启用自动重连 */
        .maxReconnectAttempts = 10,      /* 最大重连10次 */
        .reconnectIntervalMs = 1000,     /* 每次间隔1秒 */
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
    
    /* 连接到服务端 */
    printf("Connecting to server...\n");
    if (IpcConnect(ctx) != IPC_OK) {
        fprintf(stderr, "Initial connection failed, will retry...\n");
        /* 自动重连会在后台进行 */
    }
    
    printf("Commands:\n");
    printf("  1 - Send heartbeat/status request\n");
    printf("  2 - Send data report\n");
    printf("  3 - Send custom binary data\n");
    printf("  s - Show connection status\n");
    printf("  d - Disconnect manually\n");
    printf("  r - Reconnect manually\n");
    printf("  q - Quit\n\n");
    
    int msgCount = 0;
    
    /* 主循环 */
    while (g_running) {
        printf("> ");
        fflush(stdout);
        
        char cmd[16];
        if (fgets(cmd, sizeof(cmd), stdin) == NULL) {
            break;
        }
        
        switch (cmd[0]) {
            case '1': {  /* 发送心跳/状态查询 */
                if (!IpcIsConnected(ctx)) {
                    printf("Not connected to server\n");
                    break;
                }
                uint8_t payload[8] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
                SendBinaryMessage(ctx, 0x00010001, payload, sizeof(payload));
                break;
            }
            
            case '2': {  /* 发送数据上报 */
                if (!IpcIsConnected(ctx)) {
                    printf("Not connected to server\n");
                    break;
                }
                msgCount++;
                char text[64];
                snprintf(text, sizeof(text), "Hello from client #%d", msgCount);
                SendBinaryMessage(ctx, 0x00020001, (uint8_t *)text, strlen(text));
                break;
            }
            
            case '3': {  /* 发送自定义二进制数据 */
                if (!IpcIsConnected(ctx)) {
                    printf("Not connected to server\n");
                    break;
                }
                /* 示例：发送一些二进制数据 */
                uint8_t binary[32];
                for (int i = 0; i < 32; i++) {
                    binary[i] = (uint8_t)(i * 7 + msgCount);
                }
                SendBinaryMessage(ctx, 0xDEADBEEF, binary, sizeof(binary));
                break;
            }
            
            case 's':  /* 显示状态 */
                printf("Connection status: %s\n", 
                       IpcIsConnected(ctx) ? "CONNECTED" : "DISCONNECTED");
                break;
                
            case 'd':  /* 手动断开 */
                printf("Disconnecting...\n");
                IpcDisconnect(ctx);
                break;
                
            case 'r':  /* 手动重连 */
                printf("Reconnecting...\n");
                IpcReconnect(ctx);
                break;
                
            case 'q':  /* 退出 */
                g_running = 0;
                break;
                
            case '\n':
            case '\r':
                break;
                
            default:
                printf("Unknown command: %c\n", cmd[0]);
                break;
        }
    }
    
    printf("\nShutting down...\n");
    IpcDestroy(ctx);
    printf("Client exited\n");
    
    return 0;
}
