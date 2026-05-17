/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * OpenHarmony Binder IPC 客户端示例
 */

#include "oh_ipc_binder.h"

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

/* 打印二进制数据 */
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
static void OnMessage(uint32_t type, uint32_t cmdCode,
                      const uint8_t *data, uint32_t len, void *userData)
{
    (void)userData;
    
    printf("\n[CLIENT] Received message:\n");
    printf("  Type: %u\n", type);
    printf("  Command: 0x%08X\n", cmdCode);
    printf("  Length: %u\n", len);
    
    if (len > 0 && data) {
        PrintHex("  Data: ", data, len);
    }
}

/* 连接状态回调 */
static void OnConnect(bool connected, void *userData)
{
    (void)userData;
    if (connected) {
        printf("\n[CLIENT] Connected to service\n");
    } else {
        printf("\n[CLIENT] Disconnected from service\n");
        printf("[CLIENT] Auto-reconnect will be attempted if enabled\n");
    }
}

/* 死亡回调 */
static void OnDeath(void *userData)
{
    (void)userData;
    printf("\n[CLIENT] Service died\n");
}

/* 错误回调 */
static void OnError(int errorCode, const char *errorMsg, void *userData)
{
    (void)userData;
    fprintf(stderr, "[CLIENT] Error %d: %s\n", errorCode, errorMsg);
}

/* 发送二进制消息 */
static int SendBinaryMessage(OhIpcContext *ctx, uint32_t cmdCode,
                              const uint8_t *data, uint32_t len)
{
    printf("[CLIENT] Sending command 0x%08X, %u bytes...\n", cmdCode, len);
    
    int ret = OhIpcClientSend(ctx, cmdCode, data, len, true);
    if (ret == OHIPC_OK) {
        printf("[CLIENT] Send successful\n");
    } else {
        printf("[CLIENT] Send failed: %s\n", OhIpcGetErrorString(ret));
    }
    
    return ret;
}

int main(int argc, char *argv[])
{
    const char *serviceName = OHIPC_DEFAULT_SERVICE_NAME;
    
    if (argc > 1) {
        serviceName = argv[1];
    }
    
    printf("========================================\n");
    printf("OpenHarmony Binder IPC Client Demo\n");
    printf("========================================\n");
    printf("Service Name: %s\n", serviceName);
    printf("Binder Available: %s\n\n", OhIpcIsBinderAvailable() ? "YES" : "NO");
    
    if (!OhIpcIsBinderAvailable()) {
        fprintf(stderr, "Error: Binder driver not available\n");
        return 1;
    }
    
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    
    /* 配置 - 启用自动重连 */
    OhIpcConfig config = {
        .serviceName = serviceName,
        .isServer = false,
        .enableReconnect = true,
        .maxReconnectAttempts = 10,
        .reconnectIntervalMs = 1000,
        .onMessage = OnMessage,
        .onConnect = OnConnect,
        .onDeath = OnDeath,
        .onError = OnError,
        .userData = NULL
    };
    
    /* 初始化 */
    OhIpcContext *ctx = OhIpcInit(&config);
    if (!ctx) {
        fprintf(stderr, "Failed to initialize IPC\n");
        return 1;
    }
    
    /* 连接到服务 */
    printf("Connecting to service...\n");
    int ret = OhIpcClientConnect(ctx, 5000);
    if (ret != OHIPC_OK) {
        printf("Initial connection failed: %s\n", OhIpcGetErrorString(ret));
        printf("Auto-reconnect is enabled, will retry automatically\n\n");
    }
    
    /* 交互式命令 */
    printf("\nCommands:\n");
    printf("  1 - Send status query (cmd=0x00010001)\n");
    printf("  2 - Send data report (cmd=0x00020001)\n");
    printf("  3 - Send control command (cmd=0x00030001)\n");
    printf("  4 - Send custom binary data\n");
    printf("  5 - Send large data (stress test)\n");
    printf("  s - Show connection status\n");
    printf("  d - Disconnect manually\n");
    printf("  r - Reconnect manually\n");
    printf("  q - Quit\n\n");
    
    int msgCount = 0;
    
    while (g_running) {
        printf("> ");
        fflush(stdout);
        
        char cmd[16];
        if (fgets(cmd, sizeof(cmd), stdin) == NULL) {
            break;
        }
        
        /* 处理命令 */
        switch (cmd[0]) {
            case '1': {  /* 状态查询 */
                if (!OhIpcIsConnected(ctx)) {
                    printf("Not connected. Use 'r' to reconnect.\n");
                    break;
                }
                uint8_t data[] = {0x01, 0x00, 0x00, 0x00};  /* 查询状态 */
                SendBinaryMessage(ctx, 0x00010001, data, sizeof(data));
                break;
            }
            
            case '2': {  /* 数据上报 */
                if (!OhIpcIsConnected(ctx)) {
                    printf("Not connected. Use 'r' to reconnect.\n");
                    break;
                }
                msgCount++;
                char text[128];
                snprintf(text, sizeof(text), 
                         "Report #%d from client at %lu ms",
                         msgCount, (unsigned long)OhIpcGetTimestampMs());
                SendBinaryMessage(ctx, 0x00020001, (uint8_t *)text, strlen(text));
                break;
            }
            
            case '3': {  /* 控制命令 */
                if (!OhIpcIsConnected(ctx)) {
                    printf("Not connected. Use 'r' to reconnect.\n");
                    break;
                }
                uint8_t data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
                SendBinaryMessage(ctx, 0x00030001, data, sizeof(data));
                break;
            }
            
            case '4': {  /* 自定义二进制数据 */
                if (!OhIpcIsConnected(ctx)) {
                    printf("Not connected. Use 'r' to reconnect.\n");
                    break;
                }
                /* 生成一些二进制数据 */
                uint8_t data[32];
                for (int i = 0; i < 32; i++) {
                    data[i] = (uint8_t)(i * 7 + msgCount);
                }
                SendBinaryMessage(ctx, 0xDEADBEEF, data, sizeof(data));
                break;
            }
            
            case '5': {  /* 大数据压力测试 */
                if (!OhIpcIsConnected(ctx)) {
                    printf("Not connected. Use 'r' to reconnect.\n");
                    break;
                }
                /* 分配并发送 100KB 数据 */
                uint8_t *largeData = malloc(100 * 1024);
                if (largeData) {
                    for (int i = 0; i < 100 * 1024; i++) {
                        largeData[i] = (uint8_t)(i & 0xFF);
                    }
                    printf("[CLIENT] Sending 100KB data...\n");
                    SendBinaryMessage(ctx, 0x00040001, largeData, 100 * 1024);
                    free(largeData);
                }
                break;
            }
            
            case 's':  /* 显示状态 */
                printf("Connection status: %s\n",
                       OhIpcIsConnected(ctx) ? "CONNECTED" : "DISCONNECTED");
                break;
                
            case 'd':  /* 断开连接 */
                printf("Disconnecting...\n");
                OhIpcClientDisconnect(ctx);
                break;
                
            case 'r':  /* 手动重连 */
                printf("Reconnecting...\n");
                ret = OhIpcClientReconnect(ctx);
                if (ret == OHIPC_OK) {
                    printf("Reconnected successfully\n");
                } else {
                    printf("Reconnect failed: %s\n", OhIpcGetErrorString(ret));
                }
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
    
    printf("\nDisconnecting...\n");
    OhIpcClientDisconnect(ctx);
    OhIpcDestroy(ctx);
    
    printf("Client exited\n");
    return 0;
}
