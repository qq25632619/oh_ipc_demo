/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * OpenHarmony Binder IPC 服务端示例
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
static void OnMessage(uint32_t type, uint32_t cmdCode,
                      const uint8_t *data, uint32_t len, void *userData)
{
    (void)userData;
    
    printf("\n[SERVICE] Received message:\n");
    printf("  Type: %u (%s)\n", type,
           type == 1 ? "DATA" : type == 3 ? "HEARTBEAT" : "OTHER");
    printf("  Command: 0x%08X\n", cmdCode);
    printf("  Length: %u\n", len);
    
    if (len > 0 && data) {
        PrintHex("  Data: ", data, len);
        
        /* 尝试解析为文本 */
        if (len < 256) {
            int printable = 1;
            for (uint32_t i = 0; i < len; i++) {
                if (data[i] < 32 && data[i] != '\n' && data[i] != '\r' && data[i] != '\t') {
                    printable = 0;
                    break;
                }
            }
            if (printable) {
                printf("  Text: \"%.*s\"\n", (int)len, (const char *)data);
            }
        }
    }
    
    /* 根据命令码处理 */
    switch (cmdCode) {
        case 0x00010001:
            printf("  -> Status query\n");
            break;
        case 0x00020001:
            printf("  -> Data report\n");
            break;
        case 0x00030001:
            printf("  -> Control command\n");
            break;
        default:
            printf("  -> Custom command\n");
            break;
    }
}

/* 连接状态回调 */
static void OnConnect(bool connected, void *userData)
{
    (void)userData;
    if (connected) {
        printf("\n[SERVICE] Client connected\n");
    } else {
        printf("\n[SERVICE] Client disconnected\n");
    }
}

/* 死亡回调 */
static void OnDeath(void *userData)
{
    (void)userData;
    printf("\n[SERVICE] Remote client died\n");
}

/* 错误回调 */
static void OnError(int errorCode, const char *errorMsg, void *userData)
{
    (void)userData;
    fprintf(stderr, "[SERVICE] Error %d: %s\n", errorCode, errorMsg);
}

int main(int argc, char *argv[])
{
    const char *serviceName = OHIPC_DEFAULT_SERVICE_NAME;
    
    if (argc > 1) {
        serviceName = argv[1];
    }
    
    printf("========================================\n");
    printf("OpenHarmony Binder IPC Service Demo\n");
    printf("========================================\n");
    printf("Service Name: %s\n", serviceName);
    printf("Binder Available: %s\n\n", OhIpcIsBinderAvailable() ? "YES" : "NO");
    
    if (!OhIpcIsBinderAvailable()) {
        fprintf(stderr, "Error: Binder driver not available\n");
        fprintf(stderr, "This demo requires OpenHarmony Binder IPC environment\n");
        return 1;
    }
    
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    
    /* 配置 */
    OhIpcConfig config = {
        .serviceName = serviceName,
        .isServer = true,
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
    
    /* 启动服务 */
    printf("Starting service...\n");
    int ret = OhIpcServerStart(ctx);
    if (ret != OHIPC_OK) {
        fprintf(stderr, "Failed to start service: %s\n", OhIpcGetErrorString(ret));
        OhIpcDestroy(ctx);
        return 1;
    }
    
    printf("\nService started successfully!\n");
    printf("Waiting for clients...\n");
    printf("Press Ctrl+C to stop\n\n");
    
    /* 主循环 */
    while (g_running) {
        /* 服务端可以在这里处理其他任务 */
        /* 或主动向已连接的客户端发送消息 */
        
        if (OhIpcIsConnected(ctx)) {
            static int count = 0;
            count++;
            
            /* 每30秒发送一次主动消息 */
            if (count % 30 == 0) {
                uint8_t msg[16];
                msg[0] = 0x00; msg[1] = 0x05;  /* 主动推送命令 */
                msg[2] = 0x00; msg[3] = 0x01;
                msg[4] = (count >> 24) & 0xff;
                msg[5] = (count >> 16) & 0xff;
                msg[6] = (count >> 8) & 0xff;
                msg[7] = count & 0xff;
                memcpy(msg + 8, "PUSH", 4);
                
                /* 发送到客户端（简化版本可能不支持） */
                /* OhIpcServerSendToClient(ctx, 0x00050001, msg, 12); */
            }
        }
        
        sleep(1);
    }
    
    printf("\nStopping service...\n");
    OhIpcServerStop(ctx);
    OhIpcDestroy(ctx);
    
    printf("Service exited\n");
    return 0;
}
