/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 */

#include "ipc_demo_socket.h"

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static volatile sig_atomic_t g_running = 1;

static void StopServer(int signo)
{
    (void)signo;
    g_running = 0;
}

int main(void)
{
    signal(SIGINT, StopServer);
    signal(SIGTERM, StopServer);

    const char *socketPath = IpcDemoGetSocketPath();
    int serverFd = IpcDemoCreateServerSocket(socketPath);
    if (serverFd < 0) {
        return 1;
    }

    printf("ipcb listening on %s\n", socketPath);
    while (g_running) {
        int clientFd = accept(serverFd, NULL, NULL);
        if (clientFd < 0) {
            if (g_running) {
                fprintf(stderr, "accept failed\n");
            }
            continue;
        }

        char request[IPC_DEMO_BUFFER_SIZE];
        if (IpcDemoReceiveString(clientFd, request, sizeof(request)) >= 0) {
            char reply[IPC_DEMO_BUFFER_SIZE];
            printf("ipcb received: %s\n", request);
            if (snprintf(reply, sizeof(reply), "ipcb ack: %s", request) >= (int)sizeof(reply)) {
                reply[sizeof(reply) - 1] = '\0';
            }
            IpcDemoSendString(clientFd, reply);
        }
        IpcDemoCloseFd(clientFd);
    }

    IpcDemoCloseFd(serverFd);
    unlink(socketPath);
    return 0;
}
