/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 */

#include "ipc_demo_socket.h"

#include <stdio.h>

int main(int argc, char *argv[])
{
    const char *message = "hello from ipca";
    if (argc > 1) {
        message = argv[1];
    }

    int fd = IpcDemoConnectToServer(IpcDemoGetSocketPath());
    if (fd < 0) {
        fprintf(stderr, "start ipcb before running ipca\n");
        return 1;
    }

    if (IpcDemoSendString(fd, message) != 0) {
        IpcDemoCloseFd(fd);
        return 1;
    }

    char reply[IPC_DEMO_BUFFER_SIZE];
    if (IpcDemoReceiveString(fd, reply, sizeof(reply)) < 0) {
        IpcDemoCloseFd(fd);
        return 1;
    }

    printf("ipca received: %s\n", reply);
    IpcDemoCloseFd(fd);
    return 0;
}
