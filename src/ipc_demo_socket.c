/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 */

#include "ipc_demo_socket.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

const char *IpcDemoGetSocketPath(void)
{
    const char *path = getenv("IPC_DEMO_SOCKET_PATH");
    if (path != NULL && path[0] != '\0') {
        return path;
    }
    return IPC_DEMO_DEFAULT_SOCKET_PATH;
}

static int FillSockAddr(const char *path, struct sockaddr_un *addr)
{
    if (path == NULL || path[0] == '\0' || strlen(path) >= sizeof(addr->sun_path)) {
        fprintf(stderr, "invalid socket path: %s\n", path == NULL ? "(null)" : path);
        return -1;
    }

    memset(addr, 0, sizeof(*addr));
    addr->sun_family = AF_UNIX;
    strncpy(addr->sun_path, path, sizeof(addr->sun_path) - 1);
    return 0;
}

int IpcDemoCreateServerSocket(const char *path)
{
    struct sockaddr_un addr;
    if (FillSockAddr(path, &addr) != 0) {
        return -1;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        fprintf(stderr, "socket failed: %s\n", strerror(errno));
        return -1;
    }

    unlink(path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "bind failed: %s\n", strerror(errno));
        IpcDemoCloseFd(fd);
        return -1;
    }
    chmod(path, 0666);

    if (listen(fd, 8) < 0) {
        fprintf(stderr, "listen failed: %s\n", strerror(errno));
        IpcDemoCloseFd(fd);
        unlink(path);
        return -1;
    }
    return fd;
}

int IpcDemoConnectToServer(const char *path)
{
    struct sockaddr_un addr;
    if (FillSockAddr(path, &addr) != 0) {
        return -1;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        fprintf(stderr, "socket failed: %s\n", strerror(errno));
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "connect failed: %s\n", strerror(errno));
        IpcDemoCloseFd(fd);
        return -1;
    }
    return fd;
}

int IpcDemoSendString(int fd, const char *message)
{
    if (message == NULL) {
        message = "";
    }

    const char *data = message;
    size_t left = strlen(message);
    while (left > 0) {
        ssize_t sent = send(fd, data, left, 0);
        if (sent <= 0) {
            fprintf(stderr, "send failed: %s\n", strerror(errno));
            return -1;
        }
        data += sent;
        left -= (size_t)sent;
    }
    return 0;
}

int IpcDemoReceiveString(int fd, char *buffer, size_t bufferSize)
{
    if (buffer == NULL || bufferSize == 0) {
        fprintf(stderr, "invalid receive buffer\n");
        return -1;
    }

    ssize_t received = recv(fd, buffer, bufferSize - 1, 0);
    if (received <= 0) {
        fprintf(stderr, "recv failed: %s\n", strerror(errno));
        return -1;
    }
    buffer[received] = '\0';
    return (int)received;
}

void IpcDemoCloseFd(int fd)
{
    if (fd >= 0) {
        close(fd);
    }
}
