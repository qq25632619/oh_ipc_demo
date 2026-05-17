/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef APPLICATIONS_SAMPLE_IPC_DEMO_SOCKET_H
#define APPLICATIONS_SAMPLE_IPC_DEMO_SOCKET_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define IPC_DEMO_DEFAULT_SOCKET_PATH "/data/local/tmp/ipc_demo.sock"
#define IPC_DEMO_BUFFER_SIZE 1024

const char *IpcDemoGetSocketPath(void);
int IpcDemoCreateServerSocket(const char *path);
int IpcDemoConnectToServer(const char *path);
int IpcDemoSendString(int fd, const char *message);
int IpcDemoReceiveString(int fd, char *buffer, size_t bufferSize);
void IpcDemoCloseFd(int fd);

#ifdef __cplusplus
}
#endif

#endif /* APPLICATIONS_SAMPLE_IPC_DEMO_SOCKET_H */
