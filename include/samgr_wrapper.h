/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * SAMgr C Wrapper Header
 * 纯C接口，内部调用C++实现
 */

#ifndef SAMGR_WRAPPER_H
#define SAMGR_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief 注册系统服务到SAMgr
 * @param saId 系统服务ID（建议：14000-14999）
 * @param stub RemoteStub对象（来自OH_IPCRemoteStub_Create）
 * @return 0成功，非0失败
 */
int OH_SAMgr_AddSystemAbility(int32_t saId, void *stub);

/**
 * @brief 从SAMgr获取系统服务
 * @param saId 系统服务ID
 * @return RemoteProxy指针，失败返回NULL
 */
void* OH_SAMgr_GetSystemAbility(int32_t saId);

/**
 * @brief 从SAMgr移除系统服务
 * @param saId 系统服务ID
 * @return 0成功，非0失败
 */
int OH_SAMgr_RemoveSystemAbility(int32_t saId);

/**
 * @brief 检查服务是否已注册
 * @param saId 系统服务ID
 * @return 1已注册，0未注册
 */
int OH_SAMgr_CheckSystemAbility(int32_t saId);

/**
 * @brief 释放RemoteObject引用
 * @param remoteObj IRemoteObject指针
 */
void OH_SAMgr_ReleaseRemoteObject(void *remoteObj);

#ifdef __cplusplus
}
#endif

#endif /* SAMGR_WRAPPER_H */
