/*
 * Copyright (c) 2026 OpenHarmony.
 * Licensed under the Apache License, Version 2.0.
 *
 * SAMgr C++ Wrapper - 为纯C代码提供SAMgr功能
 * 
 * 这个文件是C++实现，提供C接口给oh_ipc_binder.c使用
 * 
 * 编译依赖：
 *   external_deps = [
 *     "samgr:samgr_proxy",
 *   ]
 */

/* C++头文件 */
#include <cstdint>
#include <string>

/* OpenHarmony C++头文件 */
#include "iservice_registry.h"
#include "iremote_object.h"
#include "ipc_object_stub.h"

using namespace OHOS;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief 注册系统服务到SAMgr
 * @param saId 系统服务ID
 * @param stub RemoteStub对象（来自OH_IPCRemoteStub_Create）
 * @return 0成功，非0失败
 */
int OH_SAMgr_AddSystemAbility(int32_t saId, void *stub)
{
    if (!stub) {
        return -1;
    }
    
    /* 获取SAMgr实例 */
    sptr<ISystemAbilityManager> samgr = 
        SystemAbilityManagerClient::GetInstance().GetSystemAbilityManager();
    if (!samgr) {
        return -2;
    }
    
    /* stub实际上是一个IPCObjectStub，它继承自IRemoteObject */
    IPCObjectStub *ipcStub = reinterpret_cast<IPCObjectStub*>(stub);
    if (!ipcStub) {
        return -3;
    }
    
    /* IPCObjectStub 继承自 IRemoteObject，可以直接使用 */
    sptr<IRemoteObject> remoteObj = ipcStub;
    
    int ret = samgr->AddSystemAbility(saId, remoteObj);
    return ret;
}

/**
 * @brief 从SAMgr获取系统服务
 * @param saId 系统服务ID
 * @return RemoteProxy指针，失败返回NULL
 */
void* OH_SAMgr_GetSystemAbility(int32_t saId)
{
    /* 获取SAMgr实例 */
    sptr<ISystemAbilityManager> samgr = 
        SystemAbilityManagerClient::GetInstance().GetSystemAbilityManager();
    if (!samgr) {
        return nullptr;
    }
    
    /* 获取服务 */
    sptr<IRemoteObject> remoteObj = samgr->GetSystemAbility(saId);
    if (!remoteObj) {
        return nullptr;
    }
    
    /* 增加引用计数 */
    remoteObj->IncStrongRef(nullptr);
    
    /* 返回裸指针，C层需要负责释放 */
    return remoteObj.GetRefPtr();
}

/**
 * @brief 从SAMgr移除系统服务
 * @param saId 系统服务ID
 * @return 0成功，非0失败
 */
int OH_SAMgr_RemoveSystemAbility(int32_t saId)
{
    sptr<ISystemAbilityManager> samgr = 
        SystemAbilityManagerClient::GetInstance().GetSystemAbilityManager();
    if (!samgr) {
        return -1;
    }
    
    return samgr->RemoveSystemAbility(saId);
}

/**
 * @brief 检查服务是否已注册
 * @param saId 系统服务ID
 * @return 1已注册，0未注册
 */
int OH_SAMgr_CheckSystemAbility(int32_t saId)
{
    sptr<ISystemAbilityManager> samgr = 
        SystemAbilityManagerClient::GetInstance().GetSystemAbilityManager();
    if (!samgr) {
        return 0;
    }
    
    sptr<IRemoteObject> remoteObj = samgr->CheckSystemAbility(saId);
    return remoteObj != nullptr ? 1 : 0;
}

/**
 * @brief 释放RemoteObject引用
 * @param remoteObj IRemoteObject指针
 */
void OH_SAMgr_ReleaseRemoteObject(void *remoteObj)
{
    if (remoteObj) {
        reinterpret_cast<IRemoteObject*>(remoteObj)->DecStrongRef(nullptr);
    }
}

#ifdef __cplusplus
}
#endif
