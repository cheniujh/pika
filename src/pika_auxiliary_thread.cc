// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_auxiliary_thread.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

using namespace std::chrono_literals;

PikaAuxiliaryThread::~PikaAuxiliaryThread() {
  StopThread();
  LOG(INFO) << "PikaAuxiliary thread " << thread_id() << " exit!!!";
}

void* PikaAuxiliaryThread::ThreadMain() {
  while (!should_stop()) {
    if (g_pika_server->ShouldMetaSync()) {
//      这个分支应该也是只有slave会进来
      g_pika_rm->SendMetaSyncRequest();
    } else if (g_pika_server->MetaSyncDone()) {
      // return repl_state_ == PIKA_REPL_META_SYNC_DONE
      // 只有slave会陷入下面这个函数，也就是只有slave会有Repl_Meta_Sync_Done
      g_pika_rm->RunSyncSlaveDBStateMachine();
    }
    pstd::Status s = g_pika_rm->CheckSyncTimeout(pstd::NowMicros());
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }

    g_pika_server->CheckLeaderProtectedMode();

    // TODO(whoiami) timeout
    // 这个是主节点行为，遍历每个SyncMasterDB, 再对于每个SyncMasterDB内部的slaves也遍历
    // 对于每个slaveNode：如果发现其syncWin已经消费完毕（send_offset == acked_offset）
    //    继续读取binlog文件(queue_是sequential打开，读的时候不指定offset，只是在slaveNode上自己记录实际的offset，用于同步校验之类的工作)，
    //    一条一条读binlog，每个binlog都构造一个winItem(只有LogOffset和size)，并且构造一个WriteTask，塞入到WriteQueue
    s = g_pika_server->TriggerSendBinlogSync();
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }
    // send to peer
    // 这个也是主节点行为，应该是发送binlog（毕竟前面这步准备好了writeTask了）
    /*
     * 1 会去遍历每一个slave的每个queue(每个DB都有queue)
     * 2 每个在有总大小约束前提下，将writeTask
     * Pop出来，放到一个数组中（姑且看作batch，每个batch的目标ip&port和目标DB都一样）
     * 3
     * 以单个batch为单位，调用：pika_repl_server_->SendSlaveBinlogChips(ip, port, to_send);
     *     这里面会将整个batch构造为一个Innermessage::Response，type为kBinlogSync,
     *     该resp中的bnlogSync有多条，每条对应一个binlog
     *  4 将pb序列化完毕的msg，用PikaReplServer::Write(ip, port, msg)发过去
     *     需要注意的是，如果发现maps中找不到这个连接，说明连接还没有建立，会返回并且报错
     */
    int res = g_pika_server->SendToPeer();
    if (res == 0) {
      // sleep 100 ms
      std::unique_lock lock(mu_);
      cv_.wait_for(lock, 100ms);
    } else {
      // LOG_EVERY_N(INFO, 1000) << "Consume binlog number " << res;
    }
  }
  return nullptr;
}
