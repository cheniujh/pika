// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <sys/statfs.h>
#include <memory>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"
#include "pink/include/thread_pool.h"
#include "pink/include/pink_pubsub.h"
#include "blackwidow/blackwidow.h"
#include "blackwidow/backupable.h"

#include "include/pika_conf.h"
#include "include/pika_table.h"
#include "include/pika_binlog.h"
#include "include/pika_define.h"
#include "include/pika_monitor_thread.h"
#include "include/pika_rsync_service.h"
#include "include/pika_binlog_bgworker.h"
#include "include/pika_heartbeat_thread.h"
#include "include/pika_slaveping_thread.h"
#include "include/pika_binlog_sender_thread.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_auxiliary_thread.h"

using slash::Status;
using slash::Slice;

static std::set<std::string> ShardingModeNotSupportCommands {kCmdNameDel,
                 kCmdNameMget,        kCmdNameKeys,          kCmdNameMset,
                 kCmdNameMsetnx,      kCmdNameScan,          kCmdNameScanx,
                 kCmdNamePKScanRange, kCmdNamePKRScanRange,  kCmdNameRPopLPush,
                 kCmdNameZUnionstore, kCmdNameZInterstore,   kCmdNameSUnion,
                 kCmdNameSUnionstore, kCmdNameSInter,        kCmdNameSInterstore,
                 kCmdNameSDiff,       kCmdNameSDiffstore,    kCmdNameSMove,
                 kCmdNamePfCount,     kCmdNamePfMerge};

extern PikaConf *g_pika_conf;

enum TaskType {
  kCompactAll,
  kCompactStrings,
  kCompactHashes,
  kCompactSets,
  kCompactZSets,
  kCompactList,
  kResetReplState,
  kPurgeLog,
  kStartKeyScan,
  kStopKeyScan,
  kBgSave,
};

class PikaServer {
 public:
  PikaServer();
  ~PikaServer();

  friend class InfoCmd;

  static uint64_t DiskSize(std::string path) {
    struct statfs diskInfo;
    int ret = statfs(path.c_str(), &diskInfo);
    if (ret == -1) {
      LOG(WARNING) << "Get DiskSize error: " << strerror(errno);
      return 0;
    }
    return diskInfo.f_bsize * diskInfo.f_blocks;
  }

  static void DoPurgeDir(void* arg) {
    std::string path = *(static_cast<std::string*>(arg));
    LOG(INFO) << "Delete dir: " << path << " start";
    slash::DeleteDir(path);
    LOG(INFO) << "Delete dir: " << path << " done";
    delete static_cast<std::string*>(arg);
  }

  /*
   * Get & Set
   */
  std::string host() {
    return host_;
  }
  int port() {
    return port_;
  }
  time_t start_time_s() {
    return start_time_s_;
  }
  std::string master_ip() {
    return master_ip_;
  }
  int master_port() {
    return master_port_;
  }

  int64_t sid() {
    return sid_;
  }

  void SetSid(int64_t sid) {
    sid_ = sid;
  }

  const std::shared_ptr<blackwidow::BlackWidow> db() {
    return db_;
  }

  int role() {
    slash::RWLock(&state_protector_, false);
    return role_;
  }

  bool readonly() {
    slash::RWLock(&state_protector_, false);
    if ((role_ & PIKA_ROLE_SLAVE)
      && g_pika_conf->slave_read_only()) {
      return true;
    }
    return false;
  }

  int repl_state() {
    slash::RWLock(&state_protector_, false);
    return repl_state_;
  }
  std::string repl_state_str() {
    slash::RWLock(&state_protector_, false);
    switch (repl_state_) {
      case PIKA_REPL_NO_CONNECT:
        return "no connect";
      case PIKA_REPL_SHOULD_META_SYNC:
        return "should meta sync";
      case PIKA_REPL_WAIT_META_SYNC_RESPONSE:
        return "wait meta sync response";
      case PIKA_REPL_SHOULD_MARK_TRY_CONNECT:
        return "should mark try connect";
      case PIKA_REPL_CONNECTING:
        return "connecting";
      case PIKA_REPL_ESTABLISH_SUCCESS:
        return "establish success";
      case PIKA_REPL_ERROR:
        return "error";
      default:
        return "";
    }
  }
  bool force_full_sync() {
    return force_full_sync_;
  }
  void SetForceFullSync(bool v) {
    force_full_sync_ = v;
  }

  /*
   * Table Partition use
   */
  void InitTableStruct();
  bool RebuildTableStruct(const std::vector<TableStruct>& table_structs);
  std::shared_ptr<Table> GetTable(const std::string& table_name);
  bool IsTableBinlogIoError(const std::string& table_name);
  bool IsBgSaving();
  bool IsKeyScaning();
  bool IsCompacting();
  bool IsTableExist(const std::string& table_name);
  bool IsCommandSupport(const std::string& command);
  uint32_t GetPartitionNumByTable(const std::string& table_name);
  bool GetTablePartitionBinlogOffset(const std::string& table_name,
                                     uint32_t partition_id,
                                     BinlogOffset* const boffset);
  void PreparePartitionTrySync();
  std::shared_ptr<Partition> GetTablePartitionById(
                                  const std::string& table_name,
                                  uint32_t partition_id);
  std::shared_ptr<Partition> GetTablePartitionByKey(
                                  const std::string& table_name,
                                  const std::string& key);

  /*
   * Master use
   */
  int64_t GenSid() {
    // slave_mutex has been locked from exterior
    int64_t sid = sid_;
    sid_++;
    return sid;
  }

  void DeleteSlave(int fd); // hb_fd
  void DeleteSlave(const std::string& ip, int64_t port);
  int64_t TryAddSlave(const std::string& ip, int64_t port,
                      const std::vector<TableStruct>& table_structs);
  int32_t GetSlaveListString(std::string& slave_list_str);
  bool PartitionCouldPurge(const std::string& table_name,
                           uint32_t partition_id, uint32_t index);
  void MayUpdateSlavesMap(int64_t sid, int32_t hb_fd);
  void BecomeMaster();

  slash::Mutex slave_mutex_; // protect slaves_;
  std::vector<SlaveItem> slaves_;

  /*
   * Slave use
   */
  bool SetMaster(std::string& master_ip, int master_port);
  void SyncError();
  void RemoveMaster();
  void KillMasterSyncConn();
  void ResetMetaSyncStatus();
  bool ShouldStartPingMaster();

  bool ShouldMetaSync();
  void MetaSyncDone();
  bool ShouldWaitMetaSyncResponse();
  void CheckWaitMetaSyncTimeout();
  bool ShouldMarkTryConnect();
  void MarkTryConnectDone();
  bool ShouldTrySyncPartition();
  void MarkEstablishSuccess();

  void Start();

  void Exit() {
    exit_ = true;
  }

  void SetBinlogIoError(bool error) {
    binlog_io_error_ = error;
  }

  bool BinlogIoError() {
    return binlog_io_error_;
  }

  PikaSlavepingThread* ping_thread_;

  /*
   * Server init info
   */
  bool ServerInit();

  /*
   * Blackwidow options init
   */
  void RocksdbOptionInit(blackwidow::BlackwidowOptions* bw_option);

  /*
   * ThreadPool process task
   */
  void Schedule(pink::TaskFunc func, void* arg);

  /*
   * Binlog
   */
  Binlog *logger_;
  Status AddBinlogSender(const std::string& table_name,
                         uint32_t partition_id,
                         const std::string& ip, int64_t port,
                         int64_t sid,
                         uint32_t filenum, uint64_t con_offset);

  /*
   * BGSave used
   */
  struct BGSaveInfo {
    bool bgsaving;
    time_t start_time;
    std::string s_start_time;
    std::string path;
    uint32_t filenum;
    uint64_t offset;
    BGSaveInfo() : bgsaving(false), filenum(0), offset(0){}
    void Clear() {
      bgsaving = false;
      path.clear();
      filenum = 0;
      offset = 0;
    }
  };
  BGSaveInfo bgsave_info() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_;
  }
  bool bgsaving() {
    slash::MutexLock l(&bgsave_protector_);
    return bgsave_info_.bgsaving;
  }
  void Bgsave();
  bool RunBgsaveEngine();
  void FinishBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.bgsaving = false;
  }
  void BGSaveTaskSchedule(void (*function)(void*), void* arg);


  /*
   * PurgeLog used
   */
  struct PurgeArg {
    PikaServer *p;
    uint32_t to;
    bool manual;
    bool force; // Ignore the delete window
  };
  bool PurgeLogs(uint32_t to, bool manual, bool force);
  void PurgelogsTaskSchedule(void (*function)(void*), void* arg);

  /*
   * DBSync used
   */
  struct DBSyncArg {
    PikaServer *p;
    std::string ip;
    int port;
    DBSyncArg(PikaServer *_p, const std::string& _ip, int &_port)
      : p(_p), ip(_ip), port(_port) {}
  };
  void DBSyncSendFile(const std::string& ip, int port);
  bool ChangeDb(const std::string& new_path);
  int CountSyncSlaves() {
    slash::MutexLock ldb(&db_sync_protector_);
    return db_sync_slaves_.size();
  }
  slash::Mutex & GetSlavesMutex() { return db_sync_protector_; }


  struct NewDBSyncArg {
    PikaServer* p;
    std::string ip;
    int port;
    std::string table_name;
    uint32_t partition_id;
    NewDBSyncArg(PikaServer* const _p,
                 const std::string& _ip,
                 int _port,
                 const std::string& _table_name,
                 uint32_t _partition_id)
        : p(_p), ip(_ip), port(_port),
          table_name(_table_name), partition_id(_partition_id) {}
  };
  void TryDBSync(const std::string& ip, int port,
                 const std::string& table_name,
                 uint32_t partition_id, int32_t top);
  std::string DbSyncTaskIndex(const std::string& ip, int port,
                              const std::string& table_name,
                              uint32_t partition_id);
  void DBSync(const std::string& ip, int port,
              const std::string& table_name,
              uint32_t partition_id);
  void NewDbSyncSendFile(const std::string& ip, int port,
                         const std::string& table_name,
                         uint32_t partition_id);


  //flushall & flushdb
  void PurgeDir(const std::string& path);
  void PurgeDirTaskSchedule(void (*function)(void*), void* arg);

  /*
   *Keyscan used
   */
  void KeyScanWholeTable(const std::string& table_name);
  void StopKeyScanWholeTable(const std::string& table_name);
  void KeyScanTaskSchedule(void (*function)(void*), void* arg);


  /*
   * client related
   */
  void ClientKillAll();
  int ClientKill(const std::string &ip_port);
  int64_t ClientList(std::vector<ClientInfo> *clients = nullptr);

  // rwlock_
  void RWLockWriter();
  void RWLockReader();
  void RWUnlock();

  /*
   * PubSub used
   */
  int Publish(const std::string& channel, const std::string& msg);
  void Subscribe(std::shared_ptr<pink::PinkConn> conn,
                 const std::vector<std::string>& channels,
                 const bool pattern,
                 std::vector<std::pair<std::string, int>>* result);

  int UnSubscribe(std::shared_ptr<pink::PinkConn> conn,
                  const std::vector<std::string>& channels,
                  const bool pattern,
                  std::vector<std::pair<std::string, int>>* result);

  void PubSubChannels(const std::string& pattern,
                      std::vector<std::string>* result);

  void PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result);

  int PubSubNumPat();

  /*
   * Monitor used
   */
  void AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr);
  void AddMonitorMessage(const std::string &monitor_message);
  bool HasMonitorClients();

  /*
   * Binlog Receiver use
   */
  void DispatchBinlogBG(const std::string &key, PikaCmdArgsType* argv,
          BinlogItem* binlog_item, uint64_t cur_serial, bool readonly);
  bool WaitTillBinlogBGSerial(uint64_t my_serial);
  void SignalNextBinlogBGSerial();

  /*
   * Slowlog use
   */
  void SlowlogTrim();
  void SlowlogReset();
  uint32_t SlowlogLen();
  void SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs);
  void SlowlogPushEntry(const PikaCmdArgsType& argv, int32_t time, int64_t duration);

  Status DoSameThingEveryTable(const TaskType& type);
  Status DoSameThingEveryPartition(const TaskType& type);

  /*
   *for statistic
   */
  uint64_t ServerQueryNum();
  uint64_t ServerCurrentQps();
  std::unordered_map<std::string, uint64_t> ServerExecCountTable();
  void ResetLastSecQuerynum(); /* Invoked in PikaDispatchThread's CronHandle */
  void UpdateQueryNumAndExecCountTable(const std::string& command);
  uint64_t accumulative_connections() {
    return statistic_data_.accumulative_connections;
  }
  void incr_accumulative_connections() {
    ++statistic_data_.accumulative_connections;  
  }
  void ResetStat();
  slash::RecordMutex mutex_record_;

  void SetDispatchQueueLimit(int queue_limit);

  /*
   * for RM {repl client, repl server} use
   */
  void ScheduleReplBinlogSyncTask(std::string table_partition,
      const std::shared_ptr<InnerMessage::InnerRequest> req,
      std::shared_ptr<pink::PbConn> conn,
      void* req_private_data);

  void ScheduleReplMetaSyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req,
      std::shared_ptr<pink::PbConn> conn,
      void* req_private_data);

  void ScheduleReplTrySyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req,
      std::shared_ptr<pink::PbConn> conn,
      void* req_private_data);

  void ScheduleReplDbTask(const std::string &key,
      PikaCmdArgsType* argv, BinlogItem* binlog_item,
      const std::string& table_name, uint32_t partition_id);

  bool SetBinlogAckInfo(const std::string& table, uint32_t partition, const std::string& ip, int port, uint32_t ack_filenum_start, uint64_t ack_offset_start, uint32_t ack_filenum_end, uint64_t ack_offset_end, uint64_t active_time);

  bool GetBinlogAckInfo(const std::string& table, uint32_t partition, const std::string& ip, int port,
    uint32_t* ack_file_num, uint64_t* ack_offset, uint64_t* active_time);

  /*
   * Communication use
   */
  Status SendMetaSyncRequest();
  Status SendPartitionTrySyncRequest(std::shared_ptr<Partition> partition);

  Status SendBinlogSyncRequest(const std::string& table, uint32_t partition, const std::string& ip, int port);

  // schedule repl_client thread pool
  void ScheduleReplCliTask(pink::TaskFunc func, void*arg);

  /*
   * For repl client write queue consumer
   * return value: consumed binlog task
   */
  int SendToPeer();

  /*
   * Used to trigger binglog sync process
   */
  Status TriggerSendBinlogSync();

  void SignalAuxiliary();
 private:
  std::atomic<bool> exit_;
  std::atomic<bool> binlog_io_error_;
  std::string host_;
  int port_;
  pthread_rwlock_t rwlock_;
  std::shared_ptr<blackwidow::BlackWidow> db_;

  /*
   * table use
   */
  pthread_rwlock_t tables_rw_;
  std::map<std::string, std::shared_ptr<Table>> tables_;

  time_t start_time_s_;
  bool have_scheduled_crontask_;
  struct timeval last_check_compact_time_;

  int worker_num_;
  PikaDispatchThread* pika_dispatch_thread_;
  pink::ThreadPool* pika_thread_pool_;

  PikaHeartbeatThread* pika_heartbeat_thread_;

  /*
   * Master use
   */
  int64_t sid_;

  /*
   * Slave use
   */
  pthread_rwlock_t state_protector_; //protect below, use for master-slave mode
  std::string master_ip_;
  int master_port_;
  int repl_state_;
  int role_;
  int last_meta_sync_timestamp_;
  bool force_full_sync_;

  /*
   * Bgsave use
   */
  slash::Mutex bgsave_protector_;
  slash::Mutex bgsave_thread_protector_;
  pink::BGThread bgsave_thread_;
  blackwidow::BackupEngine *bgsave_engine_;
  BGSaveInfo bgsave_info_;

  static void DoBgsave(void* arg);
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave() {
    slash::MutexLock l(&bgsave_protector_);
    bgsave_info_.Clear();
  }

  /*
   * Purgelogs use
   */
  std::atomic<bool> purging_;
  pink::BGThread purge_thread_;
  static void DoPurgeLogs(void* arg);
  bool GetBinlogFiles(std::map<uint32_t, std::string>& binlogs);
  bool CouldPurge(uint32_t index);

  /*
   * TimingTask use
   */
  void DoTimingTask();
  void AutoCompactRange();
  void AutoPurge();
  void AutoDeleteExpiredDump();
  void AutoKeepAliveRSync();

  /*
   * DBSync use
   */
  slash::Mutex db_sync_protector_;
  std::unordered_set<std::string> db_sync_slaves_;
  void TryDBSync(const std::string& ip, int port, int32_t top);
  void DBSync(const std::string& ip, int port);
  static void DoDBSync(void* arg);

  static void NewDoDbSync(void* arg);

  /*
   * Keyscan use
   */
  slash::Mutex key_scan_protector_;
  pink::BGThread key_scan_thread_;

  /*
   * Monitor use
   */
  PikaMonitorThread* monitor_thread_;

  /*
   * Rsync use
   */
  PikaRsyncService* pika_rsync_service_;

  /*
   * Pubsub use
   */
  pink::PubSubThread* pika_pubsub_thread_;

  /*
   * Communication use
   */
  PikaReplClient* pika_repl_client_;
  PikaReplServer* pika_repl_server_;
  PikaAuxiliaryThread* pika_auxiliary_thread_;

  /*
   * Binlog Receiver use
   */
  bool binlogbg_exit_;
  slash::Mutex binlogbg_mutex_;
  slash::CondVar binlogbg_cond_;
  uint64_t binlogbg_serial_;
  std::vector<BinlogBGWorker*> binlogbg_workers_;
  std::hash<std::string> str_hash;

  /*
   * Slowlog use
   */
  pthread_rwlock_t slowlog_protector_;
  uint64_t slowlog_entry_id_;
  std::list<SlowlogEntry> slowlog_list_;

  /*
   * for statistic
   */
  struct StatisticData {
    StatisticData()
        : accumulative_connections(0),
          thread_querynum(0),
          last_thread_querynum(0),
          last_sec_thread_querynum(0),
          last_time_us(0) {
    }

    slash::RWMutex statistic_lock;
    std::atomic<uint64_t> accumulative_connections;
    std::unordered_map<std::string, uint64_t> exec_count_table;
    uint64_t thread_querynum;
    uint64_t last_thread_querynum;
    uint64_t last_sec_thread_querynum;
    uint64_t last_time_us;
  };
  StatisticData statistic_data_;

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};

#endif
