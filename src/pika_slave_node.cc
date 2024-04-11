// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slave_node.h"
#include "include/pika_conf.h"

using pstd::Status;

extern std::unique_ptr<PikaConf> g_pika_conf;

/* SyncWindow */

void SyncWindow::Push(const SyncWinItem& item) {
  win_.push_back(item);
  total_size_ += item.binlog_size_;
}

bool SyncWindow::Update(const SyncWinItem& start_item, const SyncWinItem& end_item, LogOffset* acked_offset) {
  size_t start_pos = win_.size();
  size_t end_pos = win_.size();
  //  确认一下收到的这个binlogSync ackReq所提供的ackrange是属于syncWin中的哪一段
  for (size_t i = 0; i < win_.size(); ++i) {
    if (win_[i] == start_item) {
      start_pos = i;
    }
    if (win_[i] == end_item) {
      end_pos = i;
      break;
    }
  }
  if (start_pos == win_.size() || end_pos == win_.size()) {
    //    只要有一个在里面定位不到，就是有问题
    LOG(WARNING) << "Ack offset Start: " << start_item.ToString() << "End: " << end_item.ToString()
                 << " not found in binlog controller window." << std::endl
                 << "window status " << std::endl
                 << ToStringStatus();
    return false;
  }
  //  将得到ack的这批WinItem标记为acked
  for (size_t i = start_pos; i <= end_pos; ++i) {
    win_[i].acked_ = true;
    total_size_ -= win_[i].binlog_size_;
  }
  while (!win_.empty()) {
    //    只允许从队头pop，并且acked就会等于最后被pop出队的那个item的索引
    //    如果这次响应的这批其实不是队头这批（可能是中间这批），会如何：不会有item被pop（只有被ack的这批会得到确认），acked_offset会不会得到更新？
    if (win_[0].acked_) {
      *acked_offset = win_[0].offset_;
      win_.pop_front();
    } else {
      break;
    }
  }
  return true;
}

int SyncWindow::Remaining() {
  std::size_t remaining_size = g_pika_conf->sync_window_size() - win_.size();
  return static_cast<int>(remaining_size > 0 ? remaining_size : 0);
}

/* SlaveNode */

SlaveNode::SlaveNode(const std::string& ip, int port, const std::string& db_name, int session_id)
    : RmNode(ip, port, db_name, session_id)

{}

SlaveNode::~SlaveNode() = default;

Status SlaveNode::InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset) {
  binlog_reader = std::make_shared<PikaBinlogReader>();
  int res = binlog_reader->Seek(binlog, offset.filenum, offset.offset);
  if (res != 0) {
    return Status::Corruption(ToString() + "  binlog reader init failed");
  }
  return Status::OK();
}

std::string SlaveNode::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << "    Slave_state: " << SlaveStateMsg[slave_state] << "\r\n";
  tmp_stream << "    Binlog_sync_state: " << BinlogSyncStateMsg[b_state] << "\r\n";
  tmp_stream << "    Sync_window: "
             << "\r\n"
             << sync_win.ToStringStatus();
  tmp_stream << "    Sent_offset: " << sent_offset.ToString() << "\r\n";
  tmp_stream << "    Acked_offset: " << acked_offset.ToString() << "\r\n";
  tmp_stream << "    Binlog_reader activated: " << (binlog_reader != nullptr) << "\r\n";
  return tmp_stream.str();
}

Status SlaveNode::Update(const LogOffset& start, const LogOffset& end, LogOffset* updated_offset) {
  if (slave_state != kSlaveBinlogSync) {
    return Status::Corruption(ToString() + "state not BinlogSync");
  }
  *updated_offset = LogOffset();
  bool res = sync_win.Update(SyncWinItem(start), SyncWinItem(end), updated_offset);
  if (!res) {
    return Status::Corruption("UpdateAckedInfo failed");
  }
  if (*updated_offset == LogOffset()) {
    //    如果确认的这批并不是队头的这批binlog（在window中间的一批得到了确认），只会对这些binlog标记acked，并不会pop队列，也不会更新acked_offset
    // nothing to update return current acked_offset
    *updated_offset = acked_offset;
    return Status::OK();
  }
  //  如果队头这批item得到了确认，就可以pop Window的queue，这样就应当更新acked_offset
  //  所以sentoffset，是window队列中最后一个binlog的offset
  //  而acked_offset，是队头的binlog的offset - 1，也可以说是被pop出队的，最后一个binlog的offset

  // update acked_offset
  acked_offset = *updated_offset;
  return Status::OK();
}
