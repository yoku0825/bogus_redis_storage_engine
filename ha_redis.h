/* Copyright (c) 2004, 2012, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2015, yoku0825. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */
#include "hiredis/hiredis.h"

class Redis_share : public Handler_share {
public:
  mysql_mutex_t mutex;
  THR_LOCK lock;
  Redis_share();
  ~Redis_share()
  {
    thr_lock_delete(&lock);
    mysql_mutex_destroy(&mutex);
  }
};

class ha_redis: public handler
{
  THR_LOCK_DATA lock;      ///< MySQL lock
  Redis_share *share;    ///< Shared lock info
  Redis_share *get_share(); ///< Get the share
  redisContext *redis_context;
  redisReply   *redis_reply;

public:
  ha_redis(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_redis()
  {
  }

  const char *table_type() const { return "Redis"; }
  const char *index_type(uint inx) { return "HASH"; }
  const char **bas_ext() const;
  ulonglong table_flags() const
  {
    return HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE;
  }

  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return 0;
  }

  uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }
  uint max_supported_keys()          const { return 0; }
  uint max_supported_key_parts()     const { return 0; }
  uint max_supported_key_length()    const { return 0; }

  virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }
  virtual double read_time(uint, uint, ha_rows rows) { return (double) rows /  20.0+1; }

  int open(const char *name, int mode, uint test_if_locked);    // required
  int close(void);                                              // required
  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);
  int index_read_map(uchar *buf, const uchar *key,
                     key_part_map keypart_map, enum ha_rkey_function find_flag);
  int index_next(uchar *buf);
  int index_prev(uchar *buf);
  int index_first(uchar *buf);
  int index_last(uchar *buf);
  int rnd_init(bool scan);                                      //required
  int rnd_end();
  int rnd_next(uchar *buf);                                     ///< required
  int rnd_pos(uchar *buf, uchar *pos);                          ///< required
  void position(const uchar *record);                           ///< required
  int info(uint);                                               ///< required
  int extra(enum ha_extra_function operation);
  int external_lock(THD *thd, int lock_type);                   ///< required
  int delete_all_rows(void);
  int truncate();
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key);
  int delete_table(const char *from);
  int rename_table(const char * from, const char * to);
  int create(const char *name, TABLE *form,
             HA_CREATE_INFO *create_info);                      ///< required

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);     ///< required
};
