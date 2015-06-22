/* Copyright (c) 2004, 2013, Oracle and/or its affiliates. All rights reserved.
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

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_redis.h"
#include "probes_mysql.h"
#include "sql_plugin.h"

uint pos;
static handler *redis_create_handler(handlerton *hton,
                                     TABLE_SHARE *table, 
                                     MEM_ROOT *mem_root);

handlerton *redis_hton;

static const char* redis_system_database();
static bool redis_is_supported_system_table(const char *db,
                                      const char *table_name,
                                      bool is_sql_layer_system_table);
#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_Redis_share_mutex;

static PSI_mutex_info all_redis_mutexes[]=
{
  { &ex_key_mutex_Redis_share_mutex, "Redis_share::mutex", 0}
};

static void init_redis_psi_keys()
{
  const char* category= "redis";
  int count;

  count= array_elements(all_redis_mutexes);
  mysql_mutex_register(category, all_redis_mutexes, count);
}
#endif

Redis_share::Redis_share()
{
  thr_lock_init(&lock);
  mysql_mutex_init(ex_key_mutex_Redis_share_mutex,
                   &mutex, MY_MUTEX_INIT_FAST);
}


static int redis_init_func(void *p)
{
  DBUG_ENTER("redis_init_func");

#ifdef HAVE_PSI_INTERFACE
  init_redis_psi_keys();
#endif

  redis_hton= (handlerton *)p;
  redis_hton->state=                     SHOW_OPTION_YES;
  redis_hton->create=                    redis_create_handler;
  redis_hton->flags=                     HTON_CAN_RECREATE;
  redis_hton->system_database=           redis_system_database;
  redis_hton->is_supported_system_table= redis_is_supported_system_table;

  DBUG_RETURN(0);
}


Redis_share *ha_redis::get_share()
{
  Redis_share *tmp_share;

  DBUG_ENTER("ha_redis::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<Redis_share*>(get_ha_share_ptr())))
  {
    tmp_share= new Redis_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}


static handler* redis_create_handler(handlerton *hton,
                                     TABLE_SHARE *table, 
                                     MEM_ROOT *mem_root)
{
  return new (mem_root) ha_redis(hton, table);
}

ha_redis::ha_redis(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{}


static const char *ha_redis_exts[] = {
  NullS
};

const char **ha_redis::bas_ext() const
{
  return ha_redis_exts;
}

const char* ha_redis_system_database= NULL;
const char* redis_system_database()
{
  return ha_redis_system_database;
}

static st_system_tablename ha_redis_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};

static bool redis_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_redis_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}


int ha_redis::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_redis::open");

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);

  DBUG_RETURN(0);
}


int ha_redis::close(void)
{
  DBUG_ENTER("ha_redis::close");
  DBUG_RETURN(0);
}


int ha_redis::write_row(uchar *buf)
{
  DBUG_ENTER("ha_redis::write_row");

  char key_attribute_buffer[1024];
  String key_attribute(key_attribute_buffer,
                       sizeof(key_attribute_buffer),
                       &my_charset_bin);
  char val_attribute_buffer[1024];
  String val_attribute(val_attribute_buffer,
                       sizeof(val_attribute_buffer),
                       &my_charset_bin);

  for (Field **field= table->field; *field; field++)
  {
    if (strcmp((char *)(*field)->field_name, "k") == 0)
      (*field)->val_str(&key_attribute,&key_attribute);
    else if (strcmp((char *)(*field)->field_name, "v") == 0)
      (*field)->val_str(&val_attribute,&val_attribute);
  }

  redisContext *c;
  if ((c= redisConnect("127.0.0.1", 6379)))
  {
    redisReply *reply;

    if ((reply= (redisReply *)redisCommand(c, "SET %s %s",
                                           (char *)key_attribute.c_ptr_safe(),
                                           (char *)val_attribute.c_ptr_safe())))
    {
      // success?
      freeReplyObject(reply);
    }

    redisFree(c);
    DBUG_RETURN(0);
  }
  else
    DBUG_RETURN(1);
}


int ha_redis::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_redis::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


int ha_redis::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_redis::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


int ha_redis::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_redis::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_redis::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_redis::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_redis::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_redis::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_redis::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_redis::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_redis::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_redis::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_redis::rnd_init(bool scan)
{
  DBUG_ENTER("ha_redis::rnd_init");
  pos= 0;
  DBUG_RETURN(0);
}

int ha_redis::rnd_end()
{
  DBUG_ENTER("ha_redis::rnd_end");
  DBUG_RETURN(0);
}


int ha_redis::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_redis::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  rc= 0;

  redisContext *c;
  if ((c= redisConnect("127.0.0.1", 6379)))
  {
    redisReply *reply;

    if ((reply= (redisReply *)redisCommand(c, "KEYS *")))
    {
      if (pos >= reply->elements)
      {
        rc= HA_ERR_END_OF_FILE;
        freeReplyObject(reply);
        redisFree(c);
        goto end;
      }

      // success?
      redisReply *rep;
      if ((rep= (redisReply *)redisCommand(c, "GET %s", reply->element[pos]->str)))
      {
        for (Field **field= table->field; *field; field++)
        {
          if (strcmp((char *)(*field)->field_name, "k") == 0)
          {
            (*field)->store(reply->element[pos]->str, strlen(reply->element[pos]->str), system_charset_info);
            (*field)->set_notnull();
          }
          else if (strcmp((char *)(*field)->field_name, "v") == 0)
          {
            (*field)->store(rep->str, strlen(rep->str), system_charset_info);
            (*field)->set_notnull();
          }
        }

        freeReplyObject(rep);
        pos++;
      }

      freeReplyObject(reply);
    }
    redisFree(c);
  }

end:
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


void ha_redis::position(const uchar *record)
{
  DBUG_ENTER("ha_redis::position");
  DBUG_VOID_RETURN;
}


int ha_redis::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_redis::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_redis::info(uint flag)
{
  DBUG_ENTER("ha_redis::info");
  DBUG_RETURN(0);
}


int ha_redis::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_redis::extra");
  DBUG_RETURN(0);
}


int ha_redis::delete_all_rows()
{
  DBUG_ENTER("ha_redis::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


int ha_redis::truncate()
{
  DBUG_ENTER("ha_redis::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


int ha_redis::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_redis::external_lock");
  DBUG_RETURN(0);
}


THR_LOCK_DATA **ha_redis::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


int ha_redis::delete_table(const char *name)
{
  DBUG_ENTER("ha_redis::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}


int ha_redis::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_redis::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


ha_rows ha_redis::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_redis::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


int ha_redis::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_redis::create");
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */
  DBUG_RETURN(0);
}


struct st_mysql_storage_engine redis_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static struct st_mysql_sys_var* redis_system_variables[]= {
  NULL
};

static struct st_mysql_show_var func_status[]=
{
  {0,0,SHOW_UNDEF}
};

mysql_declare_plugin(redis)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &redis_storage_engine,
  "redis",
  "yoku0825",
  "bogus redis storage engine",
  PLUGIN_LICENSE_GPL,
  redis_init_func,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  redis_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
