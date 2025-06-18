import psycopg2
import time
import argparse


# use: python3 migrate_data.py --tables public.table_name --threshold 100
# ON_CONFLICT_MODE default is update
# WAIT_CLIENT_UPDATE_TIME default is 5 seconds

# 数据库连接配置
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
    "host": "localhost",
    "port": "7000"
}

def parse_args():
    parser = argparse.ArgumentParser(description='数据迁移工具')
    parser.add_argument('--tables', nargs='+', required=True,
                      help='需要检查的表名列表，格式为 schema.table_name')
    parser.add_argument('--threshold', type=int, required=True,
                      help='迁移阈值, 当routing_id对应的记录数超过此值时进行迁移')
    parser.add_argument('--conflict-mode', choices=['update', 'nothing'], default='update',
                      help='冲突处理模式: update-更新已存在的记录, nothing-忽略冲突')
    parser.add_argument('--wait-time', type=int, default=5,
                      help='等待客户端更新routing的时间(seconds)')
    return parser.parse_args()

# 解析命令行参数
args = parse_args()

# 数据表名称
check_tables = set(args.tables)
migrate_group_num_threshold = args.threshold
ON_CONFLICT_MODE = args.conflict_mode
WAIT_CLIENT_UPDATE_TIME = args.wait_time

# 连接到数据库
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# 停止 autovacuum
def stop_autovacuum():
    conn = connect_db()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("ALTER SYSTEM SET autovacuum = off;")
    cursor.execute("select pg_reload_conf() union all select pg_reload_conf() from gp_dist_random('gp_id');")
    cursor.close()
    conn.close()
    print("Autovacuum stopped.")

# 开始事务
def start_transaction():
    conn = connect_db()
    conn.autocommit = False
    cursor = conn.cursor()
    cursor.execute("BEGIN;")
    print("Transaction started.")
    return conn, cursor

# 提交事务
def commit_transaction(conn):
    conn.commit()
    print("Transaction committed.")
    conn.close()

# 获取表的主键
def get_primary_key(cursor, table):
    # get table name and schema name
    table_name = table.split('.')[1]
    schema_name = table.split('.')[0]
    print(f"get_primary_key: table name is {table_name}, schema name is {schema_name}")
    cursor.execute(f"""SELECT a.attname
	FROM pg_index i
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
	WHERE i.indrelid = (SELECT oid FROM pg_class WHERE relname = '{table_name}' 
					   AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema_name}'))
	  AND i.indisprimary
	ORDER BY a.attnum;""")
    primary_key_column = cursor.fetchall()
    primary_key_column_flat = tuple(item[0] for item in primary_key_column)
    return primary_key_column_flat

# 获取表的非主键列
def get_non_primary_key(cursor, table):
    # get table name and schema name
    table_name = table.split('.')[1]
    schema_name = table.split('.')[0]
    print(f"get_non_primary_key: table name is {table_name}, schema name is {schema_name}")
    cursor.execute(f"""WITH primary_keys AS (
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = (SELECT oid FROM pg_class WHERE relname = '{table_name}'
                        AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema_name}'))
      AND i.indisprimary
)
SELECT a.attname
FROM pg_attribute a
WHERE a.attrelid = (SELECT oid FROM pg_class WHERE relname = '{table_name}'
                    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema_name}'))
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND a.attname NOT IN (SELECT attname FROM primary_keys)
ORDER BY a.attnum;""")
    non_primary_key_column = cursor.fetchall()
    non_primary_key_column_flat = tuple(item[0] for item in non_primary_key_column)
    return non_primary_key_column_flat

# 从主表读取数据并写入附表,这里需要处理一下主键冲突的场景
def migrate_data(cursor, table, routing_id):
    primary_key_columns = get_primary_key(cursor, table)
    conflict_columns = ",".join(map(str, primary_key_columns))
    print(f"migrate_data: conflict_columns is {conflict_columns}")
    non_primary_key_columns = get_non_primary_key(cursor, table)
    print(f"migrate_data: non_primary_key_columns is {non_primary_key_columns}")
    updateSetParts = []
    for non_primary_key_column in non_primary_key_columns:
        updateSetParts.append(f"{non_primary_key_column} = excluded.{non_primary_key_column}")

    updateSet = ",".join(updateSetParts)
    aux_table = table + "_relyt_massive_group"
    sql = ""
    if len(conflict_columns) == 0:
        sql = f"INSERT INTO {aux_table} SELECT * FROM {table} WHERE routing_id = {routing_id};"
    elif ON_CONFLICT_MODE == 'update':
        sql = f"INSERT INTO {aux_table} SELECT * FROM {table} WHERE routing_id = {routing_id} on conflict ({conflict_columns}) do update set {updateSet};"
    else:
        sql = f"INSERT INTO {aux_table} SELECT * FROM {table} WHERE routing_id = {routing_id} on conflict ({conflict_columns}) do nothing;"
    print(f"migrate sql is {sql}")
    cursor.execute(sql)
    print(f"Data migrated from {table} to {aux_table} for group id {routing_id}.")

# 从主表删除指定 routing_id 的数据
def delete_data(cursor, table, routing_id):
    cursor.execute(f"DELETE FROM {table} WHERE routing_id = %s;", (routing_id,))
    print(f"Data with routing_id {routing_id} deleted from {table}.")

# 修改 routing 表
def update_routing_table(table, routing_id):
    conn = connect_db()
    cursor = conn.cursor()
    if '.' in table:
        table_name = table.split('.')[1]
        routing_table = "relyt_sys."+ table_name + "_relyt_routing"
    else:
        routing_table = "relyt_sys."+ table + "_relyt_routing"
    aux_table = table + "_relyt_massive_group"
    cursor.execute(f"INSERT INTO {routing_table} (routing_id, store_table_name) VALUES (%s, %s) on conflict (routing_id) do nothing;", (routing_id, aux_table))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Routing table updated for routing_id {routing_id}.")

# 等待客户端更新 routing
def wait_for_client_update():
    print("Waiting for clients to update routing...")
    time.sleep(WAIT_CLIENT_UPDATE_TIME)  # 假设等待 5 秒，实际时间根据需要调整

# 打开 autovacuum
def start_autovacuum():
    conn = connect_db()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("ALTER SYSTEM SET autovacuum = on;")
    cursor.execute("select pg_reload_conf() union all select pg_reload_conf() from gp_dist_random('gp_id');")
    cursor.close()
    conn.close()
    print("Autovacuum started.")


# 从指定的表里面根据筛选group数据大于migrate_records_threshold的routing_id
def get_migrate_routing_id(table, group_num_threshold):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT routing_id FROM {table} GROUP BY routing_id HAVING COUNT(*) >= {group_num_threshold}")
    routing_ids = cursor.fetchall()
    cursor.close()
    conn.close()
    routing_ids_flat = tuple(item[0] for item in routing_ids)
    return routing_ids_flat

def migrate_data_for_group(table, routing_id):
    try:
        # 1. 开始事务
        conn, cursor = start_transaction()

        # 1.1 从主表读取数据并写入附表
        migrate_data(cursor, table, routing_id)

        # 1.2 从主表删除指定 routing_id 的数据
        delete_data(cursor, table, routing_id)

        # 1.3 提交事务
        commit_transaction(conn)

        # 2. 修改 routing 表
        update_routing_table(table, routing_id)

        # 3. 等待客户端更新 routing
        wait_for_client_update()

        # 4. 重复步骤 1
        conn, cursor = start_transaction()
        migrate_data(cursor, table, routing_id)
        delete_data(cursor, table, routing_id)
        commit_transaction(conn)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Data migration completed.")

# main 函数，检查所有的表，看表当中的group id对应的数据是不是超过1000万条，对于超过1000万条的数据，进行迁移
def main():
    # 首先获取那些需要迁移数据的表和对应的goutingid，因为前面的这一步很耗时，所以先将这个数据存储在map当中,
    # 然后再进行迁移。
    table_goutingids = {}
    for table in check_tables:
        routing_ids = get_migrate_routing_id(table, migrate_group_num_threshold)
        if routing_ids is None or len(routing_ids) == 0:
            continue
        print(f"get_migrate_routing_id: table {table} group ids is {routing_ids}")
        table_goutingids[table] = routing_ids

    # 统计一下任务的总时间
    start_time = time.time()
    
    # 1. 关停autovacuum
    stop_autovacuum()

    # 2. 然后进行迁移
    for table, routing_ids in table_goutingids.items():
        for routing_id in routing_ids:
            print(f"Migrating data for table {table} and group id {routing_id}...")
            migrate_data_for_group(table, routing_id)
    
    # 3. 打开autovacuum
    start_autovacuum()
    
    # 4. 统计一下任务的总时间
    end_time = time.time()
    print(f"Total time: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()