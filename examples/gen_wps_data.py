import psycopg2
import numpy as np
from datetime import datetime

# 数据库连接配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "",
    "port": "7000"
}

# 生成随机3维向量（16位浮点）
def generate_random_vector(dimensions=3):
    # 生成16位浮点向量
    vector = np.random.rand(dimensions).astype(np.float16)
    # 转换为字符串格式，pgvecto.rs期望的格式
    return '[' + ','.join([str(x) for x in vector]) + ']'

# 测试数据生成
def generate_test_data(start_id, num_records=10, vector_dimensions=3):
    base_time = int(datetime.now().timestamp())
    data = []
    
    for i in range(1, num_records + 1):
        id = f"{start_id + i}"
        routing_id = f"{start_id + i}"
        group_id = f"{start_id + i}"
        fileid = f"{start_id + i}"
        record = {
            "id": id,
            "routing_id": routing_id,  # 3个不同的routing_id循环
            "chunk_id": i,
            "chunk_type": "text",
            "user_id": 1000 + i,
            "creator": 5000 + i,
            "sharer": 6000 + i,
            "fileid": fileid,
            "group_id": group_id,
            "ctime": base_time - (i * 3600),  # 递减的时间
            "mtime": base_time - (i * 1800),
            "y": 2023,
            "ym": 202312,
            "ymd": 20231215,
            "ext": "pdf" if i % 2 == 0 else "docx",
            "fsize": 1024 * (i + 1),
            "parent_id": 30000 + (i % 3),
            "ftype": "document",
            "version": 1,
            "index_update_time": base_time,
            "ext_group": "office",
            "vector": generate_random_vector(vector_dimensions)  # 随机向量
        }
        data.append(record)
    
    return data

# 插入数据到PostgreSQL
def insert_data_to_postgres(data, table_name):
    conn = None
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 准备插入SQL，使用CAST将字符串转换为vecf16
        insert_sql = f"""
        INSERT INTO {table_name} (
            id, routing_id, chunk_id, chunk_type, user_id, creator, sharer, 
            fileid, group_id, ctime, mtime, y, ym, ymd, ext, fsize, 
            parent_id, ftype, version, index_update_time, ext_group, vector
        ) VALUES (
            %(id)s, %(routing_id)s, %(chunk_id)s, %(chunk_type)s, %(user_id)s, 
            %(creator)s, %(sharer)s, %(fileid)s, %(group_id)s, %(ctime)s, 
            %(mtime)s, %(y)s, %(ym)s, %(ymd)s, %(ext)s, %(fsize)s, 
            %(parent_id)s, %(ftype)s, %(version)s, %(index_update_time)s, 
            %(ext_group)s, %(vector)s::vecf16
        )
        """
        
        # 批量插入
        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"成功插入 {len(data)} 条记录")
        
    except Exception as e:
        print(f"插入数据时出错: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # 如果单个向量插入成功，再批量插入
    test_data1 = generate_test_data(0, 5)  # 减少到5条记录进行测试
    insert_data_to_postgres(test_data1, "content_personal_vector_semantic_insight_vector_bge_m3_dense")

    test_data2 = generate_test_data(10, 5)  # 减少到5条记录进行测试
    insert_data_to_postgres(test_data2, "content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group")