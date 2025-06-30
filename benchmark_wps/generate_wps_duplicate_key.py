## table schema
# qsearch=# \d content_personal_vector_semantic_insight_vector_bge_m3_dense
# Partitioned table "public.content_personal_vector_semantic_insight_vector_bge_m3_dense"
#       Column       |         Type          | Collation | Nullable | Default
# -------------------+-----------------------+-----------+----------+---------
#  id                | character varying     |           | not null |
#  routing_id        | character varying     |           | not null |
#  chunk_id          | integer               |           | not null |
#  chunk_type        | character varying     |           | not null |
#  user_id           | bigint                |           | not null |
#  creator           | bigint                |           | not null |
#  sharer            | bigint                |           | not null |
#  fileid            | bigint                |           | not null |
#  group_id          | bigint                |           | not null |
#  ctime             | bigint                |           | not null |
#  mtime             | bigint                |           | not null |
#  y                 | integer               |           | not null |
#  ym                | integer               |           | not null |
#  ymd               | integer               |           | not null |
#  ext               | character varying(10) |           | not null |
#  fsize             | bigint                |           | not null |
#  parent_id         | bigint                |           | not null |
#  ftype             | character varying(50) |           | not null |
#  version           | bigint                |           | not null |
#  index_update_time | bigint                |           | not null |
#  ext_group         | character varying(50) |           | not null |
#  vector            | vecf16(3)          |           | not null |
# Partition key: RANGE (routing_id)
# Indexes:
#     "content_personal_vector_semantic_insight_vector_bge_m3_dense_pkey" PRIMARY KEY, btree (routing_id, fileid, id)
#     "insight_personal_idx_fileid" btree (fileid)
#     "insight_personal_idx_group_id" btree ("group_id")
# Distributed by: (routing_id, fileid)

# 用于生成测试数据插入content_personal_vector_semantic_insight_vector_bge_m3_dense中:
# 1. 随机生成10000个(routing_id, fileid)的组合，每个组合有10个id(从0到9)，共有10000*10=100000个主键(routing_id, fileid, id)，此为一版数据的基础数据（version=当前版本,基础数据条数=100000）
# 2. 在基础数据基础上，额外生成10%主键重复的数据（10000条），这些数据与基础数据具有相同的主键(routing_id, fileid, id)但其他字段随机
# 3. 每版数据总计：基础数据100000条 + 重复数据10000条 = 110000条
# 4. 一版数据生成后version++，生成新的一版数据
# 5. 重复步骤1-4，直到version=total_version，总数据量为total_version*110000
# 6. vector为1024维的随机向量
# 7. 其余字段随机

import random
import string
import time
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from functools import partial

# 线程本地存储，用于确保每个线程有独立的随机数生成器
thread_local = threading.local()

def get_thread_random():
    """获取线程本地的随机数生成器"""
    if not hasattr(thread_local, 'random'):
        # 为每个线程创建独立的随机数生成器
        thread_local.random = random.Random()
        thread_local.random.seed(threading.current_thread().ident + int(time.time() * 1000000))
    return thread_local.random

def generate_random_string(length=10):
    """生成随机字符串（线程安全）"""
    rand = get_thread_random()
    return ''.join(rand.choices(string.ascii_letters + string.digits, k=length))

def generate_data_batch(routing_file_combinations, version, ids_per_combination=10, batch_size=50000):
    """
    生成数据批次（简化版本 - 只有主键和version动态，其他字段固定）
    
    Args:
        routing_file_combinations: (routing_id, fileid)组合列表
        version: 当前版本号
        ids_per_combination: 每个组合生成的id数量，默认10个
        batch_size: 批处理大小，默认50000条
    
    Yields:
        batch: 数据批次列表
        
    生成的数据包含两部分：
    1. 基础数据：100000条正常数据
    2. 重复数据：10000条主键重复的数据（10%）
    总计：110000条/版
    """
    rand = get_thread_random()  # 获取线程本地的随机数生成器
    batch = []
    generated_records = []  # 用于存储已生成的记录主键，便于后续生成重复数据
    
    # 固定值 - 除了主键和version外的所有字段
    FIXED_CHUNK_TYPE = "text"
    FIXED_USER_ID = 1000000
    FIXED_CREATOR = 1000000
    FIXED_SHARER = 1000000
    FIXED_GROUP_ID = 1000000
    FIXED_CTIME = 1640995200  # 2022-01-01
    FIXED_MTIME = 1640995200  # 2022-01-01
    FIXED_Y = 2022
    FIXED_YM = 202201
    FIXED_YMD = 20220101
    FIXED_EXT = "txt"
    FIXED_FSIZE = 1024
    FIXED_PARENT_ID = 1000000
    FIXED_FTYPE = "text"
    FIXED_INDEX_UPDATE_TIME = 1640995200
    FIXED_EXT_GROUP = "text"
    # 生成固定的3维向量
    FIXED_VECTOR = [0.1, 0.2, 0.3]
    
    print(f"[线程-版本{version}] 使用简化模式：除主键和version外所有字段固定")
    
    # 第一步：生成基础正常数据（100000条）
    for routing_id, fileid in routing_file_combinations:
        for chunk_id in range(ids_per_combination):  # 每个组合生成指定数量的id
            record = [
                str(chunk_id),          # id (动态)
                routing_id,             # routing_id (动态)
                chunk_id,               # chunk_id (动态)
                FIXED_CHUNK_TYPE,       # chunk_type (固定)
                FIXED_USER_ID,          # user_id (固定)
                FIXED_CREATOR,          # creator (固定)
                FIXED_SHARER,           # sharer (固定)
                fileid,                 # fileid (动态)
                FIXED_GROUP_ID,         # group_id (固定)
                FIXED_CTIME,            # ctime (固定)
                FIXED_MTIME,            # mtime (固定)
                FIXED_Y,                # y (固定)
                FIXED_YM,               # ym (固定)
                FIXED_YMD,              # ymd (固定)
                FIXED_EXT,              # ext (固定)
                FIXED_FSIZE,            # fsize (固定)
                FIXED_PARENT_ID,        # parent_id (固定)
                FIXED_FTYPE,            # ftype (固定)
                version,                # version (动态)
                FIXED_INDEX_UPDATE_TIME, # index_update_time (固定)
                FIXED_EXT_GROUP,        # ext_group (固定)
                FIXED_VECTOR            # vector (固定)
            ]
            batch.append(record)
            generated_records.append((routing_id, fileid, str(chunk_id)))  # 记录主键用于生成重复数据
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
    
    # 第二步：生成10%主键重复的数据（10000条）
    total_records = len(routing_file_combinations) * ids_per_combination
    duplicate_count = int(total_records * 0.1)
    print(f"[线程-版本{version}] 开始生成 {duplicate_count} 条主键重复数据...")
    
    for i in range(duplicate_count):
        # 随机选择一个已存在的主键组合
        routing_id, fileid, id_str = rand.choice(generated_records)
        
        duplicate_record = [
            id_str,                     # id (与基础数据重复)
            routing_id,                 # routing_id (与基础数据重复)
            int(id_str),                # chunk_id (与基础数据重复)
            FIXED_CHUNK_TYPE,           # chunk_type (固定)
            FIXED_USER_ID,              # user_id (固定)
            FIXED_CREATOR,              # creator (固定)
            FIXED_SHARER,               # sharer (固定)
            fileid,                     # fileid (与基础数据重复)
            FIXED_GROUP_ID,             # group_id (固定)
            FIXED_CTIME,                # ctime (固定)
            FIXED_MTIME,                # mtime (固定)
            FIXED_Y,                    # y (固定)
            FIXED_YM,                   # ym (固定)
            FIXED_YMD,                  # ymd (固定)
            FIXED_EXT,                  # ext (固定)
            FIXED_FSIZE,                # fsize (固定)
            FIXED_PARENT_ID,            # parent_id (固定)
            FIXED_FTYPE,                # ftype (固定)
            version,                    # version (保持当前版本)
            FIXED_INDEX_UPDATE_TIME,    # index_update_time (固定)
            FIXED_EXT_GROUP,            # ext_group (固定)
            FIXED_VECTOR                # vector (固定)
        ]
        batch.append(duplicate_record)
        
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # 返回最后一批数据
    if batch:
        yield batch

def write_data_to_csv_batch(data_batch, csv_writer):
    """
    批量写入CSV文件
    
    Args:
        data_batch: 数据批次列表
        csv_writer: CSV写入器对象
    """
    csv_writer.writerows(data_batch)

def generate_single_version_data(version, routing_file_combinations, ids_per_combination, batch_size, output_dir, progress_interval):
    """
    生成单个版本的数据（用于多线程执行）
    
    Args:
        version: 版本号
        routing_file_combinations: (routing_id, fileid)组合列表
        ids_per_combination: 每个组合的id数量
        batch_size: 批处理大小
        output_dir: 输出目录
        progress_interval: 进度报告间隔
    
    Returns:
        tuple: (version, count, elapsed_time, csv_filename)
    """
    print(f"[线程-版本{version}] 开始生成数据...")
    start_time = time.time()
    
    # 创建CSV文件，使用更大的缓冲区以减少磁盘I/O
    csv_filename = os.path.join(output_dir, f"wps_data_version_{version}.csv")
    with open(csv_filename, 'w', newline='', encoding='utf-8', buffering=32*1024*1024) as csvfile:  # 32MB缓冲区
        csv_writer = csv.writer(csvfile)
        
        count = 0
        write_buffer = []  # 内存缓冲区，累积更多数据再写入
        buffer_size = 110000  # 缓冲区大小，累积整个版本的所有记录再写入磁盘
        
        for batch in generate_data_batch(routing_file_combinations, version, ids_per_combination, batch_size):
            write_buffer.extend(batch)
            count += len(batch)
            
            if count % progress_interval == 0:
                print(f"[线程-版本{version}] 已生成 {count:,} 条记录")
        
        # 一次性写入所有数据，最大化减少磁盘I/O
        print(f"[线程-版本{version}] 开始写入 {len(write_buffer):,} 条记录到磁盘...")
        csv_writer.writerows(write_buffer)
        csvfile.flush()
        print(f"[线程-版本{version}] 磁盘写入完成")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"[线程-版本{version}] 完成，文件: {csv_filename}")
    print(f"[线程-版本{version}] 写入 {count:,} 条记录，耗时: {elapsed_time:.2f} 秒")
    
    return version, count, elapsed_time, csv_filename

def main():
    """
    主函数：生成WPS测试数据（支持多线程）
    
    数据生成策略：
    - 每版数据包含110,000条记录
    - 基础数据：100,000条 (10,000个routing_id+fileid组合 × 10个id)
    - 重复数据：10,000条 (基础数据的10%，主键重复但其他字段随机)
    
    输出：
    - 每个版本生成一个CSV文件
    - 文件名格式：wps_data_version_{version}.csv
    - 总计：total_version × 110,000 条记录
    
    多线程模式：
    - 可以并行生成多个版本的数据
    - 线程数可配置，默认为CPU核心数
    """
    # 配置参数
    total_version = 100  # 100个版本
    combinations_count = 10000  # routing_id和fileid组合数
    ids_per_combination = 10  # 每个组合的id数量
    batch_size = 50000  # 增加批处理大小到5万条，减少内存分配次数
    base_records_per_version = combinations_count * ids_per_combination  # 基础记录数（100000）
    duplicate_records = int(base_records_per_version * 0.1)  # 重复数据（10000）
    total_records_per_version = base_records_per_version + duplicate_records  # 每版总记录数（110000）
    progress_interval = base_records_per_version  # 进度报告间隔
    
    # 多线程配置 - 在磁盘I/O瓶颈情况下使用单线程可能更高效
    max_workers = 1  # 磁盘I/O瓶颈时使用单线程避免竞争
    use_multithreading = False  # 关闭多线程，避免磁盘I/O竞争
    
    # 创建输出目录
    output_dir = "generated_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        print("=" * 60)
        print("WPS测试数据生成器")
        print("=" * 60)
        
        # 生成routing_id和fileid的组合
        print("正在生成routing_id和fileid组合...")
        routing_file_combinations = set()
        while len(routing_file_combinations) < combinations_count:
            routing_id = generate_random_string(20)
            fileid = random.randint(1000000000, 9999999999)
            routing_file_combinations.add((routing_id, fileid))
        
        routing_file_combinations = list(routing_file_combinations)
        
        print(f"生成了 {len(routing_file_combinations)} 个唯一的routing_id和fileid组合")
        print(f"每个组合生成 {ids_per_combination} 个id")
        print(f"每版数据构成：")
        print(f"  - 正常数据: {base_records_per_version:,} 条")
        print(f"  - 主键重复数据: {duplicate_records:,} 条 (10%)")
        print(f"  - 每版总计: {total_records_per_version:,} 条")
        print(f"执行模式: {'多线程' if use_multithreading else '单线程（磁盘I/O优化）'}")
        if use_multithreading:
            print(f"线程数: {max_workers}")
        else:
            print("优化策略: 单线程 + 32MB文件缓冲区 + 全内存生成")
        print("=" * 60)
        
        overall_start_time = time.time()
        
        if use_multithreading:
            # 多线程模式
            print(f"开始多线程生成 {total_version} 个版本的数据...")
            
            # 创建线程池
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_version = {}
                for version in range(total_version):
                    future = executor.submit(
                        generate_single_version_data,
                        version,
                        routing_file_combinations,
                        ids_per_combination,
                        batch_size,
                        output_dir,
                        progress_interval
                    )
                    future_to_version[future] = version
                
                # 收集结果
                completed_versions = []
                total_records = 0
                total_time = 0
                
                for future in as_completed(future_to_version):
                    version = future_to_version[future]
                    try:
                        version_num, count, elapsed_time, csv_filename = future.result()
                        completed_versions.append((version_num, count, elapsed_time, csv_filename))
                        total_records += count
                        total_time += elapsed_time
                        print(f"✓ 版本 {version_num} 完成 ({len(completed_versions)}/{total_version})")
                    except Exception as exc:
                        print(f"✗ 版本 {version} 生成失败: {exc}")
                
                # 按版本号排序结果
                completed_versions.sort(key=lambda x: x[0])
                
        else:
            # 单线程模式
            print(f"开始单线程生成 {total_version} 个版本的数据...")
            completed_versions = []
            total_records = 0
            total_time = 0
            
            for version in range(total_version):
                version_num, count, elapsed_time, csv_filename = generate_single_version_data(
                    version,
                    routing_file_combinations,
                    ids_per_combination,
                    batch_size,
                    output_dir,
                    progress_interval
                )
                completed_versions.append((version_num, count, elapsed_time, csv_filename))
                total_records += count
                total_time += elapsed_time
                print(f"✓ 版本 {version} 完成 ({version + 1}/{total_version})")
        
        overall_end_time = time.time()
        overall_elapsed = overall_end_time - overall_start_time
        
        # 输出统计信息
        print("=" * 60)
        print("生成完成！统计信息：")
        print("=" * 60)
        print(f"总版本数: {len(completed_versions)}")
        print(f"总数据量: {total_records:,} 条记录")
        print(f"其中包含:")
        print(f"  - 正常数据: {len(completed_versions) * base_records_per_version:,} 条")
        print(f"  - 主键重复数据: {len(completed_versions) * duplicate_records:,} 条")
        print(f"总耗时: {overall_elapsed:.2f} 秒")
        if use_multithreading:
            print(f"平均每版耗时: {total_time/len(completed_versions):.2f} 秒")
            print(f"并行效率: {(total_time/overall_elapsed):.1f}x")
        print(f"文件保存在: {output_dir}/ 目录下")
        print("=" * 60)
        
        # 显示各版本详细信息
        if len(completed_versions) <= 10:  # 如果版本不多，显示详细信息
            print("各版本详细信息:")
            for version_num, count, elapsed_time, csv_filename in completed_versions:
                print(f"  版本 {version_num}: {count:,} 条记录, {elapsed_time:.2f}秒, {os.path.basename(csv_filename)}")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 