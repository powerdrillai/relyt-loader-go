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
#  vector            | vecf16(1024)          |           | not null |
# Partition key: RANGE (routing_id)
# Indexes:
#     "content_personal_vector_semantic_insight_vector_bge_m3_dense_pkey" PRIMARY KEY, btree (routing_id, fileid, id)
#     "insight_personal_idx_fileid" btree (fileid)
#     "insight_personal_idx_group_id" btree ("group_id")
# Distributed by: (routing_id, fileid)

# 用于生成测试数据插入content_personal_vector_semantic_insight_vector_bge_m3_dense中:
# 1. 随机生成(routing_id, fileid)的组合，每一万条数据重新生成一次(routing_id, fileid)，每条数据的id都唯一（按照生成的数据顺序，从0开始，依次递增）
# 2. 数据总量为1亿条，每个csv文件包含100万条基础数据, 在基础数据基础上，额外生成10%主键重复的数据
# 3. vector为3维的向量
# 4. 除了(routing_id, fileid, id)，为了效率其余字段可固定

# -- 查找具有多个version的routing_id和fileid组合
# SELECT routing_id, fileid, COUNT(DISTINCT version) as version_count,
# count(*) FROM content_personal_vector_semantic_insight_vector_bge_m3_dense
# GROUP BY routing_id, fileid
# HAVING COUNT(DISTINCT version) > 0
# ORDER BY version_count DESC;

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

def generate_random_string(length=20):
    """生成随机字符串（线程安全）"""
    rand = get_thread_random()
    return ''.join(rand.choices(string.ascii_letters + string.digits, k=length))

def generate_routing_fileid_pair():
    """
    生成一个(routing_id, fileid)组合
    
    Returns:
        tuple: (routing_id, fileid)组合
    """
    routing_id = generate_random_string(20)
    fileid = random.randint(1000000000, 9999999999)
    return routing_id, fileid

def generate_csv_file(file_index, base_records_per_file, start_id, output_dir, duplicate_ratio=0):
    """
    生成单个CSV文件（包含基础数据和10%重复数据）
    每一万条数据重新生成一次(routing_id, fileid)组合
    重复数据随机插入到基础数据中，最后对数据进行打乱
    
    Args:
        file_index: 文件索引
        base_records_per_file: 每个文件的基础记录数（不包含重复数据）
        start_id: 起始ID
        output_dir: 输出目录
        
    Returns:
        tuple: (file_index, total_record_count, elapsed_time, filename)
    """
    print(f"[文件-{file_index}] 开始生成数据...")
    start_time = time.time()
    
    # 固定值 - 为了效率，除了主键外的所有字段都固定
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
    # 生成固定的1024维向量
    FIXED_VECTOR = [0.1] * 3  # 生成1024个0.1的固定向量
    
    # 每8000条数据重新生成一次(routing_id, fileid)组合
    RECORDS_PER_COMBINATION = 8000
    
    filename = os.path.join(output_dir, f"wps_batch_data_{file_index}.csv")
    
    # 使用大缓冲区以提高写入性能
    with open(filename, 'w', newline='', encoding='utf-8', buffering=32*1024*1024) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        # 预分配数据列表
        data_batch = []
        rand = get_thread_random()
        
        # 计算重复数据数量
        duplicate_count = int(base_records_per_file * duplicate_ratio)
        total_records = base_records_per_file + duplicate_count
        
        print(f"[文件-{file_index}] 生成 {base_records_per_file:,} 条基础数据 + {duplicate_count:,} 条重复数据...")
        
        current_routing_id = None
        current_fileid = None
        current_version = None
        
        # 第一步：生成所有基础数据
        print(f"[文件-{file_index}] 生成基础数据...")
        base_records_info = []  # 存储基础数据的主键信息，用于生成重复数据
        
        for i in range(base_records_per_file):
            current_id = start_id + i
            
            # 每RECORDS_PER_COMBINATION条数据重新生成一次(routing_id, fileid)组合
            if i % RECORDS_PER_COMBINATION == 0:
                current_routing_id, current_fileid = generate_routing_fileid_pair()

            # 每个(routing_id, fileid)组合内生成两个version
            # 前RECORDS_PER_COMBINATION/2条记录用version 0，后RECORDS_PER_COMBINATION/2条记录用version 1
            current_version = (i % RECORDS_PER_COMBINATION) // (RECORDS_PER_COMBINATION // 2)
            
            record = [
                str(current_id),            # id (唯一递增)
                current_routing_id,         # routing_id (每RECORDS_PER_COMBINATION条更新一次)
                current_id,                 # chunk_id (与id相同)
                FIXED_CHUNK_TYPE,           # chunk_type (固定)
                FIXED_USER_ID,              # user_id (固定)
                FIXED_CREATOR,              # creator (固定)
                FIXED_SHARER,               # sharer (固定)
                current_fileid,             # fileid (每RECORDS_PER_COMBINATION条更新一次)
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
                current_version,            # version (每RECORDS_PER_COMBINATION条更新一次)
                FIXED_INDEX_UPDATE_TIME,    # index_update_time (固定)
                FIXED_EXT_GROUP,            # ext_group (固定)
                FIXED_VECTOR                # vector (固定)
            ]
            data_batch.append(record)
            
            # 保存基础数据的主键信息，用于生成重复数据
            base_records_info.append((str(current_id), current_routing_id, current_fileid, current_version))
        
        # 第二步：生成重复数据
        print(f"[文件-{file_index}] 生成重复数据...")
        for i in range(duplicate_count):
            # 随机选择一个基础数据的主键
            base_id, base_routing_id, base_fileid, base_version = rand.choice(base_records_info)
            
            # 创建重复主键的记录，其他字段保持固定
            duplicate_record = [
                base_id,                    # id (与基础数据重复)
                base_routing_id,            # routing_id (与基础数据重复)
                int(base_id),               # chunk_id (与基础数据重复)
                FIXED_CHUNK_TYPE,           # chunk_type (固定)
                FIXED_USER_ID,              # user_id (固定)
                FIXED_CREATOR,              # creator (固定)
                FIXED_SHARER,               # sharer (固定)
                base_fileid,                # fileid (与基础数据重复)
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
                base_version,               # version (与基础数据重复)
                FIXED_INDEX_UPDATE_TIME,    # index_update_time (固定)
                FIXED_EXT_GROUP,            # ext_group (固定)
                FIXED_VECTOR                # vector (固定)
            ]
            data_batch.append(duplicate_record)
        
        # 第三步：对数据进行打乱
        print(f"[文件-{file_index}] 打乱数据顺序...")
        rand.shuffle(data_batch)
        
        # 一次性写入所有数据
        print(f"[文件-{file_index}] 开始写入 {total_records:,} 条记录到磁盘...")
        csv_writer.writerows(data_batch)
        csvfile.flush()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"[文件-{file_index}] 完成，文件: {os.path.basename(filename)}")
    print(f"[文件-{file_index}] 写入 {total_records:,} 条记录 (基础: {base_records_per_file:,}, 重复: {duplicate_count:,}), 耗时: {elapsed_time:.2f} 秒")
    
    return file_index, total_records, elapsed_time, filename

def main():
    """
    主函数：生成1亿条测试数据
    
    数据生成策略：
    - 总基础数据量：100,000,000条记录
    - 每个文件基础数据：1,000,000条记录
    - 每个文件重复数据：100,000条记录（10%）
    - 每个文件总计：1,100,000条记录
    - 文件数量：100个CSV文件 (100,000,000 ÷ 1,000,000 = 100)
    - ID生成：全局唯一，从0开始递增
    - routing_id和fileid：每1万条数据重新生成一次组合
    - 其他字段：固定值以提高生成效率
    
    输出：
    - 100个CSV文件：wps_batch_data_0.csv ~ wps_batch_data_99.csv
    - 每个文件包含110万条记录（100万基础 + 10万重复）
    """
    # 配置参数
    total_base_records = 80000
    base_records_per_file = 100000 # 每个文件的基础记录数
    duplicate_ratio = 0.1  # 重复数据比例(0-1)
    
    # 计算文件数量
    total_files = (total_base_records + base_records_per_file - 1) // base_records_per_file  # 向上取整
    
    # 计算(routing_id, fileid)组合数量
    # 每1万条数据生成一次组合，总共需要的组合数
    records_per_combination = 10_000
    total_combinations = (total_base_records + records_per_combination - 1) // records_per_combination
    
    # 多线程配置
    use_multithreading = False
    max_workers = 4  # 使用4个线程并行生成文件
    
    # 创建输出目录
    output_dir = "generated_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        print("=" * 80)
        print("WPS批量测试数据生成器")
        print("=" * 80)
        print(f"总基础数据量: {total_base_records:,} 条记录")
        print(f"重复数据比例: {duplicate_ratio:.0%}")
        print(f"预计总数据量: {int(total_base_records * (1 + duplicate_ratio)):,} 条记录")
        print(f"文件数量: {total_files} 个")
        print(f"每文件基础记录数: {base_records_per_file:,} 条")
        print(f"每文件重复记录数: {int(base_records_per_file * duplicate_ratio):,} 条")
        print(f"每文件总记录数: {int(base_records_per_file * (1 + duplicate_ratio)):,} 条")
        print(f"(routing_id, fileid)组合生成策略: 每 {records_per_combination:,} 条记录生成一次")
        print(f"预计总组合数: {total_combinations:,} 个")
        print(f"执行模式: {'多线程' if use_multithreading else '单线程'}")
        if use_multithreading:
            print(f"线程数: {max_workers}")
        print(f"输出目录: {output_dir}/")
        print("=" * 80)
        
        overall_start_time = time.time()
        
        if use_multithreading:
            # 多线程模式
            print(f"开始多线程生成 {total_files} 个文件...")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_file = {}
                for file_index in range(total_files):
                    start_id = file_index * base_records_per_file
                    
                    # 计算当前文件的基础记录数（最后一个文件可能不满）
                    current_base_records = min(base_records_per_file, total_base_records - start_id)
                    
                    future = executor.submit(
                        generate_csv_file,
                        file_index,
                        current_base_records,
                        start_id,
                        output_dir,
                        duplicate_ratio
                    )
                    future_to_file[future] = file_index
                
                # 收集结果
                completed_files = []
                total_generated_records = 0
                total_time = 0
                
                for future in as_completed(future_to_file):
                    file_index = future_to_file[future]
                    try:
                        file_idx, record_count, elapsed_time, filename = future.result()
                        completed_files.append((file_idx, record_count, elapsed_time, filename))
                        total_generated_records += record_count
                        total_time += elapsed_time
                        print(f"✓ 文件 {file_idx} 完成 ({len(completed_files)}/{total_files})")
                    except Exception as exc:
                        print(f"✗ 文件 {file_index} 生成失败: {exc}")
                
                # 按文件索引排序结果
                completed_files.sort(key=lambda x: x[0])
                
        else:
            # 单线程模式
            print(f"开始单线程生成 {total_files} 个文件...")
            completed_files = []
            total_generated_records = 0
            total_time = 0
            
            for file_index in range(total_files):
                start_id = file_index * base_records_per_file
                
                # 计算当前文件的基础记录数（最后一个文件可能不满）
                current_base_records = min(base_records_per_file, total_base_records - start_id)
                
                file_idx, record_count, elapsed_time, filename = generate_csv_file(
                    file_index,
                    current_base_records,
                    start_id,
                    output_dir,
                    duplicate_ratio
                )
                completed_files.append((file_idx, record_count, elapsed_time, filename))
                total_generated_records += record_count
                total_time += elapsed_time
                print(f"✓ 文件 {file_index} 完成 ({file_index + 1}/{total_files})")
        
        overall_end_time = time.time()
        overall_elapsed = overall_end_time - overall_start_time
        
        # 计算实际的基础记录数和重复记录数
        actual_base_records = min(total_base_records, sum(1 for _ in completed_files) * base_records_per_file)
        estimated_duplicate_records = int(actual_base_records * duplicate_ratio)
        actual_combinations = (actual_base_records + records_per_combination - 1) // records_per_combination
        
        # 输出统计信息
        print("=" * 80)
        print("生成完成！统计信息：")
        print("=" * 80)
        print(f"成功生成文件数: {len(completed_files)}")
        print(f"总数据量: {total_generated_records:,} 条记录")
        print(f"  - 基础数据: 约 {actual_base_records:,} 条")
        print(f"  - 重复数据: 约 {estimated_duplicate_records:,} 条")
        print(f"ID范围: 0 ~ {actual_base_records - 1}")
        print(f"(routing_id, fileid)组合生成策略: 每 {records_per_combination:,} 条记录生成一次")
        print(f"实际生成的组合数: 约 {actual_combinations:,} 个")
        print(f"总耗时: {overall_elapsed:.2f} 秒")
        if use_multithreading:
            print(f"平均每文件耗时: {total_time/len(completed_files):.2f} 秒")
            print(f"并行效率: {(total_time/overall_elapsed):.1f}x")
        print(f"平均生成速度: {total_generated_records/overall_elapsed:,.0f} 条/秒")
        print(f"文件保存在: {output_dir}/ 目录下")
        print("=" * 80)
        
        # 显示文件列表（如果文件不多的话）
        if len(completed_files) <= 20:
            print("生成的文件列表:")
            for file_idx, record_count, elapsed_time, filename in completed_files:
                print(f"  {os.path.basename(filename)}: {record_count:,} 条记录, {elapsed_time:.2f}秒")
        else:
            print(f"生成了 {len(completed_files)} 个文件，文件名格式: wps_batch_data_0.csv ~ wps_batch_data_{len(completed_files)-1}.csv")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()