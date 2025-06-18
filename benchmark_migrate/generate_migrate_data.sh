#!/bin/bash

# 定义输出文件路径
OUTPUT_FILE="output.csv"

# 定义向量的长度
VECTOR_LENGTH=3

# 定义初始值
INITIAL_VALUE=0.001

# 定义增量
INCREMENT=0.001

# 定义总行数
TOTAL_LINES=10000000

# split line number
SPLIT_LINE=1000000

# 定义分割后的文件名前缀
SPLIT_PREFIX="benchmark_test"

# 定义gouting_id配置数组
declare -A GROUP_LINES=(
    [100]=1000000
    [200]=2000000
    [300]=3000000
)

# 其他gouting_id的起始值
OTHER_GROUP_START=1000

# 使用 awk 生成数据
awk -v total_lines="$TOTAL_LINES" \
    -v initial_value="$INITIAL_VALUE" \
    -v increment="$INCREMENT" \
    -v vector_length="$VECTOR_LENGTH" \
    -v group_100_lines="${GROUP_LINES[100]}" \
    -v group_200_lines="${GROUP_LINES[200]}" \
    -v group_300_lines="${GROUP_LINES[300]}" \
    -v other_group_start="$OTHER_GROUP_START" '
BEGIN {
    line_count = 0
    other_group_count = 0
    
    for (i = 1; i <= total_lines; i++) {
        # 确定当前行的gouting_id
        if (i <= group_100_lines) {
            routing_id = 100
        } else if (i <= group_100_lines + group_200_lines) {
            routing_id = 200
        } else if (i <= group_100_lines + group_200_lines + group_300_lines) {
            routing_id = 300
        } else {
            # 其他gouting_id从1000开始递增
            routing_id = other_group_start + int(other_group_count / 1000000)
            other_group_count++
        }
        
        printf "%d\t", i
        printf "%d\t", routing_id
        printf "ext_%d\t", i
        printf "["
        for (j = 1; j <= vector_length; j++) {
            printf "%.3f", initial_value + (i - 1) * increment
            if (j < vector_length) {
                printf ", "
            }
        }
        printf "]\n"
    }
}' > "$OUTPUT_FILE"

echo "Data has been written to $OUTPUT_FILE"
# split the file with 100000 lines per file
split -l "$SPLIT_LINE" "$OUTPUT_FILE" "${SPLIT_PREFIX}_"