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

# 使用 awk 生成数据
awk -v total_lines="$TOTAL_LINES" -v initial_value="$INITIAL_VALUE" -v increment="$INCREMENT" -v vector_length="$VECTOR_LENGTH" '
BEGIN {
    for (i = 1; i <= total_lines; i++) {
        printf "%d\t", i
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