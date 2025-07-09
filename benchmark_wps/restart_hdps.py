#!/usr/bin/env python3
import os
import time
import subprocess
import threading
import signal
import sys

def check_process_running(process):
    """检查进程是否还在运行"""
    return process.poll() is None

def restart_database():
    """重启数据库"""
    try:
        print("重启数据库...")
        result = subprocess.run("gpstop -iar", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if result.returncode == 0:
            print("数据库重启成功")
            return True
        else:
            print(f"数据库重启失败: {result.stderr}")
            return False
    except Exception as e:
        print(f"重启数据库时发生异常: {e}")
        return False

def monitor_and_restart(process, process_id):
    """监控进程并在需要时重启数据库"""
    # 等待1分钟
    time.sleep(60)
    
    # 检查进程是否还在运行
    if check_process_running(process):
        print(f"进程 {process_id} 运行1分钟后仍在运行，重启数据库一次")
        
        # 只重启一次数据库
        if not restart_database():
            print("数据库重启失败")
        else:
            print("数据库重启完成，继续等待进程完成")
    else:
        print(f"进程 {process_id} 在1分钟内已完成")

def run_benchmark_with_monitoring():
    """运行benchmark并监控"""
    for i in range(100):
        print(f"开始第 {i+1} 次测试")
        
        # 启动benchmark进程
        process = subprocess.Popen(
            f"go run benchmark_wps.go > bench_{i}.log 2>&1",
            shell=True
        )
        
        # 启动监控线程
        monitor_thread = threading.Thread(
            target=monitor_and_restart,
            args=(process, i+1)
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # 等待进程完成
        process.wait()
        
        print(f"第 {i+1} 次测试完成")
        
        # 等待一段时间再开始下次测试
        if i < 9:  # 最后一次不需要等待
            print("等待30秒后开始下次测试...")
            time.sleep(30)

def signal_handler(signum, frame):
    """信号处理器"""
    print("收到中断信号，正在停止...")
    sys.exit(0)

if __name__ == "__main__":
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    run_benchmark_with_monitoring()
