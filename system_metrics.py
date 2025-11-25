import threading
from dataclasses import dataclass
from typing import *

import psutil


@dataclass
class SystemMetrics:
    # --- Memory (bytes) ---
    memory_total: int
    memory_used: int
    memory_used_percent: float
    memory_available: int
    memory_free: int

    # --- Threads / CPU ---
    threads_num: int  # 全スレッド数
    threads_running: int  # 走っているスレッド数
    cpu_usage_percent: float  # 全体CPU使用率
    cpu_freq_mhz: Optional[float] = None  # 現在のCPUクロック

    # --- CPU temperature (取得できる環境だけ) ---
    cpu_temp_c: Optional[float] = None

    # --- Disk / IO ---
    disk_total: Optional[int] = None  # bytes
    disk_used: Optional[int] = None  # bytes
    disk_used_percent: Optional[float] = None
    disk_read_bytes: Optional[int] = None
    disk_write_bytes: Optional[int] = None

    # --- Network ---
    net_bytes_sent: Optional[int] = None
    net_bytes_recv: Optional[int] = None

    @property
    def memory_total_gb(self) -> float:
        return self.memory_total / (1024 ** 3)
    # enddef

    @property
    def memory_used_gb(self) -> float:
        return self.memory_used / (1024 ** 3)
    # enddef

    @property
    def memory_availale_gb(self) -> float:
        return self.memory_available / (1024 ** 3)
    # enddef

    @property
    def memory_free_gb(self) -> float:
        return self.memory_free / (1024 ** 3)
    # enddef

    @property
    def gpu_mem_used_gb(self) -> Optional[float]:
        if self.gpu_mem_used is None:
            return None
        # endif

        return self.gpu_mem_used / (1024 ** 3)
    # enddef

    @classmethod
    def init(cls) -> 'SystemMetrics':
        vm = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=None)
        threads_num = psutil.cpu_count()
        threads_running = len(threading.enumerate())

        # CPU 周波数
        cpu_freq = psutil.cpu_freq()
        cpu_freq_mhz = cpu_freq.current if cpu_freq is not None else None

        # Disk
        disk_usage = psutil.disk_usage("/")
        disk_io = psutil.disk_io_counters()

        # Network
        net_io = psutil.net_io_counters()

        # CPU 温度は取れない環境も多い
        cpu_temp_c = None
        try:
            temps = psutil.sensors_temperatures()
            # ラベル名は環境依存：一例として "coretemp" や "cpu-thermal" 等を見る
            for key in ("coretemp", "cpu-thermal", "cpu_thermal"):
                if key in temps and temps[key]:
                    cpu_temp_c = temps[key][0].current
                    break
                # endif
            # endfor
        except Exception:
            cpu_temp_c = None
        # endtry

        return cls(
            # memory
            memory_total=vm.total,
            memory_used=vm.used,
            memory_used_percent=vm.percent,
            memory_available=vm.available,
            memory_free=vm.free,
            # cpu / threads
            threads_num=threads_num,
            threads_running=threads_running,
            cpu_usage_percent=cpu_percent,
            cpu_freq_mhz=cpu_freq_mhz,
            cpu_temp_c=cpu_temp_c,
            # disk
            disk_total=disk_usage.total,
            disk_used=disk_usage.used,
            disk_used_percent=disk_usage.percent,
            disk_read_bytes=disk_io.read_bytes if disk_io else None,
            disk_write_bytes=disk_io.write_bytes if disk_io else None,
            # network
            net_bytes_sent=net_io.bytes_sent if net_io else None,
            net_bytes_recv=net_io.bytes_recv if net_io else None,
            )
    # enddef
