import asyncio
import copy
import datetime
import gzip
import os
import pickle
from time import time
from typing import Callable, Optional, Any

import pytz
from tqdm import tqdm  # 命令行进度条库

# 项目内部模块导入
import utils.constants as constants  # 常量定义（路径、正则表达式等）
import utils.frozen as frozen  # 数据冻结/持久化工具
from updates.epg import get_epg  # 获取电子节目指南(EPG)数据
from updates.epg.tools import write_to_xml, compress_to_gz  # EPG数据写入XML和压缩
from updates.subscribe import get_channels_by_subscribe_urls  # 从订阅地址抓取频道数据
from utils.aggregator import ResultAggregator  # 结果聚合器（整理、缓存频道数据）
from utils.channel import get_channel_items, append_total_data, test_speed  # 频道数据处理、测速工具
from utils.config import config  # 全局配置对象（读取配置文件）
from utils.i18n import t  # 国际化翻译函数
from utils.speed import clear_cache  # 清除测速缓存
from utils.tools import (  # 工具函数集合
    get_pbar_remaining,  # 计算进度条剩余时间
    process_nested_dict,  # 处理嵌套字典（过滤、去重等）
    format_interval,  # 格式化时间间隔（秒转时分秒）
    check_ipv6_support,  # 检测系统是否支持IPv6
    get_urls_from_file,  # 从文件读取URL列表
    get_version_info,  # 获取项目版本信息
    get_urls_len,  # 统计嵌套字典中的URL总数
    get_public_url,  # 获取服务公网访问地址
    parse_times,  # 解析定时更新时间配置
    to_serializable,  # 将数据转换为可序列化格式（兼容pickle）
)
from utils.types import CategoryChannelData  # 分类频道数据类型（字典嵌套结构）
from utils.whitelist import load_whitelist_maps, get_section_entries  # 白名单加载工具

# 进度回调函数类型定义：接收任意参数，返回任意类型
ProgressCallback = Callable[..., Any]


class UpdateSource:
    """
    节目源更新核心类
    功能：异步抓取订阅频道、获取EPG、测速筛选、数据聚合、缓存持久化、定时更新
    """
    def __init__(self):
        """初始化类成员变量，所有跨阶段共享的数据都在这里定义"""
        # 黑白名单相关
        self.whitelist_maps = None  # 白名单映射表（频道名->分类等信息）
        self.blacklist = None  # 黑名单URL列表（需要过滤的地址）

        # 进度回调相关
        self.update_progress: Optional[ProgressCallback] = None  # 外部传入的进度回调函数（用于UI更新）
        self.run_ui = False  # 是否运行在UI模式（True则触发进度回调）

        # 异步任务相关
        self.tasks: list[asyncio.Task] = []  # 异步任务列表（用于取消任务）

        # 频道数据相关
        self.channel_items: CategoryChannelData = {}  # 从白名单加载的原始频道项
        self.channel_names: list[str] = []  # 所有待处理的频道名称列表
        self.subscribe_result = {}  # 从订阅地址抓取的频道结果
        self.epg_result = {}  # EPG抓取结果
        self.channel_data: CategoryChannelData = {}  # 合并后的最终频道数据

        # 进度条相关
        self.pbar: Optional[tqdm] = None  # 命令行进度条对象
        self.total = 0  # 进度条总数（测速URL数量）
        self.start_time = None  # 测速开始时间（用于计算剩余时间）

        # 异步控制相关
        self.stop_event: Optional[asyncio.Event] = None  # 停止事件（用于终止定时任务）
        self.ipv6_support = False  # 系统是否支持IPv6（影响测速和数据过滤）
        self.now = None  # 当前时间（用于日志和UI展示）

        # 结果聚合器
        self.aggregator: Optional[ResultAggregator] = None  # 数据聚合器（核心数据处理工具）

    # ----------------------------
    # 进度条更新方法
    # ----------------------------
    def pbar_update(self, name: str = "", item_name: str = ""):
        """
        更新进度条和外部进度回调
        :param name: 进度名称（如"测速"）
        :param item_name: 项目名称（如"URL"）
        """
        if not self.pbar:
            return
        # 防止进度条溢出
        if self.pbar.n < self.total:
            self.pbar.update()  # 更新命令行进度条
            # 计算剩余总数和剩余时间
            remaining_total = self.total - self.pbar.n
            remaining_time = get_pbar_remaining(n=self.pbar.n, total=self.total, start_time=self.start_time)
            # 如果有外部回调，触发UI进度更新
            if self.update_progress:
                self.update_progress(
                    t("msg.progress_desc").format(
                        name=name,
                        remaining_total=remaining_total,
                        item_name=item_name,
                        remaining_time=remaining_time,
                    ),
                    int((self.pbar.n / self.total) * 100),  # 进度百分比
                )

    # ----------------------------
    # IO: 缓存加载与保存（gzip+pickle）
    # ----------------------------
    def _load_cache(self) -> dict:
        """
        加载缓存数据（从gzip压缩的pickle文件读取）
        :return: 缓存字典，加载失败返回空字典
        """
        # 未开启历史记录或缓存文件不存在，直接返回空
        if not (config.open_history and os.path.exists(constants.cache_path)):
            return {}
        try:
            # 以二进制只读模式打开压缩文件
            with gzip.open(constants.cache_path, "rb") as f:
                return pickle.load(f) or {}  # 反序列化数据
        except Exception:  # 捕获任何异常（文件损坏、版本不兼容等）
            return {}

    def _save_cache(self, cache_result: dict):
        """
        保存缓存数据（序列化为pickle后用gzip压缩）
        :param cache_result: 要保存的缓存数据
        """
        # 将数据转换为可序列化格式（处理datetime等不兼容类型）
        serializable = to_serializable(cache_result or {})
        # 获取缓存目录路径，不存在则创建
        cache_dir = os.path.dirname(constants.cache_path)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        # 以二进制写入模式打开压缩文件，保存数据
        with gzip.open(constants.cache_path, "wb") as f:
            pickle.dump(serializable, f)

    # ----------------------------
    # stage 1: 初始化准备（加载配置、黑白名单、频道数据）
    # ----------------------------
    def _prepare_channel_data(self):
        """
        阶段1：初始化准备工作（本地操作，无网络请求）
        1. 加载白名单映射和黑名单URL
        2. 整理待处理频道列表
        3. 加载历史冻结数据
        """
        # 加载白名单映射表（从配置文件读取）
        self.whitelist_maps = load_whitelist_maps(constants.whitelist_path)
        # 加载黑名单URL列表（从文件读取）
        self.blacklist = get_urls_from_file(constants.blacklist_path, pattern_search=False)
        # 根据黑白名单筛选得到原始频道项
        self.channel_items = get_channel_items(self.whitelist_maps, self.blacklist)
        self.channel_data = {}  # 初始化最终频道数据

        # 提取所有频道名称到列表（去重）
        self.channel_names = [
            name for channel_obj in self.channel_items.values() for name in channel_obj.keys()
        ]

        # 开启历史记录时，加载已冻结的持久化数据
        if config.open_history and os.path.exists(constants.frozen_path):
            frozen.load(constants.frozen_path)

    # ----------------------------
    # stage 2: 异步并发抓取订阅数据和EPG数据
    # ----------------------------
    async def _fetch_subscribe(self, channel_names: list[str]) -> dict:
        """
        异步抓取订阅地址的频道数据
        :param channel_names: 待抓取的频道名称列表（白名单过滤后）
        :return: 抓取到的频道数据字典
        """
        # 从订阅配置文件中读取白名单订阅URL和默认订阅URL
        whitelist_subscribe_urls, default_subscribe_urls = get_section_entries(
            constants.subscribe_path,
            pattern=constants.url_pattern,
        )
        # 合并URL并去重（字典键去重法，保持顺序）
        subscribe_urls = list(dict.fromkeys(whitelist_subscribe_urls + default_subscribe_urls))
        # 打印订阅URL统计信息
        print(
            t("msg.subscribe_urls_whitelist_total").format(
                default_count=len(default_subscribe_urls),
                whitelist_count=len(whitelist_subscribe_urls),
                total=len(subscribe_urls),
            )
        )
        # 无订阅URL时打印提示并返回空
        if not subscribe_urls:
            print(t("msg.no_subscribe_urls").format(file=constants.subscribe_path))
            return {}

        # 调用订阅抓取函数，异步获取频道数据
        return await get_channels_by_subscribe_urls(
            subscribe_urls,
            names=channel_names,  # 只抓取白名单内的频道
            whitelist=whitelist_subscribe_urls,  # 白名单订阅URL（优先级更高）
            callback=self.update_progress,  # 进度回调函数
        )

    async def _fetch_epg(self, channel_names: list[str]) -> dict:
        """
        异步获取电子节目指南(EPG)数据
        :param channel_names: 待获取EPG的频道名称列表
        :return: EPG数据字典
        """
        return await get_epg(channel_names, callback=self.update_progress)

    async def visit_page(self, channel_names: list[str] = None):
        """
        并发执行订阅抓取和EPG抓取任务
        :param channel_names: 待处理的频道名称列表
        """
        channel_names = channel_names or []

        # 存储待执行的异步任务（任务名: 任务对象）
        cors: list[tuple[str, asyncio.Future]] = []
        # 根据配置开关决定是否执行订阅抓取
        if config.open_method.get("subscribe"):
            cors.append(("subscribe_result", asyncio.create_task(self._fetch_subscribe(channel_names))))
        # 根据配置开关决定是否执行EPG抓取
        if config.open_method.get("epg"):
            cors.append(("epg_result", asyncio.create_task(self._fetch_epg(channel_names))))

        # 无任务时直接返回
        if not cors:
            return

        # 并发执行所有异步任务，return_exceptions=True：单个任务失败不影响其他任务
        results = await asyncio.gather(*(c for _, c in cors), return_exceptions=True)
        # 遍历任务结果，赋值给对应的类成员变量
        for (attr, _), res in zip(cors, results):
            if isinstance(res, Exception):
                # 任务执行失败，打印错误信息并赋值为空字典
                print(f"{attr} failed: {res}")
                setattr(self, attr, {})
            else:
                # 任务执行成功，赋值结果
                setattr(self, attr, res)

    def _write_epg_files_if_needed(self):
        """
        将EPG结果写入XML文件并压缩为gz包（如果有EPG数据）
        """
        if not self.epg_result:
            return
        # 写入XML文件
        write_to_xml(self.epg_result, constants.epg_result_path)
        # 压缩为gz包
        compress_to_gz(constants.epg_result_path, constants.epg_gz_result_path)

    # ----------------------------
    # stage 3: 结果聚合器生命周期管理（启动/停止）
    # ----------------------------
    async def _start_aggregator(self, cache: dict):
        """
        启动结果聚合器
        :param cache: 加载的历史缓存数据
        """
        self.aggregator = ResultAggregator(
            base_data=self.channel_data,  # 基础频道数据
            first_channel_name=self.channel_names[0] if self.channel_names else None,  # 首个频道名（用于初始化）
            ipv6_support=self.ipv6_support,  # IPv6支持标记
            write_interval=2.0,  # 数据写入间隔（秒）
            result=cache,  # 历史缓存数据
        )
        await self.aggregator.start()  # 启动聚合器

    async def _stop_aggregator(self):
        """停止结果聚合器，释放资源"""
        if self.aggregator:
            await self.aggregator.stop()
            self.aggregator = None  # 置空避免内存泄漏

    # ----------------------------
    # stage 4: 频道URL速度测试（筛选可用地址）
    # ----------------------------
    async def _run_speed_test(self) -> CategoryChannelData:
        """
        异步执行频道URL速度测试，筛选可用地址
        :return: 测速后的频道数据
        """
        # 统计原始频道数据中的URL总数
        urls_total = get_urls_len(self.channel_data)
        # 深拷贝频道数据，避免修改原始数据
        test_data = copy.deepcopy(self.channel_data)

        # 预处理测试数据：过滤黑名单、去重、IPv6适配
        process_nested_dict(
            test_data,
            seen=set(),
            filter_host=config.speed_test_filter_host,  # 测速过滤的主机名
            ipv6_support=self.ipv6_support,  # IPv6支持标记
        )
        # 统计预处理后需要测速的URL总数
        self.total = get_urls_len(test_data)

        # 打印测速统计信息
        print(t("msg.total_urls_need_test_speed").format(total=urls_total, speed_total=self.total))

        # 无URL需要测速时，强制刷新聚合器并返回空
        if self.total <= 0:
            self.aggregator.is_last = True
            await self.aggregator.flush_once(force=True)
            return {}
        # UI模式下触发进度回调
        if self.update_progress:
            self.update_progress(
                t("msg.progress_speed_test").format(total=urls_total, speed_total=self.total),
                0,
            )

        # 记录测速开始时间（用于计算剩余时间）
        self.start_time = time()
        # 初始化命令行进度条
        self.pbar = tqdm(total=self.total, desc=t("pbar.speed_test"))
        try:
            # 异步执行测速任务
            return await test_speed(
                test_data,
                ipv6=self.ipv6_support,  # 是否启用IPv6测速
                # 测速进度回调（每完成一个URL调用一次）
                callback=lambda: self.pbar_update(name=t("pbar.speed_test"), item_name=t("pbar.url")),
                # 每完成一个URL测试，将结果添加到聚合器
                on_task_complete=self.aggregator.add_item,
            )
        finally:
            # 无论是否异常，都关闭进度条
            if self.pbar:
                self.pbar.close()
                self.pbar = None

    # ----------------------------
    # stage 5: UI模式下的更新完成通知
    # ----------------------------
    def _notify_ui_finished(self, main_start_time: float):
        """
        UI模式下，发送更新完成通知
        :param main_start_time: 程序主流程开始时间
        """
        if not self.run_ui:
            return

        # 获取服务配置状态
        open_service = config.open_service
        service_tip = t("msg.service_tip") if open_service else ""

        # 根据配置拼接提示信息
        tip = (
            # 仅运行服务，不更新数据
            t("msg.service_run_success").format(service_tip=service_tip)
            if open_service and config.open_update is False
            # 更新数据完成
            else t("msg.update_completed").format(
                time=format_interval(time() - main_start_time),  # 格式化耗时
                service_tip=service_tip,
            )
        )

        # 触发UI进度回调，传递完成状态
        if self.update_progress:
            self.update_progress(
                tip,
                100,  # 进度100%
                finished=True,  # 标记为完成
                url=f"{get_public_url()}" if open_service else None,  # 服务公网地址
                now=self.now,  # 当前时间
            )

    # ----------------------------
    # 主流程：串联所有阶段
    # ----------------------------
    async def main(self):
        """程序主入口，按顺序执行所有更新阶段"""
        try:
            # 记录主流程开始时间
            main_start_time = time()

            # 未开启更新开关时，直接通知UI完成
            if not config.open_update:
                self._notify_ui_finished(main_start_time)
                return

            # 阶段1：初始化准备（加载配置、黑白名单、频道数据）
            self._prepare_channel_data()

            # 无待处理频道时，通知UI完成
            if not self.channel_names:
                print(t("msg.no_channel_names").format(file=config.source_file))
                self._notify_ui_finished(main_start_time)
                return

            # 阶段2：异步并发抓取订阅和EPG数据
            await self.visit_page(self.channel_names)
            self.tasks = []  # 清空异步任务列表
            self._write_epg_files_if_needed()  # 保存EPG数据

            # 合并订阅数据到最终频道数据
            append_total_data(
                self.channel_items.items(),
                self.channel_data,
                self.subscribe_result,
                self.whitelist_maps,
                self.blacklist,
            )

            # 加载历史缓存数据
            cache = self._load_cache()

            # 阶段3：启动结果聚合器
            await self._start_aggregator(cache)
            try:
                # 阶段4：执行速度测试（如果开启开关）
                if config.open_speed_test:
                    clear_cache()  # 清除历史测速缓存
                    await self._run_speed_test()
                else:
                    # 未开启测速，强制刷新聚合器
                    self.aggregator.is_last = True
                    await self.aggregator.flush_once(force=True)

            finally:
                # 无论是否异常，都执行缓存保存和聚合器停止
                if config.open_history:
                    # 保存缓存数据到文件
                    self._save_cache(self.aggregator.result)
                    # 冻结数据持久化
                    frozen.save(constants.frozen_path)
                # 停止聚合器
                await self._stop_aggregator()

            # 打印更新完成信息
            print(
                t("msg.update_completed").format(
                    time=format_interval(time() - main_start_time),
                    service_tip="",
                )
            )
            # 通知UI更新完成
            self._notify_ui_finished(main_start_time)

        except asyncio.exceptions.CancelledError:
            # 捕获任务取消异常，打印提示信息
            print(t("msg.update_cancelled"))

    # ----------------------------
    # 生命周期控制：启动/停止定时任务
    # ----------------------------
    async def start(self, callback=None):
        """
        启动更新程序（支持单次更新或定时更新）
        :param callback: 外部传入的进度回调函数（用于UI）
        """
        # 默认回调函数（空操作）
        def default_callback(*args, **kwargs):
            pass

        # 设置进度回调函数，无传入则使用默认空函数
        self.update_progress = callback or default_callback
        # 是否运行在UI模式（有回调函数则为True）
        self.run_ui = True if callback else False

        # UI模式下，先触发"检测IPv6支持"的进度回调
        if self.run_ui:
            self.update_progress(t("msg.check_ipv6_support"), 0)

        # 检测系统IPv6支持（配置强制开启或自动检测）
        self.ipv6_support = config.ipv6_support or check_ipv6_support()

        # 非GitHub Actions环境，且配置了定时更新，则启动调度器
        if not os.getenv("GITHUB_ACTIONS") and (config.update_interval or config.update_times):
            await self.scheduler(asyncio.Event())
        # 配置了启动时更新，则执行单次更新
        elif config.update_startup:
            await self.main()

    def stop(self):
        """停止所有异步任务和定时更新"""
        # 取消所有未完成的异步任务
        for task in self.tasks:
            task.cancel()
        self.tasks = []  # 清空任务列表

        # 关闭命令行进度条
        if self.pbar:
            self.pbar.close()
            self.pbar = None

        # 设置停止事件，终止定时调度循环
        if self.stop_event:
            self.stop_event.set()

    async def scheduler(self, stop_event: asyncio.Event):
        """
        定时调度器（支持按时间点/按间隔更新）
        :param stop_event: 停止事件（用于终止调度循环）
        """
        self.stop_event = stop_event
        # 加载配置的时区（如Asia/Shanghai）
        tz = pytz.timezone(config.time_zone)
        # 获取定时更新模式（time: 按时间点，interval: 按间隔）
        mode = config.update_mode
        # 解析定时更新时间配置（如["00:00", "12:00"]）
        update_times = parse_times(config.update_times)

        try:
            # 获取当前时间（带时区）
            self.now = datetime.datetime.now(tz)
            # 配置了启动时更新，则先执行一次更新
            if config.update_startup:
                await self.main()

            # 定时更新循环（直到收到停止事件）
            while not stop_event.is_set():
                # 更新当前时间
                self.now = datetime.datetime.now(tz)

                if mode == "time" and update_times:
                    # 时间点模式：计算下次更新时间
                    candidates = []
                    for h, m in update_times:
                        # 构造当天的目标时间
                        candidate = self.now.replace(hour=h, minute=m, second=0, microsecond=0)
                        # 目标时间已过，则顺延到次日
                        if candidate <= self.now:
                            candidate = candidate + datetime.timedelta(days=1)
                        candidates.append(candidate)
                    # 取最近的一个目标时间
                    next_time = min(candidates)
                    # 计算需要等待的秒数
                    wait_seconds = (next_time - self.now).total_seconds()
                    # 打印下次更新时间
                    print(t("msg.schedule_update_time").format(time=next_time.strftime("%Y-%m-%d %H:%M:%S")))

                    try:
                        # 等待目标时间或停止事件
                        await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                        if stop_event.is_set():
                            break  # 收到停止事件，退出循环
                    except asyncio.TimeoutError:
                        # 等待超时，执行更新任务
                        self.now = datetime.datetime.now(tz)
                        await self.main()
                else:
                    # 间隔模式：按配置的小时数循环更新
                    next_time = self.now + datetime.timedelta(hours=config.update_interval)
                    print(t("msg.schedule_update_time").format(time=next_time.strftime("%Y-%m-%d %H:%M:%S")))

                    try:
                        # 等待间隔时间或停止事件
                        await asyncio.wait_for(stop_event.wait(), timeout=config.update_interval * 3600)
                    except asyncio.TimeoutError:
                        # 等待超时，执行更新任务
                        self.now = datetime.datetime.now(tz)
                        await self.main()

        except asyncio.CancelledError:
            # 捕获任务取消异常，打印提示信息
            print(t("msg.schedule_cancelled"))


if __name__ == "__main__":
    """命令行直接运行时的入口"""
    # 获取项目版本信息并打印
    info = get_version_info()
    print(t("msg.version_info").format(name=info["name"], version=info["version"]))
    # 创建异步事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # 初始化更新程序并启动
    update_source = UpdateSource()
    loop.run_until_complete(update_source.start())
