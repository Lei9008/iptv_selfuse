import os
import sys
import time

# 向上添加一级目录到Python环境路径，解决跨目录模块导入问题（当前脚本大概率在service目录，需导入上层utils等模块）
sys.path.append(os.path.dirname(sys.path[0]))
# Flask核心模块导入：构建Web应用、静态文件发送、响应构造、请求处理、JSON返回、流式响应
from flask import Flask, send_from_directory, make_response, request, jsonify, Response
# 项目内部工具函数：获取结果文件内容、资源路径转换、获取公网访问地址
from utils.tools import get_result_file_content, resource_path, get_public_url
# 项目全局配置对象（读取配置文件中的端口、开关等配置）
from utils.config import config
# 项目常量定义（文件路径、提示信息等）
import utils.constants as constants
# 程序退出清理模块：用于注册程序退出时的收尾函数（如停止RTMP服务）
import atexit
# RTMP/HLS相关服务模块：流服务启动/停止、路径配置、锁对象、流状态管理
from service.rtmp import start_rtmp_service, stop_rtmp_service, app_rtmp_url, hls_temp_path, \
    STREAMS_LOCK, hls_running_streams, start_hls_to_rtmp, hls_last_access, \
    HLS_WAIT_TIMEOUT, HLS_WAIT_INTERVAL
# 日志模块：用于控制Flask默认日志输出级别
import logging
# 国际化翻译函数：支持多语言提示信息
from utils.i18n import t
# Werkzeug安全工具：处理文件名，防止路径遍历攻击
from werkzeug.utils import secure_filename
# MIME类型猜测：用于返回正确的文件响应头
import mimetypes

# 初始化Flask Web应用实例
app = Flask(__name__)
# 获取Flask默认的werkzeug日志对象（用于控制访问日志输出）
log = logging.getLogger('werkzeug')
# 关闭werkzeug冗余日志，仅保留ERROR级别日志（减少控制台输出干扰）
log.setLevel(logging.ERROR)


# ----------------------------
# 核心接口：节目源文件访问（多格式/多类型）
# ----------------------------
@app.route("/")
def show_index():
    """
    根路径接口：返回默认节目源文件
    优先返回HLS格式（开启RTMP时），否则返回最终生成的节目源文件
    格式优先返回m3u（开启m3u结果开关时），否则返回txt
    """
    return get_result_file_content(
        path=constants.hls_result_path if config.open_rtmp else config.final_file,
        file_type="m3u" if config.open_m3u_result else "txt"
    )


@app.route("/txt")
def show_txt():
    """返回最终节目源的txt格式文件"""
    return get_result_file_content(path=config.final_file, file_type="txt")


@app.route("/ipv4/txt")
def show_ipv4_txt():
    """返回IPv4专属节目源的txt格式文件"""
    return get_result_file_content(path=constants.ipv4_result_path, file_type="txt")


@app.route("/ipv6/txt")
def show_ipv6_txt():
    """返回IPv6专属节目源的txt格式文件"""
    return get_result_file_content(path=constants.ipv6_result_path, file_type="txt")


@app.route("/hls")
def show_hls():
    """
    返回HLS格式节目源文件
    格式优先返回m3u（开启m3u结果开关时），否则返回txt
    """
    return get_result_file_content(path=constants.hls_result_path,
                                   file_type="m3u" if config.open_m3u_result else "txt")


@app.route("/hls/txt")
def show_hls_txt():
    """返回HLS格式节目源的txt格式文件"""
    return get_result_file_content(path=constants.hls_result_path, file_type="txt")


@app.route("/hls/ipv4/txt")
def show_hls_ipv4_txt():
    """返回HLS+IPv4专属节目源的txt格式文件"""
    return get_result_file_content(path=constants.hls_ipv4_result_path, file_type="txt")


@app.route("/hls/ipv6/txt")
def show_hls_ipv6_txt():
    """返回HLS+IPv6专属节目源的txt格式文件"""
    return get_result_file_content(path=constants.hls_ipv6_result_path, file_type="txt")


@app.route("/m3u")
def show_m3u():
    """返回最终节目源的m3u格式文件（播放器常用格式）"""
    return get_result_file_content(path=config.final_file, file_type="m3u")


@app.route("/hls/m3u")
def show_hls_m3u():
    """返回HLS格式节目源的m3u格式文件"""
    return get_result_file_content(path=constants.hls_result_path, file_type="m3u")


@app.route("/ipv4/m3u")
def show_ipv4_m3u():
    """返回IPv4专属节目源的m3u格式文件"""
    return get_result_file_content(path=constants.ipv4_result_path, file_type="m3u")


@app.route("/ipv4")
def show_ipv4_result():
    """
    IPv4专属节目源默认接口
    优先返回HLS格式（开启RTMP时），否则返回普通IPv4节目源
    格式优先返回m3u（开启m3u结果开关时），否则返回txt
    """
    return get_result_file_content(
        path=constants.hls_ipv4_result_path if config.open_rtmp else constants.ipv4_result_path,
        file_type="m3u" if config.open_m3u_result else "txt"
    )


@app.route("/hls/ipv4")
def show_hls_ipv4():
    """
    HLS+IPv4专属节目源默认接口
    格式优先返回m3u（开启m3u结果开关时），否则返回txt
    """
    return get_result_file_content(
        path=constants.hls_ipv4_result_path,
        file_type="m3u" if config.open_m3u_result else "txt"
    )


@app.route("/ipv6/m3u")
def show_ipv6_m3u():
    """返回IPv6专属节目源的m3u格式文件"""
    return get_result_file_content(path=constants.ipv6_result_path, file_type="m3u")


@app.route("/ipv6")
def show_ipv6_result():
    """
    IPv6专属节目源默认接口
    优先返回HLS格式（开启RTMP时），否则返回普通IPv6节目源
    格式优先返回m3u（开启m3u结果开关时），否则返回txt
    """
    return get_result_file_content(
        path=constants.hls_ipv6_result_path if config.open_rtmp else constants.ipv6_result_path,
        file_type="m3u" if config.open_m3u_result else "txt"
    )


@app.route("/hls/ipv6")
def show_hls_ipv6():
    """
    HLS+IPv6专属节目源默认接口
    格式优先返回m3u（开启m3u结果开关时），否则返回txt
    """
    return get_result_file_content(
        path=constants.hls_ipv6_result_path,
        file_type="m3u" if config.open_m3u_result else "txt"
    )


@app.route("/hls/ipv4/m3u")
def show_hls_ipv4_m3u():
    """返回HLS+IPv4专属节目源的m3u格式文件"""
    return get_result_file_content(path=constants.hls_ipv4_result_path, file_type="m3u")


@app.route("/hls/ipv6/m3u")
def show_hls_ipv6_m3u():
    """返回HLS+IPv6专属节目源的m3u格式文件"""
    return get_result_file_content(path=constants.hls_ipv6_result_path, file_type="m3u")


@app.route("/content")
def show_content():
    """
    返回节目源文件的原始内容接口
    优先返回HLS格式（开启RTMP时），否则返回最终生成的节目源文件
    格式优先返回m3u（开启m3u结果开关时），否则返回txt
    """
    return get_result_file_content(
        path=constants.hls_result_path if config.open_rtmp else config.final_file,
        file_type="m3u" if config.open_m3u_result else "txt",
        show_content=True
    )


# ----------------------------
# 辅助接口：/favicon & logo 静态资源
# ----------------------------
@app.route("/favicon.ico")
def favicon():
    """返回网站/favicon.ico图标（浏览器标签页图标）"""
    return send_from_directory(
        resource_path(''),  # 资源根目录
        'favicon.ico',  # 图标文件名
        mimetype='image/vnd.microsoft.icon'  # 图标MIME类型
    )


@app.route('/logo/<path:filename>')
def show_logo(filename):
    """
    返回频道logo图片静态资源
    做了安全校验，防止路径遍历攻击
    :param filename: logo文件名（带扩展名）
    """
    # 1. 校验文件名是否为空
    if not filename:
        return jsonify({"error": "filename required"}), 400

    # 2. 安全处理文件名，过滤危险字符和路径遍历符号（如../）
    safe_name = secure_filename(filename)
    # 3. 拼接logo文件的完整路径
    logo_dir = resource_path(constants.channel_logo_path)
    file_path = os.path.join(logo_dir, safe_name)

    # 4. 校验文件是否存在
    if not os.path.exists(file_path):
        return jsonify({"error": "logo not found"}), 404

    # 5. 猜测文件MIME类型（确保返回正确的响应头）
    mime_type, _ = mimetypes.guess_type(safe_name)
    # 6. 返回静态logo文件
    return send_from_directory(
        logo_dir,
        safe_name,
        mimetype=mime_type or 'application/octet-stream'  # 兜底MIME类型
    )


# ----------------------------
# 数据接口：EPG 电子节目指南
# ----------------------------
@app.route("/epg/epg.xml")
def show_epg():
    """返回EPG数据的XML格式文件（明文，易解析）"""
    return get_result_file_content(
        path=constants.epg_result_path,
        file_type="xml",
        show_content=False
    )


@app.route("/epg/epg.gz")
def show_epg_gz():
    """返回EPG数据的gz压缩格式文件（体积小，适合网络传输）"""
    return get_result_file_content(
        path=constants.epg_gz_result_path,
        file_type="gz",
        show_content=False
    )


# ----------------------------
# 日志接口：运行日志查询
# ----------------------------
@app.route("/log/result")
def show_result_log():
    """返回节目源更新结果日志"""
    if os.path.exists(constants.result_log_path):
        with open(constants.result_log_path, "r", encoding="utf-8") as file:
            content = file.read()
    else:
        content = constants.waiting_tip  # 日志文件不存在时返回默认等待提示
    # 构造响应，指定MIME类型为纯文本
    response = make_response(content)
    response.mimetype = "text/plain"
    return response


@app.route("/log/speed-test")
def show_speed_log():
    """返回频道URL测速日志"""
    if os.path.exists(constants.speed_test_log_path):
        with open(constants.speed_test_log_path, "r", encoding="utf-8") as file:
            content = file.read()
    else:
        content = constants.waiting_tip
    response = make_response(content)
    response.mimetype = "text/plain"
    return response


@app.route("/log/statistic")
def show_statistic_log():
    """返回节目源统计日志（如有效频道数、URL数等）"""
    if os.path.exists(constants.statistic_log_path):
        with open(constants.statistic_log_path, "r", encoding="utf-8") as file:
            content = file.read()
    else:
        content = constants.waiting_tip
    response = make_response(content)
    response.mimetype = "text/plain"
    return response


@app.route("/log/nomatch")
def show_nomatch_log():
    """返回无匹配频道日志（如订阅中无法匹配白名单的频道）"""
    if os.path.exists(constants.nomatch_log_path):
        with open(constants.nomatch_log_path, "r", encoding="utf-8") as file:
            content = file.read()
    else:
        content = constants.waiting_tip
    response = make_response(content)
    response.mimetype = "text/plain"
    return response


# ----------------------------
# 核心功能：HLS 流代理（配合RTMP服务）
# ----------------------------
@app.route('/hls_proxy/<channel_id>', methods=['GET'])
def hls_proxy(channel_id):
    """
    HLS流代理接口：转发指定频道的HLS流（.m3u8），供播放器播放
    :param channel_id: 频道唯一标识ID
    """
    # 1. 校验频道ID是否为空
    if not channel_id:
        return jsonify({t("name.error"): t("msg.error_channel_id_required")}), 400

    # 2. 拼接HLS流文件（.m3u8）的完整路径
    channel_file = f'{channel_id}.m3u8'
    m3u8_path = os.path.join(hls_temp_path, channel_file)

    # 3. 检查并启动HLS转RTMP流进程（加锁保证线程安全）
    need_start = False
    with STREAMS_LOCK:  # 全局锁，防止多请求同时操作hls_running_streams
        proc = hls_running_streams.get(channel_id)
        # 进程不存在或已退出，则需要重新启动
        if not proc or proc.poll() is not None:
            need_start = True
            # 清理已退出的进程记录
            if channel_id in hls_running_streams:
                hls_running_streams.pop(channel_id, None)

    # 4. 启动HLS转RTMP流进程
    if need_start:
        host = f"{app_rtmp_url}/hls"  # RTMP服务HLS流地址
        start_hls_to_rtmp(host, channel_id)  # 启动转流进程

    # 5. 等待HLS流文件就绪（确保有足够的分片，可正常播放）
    hls_min_segments = 3  # 最小需要3个分片才能正常播放
    waited = 0.0  # 已等待时间
    while waited < HLS_WAIT_TIMEOUT:  # 超时时间内循环等待
        if os.path.exists(m3u8_path):
            try:
                with open(m3u8_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                # 统计分片数量（#EXTINF是HLS分片标识）
                segment_count = content.count('#EXTINF')
                # 检查是否以不连续标记结尾（避免播放异常）
                ends_with_discont = content.rstrip().endswith('#EXT-X-DISCONTINUITY')
                # 分片数量足够且无异常结尾，说明流已就绪
                if segment_count >= hls_min_segments and not ends_with_discont:
                    break
            except Exception as e:
                print(t("msg.error_channel_id_m3u8_read_info").format(channel_id=channel_id, info=e))
        # 等待指定间隔后重试
        time.sleep(HLS_WAIT_INTERVAL)
        waited += HLS_WAIT_INTERVAL

    # 6. HLS流文件未就绪，返回503服务不可用
    if not os.path.exists(m3u8_path):
        return jsonify({t("name.error"): t("msg.m3u8_hls_not_ready")}), 503

    # 7. 读取HLS流文件内容，准备返回
    try:
        with open(m3u8_path, 'rb') as f:
            data = f.read()
    except Exception as e:
        print(t("msg.error_channel_id_m3u8_read_info").format(channel_id=channel_id, info=e))
        return jsonify({t("name.error"): t("msg.error_m3u8_read")}), 500

    # 8. 更新该频道的最后访问时间（用于后续清理闲置流）
    now = time.time()
    with STREAMS_LOCK:
        hls_last_access[channel_id] = now

    # 9. 返回HLS流数据，指定正确的MIME类型（苹果播放器标准MIME类型）
    return Response(data, mimetype='application/vnd.apple.mpegurl')


# ----------------------------
# 回调接口：RTMP 流结束回调
# ----------------------------
@app.post('/on_done')
def on_done():
    """
    RTMP服务流结束回调接口（POST请求）
    当RTMP流停止时，由RTMP服务调用该接口通知Web服务
    """
    # 获取表单中的频道ID
    form = request.form
    channel_id = form.get('name', '')
    # 打印流结束日志
    print(t("msg.rtmp_on_done").format(channel_id=channel_id))
    # 返回空响应，标识回调成功
    return ''


# ----------------------------
# 服务启动与停止
# ----------------------------
def run_service():
    """
    启动Flask Web服务和相关辅助服务（如RTMP）
    是整个Web服务的入口函数
    """
    try:
        # 非GitHub Actions环境下执行（避免CI/CD环境启动服务）
        if not os.getenv("GITHUB_ACTIONS"):
            # Windows平台下，开启RTMP服务则启动对应的进程
            if config.open_rtmp and sys.platform == "win32":
                start_rtmp_service()
            # 获取公网访问地址，用于打印API提示
            public_url = get_public_url()
            base_api = f"{public_url}/hls" if config.open_rtmp else public_url
            # 打印各类API访问地址（方便用户查看和使用）
            print(t("msg.statistic_log_path").format(path=f"{public_url}/log/statistic"))
            print(t("msg.ipv4_api").format(api=f"{base_api}/ipv4"))
            print(t("msg.ipv6_api").format(api=f"{base_api}/ipv6"))
            print(t("msg.full_api").format(api=base_api))
            # 启动Flask Web服务
            app.run(
                host="127.0.0.1",  # 绑定本地回环地址，仅本地可访问（如需外网访问改为0.0.0.0）
                port=config.app_port  # 从配置文件读取服务端口
            )
    except Exception as e:
        # 捕获服务启动异常，打印错误信息
        print(t("msg.error_service_start_failed").format(info=e))


if __name__ == "__main__":
    """命令行直接运行时的入口"""
    # Windows平台下，开启RTMP服务则注册程序退出时的清理函数（停止RTMP服务）
    if config.open_rtmp and sys.platform == "win32":
        atexit.register(stop_rtmp_service)
    # 启动Web服务
    run_service()
