"""
astrbot_plugin_chksz_music
==========================
ChKSz 多平台音乐点歌插件，支持 QQ 音乐 / 网易云 / 酷狗，可在聊天里查看和切换音源。

接口实测记录（2026-09-01 验证可用，参数名勿改）：
  通用：鉴权参数名为 apikey（不是 key），缺失会返回 401。

  QQ   GET /api/qq_music?apikey=&msg=关键词
           -> {code,msg,count,list:[{n,name,singer,album,pay,mid}]}
       GET /api/qq_music?apikey=&msg=关键词&n=1&size=320k
           -> {code,msg,name,singer,album,url,cover,lrc,interval,bitrate,format,mid}   (平铺，无 data 包裹)
       音质 size: 128k / 320k / flac / hires / master

  网易 GET /api/163_search?apikey=&keyword=关键词
           -> {code,msg,data:{songs:[{id,name,artists,album,picUrl,duration}]}}
       GET /api/163_music?apikey=&id=歌曲ID&level=exhigh
           -> {code,msg,data:{id,url,br,level,size,md5,name,artist,album,picUrl}}      (包在 data 里)
       音质 level: standard / exhigh / lossless / hires / jyeffect / sky / jymaster

  酷狗 GET /api/kugou_music?apikey=&msg=关键词
           -> {code,msg,keyword,total,list:[{n,id,name,singer,album,duration}]}
       GET /api/kugou_music?apikey=&msg=关键词&n=1&size=320k
           -> {code,msg,name,singer,album,url,cover,lrc,interval,bitrate,format,id}    (平铺)
       音质 size: 128k / 320k / flac / hires / master

用法：
  /music 晴天          用当前音源点歌
  /music 3 晴天        指定搜索结果里的第 3 首
  /music 163 晴天      临时用网易云搜这一首（不改默认音源）
  /music source        查看当前音源和可选音源
  /music source 163    切换默认音源（写入插件配置，重启后保留）
别名：/点歌 /音乐 /播放 /musicsrc /音源

发送方式（重要）：
  不要用 Record 发大音频。AstrBot 的 aiocqhttp 适配器对 Record/Image 组件会无条件
  调 convert_to_base64()，把整段音频编成 base64 塞进 WebSocket —— 4MB 音频变成
  5.6MB 字符串，NapCat 侧会直接抛 "RangeError: Max payload size exceeded" 并断开连接。

  File 组件则不同，它的 to_dict() 会优先走 URL（见 astrbot/core/message/components.py
  File.to_dict）：配了 callback_api_base 就把本地文件注册成 http URL，否则原样透传 url，
  aiocqhttp 适配器只在 file 是绝对路径且不含 :// 时才转成 file:// URI。
  因此大文件一律走 File + URL，让协议端自己下载，ws 上只传一个链接。

  send_mode:
    url    默认。把音频直链交给协议端下载，AstrBot 不中转、不做 base64，最省资源。
    file   AstrBot 先下载，再交给文件服务。需要配置 callback_api_base，
           Docker 部署（AstrBot 和 NapCat 不同容器）必须配，否则退化成 file:// 路径，
           协议端读不到。
    record 语音条，base64 走 ws。仅小文件可用，超过 max_record_mb 会自动降级为 url。
"""

import asyncio
import os
import re
import tempfile
import urllib.parse
from contextlib import asynccontextmanager

import aiohttp

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

CHKSZ_BASE = "https://api.chksz.com"

NO_KEY_HINT = (
    "⚠️ 尚未配置 API Key，请在 AstrBot 管理面板的本插件配置里填写 "
    "api_key（chksz_ 开头）后重试"
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 业务错误码（HTTP 200 + JSON code）提示
API_CODE_HINT = {
    400: "请求参数有误",
    401: "API Key 无效或未授权，请在插件配置里填写正确的 api_key",
    402: "API Key 额度已用尽",
    403: "API Key 无权访问该接口",
    404: "接口不存在",
    429: "请求过于频繁，已被限流，请稍后再试",
    500: "服务端异常，请稍后重试",
}

# 音源定义
PLATFORMS = {
    "qq": {
        "label": "QQ音乐",
        "search_path": "/api/qq_music",
        "resolve_path": "/api/qq_music",
        "kw_param": "msg",
        "quality_param": "size",
        "default_quality": "320k",
        "list_path": ["list"],          # 搜索结果列表在 JSON 中的位置
        "id_field": "n",                # 列表中用于解析的字段
        "detail_wrapped": False,        # 解析结果是否包在 data 里
    },
    "163": {
        "label": "网易云音乐",
        "search_path": "/api/163_search",
        "resolve_path": "/api/163_music",
        "kw_param": "keyword",
        "quality_param": "level",
        "default_quality": "exhigh",
        "list_path": ["data", "songs"],
        "id_field": "id",
        "detail_wrapped": True,
    },
    "kugou": {
        "label": "酷狗音乐",
        "search_path": "/api/kugou_music",
        "resolve_path": "/api/kugou_music",
        "kw_param": "msg",
        "quality_param": "size",
        "default_quality": "320k",
        "list_path": ["list"],
        "id_field": "n",
        "detail_wrapped": False,
    },
}

# 音源别名（小写匹配）
PLATFORM_ALIASES = {
    "qq": "qq", "qq音乐": "qq", "qqmusic": "qq", "tx": "qq", "腾讯": "qq", "腾讯音乐": "qq",
    "163": "163", "网易": "163", "网易云": "163", "网易云音乐": "163", "netease": "163", "wy": "163", "wyy": "163",
    "kugou": "kugou", "酷狗": "kugou", "酷狗音乐": "kugou", "kg": "kugou",
}

# 用于剥离命令前缀的词（长词在前）
COMMAND_WORDS = [
    "musicsrc", "music", "song",
    "切换音源", "切换源", "音乐源", "音源", "点歌", "音乐", "播放",
]

SOURCE_WORDS = ("source", "src", "切换音源", "切换源", "音乐源", "音源")

HELP_TEXT = """🎵 ChKSz 点歌插件

/music 歌曲名        先列出候选，回复序号后下载
/music 3 歌曲名      跳过选择，直接下载第 3 首
/music 163 歌曲名    临时用网易云搜这首
/music source        查看当前音源
/music source 163    切换默认音源

可用音源：qq（QQ音乐）/ 163（网易云）/ kugou（酷狗）
别名：/点歌 /音乐 /播放 /musicsrc /音源"""


def resolve_platform(word: str):
    """把用户输入的音源词标准化成 qq/163/kugou，无法识别返回 None。"""
    if not word:
        return None
    return PLATFORM_ALIASES.get(str(word).strip().lower().lstrip("/"))


def safe_filename(text: str, limit: int = 60) -> str:
    """生成安全文件名。注意不能只留 isalnum()，否则中文歌名会被清空。"""
    cleaned = re.sub(r'[\\/:*?"<>|\r\n\t]', "", str(text or "")).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return (cleaned[:limit] or "unknown").strip(" .")


def get_temp_dir() -> str:
    """优先用 AstrBot 的 temp 目录，取不到就退回系统临时目录。"""
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_temp_path

        path = get_astrbot_temp_path()
        if path:
            os.makedirs(path, exist_ok=True)
            return path
    except Exception:
        pass
    path = os.path.join(tempfile.gettempdir(), "astrbot_chksz_music")
    os.makedirs(path, exist_ok=True)
    return path


def human_size(num_bytes) -> str:
    try:
        return f"{int(num_bytes) / 1048576:.1f}MB"
    except Exception:
        return "未知"


@register(
    "astrbot_plugin_chksz_music",
    "Atri",
    "ChKSz 多平台音乐点歌插件，支持 QQ 音乐 / 网易云 / 酷狗，可在线切换音源",
    "v1.1.0",
    "",
)
class ChKSzMusicPlugin(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config or {}
        self.platform = self._read_platform()

    # ------------------------------------------------------------------ 配置

    def _cfg(self, key, default=None):
        try:
            value = self.config.get(key, default)
        except Exception:
            value = default
        return default if value is None else value

    def _read_platform(self) -> str:
        """读取默认音源，非法值回落到 qq。"""
        raw = str(self._cfg("platform", "qq")).strip().lower()
        if raw in PLATFORMS:
            return raw
        resolved = resolve_platform(raw)
        return resolved or "qq"

    def _api_key(self) -> str:
        return str(self._cfg("api_key", "") or "").strip()

    def _quality(self, platform: str) -> str:
        quality = str(self._cfg("quality", "") or "").strip()
        return quality or PLATFORMS[platform]["default_quality"]

    def _save_platform(self, platform: str) -> bool:
        """把音源写回插件配置，失败也不影响内存里的切换。"""
        self.platform = platform
        try:
            self.config["platform"] = platform
            if hasattr(self.config, "save_config"):
                self.config.save_config()
            return True
        except Exception as e:
            logger.warning(f"[chksz_music] 音源持久化失败（仅本次生效）: {e}")
            return False

    # ------------------------------------------------------------------ 命令

    @filter.command("music", alias={"点歌", "音乐", "播放", "song"})
    async def music(self, event: AstrMessageEvent):
        text = self._strip_command(event)
        if not text:
            yield event.plain_result(HELP_TEXT)
            return

        # 音源管理子命令：/music source [音源]
        source_arg = self._match_source_cmd(text)
        if source_arg is not None:
            if not source_arg:
                yield event.plain_result(self._source_status())
                return
            target = resolve_platform(source_arg)
            if not target:
                yield event.plain_result(
                    f"❌ 未知音源：{source_arg}\n可用音源：qq / 163 / kugou"
                )
                return
            saved = self._save_platform(target)
            tip = "" if saved else "（写入配置失败，本次会话内有效）"
            yield event.plain_result(
                f"✅ 默认音源已切换为 {PLATFORMS[target]['label']}{tip}"
            )
            return

        # 点歌：解析 [临时音源] [序号] 关键词
        platform, index, keyword = self._parse_query(text)

        if index is not None:
            # 显式带了序号（/music 3 晴天），直接下载，跳过选择
            if not keyword:
                yield event.plain_result("❓ 想听什么歌？\n用法：/music 3 晴天")
                return
            async for result in self._play(event, platform, keyword, index):
                yield result
            return

        if not keyword:
            yield event.plain_result("❓ 想听什么歌？\n用法：/music 晴天")
            return

        # 没带序号：先列候选，等用户回复序号再下载
        async for result in self._choose_then_play(event, platform, keyword):
            yield result

    @filter.command("musicsrc", alias={"音源", "音乐源", "切换音源"})
    async def musicsrc(self, event: AstrMessageEvent):
        """独立音源管理命令：/musicsrc 查看，/musicsrc 163 切换。"""
        arg = self._strip_command(event)
        if not arg:
            yield event.plain_result(self._source_status())
            return
        target = resolve_platform(arg)
        if not target:
            yield event.plain_result(f"❌ 未知音源：{arg}\n可用音源：qq / 163 / kugou")
            return
        saved = self._save_platform(target)
        tip = "" if saved else "（写入配置失败，本次会话内有效）"
        yield event.plain_result(
            f"✅ 默认音源已切换为 {PLATFORMS[target]['label']}{tip}"
        )

    # ------------------------------------------------------------------ 解析

    @staticmethod
    def _strip_command(event: AstrMessageEvent) -> str:
        """去掉消息开头的 / 和命令名，只留参数部分。"""
        try:
            raw = (event.message_str or "").strip()
        except Exception:
            try:
                raw = (event.get_message_str() or "").strip()
            except Exception:
                raw = ""
        if not raw:
            return ""
        raw = raw.lstrip("/!！.。 ")
        for word in sorted(COMMAND_WORDS, key=len, reverse=True):
            if raw.lower().startswith(word.lower()):
                return raw[len(word):].strip()
        return raw

    @staticmethod
    def _match_source_cmd(text: str):
        """命中音源子命令返回参数串（可能为空串），否则返回 None。"""
        match = re.match(
            r"^(source|src|切换音源|切换源|音乐源|音源|源)\s*(.*)$",
            text,
            re.IGNORECASE,
        )
        if match:
            return match.group(2).strip()
        return None

    def _parse_query(self, text: str):
        """解析 [音源] [序号] 关键词，返回 (platform, index, keyword)。

        index 为 None 表示用户没指定序号 —— 此时走"先列候选、再等序号"的两步流程；
        显式给了序号（如 /music 3 晴天）则直接下载，跳过选择。
        """
        platform = self.platform
        rest = text.strip()

        # 临时音源前缀
        first = rest.split()[0] if rest.split() else ""
        override = resolve_platform(first)
        if override:
            platform = override
            rest = rest[len(first):].strip()
            if not rest:
                return platform, None, ""

        # 序号前缀
        index = None
        match = re.match(r"^(\d{1,2})[\s.、,，]+(.+)$", rest)
        if match:
            index = max(1, int(match.group(1)))
            rest = match.group(2).strip()

        return platform, index, rest

    def _source_status(self) -> str:
        current = PLATFORMS[self.platform]
        lines = [
            "🎛️ 音源状态",
            f"当前音源：{current['label']}（{self.platform}）",
            f"当前音质：{self._quality(self.platform)}",
            "",
            "可用音源：",
        ]
        for key, meta in PLATFORMS.items():
            mark = "✅" if key == self.platform else "　"
            lines.append(f"{mark} {key}　{meta['label']}")
        lines.append("")
        lines.append("切换：/music source 163  或  /musicsrc kugou")
        return "\n".join(lines)

    # ------------------------------------------------------------------ 主流程

    @asynccontextmanager
    async def _session_ctx(self, session=None):
        """复用外部 session，没有就自己建一个（两步流程里搜索和下载共用一个连接）。"""
        if session is not None:
            yield session
        else:
            timeout = aiohttp.ClientTimeout(total=120, connect=15)
            headers = {"User-Agent": UA, "Accept": "application/json"}
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as s:
                yield s

    async def _play(
        self,
        event: AstrMessageEvent,
        platform: str,
        keyword: str,
        index: int,
        session=None,
    ):
        """搜索 + 解析 + 下载 + 发送。"""
        conf = PLATFORMS[platform]

        try:
            async with self._session_ctx(session) as session:
                # Step 1 搜索
                data = await self._search(session, platform, keyword)
                if isinstance(data, str):
                    yield event.plain_result(data)
                    return

                songs = data
                if not songs:
                    yield event.plain_result(
                        f"🔍 在{conf['label']}没搜到「{keyword}」，换个关键词或换音源试试"
                    )
                    return

                if index > len(songs):
                    yield event.plain_result(
                        f"❌ 序号 {index} 超出范围，{conf['label']}只返回了 {len(songs)} 条结果"
                    )
                    return

                picked = songs[index - 1]
                song_ref = picked.get(conf["id_field"])

                # Step 2 解析播放链接
                detail = await self._resolve(
                    session, platform, keyword, song_ref
                )
                if isinstance(detail, str):
                    yield event.plain_result(detail)
                    return

                info = self._normalize(platform, detail)
                if not info.get("url"):
                    yield event.plain_result(
                        f"⚠️ 「{info.get('name') or keyword}」没有可播放链接，"
                        f"可能是版权限制，换个音源试试（当前：{conf['label']}）"
                    )
                    return

                # Step 3 决定发送方式，并按需下载
                filename = "{}_{}{}".format(
                    safe_filename(info.get("name") or keyword),
                    safe_filename(info.get("singer") or "", 20),
                    info.get("ext") or ".mp3",
                )
                mode = str(self._cfg("send_mode", "url") or "url").strip().lower()
                if mode not in ("url", "file", "record"):
                    mode = "url"
                max_bytes = int(self._cfg("max_size_mb", 60) or 60) * 1024 * 1024
                max_record_bytes = int(self._cfg("max_record_mb", 1) or 1) * 1024 * 1024

                # 探测直链可达性与体积（Range 只取 1 字节，成本极低）
                probe = await self._probe_size(session, info["url"])

                # 语音条会把音频 base64 后塞进 WebSocket，大文件必然撑爆连接，超限就降级
                if mode == "record" and (probe is None or probe > max_record_bytes):
                    logger.warning(
                        f"[chksz_music] 语音条需 base64 走 WebSocket，体积 "
                        f"{human_size(probe) if probe else '未知'} 超限，自动改用直链发送"
                    )
                    mode = "url"

                # 直链探测不通时，改为 AstrBot 自己下载再发
                if mode == "url" and probe is None:
                    logger.warning("[chksz_music] 直链探测失败，改为 AstrBot 下载后发送")
                    mode = "file"

                filepath = None
                size_text = self._quality(platform)
                if mode in ("file", "record"):
                    temp_dir = get_temp_dir()
                    filepath = os.path.join(temp_dir, "chksz_" + filename)
                    ok, err = await self._download(
                        session, info["url"], filepath, max_bytes
                    )
                    if not ok:
                        yield event.plain_result(f"⬇️ 下载失败：{err}")
                        return
                    size_text = human_size(os.path.getsize(filepath))

        except aiohttp.ClientError as e:
            logger.warning(f"[chksz_music] 网络请求失败: {e}")
            yield event.plain_result(f"🌐 网络请求失败：{type(e).__name__}，请稍后重试")
            return
        except asyncio.TimeoutError:
            yield event.plain_result("⏱️ 请求超时，服务端响应过慢，请稍后重试")
            return
        except Exception as e:
            logger.exception(f"[chksz_music] 点歌异常: {e}")
            yield event.plain_result(f"💥 点歌出错：{type(e).__name__}: {e}")
            return

        # Step 4 发送
        #
        # 关键：不要用 Record 发大文件。AstrBot 的 aiocqhttp 适配器对 Record 会
        # 无条件走 convert_to_base64()，把整段音频编成 base64 塞进 WebSocket，
        # 4MB 音频 -> 5.6MB 字符串，NapCat 侧直接报 "Max payload size exceeded"
        # 并断开连接。File 组件则走 URL，协议端自行下载，ws 上只传一个链接。
        caption = "🎵 {}\n👤 {}\n💿 {} · {} · {}".format(
            info.get("name") or keyword,
            info.get("singer") or "未知歌手",
            info.get("album") or "未知专辑",
            conf["label"],
            size_text,
        )
        yield event.plain_result(caption)

        file_comp = getattr(Comp, "File", None)
        try:
            if mode == "record" and filepath:
                yield event.chain_result([Comp.Record(file=filepath)])
            elif mode == "file" and filepath:
                if file_comp:
                    # 配了 callback_api_base 时 AstrBot 会注册成 http URL；
                    # 没配则退化为 file:// 路径，Docker 部署下协议端读不到。
                    yield event.chain_result([file_comp(name=filename, file=filepath)])
                else:
                    yield event.plain_result(
                        "⚠️ 当前 AstrBot 版本没有 File 组件，请把 send_mode 改为 url"
                    )
            else:
                if file_comp:
                    # 直链交给协议端自己下载，AstrBot 不中转、不做 base64
                    yield event.chain_result([file_comp(name=filename, url=info["url"])])
                else:
                    yield event.plain_result(
                        "⚠️ 当前 AstrBot 版本没有 File 组件，无法发送直链，请升级 AstrBot 或改用 record"
                    )
        except Exception as e:
            logger.exception(f"[chksz_music] 发送音频失败: {e}")
            yield event.plain_result(f"📤 发送失败：{type(e).__name__}: {e}")

        # 延迟清理，避免框架还没读完文件就被删掉
        if filepath:
            asyncio.create_task(self._delayed_remove(filepath))

    # ------------------------------------------------------------------ 两步选择

    def _format_list(self, label: str, keyword: str, songs: list, show: int) -> str:
        """把候选歌曲格式化成带编号的列表。"""
        lines = [f"🔍 {label}搜索「{keyword}」，共 {len(songs)} 条："]
        for i in range(show):
            song = songs[i] or {}
            name = song.get("name") or "未知"
            singer = (
                song.get("singer")
                or song.get("artists")
                or song.get("artist")
                or "未知"
            )
            if isinstance(singer, list):
                singer = "/".join(
                    str(a.get("name", "")) if isinstance(a, dict) else str(a)
                    for a in singer
                )
            album = song.get("album") or ""
            tail = f" · {album}" if album else ""
            lines.append(f"  {i + 1}. {name} — {singer}{tail}")
        if len(songs) > show:
            lines.append(f"  … 仅显示前 {show} 条")
        lines.append("")
        lines.append(f"回复序号 1-{show} 开始下载，回复 0 取消")
        return "\n".join(lines)

    async def _wait_text(self, event: AstrMessageEvent, timeout: int = 60):
        """等待用户在同一个会话里再发一条消息，返回其文本；超时或不支持时返回 None。

        用 AstrBot 官方的 SessionWaiter 接管该会话的下一条消息，并显式
        stop_event() + should_call_llm(False)，避免用户回的这个数字
        又去触发 LLM 或别的指令。
        """
        try:
            from astrbot.core.utils.session_waiter import (
                DefaultSessionFilter,
                SessionController,
                SessionWaiter,
            )

            try:
                from astrbot.core.utils.session_waiter import FILTERS
            except Exception:
                FILTERS = None
        except Exception as e:
            logger.warning(f"[chksz_music] 当前 AstrBot 不支持会话等待: {e}")
            return None

        session_filter = DefaultSessionFilter()
        session_id = session_filter.filter(event)
        box: dict = {"text": None}

        async def handler(controller: SessionController, ev: AstrMessageEvent):
            # 阻断，防止这条回复再去走 LLM / 其他指令
            try:
                ev.stop_event()
                ev.should_call_llm(False)
            except Exception:
                pass
            box["text"] = self._strip_command(ev).strip()
            controller.stop()

        waiter = SessionWaiter(session_filter, session_id, False)
        if FILTERS is not None:
            FILTERS.append(session_filter)
        try:
            await waiter.register_wait(handler, timeout)
        except TimeoutError:
            return None
        except Exception as e:
            logger.warning(f"[chksz_music] 等待用户选择失败: {e}")
            return box.get("text")
        return box.get("text")

    async def _choose_then_play(
        self, event: AstrMessageEvent, platform: str, keyword: str, session=None
    ):
        """先搜索列出候选，等用户回复序号后再下载。"""
        conf = PLATFORMS[platform]
        timeout = int(self._cfg("select_timeout", 60) or 60)
        try:
            async with self._session_ctx(session) as session:
                songs = await self._search(session, platform, keyword)
                if isinstance(songs, str):
                    yield event.plain_result(songs)
                    return
                if not songs:
                    yield event.plain_result(
                        f"🔍 在{conf['label']}没搜到「{keyword}」，换个关键词或换音源试试"
                    )
                    return

                show = min(len(songs), max(1, int(self._cfg("search_count", 5) or 5)))
                yield event.plain_result(
                    self._format_list(conf["label"], keyword, songs, show)
                )

                # 最多问 3 轮，避免用户乱输时无限等待
                for _ in range(3):
                    raw = await self._wait_text(event, timeout=timeout)
                    if raw is None:
                        yield event.plain_result(
                            "⏱️ 超时未选择，已取消。\n也可以直接 /music 3 晴天 跳过选择"
                        )
                        return
                    if raw.lower() in (
                        "0", "取消", "cancel", "q", "quit", "exit", "退出",
                    ):
                        yield event.plain_result("已取消 ❌")
                        return
                    if raw.isdigit():
                        pick = int(raw)
                        if 1 <= pick <= len(songs):
                            async for r in self._play(
                                event, platform, keyword, pick, session=session
                            ):
                                yield r
                            return
                        yield event.plain_result(f"❌ 请输入 1-{show} 之间的序号（0 取消）")
                        continue
                    yield event.plain_result(f"❌ 请回复数字序号 1-{show}，或回复 0 取消")

                yield event.plain_result("❌ 无效输入次数过多，已取消")
        except aiohttp.ClientError as e:
            logger.warning(f"[chksz_music] 网络请求失败: {e}")
            yield event.plain_result(f"🌐 网络请求失败：{type(e).__name__}，请稍后重试")
        except asyncio.TimeoutError:
            yield event.plain_result("⏱️ 请求超时，服务端响应过慢，请稍后重试")
        except Exception as e:
            logger.exception(f"[chksz_music] 搜索异常: {e}")
            yield event.plain_result(f"💥 搜索出错：{type(e).__name__}: {e}")

    # ------------------------------------------------------------------ 接口调用

    async def _search(self, session, platform: str, keyword: str):
        """搜索歌曲，成功返回 list[dict]，失败返回错误文案 str。"""
        conf = PLATFORMS[platform]
        api_key = self._api_key()
        if not api_key:
            return NO_KEY_HINT
        params = {
            "apikey": api_key,
            conf["kw_param"]: keyword,
        }
        url = "{}{}?{}".format(
            CHKSZ_BASE, conf["search_path"], urllib.parse.urlencode(params)
        )

        data = await self._get_json(session, url)
        if isinstance(data, str):
            return data

        node = data
        for key in conf["list_path"]:
            if not isinstance(node, dict):
                return "⚠️ 搜索结果格式异常"
            node = node.get(key)
        if not isinstance(node, list) or not node:
            return f"🔍 在{conf['label']}没搜到「{keyword}」"
        return node

    async def _resolve(self, session, platform: str, keyword: str, song_ref):
        """解析播放链接，成功返回详情 dict，失败返回错误文案 str。"""
        conf = PLATFORMS[platform]
        api_key = self._api_key()
        if not api_key:
            return NO_KEY_HINT
        params = {"apikey": api_key}

        if conf["id_field"] == "n":
            # QQ / 酷狗：按关键词 + 序号解析
            params[conf["kw_param"]] = keyword
            params["n"] = song_ref
        else:
            # 网易：按歌曲 ID 解析
            params["id"] = song_ref

        quality = self._quality(platform)
        quality_param = conf["quality_param"]

        def build_url(extra_quality: str) -> str:
            payload = dict(params)
            if extra_quality:
                payload[quality_param] = extra_quality
            return "{}{}?{}".format(
                CHKSZ_BASE, conf["resolve_path"], urllib.parse.urlencode(payload)
            )

        data = await self._get_json(session, build_url(quality))
        if isinstance(data, str) and quality:
            # 部分平台不接受某些音质值（如酷狗传 128k 会返回 404），去掉音质重试一次。
            # 稍作等待，避免连续请求触发限流。
            logger.warning(f"[chksz_music] 带音质解析失败，去掉音质重试：{data}")
            await asyncio.sleep(1.2)
            data = await self._get_json(session, build_url(""), retries=1)
        if isinstance(data, str):
            return data

        if conf["detail_wrapped"]:
            data = data.get("data") if isinstance(data, dict) else None
        if not isinstance(data, dict) or not data.get("url"):
            return "⚠️ 没能解析出播放链接，可能是版权限制，换个音源试试"
        return data

    async def _get_json(self, session, url: str, retries: int = 3):
        """统一处理 HTTP 状态码与业务 code，遇到服务端抖动自动重试。

        实测酷狗接口会间歇性返回 404（多节点中部分节点异常），重试即可恢复。
        鉴权、限流类错误不重试，避免白白消耗额度。
        """
        last_err = "⚠️ 未知错误"
        for attempt in range(retries + 1):
            data, err, retryable = await self._request(session, url)
            if data is not None:
                return data
            last_err = err
            if not retryable or attempt >= retries:
                break
            logger.warning(f"[chksz_music] 请求失败，重试 {attempt + 1}/{retries}：{err}")
            await asyncio.sleep(0.8 * (attempt + 1))
        return last_err

    async def _request(self, session, url: str):
        """单次请求，返回 (data, err, retryable)。"""
        try:
            async with session.get(url) as resp:
                if resp.status in (401, 402, 403, 429):
                    return (
                        None,
                        f"⚠️ {API_CODE_HINT.get(resp.status, '请求被拒绝')}",
                        False,
                    )
                if resp.status >= 500:
                    return None, f"⚠️ 服务端错误：HTTP {resp.status}", True
                if resp.status != 200:
                    return (
                        None,
                        f"⚠️ API 返回异常状态码：HTTP {resp.status}",
                        resp.status == 404,
                    )

                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    text = (await resp.text())[:200]
                    return None, f"⚠️ API 返回内容不是 JSON：{text}", False
        except asyncio.TimeoutError:
            return None, "⏱️ 请求超时", True
        except aiohttp.ClientError as e:
            return None, f"🌐 {type(e).__name__}: {e}", True

        if not isinstance(data, dict):
            return None, "⚠️ API 返回格式异常", False

        code = data.get("code")
        if code is not None and code != 200:
            # 接口自带的 msg 通常比静态提示更准确（如酷狗的"未找到匹配的歌曲"）
            msg = str(data.get("msg") or "").strip()
            text = msg or API_CODE_HINT.get(code) or "未知错误"
            return None, f"⚠️ API 错误 {code}：{text}", code in (404, 500, 502, 503)
        return data, None, False

    @staticmethod
    def _normalize(platform: str, detail: dict) -> dict:
        """把三个平台的不同字段统一成一份信息。"""
        url = detail.get("url") or ""
        ext = "." + str(detail.get("format") or "").strip().lstrip(".")
        if not ext or ext == ".":
            # 网易没有 format 字段，从 URL 后缀推断
            path = urllib.parse.urlparse(url).path
            suffix = os.path.splitext(path)[1].lower()
            ext = suffix if suffix in (".mp3", ".flac", ".m4a", ".wav", ".ogg") else ".mp3"

        singer = detail.get("singer") or detail.get("artist") or detail.get("artists") or ""
        if isinstance(singer, list):
            singer = "/".join(
                str(a.get("name", "")) if isinstance(a, dict) else str(a) for a in singer
            )

        return {
            "url": url,
            "name": detail.get("name") or "",
            "singer": str(singer),
            "album": detail.get("album") or "",
            "cover": detail.get("cover") or detail.get("picUrl") or "",
            "ext": ext,
            "interval": detail.get("interval") or "",
        }

    async def _probe_size(self, session, url: str):
        """探测直链可达性与体积。

        只取 1 字节，成本极低。
        返回：总字节数（>0）、0（可达但拿不到大小）、None（不可达）。
        """
        headers = {"User-Agent": UA, "Range": "bytes=0-0"}
        try:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status not in (200, 206):
                    return None

                content_range = resp.headers.get("Content-Range")
                if content_range and "/" in content_range:
                    total = content_range.rsplit("/", 1)[-1]
                    if total.isdigit():
                        return int(total)

                length = resp.headers.get("Content-Length")
                if length and length.isdigit():
                    # 206 时这个值是本次实际长度（1），不代表总量
                    return int(length) if resp.status == 200 else 0
                return 0
        except Exception as e:
            logger.debug(f"[chksz_music] 直链探测失败: {type(e).__name__}: {e}")
            return None

    async def _download(self, session, url: str, filepath: str, max_bytes: int):
        """流式下载并限制体积，返回 (是否成功, 错误信息)。"""
        headers = {"User-Agent": UA, "Referer": "https://y.qq.com/"}
        timeout = aiohttp.ClientTimeout(total=300, connect=20, sock_read=60)
        try:
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"

                total = resp.headers.get("Content-Length")
                if total and total.isdigit() and int(total) > max_bytes:
                    return False, (
                        f"文件 {human_size(total)} 超过上限 "
                        f"{human_size(max_bytes)}，已取消。可降低音质或调高 max_size_mb"
                    )

                written = 0
                with open(filepath, "wb") as fp:
                    async for chunk in resp.content.iter_chunked(65536):
                        if not chunk:
                            continue
                        fp.write(chunk)
                        written += len(chunk)
                        if written > max_bytes:
                            fp.close()
                            self._silent_remove(filepath)
                            return False, (
                                f"文件超过上限 {human_size(max_bytes)}，已取消下载"
                            )

                if written == 0:
                    self._silent_remove(filepath)
                    return False, "下载内容为空"
                return True, None
        except asyncio.TimeoutError:
            self._silent_remove(filepath)
            return False, "下载超时"
        except Exception as e:
            self._silent_remove(filepath)
            return False, f"{type(e).__name__}: {e}"

    @staticmethod
    def _silent_remove(path: str):
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    @staticmethod
    async def _delayed_remove(path: str, delay: int = 30):
        await asyncio.sleep(delay)
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
