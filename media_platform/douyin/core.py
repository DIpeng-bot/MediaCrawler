# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：  
# 1. 不得用于任何商业用途。  
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。  
# 3. 不得进行大规模爬取或对平台造成运营干扰。  
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。   
# 5. 不得用于任何非法或不当的用途。
#   
# 详细许可条款请参阅项目根目录下的LICENSE文件。  
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。  


import asyncio
import os
import random
from asyncio import Task
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

import config
from base.base_crawler import AbstractCrawler
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import douyin as douyin_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import DOUYINClient
from .exception import DataFetchError
from .field import PublishTimeType
from .login import DouYinLogin


class DouYinCrawler(AbstractCrawler):
    context_page: Page
    dy_client: DOUYINClient
    browser_context: BrowserContext

    def __init__(self) -> None:
        self.index_url = "https://www.douyin.com"

    async def start(self) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY:
            ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            # Launch a browser context.
            chromium = playwright.chromium
            self.browser_context = await self.launch_browser(
                chromium,
                None,
                user_agent=None,
                headless=config.HEADLESS
            )
            # stealth.min.js is a js script to prevent the website from detecting the crawler.
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)

            self.dy_client = await self.create_douyin_client(httpx_proxy_format)
            if not await self.dy_client.pong(browser_context=self.browser_context):
                login_obj = DouYinLogin(
                    login_type=config.LOGIN_TYPE,
                    login_phone="",  # you phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES
                )
                await login_obj.begin()
                await self.dy_client.update_cookies(browser_context=self.browser_context)
            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                # Search for notes and retrieve their comment information.
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # Get the information and comments of the specified post
                await self.get_specified_awemes()
            elif config.CRAWLER_TYPE == "creator":
                # Get the information and comments of the specified creator
                await self.get_creators_and_videos()

            utils.logger.info("[DouYinCrawler.start] Douyin Crawler finished ...")

    async def search(self) -> None:
        utils.logger.info("[DouYinCrawler.search] Begin search douyin keywords")
        try:
            dy_limit_count = 10  # douyin limit page fixed value
            if config.CRAWLER_MAX_NOTES_COUNT < dy_limit_count:
                config.CRAWLER_MAX_NOTES_COUNT = dy_limit_count
            start_page = config.START_PAGE  # start page number
            
            download_tasks = []  # Track all download tasks
            total_processed = 0  # Track total processed items
            
            for keyword in config.KEYWORDS:
                if total_processed >= config.CRAWLER_MAX_NOTES_COUNT:
                    utils.logger.info(f"Reached maximum notes count ({config.CRAWLER_MAX_NOTES_COUNT}), stopping search")
                    break
                    
                keyword = keyword.strip()
                source_keyword_var.set(keyword)
                utils.logger.info(f"[DouYinCrawler.search] Current keyword: {keyword}")
                aweme_list: List[str] = []
                page = 0
                dy_search_id = ""
                
                while (page - start_page + 1) * dy_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                    if page < start_page:
                        utils.logger.info(f"[DouYinCrawler.search] Skip {page}")
                        page += 1
                        continue
                        
                    try:
                        utils.logger.info(f"[DouYinCrawler.search] search douyin keyword: {keyword}, page: {page}")
                        posts_res = await self.dy_client.search_info_by_keyword(
                            keyword=keyword,
                            offset=page * dy_limit_count - dy_limit_count,
                            publish_time=PublishTimeType(config.PUBLISH_TIME_TYPE),
                            search_id=dy_search_id
                        )
                        utils.logger.debug(f"Got search results: {len(posts_res.get('data', []))} items")
                    except DataFetchError:
                        utils.logger.error(f"[DouYinCrawler.search] search douyin keyword: {keyword} failed")
                        break

                    page += 1
                    if "data" not in posts_res:
                        utils.logger.error(
                            f"[DouYinCrawler.search] search douyin keyword: {keyword} failed，账号也许被风控了。")
                        break
                        
                    dy_search_id = posts_res.get("extra", {}).get("logid", "")
                    
                    for post_item in posts_res.get("data"):
                        if total_processed >= config.CRAWLER_MAX_NOTES_COUNT:
                            break
                            
                        try:
                            aweme_info: Dict = post_item.get("aweme_info") or \
                                               post_item.get("aweme_mix_info", {}).get("mix_items")[0]
                            utils.logger.debug(f"Processing aweme: {aweme_info.get('aweme_id')}")
                        except TypeError:
                            continue
                            
                        aweme_list.append(aweme_info.get("aweme_id", ""))
                        await douyin_store.update_douyin_aweme(aweme_item=aweme_info)
                        
                        # Add download task to the list instead of downloading immediately
                        if config.ENABLE_GET_IMAGES and aweme_info:
                            video_url = self._get_video_url(aweme_info)
                            if video_url:
                                save_dir = os.path.join(config.DOWNLOAD_PATH, "videos", "douyin")
                                save_path = os.path.join(save_dir, f"{aweme_info.get('aweme_id')}.mp4")
                                download_tasks.append(self.dy_client.download_video(video_url, save_path))
                        
                        total_processed += 1
                        utils.logger.info(f"Processed {total_processed}/{config.CRAWLER_MAX_NOTES_COUNT} items")
                        
                    if total_processed >= config.CRAWLER_MAX_NOTES_COUNT:
                        utils.logger.info(f"Reached maximum notes count ({config.CRAWLER_MAX_NOTES_COUNT}), stopping search")
                        break
                
                utils.logger.info(f"[DouYinCrawler.search] keyword:{keyword}, aweme_list:{aweme_list}")
                await self.batch_get_note_comments(aweme_list)
            
            # Wait for all downloads to complete before closing
            if download_tasks:
                utils.logger.info(f"Waiting for {len(download_tasks)} downloads to complete...")
                await asyncio.gather(*download_tasks)
                
        except Exception as e:
            utils.logger.error(f"Search error: {e}")
            utils.logger.exception("Detailed error:")
        finally:
            # 确保浏览器正确关闭
            try:
                if hasattr(self, 'browser_context') and self.browser_context:
                    await self.browser_context.close()
            except Exception as e:
                utils.logger.error(f"Error closing browser: {e}")
                
    def _get_video_url(self, aweme_info: Dict) -> Optional[str]:
        """Helper method to extract video URL from aweme info"""
        video_urls = []
        if 'video' in aweme_info:
            # Try play_addr
            play_urls = aweme_info['video'].get('play_addr', {}).get('url_list', [])
            if play_urls:
                video_urls.extend(play_urls)
            # Try download_addr
            download_urls = aweme_info['video'].get('download_addr', {}).get('url_list', [])
            if download_urls:
                video_urls.extend(download_urls)
            # Try bit_rate
            bit_rate = aweme_info['video'].get('bit_rate', [])
            if bit_rate:
                for rate in bit_rate:
                    if 'play_addr' in rate:
                        bit_rate_urls = rate['play_addr'].get('url_list', [])
                        if bit_rate_urls:
                            video_urls.extend(bit_rate_urls)
        
        # Prefer HTTPS URLs
        video_url = next((url for url in video_urls if url and url.startswith('https')), None)
        if not video_url:
            video_url = next((url for url in video_urls if url), None)
            
        return video_url

    async def get_specified_awemes(self):
        """Get the information and comments of the specified post"""
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_aweme_detail(aweme_id=aweme_id, semaphore=semaphore) for aweme_id in config.DY_SPECIFIED_ID_LIST
        ]
        aweme_details = await asyncio.gather(*task_list)
        for aweme_detail in aweme_details:
            if aweme_detail is not None:
                await douyin_store.update_douyin_aweme(aweme_detail)
        await self.batch_get_note_comments(config.DY_SPECIFIED_ID_LIST)

    async def get_aweme_detail(self, aweme_id: str, semaphore: asyncio.Semaphore) -> Any:
        """Get note detail"""
        async with semaphore:
            try:
                aweme_detail = await self.dy_client.get_video_by_id(aweme_id)
                utils.logger.debug(f"Got aweme detail: {aweme_id}")
                if config.ENABLE_GET_IMAGES and aweme_detail:
                    utils.logger.debug(f"ENABLE_GET_IMAGES is True, processing video download")
                    # 获取视频下载地址
                    video_url = aweme_detail.get('video', {}).get('play_addr', {}).get('url_list', [])[0]
                    utils.logger.debug(f"Video URL: {video_url}")
                    if video_url:
                        # 确保目录存在
                        save_dir = os.path.join(config.DOWNLOAD_PATH, "videos", "douyin")
                        os.makedirs(save_dir, exist_ok=True)
                        utils.logger.debug(f"Created directory: {save_dir}")
                        
                        # 下载视频
                        save_path = os.path.join(save_dir, f"{aweme_id}.mp4")
                        utils.logger.info(f"Downloading video: {aweme_id} to {save_path}")
                        await self.dy_client.download_video(video_url, save_path)
                else:
                    utils.logger.debug(f"Skipping video download: ENABLE_GET_IMAGES={config.ENABLE_GET_IMAGES}, aweme_detail exists={bool(aweme_detail)}")
                return aweme_detail
            except DataFetchError as ex:
                utils.logger.error(f"[DouYinCrawler.get_aweme_detail] Get aweme detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(
                    f"[DouYinCrawler.get_aweme_detail] have not fund note detail aweme_id:{aweme_id}, err: {ex}")
                return None

    async def batch_get_note_comments(self, aweme_list: List[str]) -> None:
        """
        Batch get note comments
        """
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(f"[DouYinCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        task_list: List[Task] = []
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        for aweme_id in aweme_list:
            task = asyncio.create_task(
                self.get_comments(aweme_id, semaphore), name=aweme_id)
            task_list.append(task)
        if len(task_list) > 0:
            await asyncio.wait(task_list)

    async def get_comments(self, aweme_id: str, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            try:
                # 将关键词列表传递给 get_aweme_all_comments 方法
                await self.dy_client.get_aweme_all_comments(
                    aweme_id=aweme_id,
                    crawl_interval=random.random(),
                    is_fetch_sub_comments=config.ENABLE_GET_SUB_COMMENTS,
                    callback=douyin_store.batch_update_dy_aweme_comments,
                    max_count=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES
                )
                utils.logger.info(
                    f"[DouYinCrawler.get_comments] aweme_id: {aweme_id} comments have all been obtained and filtered ...")
            except DataFetchError as e:
                utils.logger.error(f"[DouYinCrawler.get_comments] aweme_id: {aweme_id} get comments failed, error: {e}")

    async def get_creators_and_videos(self) -> None:
        """
        Get the information and videos of the specified creator
        """
        utils.logger.info("[DouYinCrawler.get_creators_and_videos] Begin get douyin creators")
        for user_id in config.DY_CREATOR_ID_LIST:
            creator_info: Dict = await self.dy_client.get_user_info(user_id)
            if creator_info:
                await douyin_store.save_creator(user_id, creator=creator_info)

            # Get all video information of the creator
            all_video_list = await self.dy_client.get_all_user_aweme_posts(
                sec_user_id=user_id,
                callback=self.fetch_creator_video_detail
            )

            video_ids = [video_item.get("aweme_id") for video_item in all_video_list]
            await self.batch_get_note_comments(video_ids)

    async def fetch_creator_video_detail(self, video_list: List[Dict]):
        """
        Concurrently obtain the specified post list and save the data
        """
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_aweme_detail(post_item.get("aweme_id"), semaphore) for post_item in video_list
        ]

        note_details = await asyncio.gather(*task_list)
        for aweme_item in note_details:
            if aweme_item is not None:
                await douyin_store.update_douyin_aweme(aweme_item)

    @staticmethod
    def format_proxy_info(ip_proxy_info: IpInfoModel) -> Tuple[Optional[Dict], Optional[Dict]]:
        """format proxy info for playwright and httpx"""
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",
            "username": ip_proxy_info.user,
            "password": ip_proxy_info.password,
        }
        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"
        }
        return playwright_proxy, httpx_proxy

    async def create_douyin_client(self, httpx_proxy: Optional[str]) -> DOUYINClient:
        """Create douyin client"""
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())  # type: ignore
        douyin_client = DOUYINClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": await self.context_page.evaluate("() => navigator.userAgent"),
                "Cookie": cookie_str,
                "Host": "www.douyin.com",
                "Origin": "https://www.douyin.com/",
                "Referer": "https://www.douyin.com/",
                "Content-Type": "application/json;charset=UTF-8"
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return douyin_client

    async def launch_browser(
            self,
            chromium: BrowserType,
            playwright_proxy: Optional[Dict],
            user_agent: Optional[str],
            headless: bool = True
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        if config.SAVE_LOGIN_STATE:
            user_data_dir = os.path.join(os.getcwd(), "browser_data",
                                         config.USER_DATA_DIR % config.PLATFORM)  # type: ignore
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                accept_downloads=True,
                headless=headless,
                proxy=playwright_proxy,  # type: ignore
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )  # type: ignore
            return browser_context
        else:
            browser = await chromium.launch(headless=headless, proxy=playwright_proxy)  # type: ignore
            browser_context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context

    async def close(self) -> None:
        """Close browser context"""
        await self.browser_context.close()
        utils.logger.info("[DouYinCrawler.close] Browser context closed ...")
