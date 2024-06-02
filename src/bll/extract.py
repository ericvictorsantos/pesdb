# coding: utf-8
""" Data Extraction Module """

# built-in
from time import sleep
from re import compile as re_compile
from logging import getLogger

# installed
from bs4 import BeautifulSoup
from requests import get as requests_get

# custom
from src.dal.file import File


log = getLogger('extract')


class Extract:
    """
    Data extraction class.

    Attributes
    ----------

    Methods
    -------
    None.
    """

    def __init__(self, config):
        self.file = File(config)
        self.url = config['params']['url']
        self.layer = 'bronze'

    def page_links(self):
        """
        Get page links.

        Parameters
        ----------
        None.

        Returns
        -------
        page_links : list[tuple[int, str]]
            Page number and url.
        """

        log.info('page_links...')

        file_name = 'links'
        file_path = f'{self.layer}/links/index/{file_name}.bin'
        page_links = self.file.load(file_path)

        if page_links is None:
            page_links = []
            log.info(f'url: {self.url}')
            page_text = requests_get(self.url).text
            bs = BeautifulSoup(page_text, "html.parser")
            hrefs = bs.find('div', class_='pages').find_all('a')
            last_page = int(hrefs[-1].text)

            for page_number in range(1, last_page + 1):
                page_link = f'{self.url}/?page={page_number}'
                page_links.append((page_number, page_link))

            self.file.save(page_links, file_path)
        else:
            log.info('using cache.')

        log.info(f'page_links: {len(page_links)}')

        log.info(f'page_links done!')

        return page_links

    def page_data(self, page_link):
        """
        Get page data.

        Parameters
        ----------
        page_link : tuple[int, str].
            Page number and url.

        Returns
        -------
        None
        """

        log.info('page_data...')

        page_number, page_url = page_link
        file_name = f'page_{page_number}'
        file_path = f'{self.layer}/pages/index/{file_name}.html'
        html = self.file.exists(file_path)

        if html is False:
            log.info(f'url: {page_url}')
            response = requests_get(page_url)
            html = response.text
            self.file.save(html, file_path)

        log.info(f'page_data done!')

    def player_links(self, page_link):
        """
        Get player links.

        Parameters
        ----------
        page_link : tuple[int, str].
            Page number and url.

        Returns
        -------
        player_links : list[tuple[int, str]]
            Player number and URL.
        """

        log.info('player_links...')

        page_number, _ = page_link
        re_number = re_compile(r'\d+')
        re_href = re_compile(r'\./\?id=\d+')
        file_name = f'links_{page_number}'
        file_path = f'{self.layer}/links/player/{file_name}.bin'
        player_links = self.file.load(file_path)

        if player_links is None:
            player_links = []
            html = self.file.load(f'{self.layer}/pages/index/page_{page_number}.html')

            if html:
                bs = BeautifulSoup(html, "html.parser")
                player_ids = bs.find('table').find_all('a', href=re_href)

                for player_id in player_ids:
                    player_id = int(re_number.search(player_id.attrs['href']).group(0))
                    player_link = f'{self.url}/?id={player_id}'
                    player_links.append((player_id, player_link))

                self.file.save(player_links, file_path)

        log.info(f'player_links {len(player_links)}')

        log.info(f'player_links done!')

        return player_links

    def player_data(self, player_link):
        """
        Get player links.

        Parameters
        ----------
        player_link : tuple[int, str].
            Player number and url.

        Returns
        -------
        None.
        """

        log.info('player_links...')

        player_number, player_url = player_link
        file_name = f'player_{player_number}'
        file_path = f'{self.layer}/pages/player/{file_name}.html'
        html = self.file.exists(file_path)

        if html is False:
            log.info(f'url: {player_url}')
            response = requests_get(player_url)
            html = response.text
            self.file.save(html, file_path)
            sleep(2)

        log.info(f'player_links done!')
