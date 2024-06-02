# coding: utf-8
""" Data Transformation Module """

# built-in
from logging import getLogger
from collections import defaultdict
from re import compile as re_compile

# installed
from bs4 import BeautifulSoup
from pandas import DataFrame

# custom
from src.dal.file import File


log = getLogger('transform')


class Transform:
    """
    Data transformation Class

    Attributes
    ----------

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self, config):
        self.file = File(config)
        self.layer = 'silver'

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

        player_number, _ = player_link
        re_key_value = re_compile(r'.*:.*')
        file_name = f'player_{player_number}'
        file_path = f'{self.layer}/players/{file_name}.bin'
        player_data = self.file.load(file_path)

        if player_data is None:
            player_data = {'Player ID': player_number, 'Squad Number': None}
            html = self.file.load(f'bronze/pages/player/player_{player_number}.html')
            bs = BeautifulSoup(html, "html.parser")
            tables = bs.find('table', {'id': 'table_0'}).find_all('table')[:2]
            for table in tables:
                values = re_key_value.findall(table.text)
                for value in values:
                    key, value = value.split(':')
                    player_data[key] = value

            table = bs.find('table', {'id': 'table_0'}).find_all('table')[2]
            player_data_tmp = defaultdict(list)
            for tr in table.find_all('tr'):
                if tr.find('th'):
                    th_text = tr.text
                    continue
                player_data_tmp[th_text].append(tr.text)

            player_data.update(player_data_tmp)

            player_data = DataFrame([player_data])
            self.file.save(player_data, file_path)
        else:
            log.info('using cache.')

        log.info(f'player_links done!')
