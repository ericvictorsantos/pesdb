# coding: utf-8
""" Main Module """

# built-in
from logging import getLogger

# installed

# custom
from config import Config
from src.bll.load import Load
from src.bll.extract import Extract
from src.bll.transform import Transform


log = getLogger('main')


class Main:
    """
    Class main of job.

    Attributes
    ----------
    None.

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self):
        self.config = Config().load_config()

    def run(self):
        """
        Run job.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        log.info('----- Start -----')

        extract = Extract(self.config)
        transform = Transform(self.config)
        load = Load(self.config)
        page_links = extract.page_links()
        for page_link in page_links:
            extract.page_data(page_link)
            player_links = extract.player_links(page_link)
            for player_link in player_links:
                extract.player_data(player_link)
                transform.player_data(player_link)
                load.player_data(player_link)

        log.info('----- End -----')


if __name__ == '__main__':
    Main().run()
