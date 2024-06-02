# coding: utf-8
""" Data Load Module """

# built-in
from logging import getLogger

# installed

# custom
from src.dal.file import File


log = getLogger('load')


class Load:
    """
    Data load class.

    Attributes
    ----------

    Methods
    -------
    player_data
        Load player data.
    """

    def __init__(self, config):
        self.file = File(config)
        self.layer = 'gold'

    def player_data(self, player):
        """
        Load player data.

        Parameters
        ----------
        player : tuple[int, str]
            Player number and url.

        Returns
        -------
        None
        """

        log.info('player_data...')
        player_number, _ = player
        file_name = f'player_{player_number}'
        file_path = f'{self.layer}/players.csv'
        players = self.file.load(file_path)

        player_data = self.file.load(f'silver/players/{file_name}.bin')
        if players is None:
            self.file.save(player_data, file_path)
        else:
            if player_number not in players['Player ID'].tolist():
                self.file.append(player_data, file_path)

        log.info(f'player_data done!')
