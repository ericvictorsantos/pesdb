# coding: utf-8
""" Config Application """

# built-in
import logging
from os import path as os_path

# installed
from toml import load as toml_load

# custom


logging.basicConfig(
    format='[%(asctime)s] - [%(name)s] - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)


class Config:
    """
    Class configuration job.

    Attributes
    ----------
    job_path : str
        Path of job.

    Methods
    -------
    load_config()
        Load configuration job from toml.
    """

    def __init__(self):
        self.job_path = os_path.dirname(__file__)

    def load_config(self):
        """
        Load configuration from .toml.

        Parameters
        ----------
        None.

        Returns
        -------
        config : dict
            Job configuration.
        """

        config = toml_load(f'{self.job_path}/config.toml')

        config['data'] = {
            'path': f'{self.job_path}/data'
        }

        return config
