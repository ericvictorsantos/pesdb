# coding: utf-8
""" File """

# built-in
from pathlib import Path
from os import path as os_path
from pickle import dump as pck_save, load as pck_load

# installed
from pandas import read_csv as pd_read_csv

# custom


class File:
    """
    Class data file.

    Attributes
    ----------

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self, config):
        self.data_path = config['data']['path']

    def exists(self, file_path):
        """
        File exists.

        Parameters
        ----------
        file_path : str
            File path.

        Returns
        -------
        flag : bool
            If path exists.
        """

        file_path = f'{self.data_path}/{file_path}'

        flag = True if os_path.exists(file_path) else False

        return flag

    def load(self, file_name):
        """
        Load data.

        Parameters
        ----------
        file_name : str
            File name to data.

        Returns
        -------
        data : Any
            Any type with values.
        """

        file_path = f'{self.data_path}/{file_name}'

        if os_path.exists(file_path):
            extension = Path(file_path).suffix
            if extension == '.csv':
                data = pd_read_csv(file_path)
            elif extension == '.bin':
                with open(file_path, 'rb') as context:
                    data = pck_load(context)
            else:
                with open(file_path, 'r') as context:
                    data = context.read()
        else:
            data = None

        return data

    def save(self, data, file_name):
        """
        Write data.

        Parameters
        ----------
        data : Any
            Data.
        file_name : str
            File name to file data.

        Returns
        -------
        None
        """

        file_path = f'{self.data_path}/{file_name}'
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        extension = Path(file_path).suffix
        if extension == '.bin':
            with open(file_path, 'wb') as context:
                pck_save(data, context)
        elif extension == '.csv':
            data.to_csv(file_path, index=False)
        else:
            with open(file_path, 'w') as context:
                context.write(data)

    def append(self, data, file_name):
        """
        Append data.

        Parameters
        ----------
        data : Any
            Data.
        file_name : str
            File name to file data.

        Returns
        -------
        None
        """

        file_path = f'{self.data_path}/{file_name}'
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(file_path, index=False, mode='a', header=False)
