#  
# File: ClassAnagraficaComuni.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
# Project: DataScience
#

import logging

import numpy as np
import pandas as pd


class AnagraficaComuni(object):

    def __init__(self, file_comuni_anagrafica):
        self.__file_comuni_anagrafica = file_comuni_anagrafica
        self.__log = logging.getLogger('AnagraficaComuni')

    # ----------------------------------------
    # __load_anagrafica_comuni
    # ----------------------------------------
    def load_data_file(self):
        self.__log.info("load_data_file >>")
        rv = False
        try:
            self.__log.debug("Data file to load: {path}".format(path=self.__file_comuni_anagrafica))
            df_comuni = pd.read_csv(self.__file_comuni_anagrafica, sep=';', quotechar='"', encoding='utf8')
            df_comuni.columns.values[0] = "Codice Regione"
            df_comuni.columns.values[2] = "Codice Provincia"
            df_comuni.columns.values[5] = "Comune"
            df_comuni.columns.values[11] = "Nome Provincia"
            df_comuni.columns.values[14] = "Codice Comune"

            # Riduco le colonne tenendo solo quelle che servono.
            self.df_comuni = df_comuni[
                ["Codice Regione",
                 "Codice Provincia",
                 "Codice Comune",
                 "Comune",
                 "Denominazione regione",
                 "Nome Provincia"]
            ]
            rv = True

        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))
        self.__log.info("load_data_file ({rv}) <<".format(rv=rv))
        return rv


