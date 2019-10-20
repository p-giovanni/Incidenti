# 
# File: ClassIncidenti.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
# Project: DataScience
# 

import logging

import numpy as np
import pandas as pd

# ----------------------------------------
# class Incidenti
# ----------------------------------------
class Incidenti(object):

    def __init__(self,
                 file_incidenti,
                 anagrafica_comuni,
                 support_dataframes,
                 data_file_configurations):
        """

        :param file_incidenti:
        :param anagrafica_comuni:
        :param support_dataframes:
        :param data_file_configurations:
        """
        self.__log = logging.getLogger('Incidenti')
        self.__file_incidenti = file_incidenti
        self.__anagrafica_comuni = anagrafica_comuni
        self.__support_dataframes = support_dataframes
        self.__incidenti_columns = data_file_configurations['columns']
        self.__to_be_converted = data_file_configurations["convert_to_number"]
        self.__data_configurations = data_file_configurations
        self.__loaded = False
        self.df_incidenti = None

    # ----------------------------------------
    # get_version
    # ----------------------------------------
    @staticmethod
    def get_version():
        return "00.00.05 alfa"

    # ----------------------------------------
    # get_data_configurations
    # ----------------------------------------
    def get_data_configurations(self):
        return self.__data_configurations

    # ----------------------------------------
    # __load_incidenti
    # ----------------------------------------
    def __load_incidenti(self):
        """
        Carica il file dati da CSV in una tabella Pandas.

        :return: True in caso di successo, False per errore.
        """
        self.__log.info("__load_incidenti >>")
        rv = False
        try:
            # Leggo il file dati.
            # Nota che impongo il tipo di dato per alcune colonne.
            self.df_incidenti = pd.read_csv(self.__file_incidenti,
                                            sep='\t',
                                            dtype={2: 'str', 7: 'object', 114: 'object', 115: 'object'})

            self.df_incidenti = self.df_incidenti[self.__incidenti_columns]

            # Il codice univoco del comune e' "cod provincia"+"cod comune". Creo una colonna che lo contenga.
            self.df_incidenti['s_provincia'] = self.df_incidenti["provincia"].apply(str)
            self.df_incidenti["Codice Comune"] = self.df_incidenti["s_provincia"] + self.df_incidenti["comune"]
            self.df_incidenti["Codice Comune"] = self.df_incidenti["Codice Comune"].apply(np.int64)

            # Change all the column containing the outcome of an incident in an integer type.
            for col in self.__to_be_converted:
                self.df_incidenti[col].replace(' ', '0', inplace=True)
                self.df_incidenti[col] = self.df_incidenti[col].apply(np.int64)
            rv = True

        except Exception as ex:
            self.__log.error("Load error: {ex} - {col}".format(ex=str(ex),col=col))
        self.__log.info("__load_incidenti ({rv}) <<".format(rv=rv))
        return rv

    # ----------------------------------------
    # __get_number_of_incidents
    # ----------------------------------------
    def __get_number_of_incidents(self):
        self.__log.info("__get_number_of_incidents >>")
        result = self.df_incidenti.shape[0]
        self.__log.info("__get_number_of_incidents <<")
        return result

    # ----------------------------------------
    # get_support_dataframes
    # ----------------------------------------
    def get_support_dataframes(self):
        return self.__support_dataframes

    # ----------------------------------------
    # load_data_files
    # ----------------------------------------
    def load_data_files(self):
        """
        Questo metodo carica il file dati in una tabella Pandas.

        Esegue anche una join con la tabella dell'anagrafica dei
        comuni Italiani per avere i codici comune decodificati con
        il nome.

        :return: True in caso di successo, False per errore.
        """
        self.__log.info("load_data_files >>")
        rv = False
        try:
            rv = self.__anagrafica_comuni.load_data_file()
            if rv == True:
                rv = self.__load_incidenti()
                if rv == True:
                    # Join sui due dataset per aggiungere il nome del comune al codice.
                    self.df_incidenti = pd.merge(self.df_incidenti.set_index('Codice Comune', drop=False),
                                                 self.__anagrafica_comuni.df_comuni.set_index('Codice Comune', drop=False),
                                                 left_index=True,
                                                 right_index=True,
                                                 how="inner")
                    self.__log.debug(str(self.df_incidenti.columns.values))

                    # Pulizia delle colonne che non servono - per debug guarda il passo prima.
                    self.df_incidenti = self.df_incidenti[['Nome Provincia', 'Comune'] + self.__incidenti_columns]
                    self.df_incidenti.columns.values[0] = "Provincia"

                    self.__support_dataframes.load_dataframes()

                    self.__loaded = True
                    rv = True
            else:
                self.__log.error("Error loading \"anagrafica comuni\".")

        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))

        self.__log.info("load_data_files ({rv}) <<".format(rv=rv))
        return rv

