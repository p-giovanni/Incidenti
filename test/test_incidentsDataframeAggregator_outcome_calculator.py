# 
# File: test_incidentsDataframeAggregator_outcome_calculator.py
# Author(s): Ing. Giovanni Rizzardi - Spring 2019
# Project: DataScience 
# 

import os
import json
import logging
from unittest import TestCase

import pandas as pd

from src.ClassIncidenti import Incidenti
from src.ClassAnagraficaComuni import AnagraficaComuni
from src.ClassIncidentsDataframeAggregator import IncidentsDataframeAggregator
from src.ClassSupportDecodeDataframes import SupportDecodeDataframes


class TestIncidentsDataframeAggregator(TestCase):
    """
    Unit tests for the method:
            - IncidentsDataframeAggregator->__calculate_incident_outcome

    NOTE: this is a private method so this test could be removed.

    """

    def setUp(self) -> None:
        self.__log = logging.getLogger('IncidentsDataframeAggregator')

        s_config = '{"data_structure":{"columns":[], "convert_to_number": [],"incident_outcome_columns":["COL01","COL02","COL03"]}}'
        config = json.loads(s_config)
        support_dataframes = SupportDecodeDataframes()
        support_dataframes.load_dataframes()

        incidenti = Incidenti(file_incidenti = os.path.join("/fake_dir/", "fake_file_name.csv"),
                              anagrafica_comuni = AnagraficaComuni("/fake_dir/fake_filename.csv"),
                              support_dataframes = support_dataframes,
                              data_file_configurations=config["data_structure"])
        test_df = {
            "Comune":          ["Milano", "Como", "Como", "Cernusco Sul Naviglio","Roncolate"],
            "comune":          ["1001",   "1000", "1000", "1002",                 "1003"],
            "Provincia":       ["MI",     "CO",   "CO",   "MI",                    "BG"],
            "COL01":           [ 0,        1,      1,      1,                       1],
            "COL02":           [ 4,        3,      1,      2,                       2],
            "COL03":           [ 1,        1,      2,      2,                       4]
        }
        incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(incidenti)

    def test___calculate_incident_outcome(self):
        try:
            expected_incolumi = 7
            expected_feriti = 4
            expected_morti = 3

            result_df = self.aggregator._IncidentsDataframeAggregator__calculate_incident_outcome()
            self.assertTrue(result_df, "Must return the ok result.")
            df = self.aggregator._IncidentsDataframeAggregator__df()
            n_incolumi = df['Incolumi'].sum()
            n_feriti = df['Feriti'].sum()
            n_morti = df['Morti'].sum()

            self.assertTrue(n_incolumi == expected_incolumi, "Incolumi - check decode and count.")
            self.assertTrue(n_feriti == expected_feriti, "Morti - check decode and count.")
            self.assertTrue(n_morti == expected_morti, "Morti - check decode and count.")
            pass
        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test___calculate_incident_outcome_call_more_than_once(self):
        try:
            expected_incolumi = 7
            expected_feriti = 4
            expected_morti = 3

            result_df = self.aggregator._IncidentsDataframeAggregator__calculate_incident_outcome()
            self.assertTrue(result_df, "Must return the ok result.")

            result_df = self.aggregator._IncidentsDataframeAggregator__calculate_incident_outcome()
            self.assertTrue(result_df, "Must return the ok result.")

            df = self.aggregator._IncidentsDataframeAggregator__df()
            n_incolumi = df['Incolumi'].sum()
            n_feriti = df['Feriti'].sum()
            n_morti = df['Morti'].sum()

            self.assertTrue(n_incolumi == expected_incolumi, "Incolumi - check decode and count.")
            self.assertTrue(n_feriti == expected_feriti, "Morti - check decode and count.")
            self.assertTrue(n_morti == expected_morti, "Morti - check decode and count.")
            pass
        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
