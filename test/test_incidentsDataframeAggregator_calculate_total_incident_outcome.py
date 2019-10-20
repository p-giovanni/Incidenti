# 
# File: test_incidentsDataframeAggregator_calculate_total_incident_outcome.py
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
            - IncidentsDataframeAggregator->calculate_total_incident_outcome

    """

    def setUp(self) -> None:
        self.__log = logging.getLogger('IncidentsDataframeAggregator')

        s_config = '{"data_structure":{"columns":[], ' \
                    '"convert_to_number": [],' \
                    '"incident_outcome_columns":["COL01","COL02","COL03"],' \
                    '"other_injured_columns": ["OINJ01", "OINJ02"],' \
                    '"other_deadh_columns": ["ODEA01"]' \
                    '}}'
        config = json.loads(s_config)
        support_dataframes = SupportDecodeDataframes()
        support_dataframes.load_dataframes()

        self.incidenti = Incidenti(file_incidenti = os.path.join("/fake_dir/", "fake_file_name.csv"),
                                   anagrafica_comuni = AnagraficaComuni("/fake_dir/fake_filename.csv"),
                                   support_dataframes = support_dataframes,
                                   data_file_configurations=config["data_structure"])
        test_df = {
            "Comune":          ["Milano", "Como", "Como", "Cernusco Sul Naviglio","Roncolate", "Vimodrone"],
            "comune":          ["1001",   "1000", "1000", "1002",                 "1003",      "1004"],
            "Provincia":       ["MI",     "CO",   "CO",   "MI",                    "BG",       "MI"],

            "COL01":           [ 0,        1,      1,      1,                       1,          4], # Incolume: 4, Morto: 1
            "COL02":           [ 4,        3,      1,      2,                       2,          1], # Incolume: 2, Morto: 2, Ferito: 2
            "COL03":           [ 1,        1,      2,      2,                       4,          1], # Incolume: 3, Morto: 1, Ferito: 2

            "ODEA01":          [ 0,        0,      2,      1,                       0,          1], # 4

            "OINJ01":          [ 3,        0,      0,      0,                       5,          0], # 8
            "OINJ02":          [ 6,        0,      1,      1,                       0,          1]  # 9
        }
        self.incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(self.incidenti)

    def test_calculate_total_incident_outcome(self):
        try:
            expected_o_feriti = 17
            expected_o_morti = 4

            expected_incolumi = 9
            expected_feriti = 4 + expected_o_feriti
            expected_morti = 4 + expected_o_morti

            result = self.aggregator.calculate_total_incident_outcome()
            self.assertTrue(result, "Must return the OK code.")

            injured_num = self.incidenti.df_incidenti["Feriti"].sum()
            dead_num = self.incidenti.df_incidenti["Morti"].sum()
            uninjured_num = self.incidenti.df_incidenti["Incolumi"].sum()

            self.assertTrue(uninjured_num == expected_incolumi, "Check totals - incolumi.")
            self.assertTrue(dead_num == expected_morti, "Check totals - morti.")
            self.assertTrue(injured_num == expected_feriti, "Check totals - feriti.")

            other_injured = self.incidenti.df_incidenti["TotAltriFeriti"].sum()
            other_dead = self.incidenti.df_incidenti["TotAltriMorti"].sum()
            self.assertTrue(expected_o_feriti == other_injured, "Chech partial - injured count.")
            self.assertTrue(expected_o_morti == other_dead, "Chech partial - dead count.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_calculate_total_incident_outcome_call_more_than_once(self):
        try:
            expected_o_feriti = 17
            expected_o_morti = 4

            expected_incolumi = 9
            expected_feriti = 4 + expected_o_feriti
            expected_morti = 4 + expected_o_morti

            result = self.aggregator.calculate_total_incident_outcome()
            self.assertTrue(result, "Must return the OK (1) code.")

            result = self.aggregator.calculate_total_incident_outcome()
            self.assertTrue(result, "Must return the OK (2) code.")

            injured_num = self.incidenti.df_incidenti["Feriti"].sum()
            dead_num = self.incidenti.df_incidenti["Morti"].sum()
            uninjured_num = self.incidenti.df_incidenti["Incolumi"].sum()

            self.assertTrue(uninjured_num == expected_incolumi, "Check totals - incolumi.")
            self.assertTrue(dead_num == expected_morti, "Check totals - morti.")
            self.assertTrue(injured_num == expected_feriti, "Check totals - feriti.")

            other_injured = self.incidenti.df_incidenti["TotAltriFeriti"].sum()
            other_dead = self.incidenti.df_incidenti["TotAltriMorti"].sum()
            self.assertTrue(expected_o_feriti == other_injured, "Chech partial - injured count.")
            self.assertTrue(expected_o_morti == other_dead, "Chech partial - dead count.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")


