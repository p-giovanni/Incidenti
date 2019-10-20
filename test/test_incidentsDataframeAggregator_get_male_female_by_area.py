# 
# File: test_incidentsDataframeAggregator_get_male_female_by_area.py
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


class TestIncidentsDataframeAggregator_get_male_female_by_area(TestCase):
    """
    Unit tests for the method:
            - IncidentsDataframeAggregator->get_male_female_by_area

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
            "Comune":                        ["Milano", "Como", "Como", "Cernusco Sul Naviglio","Roncolate", "Vimodrone", "Almenno"],
            "Provincia":                     ["MI",     "CO",   "CO",   "MI",                    "BG",       "MI",        "AL"],
            "comune":                        ["1001",   "1000", "1000", "1002",                 "1003",      "1004",      "1005"],
            "veicolo__a___sesso_conducente": [ 1,        1,      2,      0,                      1,           1,            0],
        }
        self.incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(self.incidenti)

    def test_get_male_female_by_area_nation_wide(self):
        try:
            expected_male = 4
            expected_female = 1
            expected_unknown = 2    

            result = self.aggregator.get_male_female_by_area("nation_wide")
            self.assertTrue(result is not None, "None return not allowed.")

            result.set_index('Sesso conducente', inplace=True)
            result_male = result.loc['maschio','Numero']
            self.assertTrue(result_male == expected_male, "Check numeric result - male.")

            result_female = result.loc['femmina','Numero']
            self.assertTrue(result_female == expected_female, "Check numeric result - female.")

            result_unknown = result.loc['non dato','Numero']
            self.assertTrue(result_unknown == expected_unknown, "Check numeric result - unknown.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_male_female_by_area_province(self):
        try:
            expected_male = 2
            expected_unknown = 1

            result = self.aggregator.get_male_female_by_area("province", "MI")
            self.assertTrue(result is not None, "None return not allowed.")

            result.set_index('Sesso conducente', inplace=True)
            result_male = result.loc['maschio','Numero']
            self.assertTrue(result_male == expected_male, "Check numeric result - male.")

            self.assertFalse('femmina' in result.index, "Check numeric result - female.")

            result_unknown = result.loc['non dato','Numero']
            self.assertTrue(result_unknown == expected_unknown, "Check numeric result - unknown.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_male_female_by_area_city(self):
        try:
            expected_male = 1
            expected_female = 1

            result = self.aggregator.get_male_female_by_area("city", "Como")
            self.assertTrue(result is not None, "None return not allowed.")

            result.set_index('Sesso conducente', inplace=True)
            result_male = result.loc['maschio','Numero']
            self.assertTrue(result_male == expected_male, "Check numeric result - male.")

            result_female = result.loc['femmina','Numero']
            self.assertTrue(result_female == expected_female, "Check numeric result - female.")

            self.assertFalse('non dato' in result.index, "Check numeric result - unknown.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_male_female_by_area_unknown_area(self):
        try:
            result = self.aggregator.get_male_female_by_area("province", "NOT KNOWN")
            self.assertTrue(result is None, "No result returned - province.")

            result = self.aggregator.get_male_female_by_area("city", "NOT KNOWN")
            self.assertTrue(result is None, "No result returned - city.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
