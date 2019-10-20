# 
# File: test_incidentsDataframeAggregator_get_number_of_incidents_by_hour.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
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

class TestIncidentsDataframeAggregator_get_number_of_incidents_by_hour(TestCase):
    """
    Unit tests for the method:
            - IncidentsDataframeAggregator->get_number_of_incidents_by_hour

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
            "OINJ02":          [ 6,        0,      1,      1,                       0,          1], # 9
            "natura_incidente":[11,        1,      1,      2,                       2,          1],
            "Ora":             [15,        2,     10,      2,                       2,         11],
        }
        self.incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(self.incidenti)

    def test_get_number_of_incidents_by_hour_unknown_area(self):
        try:
            result = self.aggregator.get_number_of_incidents_by_hour("UNKNOWN")
            self.assertTrue(result is None, "No result must be returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_number_of_incidents_by_hour_nation_wide_area(self):
        try:
            expected_results = {
                15: 1,
                10: 1,
                11: 1,
                 2: 3
            }
            result_df = self.aggregator.get_number_of_incidents_by_hour("nation_wide")
            self.assertTrue(result_df is not None, "None return not allowed.")
            self.assertTrue(result_df.shape[0] == len(expected_results), "Check the number of found results.")

            for key in expected_results.keys():
                rs = result_df.loc[key]['Numero']
                self.assertTrue(rs == expected_results[key], "Check result for hour {hh}".format(hh=key))

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_number_of_incidents_by_hour_province_area(self):
        try:
            expected_results = {
                15: 1,
                11: 1,
                 2: 1
            }
            result_df = self.aggregator.get_number_of_incidents_by_hour("province", "MI")
            self.assertTrue(result_df is not None, "None return not allowed.")
            self.assertTrue(result_df.shape[0] == len(expected_results), "Check the number of found results.")

            for key in expected_results.keys():
                rs = result_df.loc[key]['Numero']
                self.assertTrue(rs == expected_results[key], "Check result for hour {hh}".format(hh=key))

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_number_of_incidents_by_hour_city_area(self):
        try:
            expected_results = {
                 2: 1,
                10: 1
            }
            result_df = self.aggregator.get_number_of_incidents_by_hour("city", "Como")
            self.assertTrue(result_df is not None, "None return not allowed.")
            self.assertTrue(result_df.shape[0] == len(expected_results), "Check the number of found results.")

            for key in expected_results.keys():
                rs = result_df.loc[key]['Numero']
                self.assertTrue(rs == expected_results[key], "Check result for hour {hh}".format(hh=key))

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
