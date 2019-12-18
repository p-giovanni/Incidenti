# 
# File: test_incidentsDataframeAggregator_get_number_of_incidents_by_provincia.py
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


class TestIncidentsDataframeAggregator_get_number_of_incidents_by_provincia(TestCase):
    """
    Unit tests for the method:
            - IncidentsDataframeAggregator->get_number_of_incidents_by_provincia

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
            "Comune":          ["Milano", "Como", "Como", "Cernusco Sul Naviglio","Roncolate", "Vimodrone", "Almenno"],
            "Provincia":       ["MI",     "CO",   "CO",   "MI",                    "BG",       "MI",         "AL"],
            "comune":          ["1001",   "1000", "1000", "1002",                 "1003",      "1004",       "1005"]
        }
        self.incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(self.incidenti)

    def test_get_number_of_incidents_by_provincia(self):
        try:
            expected_result = 3
            result = self.aggregator.get_number_of_incidents_by_provincia("MI")
            self.assertTrue(expected_result == result, "Check the result for MI.")

            expected_result = 1
            result = self.aggregator.get_number_of_incidents_by_provincia("AL")
            self.assertTrue(expected_result == result, "Check the result for BG.")

            expected_result = 2
            result = self.aggregator.get_number_of_incidents_by_provincia("CO")
            self.assertTrue(expected_result == result, "Check the result for CO.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_number_of_incidents_by_provincia_not_existent(self):
        try:
            expected_result = 0
            result = self.aggregator.get_number_of_incidents_by_provincia("DOES_NOT_EXIST")
            self.assertTrue(expected_result == result, "Check the result for DOES_NOT_EXIST.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
