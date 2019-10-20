# 
# File: test_incidentsDataframeAggregator.py
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
            - IncidentsDataframeAggregator->get_number_of_incidents_by_comune

    """
    def setUp(self) -> None:
        self.__log = logging.getLogger('IncidentsDataframeAggregator')

        s_config = '{"data_structure":{"columns":[], "convert_to_number": []}}'
        config = json.loads(s_config)

        incidenti = Incidenti(file_incidenti = os.path.join("/fake_dir/", "fake_file_name.csv"),
                              anagrafica_comuni = AnagraficaComuni("/fake_dir/fake_filename.csv"),
                              support_dataframes = SupportDecodeDataframes(),
                              data_file_configurations=config["data_structure"])
        test_df = {
            "Comune":         ["Milano", "Como", "Como", "Cernusco Sul Naviglio", "Roncolate"],
            "comune":         ["1001",   "1000", "1000", "1002",                  "1003" ],
            "provincia":      ["MI",     "CO",   "CO",   "MI",                    "BG"],
            "tipo_veicolo_a": [ 1,        1,      2,      1,                      2]
        }
        incidenti.df_incidenti = pd.DataFrame.from_records(test_df)
        self.aggregator = IncidentsDataframeAggregator(incidenti)

    def test_get_number_of_incidents_by_comune(self):
        """

        :return:
        """
        try:
            expected_result = 2
            result = self.aggregator.get_number_of_incidents_by_comune("Como")
            self.assertTrue(result == expected_result, "Check result - multiple rows returned.")

            expected_result = 1
            result = self.aggregator.get_number_of_incidents_by_comune("Roncolate")
            self.assertTrue(result == expected_result, "Check result - single row returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")


    def test_get_number_of_incidents_by_not_existing_comune(self):
        """

        :return:
        """
        result = 0
        expected_result = 0
        try:
            result = self.aggregator.get_number_of_incidents_by_comune("Pero")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

        self.assertTrue(result == expected_result)