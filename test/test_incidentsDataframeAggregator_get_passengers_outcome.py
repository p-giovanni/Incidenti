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

class TestIncidentsDataframeAggregator_get_passengers_outcome(TestCase):
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
            "Comune":          ["Milano", "Como", "Como", "Cernusco Sul Naviglio","Roncolate", "Vimodrone" ,"Peschiera Borromeo"],
            "comune":          ["1001",   "1000", "1000", "1002",                 "1003",      "1004"      ,"1005"],
            "Provincia":       ["MI",     "CO",   "CO",   "MI",                    "BG",       "MI"        ,"MI"],
            "COL01":           [ 4,        2,      2,      1,                       1,          3          ,2 ],
            # COL01 -> Incolume: 2, Feriti: 2 ,Morto: 1+1
            "COL02":           [ 4,        2,      2,      1,                       1,          3          ,1 ]
            # COL02[MI] -> Incolume: 2, Feriti: 2 ,Morto: 1+1
        }
        self.incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(self.incidenti)

    def test_get_passengers_outcome_unknown_area(self):
        try:
            result = self.aggregator.get_passengers_outcome(area="UNKNOWN", passenger_column="XXX")
            self.assertTrue(result is None, "No result must be returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_passengers_outcome_city_none(self):
        try:
            result = self.aggregator.get_passengers_outcome(area="city", name=None, passenger_column="XXX")
            self.assertTrue(result is None, "No result must be returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_passengers_outcome_column_none(self):
        try:
            result = self.aggregator.get_passengers_outcome(area="city", name="Vimodrone", passenger_column=None)
            self.assertTrue(result is None, "No result must be returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_passengers_outcome_nation_wide_area(self):
        try:
            expected_results = {
                "Incolume": 2,
                "Ferito": 3,
                "Morto nelle 24 ore": 1,
                "Morto entro il trentesimo giorno": 1
            }
            result_df = self.aggregator.get_passengers_outcome("nation_wide", passenger_column="COL01")
            self.assertTrue(result_df is not None, "None return not allowed.")
            self.assertTrue(result_df.shape[0] == len(expected_results), "Check the number of found results.")

            result_df = result_df[["descrizione","Numero"]]
            result_df.set_index("descrizione", inplace=True)
            for key in expected_results.keys():
                self.assertTrue(result_df.loc[key]['Numero'] == expected_results[key], "Check key value {k}".format(k=key))
            pass
        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_passengers_outcome_nation_province(self):
        try:
            expected_results = {
                "Incolume": 2,
                "Morto nelle 24 ore": 1,
                "Morto entro il trentesimo giorno": 1
            }
            result_df = self.aggregator.get_passengers_outcome("province", passenger_column="COL02", name="MI")
            self.assertTrue(result_df is not None, "None return not allowed.")
            self.assertTrue(result_df.shape[0] == len(expected_results), "Check the number of found results.")

            result_df = result_df[["descrizione","Numero"]]
            result_df.set_index("descrizione", inplace=True)
            for key in expected_results.keys():
                self.assertTrue(result_df.loc[key]['Numero'] == expected_results[key], "Check key value {k}".format(k=key))
        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_passengers_outcome_nation_city(self):
        try:
            expected_results = {
                "Ferito": 2,
            }
            result_df = self.aggregator.get_passengers_outcome("city", passenger_column="COL02", name="Como")
            self.assertTrue(result_df is not None, "None return not allowed.")
            self.assertTrue(result_df.shape[0] == len(expected_results), "Check the number of found results.")

            result_df = result_df[["descrizione","Numero"]]
            result_df.set_index("descrizione", inplace=True)
            for key in expected_results.keys():
                self.assertTrue(result_df.loc[key]['Numero'] == expected_results[key], "Check key value {k}".format(k=key))

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
