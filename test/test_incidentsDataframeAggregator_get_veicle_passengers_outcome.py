# 
# File: test_incidentsDataframeAggregator_get_veicle_passengers_outcome.py
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

class TestIncidentsDataframeAggregator_get_veicle_passengers_outcome(TestCase):
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

            "veicolo__a___esito_conducente": [1, 1, 3, 0, 1, 0, 0],
            "veicolo__a___passeggeri_an35":  [1, 2, 1, 0, 1, 0, 0],
            "veicolo__a___esito_passegg38":  [2, 1, 0, 0, 3, 0, 0],
            "veicolo__a___esito_passegg41":  [2, 1, 2, 0, 4, 0, 0],
            "veicolo__a___esito_passegg44":  [3, 3, 4, 0, 0, 0, 0]
        }
        self.incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(self.incidenti)
        self.outcome_labels = ["Incolume", "Ferito", "Morto nelle 24 ore", "Morto entro il trentesimo giorno", "Non dato"]

    def test_get_passengers_outcome_unknown_area(self):
        try:
            result = self.aggregator.get_veicle_passengers_outcome(area="UNKNOWN", city=None)
            self.assertTrue(result is None, "No result must be returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_passengers_outcome_city_none(self):
        try:
            result = self.aggregator.get_veicle_passengers_outcome(area="city", city=None)
            self.assertTrue(result is None, "No result must be returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_veicle_passengers_outcome_city(self):
        try:
            conducente_expected = pd.Series([1, 0, 1, 0, 0], index=self.outcome_labels)
            conducente_expected.sort_index(inplace=True)

            pass_ant_expected = pd.Series([1, 1, 0, 0, 0], index=self.outcome_labels)
            pass_ant_expected.sort_index(inplace=True)

            pass_pos_1_expected = pd.Series([1, 0, 0, 0, 1], index=self.outcome_labels)
            pass_pos_1_expected.sort_index(inplace=True)

            pass_pos_2_expected = pd.Series([1, 1, 0, 0, 0], index=self.outcome_labels)
            pass_pos_2_expected.sort_index(inplace=True)

            pass_pos_3_expected = pd.Series([0, 0, 1, 1, 0], index=self.outcome_labels)
            pass_pos_3_expected.sort_index(inplace=True)

            df_result = self.aggregator.get_veicle_passengers_outcome("city", "Como")
            self.assertTrue(df_result is not None, "A valid result must be returned.")

            conducente_df = df_result.loc["Conducente"]
            conducente_df.sort_index(inplace=True)
            self.assertTrue(conducente_df.equals(conducente_expected), "Check conducente.")

            pass_ant_df = df_result.loc["Passeggero anteriore"]
            pass_ant_df.sort_index(inplace=True)
            self.assertTrue(pass_ant_df.equals(pass_ant_expected), "Check passeggero anteriore.")

            pass_pos_1_df = df_result.loc["Passeggero posteriore 1"]
            pass_pos_1_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_1_df.equals(pass_pos_1_expected), "Check passeggero posteriore 1.")

            pass_pos_2_df = df_result.loc["Passeggero posteriore 2"]
            pass_pos_2_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_2_df.equals(pass_pos_2_expected), "Check passeggero posteriore 2.")

            pass_pos_3_df = df_result.loc["Passeggero posteriore 3"]
            pass_pos_3_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_3_df.equals(pass_pos_3_expected), "Check passeggero posteriore 3.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_veicle_passengers_outcome_province(self):
        try:
            conducente_expected = pd.Series([1, 0, 0, 0, 0], index=self.outcome_labels)
            conducente_expected.sort_index(inplace=True)

            pass_ant_expected = pd.Series([1, 0, 0, 0, 0], index=self.outcome_labels)
            pass_ant_expected.sort_index(inplace=True)

            pass_pos_1_expected = pd.Series([0, 0, 1, 0, 0], index=self.outcome_labels)
            pass_pos_1_expected.sort_index(inplace=True)

            pass_pos_2_expected = pd.Series([0, 0, 0, 1, 0], index=self.outcome_labels)
            pass_pos_2_expected.sort_index(inplace=True)

            pass_pos_3_expected = pd.Series([0, 0, 0, 0, 1], index=self.outcome_labels)
            pass_pos_3_expected.sort_index(inplace=True)

            df_result = self.aggregator.get_veicle_passengers_outcome("province", "BG")
            self.assertTrue(df_result is not None, "A valid result must be returned.")

            conducente_df = df_result.loc["Conducente"]
            conducente_df.sort_index(inplace=True)
            self.assertTrue(conducente_df.equals(conducente_expected), "Check conducente.")

            pass_ant_df = df_result.loc["Passeggero anteriore"]
            pass_ant_df.sort_index(inplace=True)
            self.assertTrue(pass_ant_df.equals(pass_ant_expected), "Check passeggero anteriore.")

            pass_pos_1_df = df_result.loc["Passeggero posteriore 1"]
            pass_pos_1_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_1_df.equals(pass_pos_1_expected), "Check passeggero posteriore 1.")

            pass_pos_2_df = df_result.loc["Passeggero posteriore 2"]
            pass_pos_2_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_2_df.equals(pass_pos_2_expected), "Check passeggero posteriore 2.")

            pass_pos_3_df = df_result.loc["Passeggero posteriore 3"]
            pass_pos_3_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_3_df.equals(pass_pos_3_expected), "Check passeggero posteriore 3.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_veicle_passengers_outcome_nation_wide(self):
        try:
            conducente_expected = pd.Series([3, 0, 1, 0, 3], index=self.outcome_labels)
            conducente_expected.sort_index(inplace=True)

            pass_ant_expected = pd.Series([3, 1, 0, 0, 3], index=self.outcome_labels)
            pass_ant_expected.sort_index(inplace=True)

            pass_pos_1_expected = pd.Series([1, 1, 1, 0, 4], index=self.outcome_labels)
            pass_pos_1_expected.sort_index(inplace=True)

            pass_pos_2_expected = pd.Series([1, 2, 0, 1, 3], index=self.outcome_labels)
            pass_pos_2_expected.sort_index(inplace=True)

            pass_pos_3_expected = pd.Series([0, 0, 2, 1, 4], index=self.outcome_labels)
            pass_pos_3_expected.sort_index(inplace=True)

            df_result = self.aggregator.get_veicle_passengers_outcome("nation_wide", city=None)
            self.assertTrue(df_result is not None, "A valid result must be returned.")

            conducente_df = df_result.loc["Conducente"]
            conducente_df.sort_index(inplace=True)
            self.assertTrue(conducente_df.equals(conducente_expected), "Check conducente.")

            pass_ant_df = df_result.loc["Passeggero anteriore"]
            pass_ant_df.sort_index(inplace=True)
            self.assertTrue(pass_ant_df.equals(pass_ant_expected), "Check passeggero anteriore.")

            pass_pos_1_df = df_result.loc["Passeggero posteriore 1"]
            pass_pos_1_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_1_df.equals(pass_pos_1_expected), "Check passeggero posteriore 1.")

            pass_pos_2_df = df_result.loc["Passeggero posteriore 2"]
            pass_pos_2_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_2_df.equals(pass_pos_2_expected), "Check passeggero posteriore 2.")

            pass_pos_3_df = df_result.loc["Passeggero posteriore 3"]
            pass_pos_3_df.sort_index(inplace=True)
            self.assertTrue(pass_pos_3_df.equals(pass_pos_3_expected), "Check passeggero posteriore 3.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
