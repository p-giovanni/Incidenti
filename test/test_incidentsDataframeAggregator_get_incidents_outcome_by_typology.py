# 
# File: test_incidentsDataframeAggregator_get_incidents_outcome_by_typology.py
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

class TestIncidentsDataframeAggregator(TestCase):
    """
    Unit tests for the method:
            - IncidentsDataframeAggregator->get_incidents_outcome_by_typology

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
            "natura_incidente":[11,        1,      1,      2,                       2,          1]
        }
        self.incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(self.incidenti)

    def test_get_incidents_outcome_by_typology_unknown_area(self):
        try:
            result = self.aggregator.get_incidents_outcome_by_typology("UNKNOWN")
            self.assertTrue(result is None, "No result must be returned.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_incidents_outcome_by_typology_nation_wide(self):
        try:
            fi_expected_feriti = 9
            fi_expected_morti = 1
            fi_expected_incolumi = 1

            result_df = self.aggregator.get_incidents_outcome_by_typology("nation_wide")
            self.assertTrue(result_df is not None, "Must be a valid dataframe.")

            result_df.set_index('natura_incidente', inplace=True)
            frenata_improvvisa = result_df.loc[11]
            self.assertTrue(frenata_improvvisa['Morti'] == fi_expected_morti, "Check value for Morti.")
            self.assertTrue(frenata_improvvisa['Feriti'] == fi_expected_feriti, "Check value for Feriti.")
            self.assertTrue(frenata_improvvisa['Incolumi'] == fi_expected_incolumi, "Check value for Incolumi.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_incidents_outcome_by_typology_provincia(self):
        try:
            scf_expected_feriti = 3
            scf_expected_morti = 1
            scf_expected_incolumi = 1

            result_df = self.aggregator.get_incidents_outcome_by_typology("province", "MI")
            self.assertTrue(result_df is not None, "Must be a valid dataframe.")

            result_df.set_index('natura_incidente', inplace=True)
            scontro_front_lat = result_df.loc[2]
            self.assertTrue(scontro_front_lat['Morti']    == scf_expected_morti, "Check value for Morti.")
            self.assertTrue(scontro_front_lat['Feriti']   == scf_expected_feriti, "Check value for Feriti.")
            self.assertTrue(scontro_front_lat['Incolumi'] == scf_expected_incolumi, "Check value for Incolumi.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_incidents_outcome_by_typology_city(self):
        try:
            scf_expected_feriti = 3
            scf_expected_morti = 1
            scf_expected_incolumi = 1

            result_df = self.aggregator.get_incidents_outcome_by_typology("city", "Cernusco Sul Naviglio")
            self.assertTrue(result_df is not None, "Must be a valid dataframe.")

            result_df.set_index('natura_incidente', inplace=True)
            scontro_front_lat = result_df.loc[2]
            self.assertTrue(scontro_front_lat['Morti']    == scf_expected_morti, "Check value for Morti.")
            self.assertTrue(scontro_front_lat['Feriti']   == scf_expected_feriti, "Check value for Feriti.")
            self.assertTrue(scontro_front_lat['Incolumi'] == scf_expected_incolumi, "Check value for Incolumi.")

            scf_expected_feriti = 2
            scf_expected_morti = 3
            scf_expected_incolumi = 4

            result_df = self.aggregator.get_incidents_outcome_by_typology("city", "Como")
            self.assertTrue(result_df is not None, "Must be a valid dataframe.")

            result_df.set_index('natura_incidente', inplace=True)
            scontro_front_lat = result_df.loc[1]
            self.assertTrue(scontro_front_lat['Morti']    == scf_expected_morti, "Check value for Morti.")
            self.assertTrue(scontro_front_lat['Feriti']   == scf_expected_feriti, "Check value for Feriti.")
            self.assertTrue(scontro_front_lat['Incolumi'] == scf_expected_incolumi, "Check value for Incolumi.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
