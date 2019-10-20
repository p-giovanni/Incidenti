# 
# File: test_incidentsDataframeAggregator_count_by_incident_typology.py
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
            - IncidentsDataframeAggregator->get_count_of_incidents_by_typology

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
            "Comune":           ["Milano", "Como", "Como", "Cernusco Sul Naviglio","Roncolate", "Almenno", "Vimodrone"],
            "comune":           ["1001",   "1000", "1000", "1002",                 "1003",      "1004",    "1005"],
            "Provincia":        ["MI",     "CO",   "CO",   "MI",                   "BG",        "BG",      "MI"],
            "natura_incidente": [ 1,        1,      4,      10,                     1,           4,         4],
        }
        incidenti.df_incidenti = pd.DataFrame.from_records(test_df)

        self.aggregator = IncidentsDataframeAggregator(incidenti)

    def test_get_count_of_incidents_by_typology_first_parameter(self):
        try:
            result_df = self.aggregator.get_count_of_incidents_by_typology("WRONG_FIRST_PARAMETER")
            self.assertTrue(result_df is None,"Wrong first parameter - return None.")
        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_count_of_incidents_by_typology_nation_wide(self):
        try:
            expected_scontro_frontale = 3
            expected_tamponamento = 3
            expected_fuoriuscita = 1

            result_df = self.aggregator.get_count_of_incidents_by_typology("nation_wide")
            self.assertTrue(result_df is not None, "None return not allowed.")

            result_df.set_index('descrizione', inplace=True)
            scontro_frontale = result_df[['Numero']].loc['Scontro frontale'][0]
            scontro_tamponamento = result_df[['Numero']].loc['Tamponamento'][0]
            scontro_fuoriuscita = result_df[['Numero']].loc['Fuoriuscita'][0]

            self.assertTrue(scontro_frontale == expected_scontro_frontale, "Check count - scontro frontale.")
            self.assertTrue(scontro_tamponamento == expected_tamponamento, "Check count - tamponamento.")
            self.assertTrue(scontro_fuoriuscita == expected_fuoriuscita, "Check count - fuoriuscita.")

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_count_of_incidents_by_typology_province(self):
        try:
            expected_scontro_frontale = 1
            expected_tamponamento = 1
            expected_fuoriuscita = 1

            provincia = "MI"
            result_df = self.aggregator.get_count_of_incidents_by_typology("province", provincia)
            self.assertTrue(result_df is not None, "None return not allowed.")

            result_df.set_index('descrizione', inplace=True)
            scontro_frontale = result_df[['Numero']].loc['Scontro frontale'][0]
            scontro_tamponamento = result_df[['Numero']].loc['Tamponamento'][0]
            scontro_fuoriuscita = result_df[['Numero']].loc['Fuoriuscita'][0]

            self.assertTrue(scontro_frontale == expected_scontro_frontale, "Check count {prov} - scontro frontale.".format(prov=provincia))
            self.assertTrue(scontro_tamponamento == expected_tamponamento, "Check count {prov} - tamponamento.".format(prov=provincia))
            self.assertTrue(scontro_fuoriuscita == expected_fuoriuscita, "Check count {prov} - fuoriuscita.".format(prov=provincia))

            expected_scontro_frontale = 1
            expected_tamponamento = 1

            provincia = "BG"
            result_df = self.aggregator.get_count_of_incidents_by_typology("province", provincia)
            self.assertTrue(result_df is not None, "None return not allowed.")

            result_df.set_index('descrizione', inplace=True)
            scontro_frontale = result_df[['Numero']].loc['Scontro frontale'][0]
            scontro_tamponamento = result_df[['Numero']].loc['Tamponamento'][0]
            empty_fuoriuscita = 'Fuoriuscita' in result_df.index

            self.assertTrue(scontro_frontale == expected_scontro_frontale, "Check count {prov} - scontro frontale.".format(prov=provincia))
            self.assertTrue(scontro_tamponamento == expected_tamponamento, "Check count {prov} - tamponamento.".format(prov=provincia))
            self.assertTrue(empty_fuoriuscita == False, "Check count {prov} - fuoriuscita empty.".format(prov=provincia))

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_count_of_incidents_by_typology_not_existent_province(self):
        try:
            provincia = "DOES_NOT_EXIST"
            result_df = self.aggregator.get_count_of_incidents_by_typology("province", provincia)
            self.assertTrue(result_df.empty == True, "An empty dataframe expected.")
        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_count_of_incidents_by_typology_not_existent_comune(self):
        try:
            comune = "DOES_NOT_EXIST"
            result_df = self.aggregator.get_count_of_incidents_by_typology("city", comune)
            self.assertTrue(result_df.empty == True, "An empty dataframe expected.")
        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")

    def test_get_count_of_incidents_by_typology_city(self):
        try:
            expected_scontro_frontale = 1
            expected_tamponamento = 1

            comune = "Como"
            result_df = self.aggregator.get_count_of_incidents_by_typology("city", comune)
            self.assertTrue(result_df is not None, "A valid dataframe expected.")

            result_df.set_index('descrizione', inplace=True)
            scontro_frontale = result_df[['Numero']].loc['Scontro frontale'][0]
            scontro_tamponamento = result_df[['Numero']].loc['Tamponamento'][0]

            empty_fuoriuscita = 'Fuoriuscita' in result_df.index
            self.assertTrue(scontro_frontale == expected_scontro_frontale, "Check count {city} - scontro frontale.".format(city=comune))
            self.assertTrue(scontro_tamponamento == expected_tamponamento, "Check count {city} - tamponamento.".format(city=comune))
            self.assertTrue(empty_fuoriuscita == False, "Check count {prov} - fuoriuscita empty.".format(prov=comune))

        except Exception as ex:
            self.fail("Exception received - but not expected - test fails.")
