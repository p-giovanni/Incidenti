# 
# File: IncidentsDataframeAggregator.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
# Project: DataScience
# 

import time
import logging

import numpy as np
import pandas as pd

class IncidentsDataframeAggregator(object):

    area_type = ["nation_wide", "province", "city"]

    veicle_col_decode = {
        "a": {
            "tipo_veicolo": "tipo_veicolo_a",
            "prefix": "veicolo__a___",
            "sex": "sesso_conducente"
        },
        "b": {
            "tipo_veicolo": "tipo_veicoli__b_",
            "prefix": "veicolo__b___",
            "sex": "sesso_conducente"
        },
        "c": {
            "tipo_veicolo": "tipo_veicoli__c_",
            "prefix": "veicolo__c___",
            "sex": "sesso_conducente"
        }
    }

    def __init__(self, incidents):
        self.__log = logging.getLogger('IncidentsDataframeAggregator')
        self.__incidents = incidents
        
    # ----------------------------------------
    # get_version
    # ----------------------------------------
    @staticmethod
    def get_version():
        return "00.00.05 alfa"

    # ----------------------------------------
    # get_data_configurations
    # ----------------------------------------
    def get_data_configurations(self):
        return self.__incidents.get_data_configurations()

    # ----------------------------------------
    # __df
    # ----------------------------------------
    def __df(self):
        return self.__incidents.df_incidenti

    # ----------------------------------------
    # __support_df
    # ----------------------------------------
    def __support_df(self):
        return self.__incidents.get_support_dataframes()

    # ----------------------------------------
    # __check_area_param
    # ----------------------------------------
    def __check_area_param(self, area):
        """
        Vefifica che il valore di area faccia parte
        dei valori ammessi.

        :param area:
        :return:
        """
        self.__log.info("__check_area_param ({area}) >>".format(area=area))
        rv = False
        if area not in IncidentsDataframeAggregator.area_type:
            msg = "Area {area} parameter not known.".format(area=area)
            self.__log.error(msg)
        else:
            rv = True
        self.__log.info("__check_area_param ({rv}) <<".format(rv=rv))
        return rv

    # ----------------------------------------
    # calculate_total_incident_outcome
    # ----------------------------------------
    def calculate_total_incident_outcome(self):
        """
        Calculate the total incident outcome for the three categores:
            - uninjured;
            - injured;
            - dead
        This is done summing the result of __calculate_incident_outcome and
        the result of the values contained in the columns in the lists;
            - other_injured_columns
            - other_deadh_columns

        :return:
        """
        self.__log.info("calculate_total_incident_outcome >>")
        rv = False
        data_configurations = self.get_data_configurations()
        try:
            cols_feriti = data_configurations["other_injured_columns"]
            cols_morti = data_configurations["other_deadh_columns"]

            self.__df()["TotAltriFeriti"] = self.__df()[cols_feriti].sum(axis=1)
            self.__df()["TotAltriMorti"] = self.__df()[cols_morti].sum(axis=1)
            self.__log.debug("TotFeriti: {feriti} - TotMorti: {morti}.".format(feriti=self.__df()["TotAltriFeriti"].sum(),
                                                                               morti=self.__df()["TotAltriMorti"].sum()))

            rv = self.__calculate_incident_outcome()
            if rv == True:
                # Test tracing - so I can verify the final count.
                self.__log.debug("BEFORE - Feriti: {feriti} - Morti: {morti}".format(feriti=self.__df()["Feriti"].sum(),
                                                                                     morti=self.__df()["Morti"].sum()))

                self.__df()["Feriti"] = self.__df()[["TotAltriFeriti", "Feriti"]].sum(axis=1)
                self.__df()["Morti"] = self.__df()[["TotAltriMorti", "Morti"]].sum(axis=1)

                self.__log.debug("AFTER - Feriti: {feriti} - Morti: {morti}".format(feriti=self.__df()["Feriti"].sum(),
                                                                                    morti=self.__df()["Morti"].sum()))

        except Exception as ex:
            self.__log.error("Exit in error - {ex}".format(ex=str(ex)))

        self.__log.info("calculate_total_incident_outcome ({rv}) <<".format(rv=rv))
        return rv

    # ----------------------------------------
    # calculate_incident_outcome
    # ----------------------------------------
    def __calculate_incident_outcome(self):
        """
        This method calculates the total figures of the incident outcome.
        The calculus is made using the list of column given in the configuration
        section: data_structure.incident_outcome_columns.

        The dataset is browsed sequentially and the values decoded using
        the support table SupportDecodeDataframes.esito_incidente.

        To test that the values are correct you have to do this query:
        num1 = incidenti.df_incidenti["veicolo__a___esito_conducente"].value_counts()
            num2 = incidenti.df_incidenti["veicolo__a___passeggeri_an35"].value_counts()
            num3 = incidenti.df_incidenti["veicolo__a___esito_passegg38"].value_counts()
            num4 = incidenti.df_incidenti["veicolo__a___esito_passegg41"].value_counts()
            num5 = incidenti.df_incidenti["veicolo__a___esito_passegg44"].value_counts()
            (num1,num2,num3,num4,num5)

        The result - summed for the same code - must be the same returned by this one:
                    df_incidenti['Incolumi'].sum()
                    df_incidenti['Feriti'].sum()
                    df_incidenti['Morti'].sum()

        :return: 
        """
        self.__log.info("__calculate_incident_outcome >>")
        start = time.time()
        result = False
        try:
            def calculate(row):
                if self.row_counter >= self.row_counter_limit:
                    self.__log.debug("{wi} -> {cnt}".format(wi=self.wich_result, cnt=self.row_counter))
                    self.row_counter_limit += 60000
                self.row_counter += 1

                counter_death_num = 0
                counter_ingured_num = 0
                counter_uninjured_num = 0

                for current_coll in self.outcome_columns:
                    outcome_code = row[current_coll]
                    code = outcome['codice'].get(outcome_code)
                    if code is not None:
                        description = outcome['descrizione'][code]
                        if description == "Incolume":
                            counter_uninjured_num += 1
                        elif description == "Ferito":
                            counter_ingured_num += 1
                        elif description.startswith("Morto"):
                            counter_death_num += 1
                    else:
                        msg = "Cannot decode {o_code}".format(o_code=code)
                        self.__log.error(msg)
                        raise Exception(msg)

                if self.wich_result.startswith("u"):
                    agg_result = counter_uninjured_num
                elif self.wich_result.startswith("i"):
                    agg_result = counter_ingured_num
                else:
                    agg_result = counter_death_num
                return agg_result

            self.outcome_columns = self.get_data_configurations()["incident_outcome_columns"]
            self.__df()['Incolumi'] = np.nan
            self.__df()['Feriti'] = np.nan
            self.__df()['Morti'] = np.nan

            outcome = self.__incidents.get_support_dataframes().esito_incidente.to_dict()

            self.row_counter = 0
            self.row_counter_limit = 60000
            self.wich_result = "uninjured_count"
            self.__df()['Incolumi'] = self.__df()[self.outcome_columns].apply(calculate, axis=1)

            self.row_counter = 0
            self.row_counter_limit = 60000
            self.wich_result = "injured_count"
            self.__df()['Feriti'] = self.__df()[self.outcome_columns].apply(calculate, axis=1)

            self.row_counter = 0
            self.row_counter_limit = 60000
            self.wich_result = "deaths_count"
            self.__df()['Morti'] = self.__df()[self.outcome_columns].apply(calculate, axis=1)

            del self.outcome_columns
            del self.row_counter
            del self.row_counter_limit
            del self.wich_result

            result = True
            end = time.time()
            self.__log.info("Execution time: {diff} sec.".format(diff=end-start))

        except Exception as ex:
            self.__log.error("Received an error -> {ex}".format(ex=str(ex)))
        self.__log.info("__calculate_incident_outcome ({rv}) <<".format(rv=result))
        return result

    # ----------------------------------------
    # get_number_of_incidents_by_provincia
    # ----------------------------------------
    def get_number_of_incidents_by_provincia(self, nome_provincia):
        """
        Counts the number of incidents for the given provincia.

        :param nome_provincia:
        :return: an integer or None in case of error;
        """
        self.__log.info("get_number_of_incidents_by_provincia ({name}) >>".format(name=nome_provincia))
        result = None
        try:
            result = self.__df().set_index(['Provincia']).sort_index(level=0).loc[[(nome_provincia)]].count()
            result = result['Comune']
        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))
            if type(ex) is KeyError:
                result = 0
        self.__log.info("get_number_of_incidents_by_provincia ({res}) <<".format(res=result))
        return result

    # ----------------------------------------
    # get_count_of_incidents_by_typology
    # ----------------------------------------
    def get_incidents_outcome_by_typology(self, area, name=None):
        """
        This method aggregates the incidents by typology (column "natura_incidente") and
        and sums the value of:
            - Incolumi, Feriti, Morti
        for each typology value.

        :param area:
        :param name:
        :return: the aggregated dataframe or None in error;
        """
        self.__log.info("get_incidents_outcome_by_typology ({area}-{name}) >>".format(area=area, name=name))
        result = None
        try:
            if self.__check_area_param(area) is False:
                self.__log.info("get_count_of_incidents_by_typology (None) <<")
                return None

            actual_df = None
            self.calculate_total_incident_outcome()
            if area == "nation_wide":
                actual_df = self.__df()
            elif area == "province":
                mask = self.__df()['Provincia'] == name
                actual_df = self.__df()[mask]
                pass
            elif area == "city":
                mask = self.__df()['Comune'] == name
                actual_df = self.__df()[mask]

            aggregated_df = actual_df[["natura_incidente", "Incolumi", "Feriti", "Morti"]] \
                                         .groupby(["natura_incidente"]) \
                                         .agg({'Incolumi': 'sum', 'Feriti': 'sum', 'Morti': 'sum'})
            aggregated_df.reset_index(inplace=True)

            natura_incidente_decode = self.__support_df().natura_incidente
            natura_incidente_decode['codice'] = natura_incidente_decode['codice'].astype(np.int64)
            aggregated_df['natura_incidente'] = aggregated_df['natura_incidente'].astype(np.int64)

            result = pd.merge(aggregated_df, natura_incidente_decode,
                              left_on='natura_incidente',
                              right_on='codice')

        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))

        self.__log.info("get_incidents_outcome_by_typology  ({res}) <<".format(res=result is not None))
        return result

    # ----------------------------------------
    # get_count_of_incidents_by_typology
    # ----------------------------------------
    def get_count_of_incidents_by_typology(self, area, name=None):
        """
        Torna il numero di incidenti per tipologia (frontale, tamponamento,
        ecc.).
        E' possibile filtrare per una delle tre aree possibili (parametro
        area)

        :param area: valore tra: "nation_wide", "province", "city";
        :param name: nome di una provincia o di una citta'/comune;
        :return: a dataframe or None in case of error;
        """
        self.__log.info("get_count_of_incidents_by_typology ({area}-{name}) >>".format(area=area, name=name))
        result = None
        try:
            if self.__check_area_param(area) is False:
                self.__log.info("get_count_of_incidents_by_typology (None) <<")
                return None

            natura_incidente = None
            if area == "nation_wide":
                natura_incidente = self.__df()["natura_incidente"].value_counts()

            elif area == "province":
                if name is None:
                    msg = "The provincia parameter is None."
                    self.__log.error(msg)
                    self.__log.info("get_count_of_incidents_by_typology (None) <<")
                    return None
                mask = self.__df()['Provincia'] == name
                natura_incidente = self.__df()[mask]["natura_incidente"].value_counts()

            elif area == "city":
                if name is None:
                    msg = "The city parameter is None."
                    self.__log.error(msg)
                    self.__log.info("get_count_of_incidents_by_typology (None) <<")
                    return None
                mask = self.__df()['Comune'] == name
                natura_incidente = self.__df()[mask]["natura_incidente"].value_counts()

            natura_incidente = pd.Series.to_frame(natura_incidente)
            natura_incidente['codice'] = natura_incidente.index
            natura_incidente.rename(index=str,
                                    columns={"natura_incidente": "Numero"},
                                    inplace=True)

            natura_incidente_decode = self.__support_df().natura_incidente
            natura_incidente_decode['codice'] = natura_incidente_decode['codice'].astype(np.int64)

            result = pd.merge(natura_incidente, natura_incidente_decode,
                              left_on='codice',
                              right_on='codice')

        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))

        self.__log.info("get_count_of_incidents_by_typology  ({res}) <<".format(res=result is not None))
        return result

    # ----------------------------------------
    # get_male_female_by_area
    # ----------------------------------------
    def get_male_female_by_area(self, area, name=None, driver_sex_column_name="a"):
        """
        Torna il numero di incidenti provocati (veicolo A) da maschi/femmine
        nell'area definita dal parametro.

        :param area: valore tra: "nation_wide", "province", "city";
        :param name: nome di una provincia o di una citta'/comune;
        :param driver_sex_column_name: ;
        :return:
        """
        self.__log.info("get_male_female_by_area ({area} - {name}) >>".format(area=area, name=name))
        result = None
        try:
            if self.__check_area_param(area) is False:
                self.__log.info("get_male_female_by_area (None) <<")
                return None

            if IncidentsDataframeAggregator.veicle_col_decode.get(driver_sex_column_name.lower()) is None:
                msg = "Veicle value {col} parameter not known.".format(col=driver_sex_column_name)
                self.__log.error(msg)
                self.__log.info("get_male_female_by_area (None) <<")
                return None

            prefix = IncidentsDataframeAggregator.veicle_col_decode.get(driver_sex_column_name.lower())['prefix']
            sex = IncidentsDataframeAggregator.veicle_col_decode.get(driver_sex_column_name.lower())['sex']
            driver_sex_column_name = "{p}{s}".format(p=prefix, s=sex)
            self.__log.debug("Col name: {col}".format(col=driver_sex_column_name))

            if area == "nation_wide":
                result = self.__filter_sesso_conducente_nation_wide(driver_sex_column_name=driver_sex_column_name)

            elif area == "province":
                if name is None:
                    msg = "The provincia parameter is None."
                    self.__log.error(msg)
                    self.__log.info("get_male_female_by_area (None) <<")
                    return None
                result = self.__filter_sesso_conducente_province(province_name=name,
                                                                 driver_sex_column_name=driver_sex_column_name)

            else:
                if name is None:
                    msg = "The city name parameter is None."
                    self.__log.error(msg)
                    self.__log.info("get_male_female_by_area (None) <<")
                    return None

                result = self.__filter_sesso_conducente_city(city_name=name,
                                                             driver_sex_column_name=driver_sex_column_name)

        except Exception as ex:
            self.__log.error("Exit in error - {ex}".format(ex=str(ex)))
        self.__log.info("get_male_female_by_area ({res}) <<".format(res=result is not None))
        return result

    # ----------------------------------------
    # __filter_sesso_conducente_nation_wide
    # ----------------------------------------
    def __filter_sesso_conducente_nation_wide(self, driver_sex_column_name="veicolo__a___sesso_conducente"):
        """
        Esegue il conteggio dei valori della colonna veicolo__a___sesso_conducente
        (che indica il sesso del conducente) e ne torna il valore.

        :param driver_sex_column_name:
        :return: DataFrame
        """
        self.__log.info("__filter_sesso_conducente_nation_wide >>")
        genres_count = self.__df()[driver_sex_column_name].value_counts()
        genres = genres_count.index
        genres_count = pd.Series.to_frame(genres_count)
        genres_count['codice'] = list(genres)

        # Elimino i valori non numerici ed imposto il tipo corretto.
        genres_count['codice'].replace(' ', 0, inplace=True)
        genres_count['codice'] = genres_count['codice'].astype(np.int64)
        genres_count.set_index('codice', inplace=True)

        self.__log.debug(genres_count.to_string())
        self.__log.debug(self.__support_df().genere_persona.to_string())

        df_merged = pd.merge(self.__support_df().genere_persona,
                             genres_count,
                             left_index=True,
                             right_index=True)
        result = df_merged[['descrizione', driver_sex_column_name]]
        result.rename(index=str,
                      columns={driver_sex_column_name: "Numero", "descrizione": "Sesso conducente"},
                      inplace=True)

        self.__log.info("__filter_sesso_conducente_nation_wide  ({res}) <<".format(res=result is not None))
        return result

    # ----------------------------------------
    # __filter_sesso_conducente_province
    # ----------------------------------------
    def __filter_sesso_conducente_province(self, province_name, driver_sex_column_name="veicolo__a___sesso_conducente"):
        """
        Seleziona tutti gli incidenti in una data provincia (parametro)
        e torna il conteggio dei valori nella colonna veicolo__a___sesso_conducente
        che indica il sesso del conducente.

        :param province_name:
        :param driver_sex_column_name:
        :return:
        """
        self.__log.info("__filter_sesso_conducente_province  ({name}) <<".format(name=province_name))

        # Seleziono l'area richiesta.
        result = self.__df().set_index(['Provincia']).sort_index(level=0).loc[(province_name, driver_sex_column_name)]

        # Dalla serie prodotta eseguo il conteggio sul codice sesso.
        result = result.value_counts()
        result = pd.Series.to_frame(result)
        result.reset_index(inplace=True)

        result.rename(index=str,
                      columns={driver_sex_column_name: "Numero", "index": "codice", "descrizione": "Sesso conducente"},
                      inplace=True)
        result.replace(' ', 0, inplace=True)
        result['codice'] = result['codice'].astype(np.int64)
        result.set_index('codice', inplace=True)

        result = pd.merge(result,
                          self.__support_df().genere_persona,
                          left_index=True,
                          right_index=True)
        result.rename(index=str,
                      columns={"descrizione": "Sesso conducente"},
                      inplace=True)
        self.__log.info("__filter_sesso_conducente_province  ({res}) <<".format(res=result is not None))
        return result

    # ----------------------------------------
    # __filter_sesso_conducente_city
    # ----------------------------------------
    def __filter_sesso_conducente_city(self, city_name, driver_sex_column_name="veicolo__a___sesso_conducente"):
        self.__log.info("__filter_sesso_conducente_city  ({name}) <<".format(name=city_name))
        result = self.__df().set_index(['Comune']).sort_index(level=0).loc[(city_name, driver_sex_column_name)]

        result = result.value_counts()
        result = pd.Series.to_frame(result)
        result.reset_index(inplace=True)

        result.replace(' ', 0, inplace=True)
        self.__log.debug("Columns: {col}".format(col=str(result.columns.values)))
        result.rename(index=str,
                      columns={driver_sex_column_name: "Numero", "index": "codice", "descrizione": "Sesso conducente"},
                      inplace=True)
        result['codice'] = result['codice'].astype(np.int64)

        self.__log.debug("Columns: {col}".format(col=str(result.columns.values)))
        result.set_index('codice', inplace=True)

        result = pd.merge(result,
                          self.__support_df().genere_persona,
                          left_index=True,
                          right_index=True)

        result.rename(index=str,
                      columns={"descrizione": "Sesso conducente"},
                      inplace=True)
        self.__log.debug("Columns: {col}".format(col=str(result.columns.values)))
        self.__log.info("__filter_sesso_conducente_city  ({res}) <<".format(res=result is not None))
        return result

    # ----------------------------------------
    # get_veicle_passengers_outcome
    # ----------------------------------------
    def get_veicle_passengers_outcome(self, area, city):
        self.__log.info("get_veicle_passengers_outcome >>")
        rv = False
        bar_df = None

        if self.__check_area_param(area) is False:
            self.__log.info("get_number_of_incidents_by_hour (None) <<")
            return None
        try:
            df_list = [None] * 5

            esito = self.__support_df().esito_incidente
            code_uninjred = esito[(esito['descrizione'] == "Incolume")]['codice'].values[0]
            code_unknown = esito[(esito['descrizione'] == "Non dato")]['codice'].values[0]

            bar_index = [
                "Conducente",
                "Passeggero anteriore",
                "Passeggero posteriore 1",
                "Passeggero posteriore 2",
                "Passeggero posteriore 3"
            ]
            bar_records = {
                "Incolume": [],
                "Ferito": [],
                "Morto nelle 24 ore": [],
                "Morto entro il trentesimo giorno": [],
                "Non dato": []
            }
            df_list[0] = self.get_passengers_outcome(area=area, name=city,
                                                           passenger_column="veicolo__a___esito_conducente")
            df_list[1] = self.get_passengers_outcome(area=area, name=city,
                                                           passenger_column="veicolo__a___passeggeri_an35")
            df_list[2] = self.get_passengers_outcome(area=area, name=city,
                                                           passenger_column="veicolo__a___esito_passegg38")
            df_list[3] = self.get_passengers_outcome(area=area, name=city,
                                                           passenger_column="veicolo__a___esito_passegg41")
            df_list[4] = self.get_passengers_outcome(area=area, name=city,
                                                           passenger_column="veicolo__a___esito_passegg44")
            for df in df_list:
                if df is None:
                    self.__log.error("One or more get_passengers_outcome failed - exit in error.")
                    return None

            for df, idx in zip(df_list, range(0, len(df_list))):
                #try:
                #    df.drop(code_unknown, inplace=True)
                #except KeyError as ke:
                #    pass
                df.set_index("descrizione", inplace=True)
                df_t = df.transpose()

                for key in bar_records.keys():
                    if key in df_t.columns:
                        value = df_t[key].values[0]
                    else:
                        value = 0
                    bar_records[key].append(value)
                pass

            bar_df = pd.DataFrame.from_records(bar_records, index=bar_index)

        except Exception as ex:
            self.__log.error("Exception - {ex}".format(ex=str(ex)))

        self.__log.info("get_veicle_passengers_outcome <<")
        return bar_df

    # ----------------------------------------
    # get_passengers_outcome
    # ----------------------------------------
    def get_passengers_outcome(self, area, passenger_column, name=None):
        self.__log.info("get_passengers_outcome ({area} - {name}) >>".format(name=name,area=area))
        if self.__check_area_param(area) is False:
            self.__log.info("get_number_of_incidents_by_hour (None) <<")
            return None
        result = None
        actual_df = None
        try:
            if area == "nation_wide":
                actual_df = self.__df()
            elif area == "province":
                if name is None:
                    self.__log.error("Province parameter is empty - exit in error <<")
                    return None

                mask = self.__df()['Provincia'] == name
                actual_df = self.__df()[mask]
                pass
            elif area == "city":
                if name is None:
                    self.__log.error("City parameter is empty - exit in error <<")
                    return None
                mask = self.__df()['Comune'] == name
                actual_df = self.__df()[mask]

            counted = actual_df[passenger_column].value_counts().to_frame("Numero")
            counted.reset_index(inplace=True)
            counted.rename(columns={"index": 'codice'}, inplace=True)
            esito_incidente_decode = self.__support_df().esito_incidente

            result = pd.merge(counted, esito_incidente_decode,
                          left_on='codice',
                          right_on='codice')
            result.set_index("codice", inplace=True)

        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))

        self.__log.info("get_passengers_outcome ({res}) <<".format(res=result is not None))
        return result


    # ----------------------------------------
    # get_number_of_incidents_by_hour
    # ----------------------------------------
    def get_number_of_incidents_by_hour(self, area, name=None,):
        """
        Returns a dataframe containing the hourly distribution of
        the incidents.

        :param area: valore tra: "nation_wide", "province", "city";
        :param name: nome di una provincia o di una citta'/comune;
        :return: the aggregated dataframe or None in error;
        """
        self.__log.info("get_number_of_incidents_by_hour ({area} - {name}) >>".format(name=name,area=area))
        if self.__check_area_param(area) is False:
            self.__log.info("get_number_of_incidents_by_hour (None) <<")
            return None
        result = None
        actual_df = None
        try:
            if area == "nation_wide":
                actual_df = self.__df()
            elif area == "province":
                mask = self.__df()['Provincia'] == name
                actual_df = self.__df()[mask]
                pass
            elif area == "city":
                mask = self.__df()['Comune'] == name
                actual_df = self.__df()[mask]
            
            result = actual_df["Ora"].value_counts().to_frame("Numero")

            result.sort_index(inplace=True)

        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))

        self.__log.info("get_number_of_incidents_by_hour ({res}) <<".format(res=result is not None))
        return result

    # ----------------------------------------
    # get_number_of_incidents_by_comune
    # ----------------------------------------
    def get_number_of_incidents_by_comune(self, nome_comune):
        """
        Calculates the number of incidents in a given city (comune).
        :param nome_comune:
        :return: the number of incidents for the given city name;
        """
        self.__log.info("get_number_of_incidents_by_comune ({name}) >>".format(name=nome_comune))
        result = None
        try:
            result = self.__df().set_index(['Comune']).sort_index(level=0).loc[[(nome_comune)]].count()
            result = result['comune']
        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))
            if type(ex) is KeyError:
                result = 0
        self.__log.info("get_number_of_incidents_by_comune ({res}) <<".format(res=result))
        return result

    # ----------------------------------------
    # get_cities_without_incidents
    # ----------------------------------------
    def get_cities_without_incidents(self):
        """
        Estraggo i comuni che non hanno avuto incidenti
        Il criterio e':
           (tutti i comuni della provincia MI) SOTTRAGGO (l'elenco dei comuni di provincia MI del dataframe incidenti)

        :return:
        """
        self.__log.info("get_cities_without_incidents >>")
        #mask_provincia_milano = None
        #df_provincia_milano = self.df_incidenti[mask_provincia_milano]
        #
        #incidenti_all_comuni_pr_milano = df_provincia_milano[['Codice Comune', 'Comune']].drop_duplicates()
        #comuni_pr_milano = self.df_comuni[(self.df_comuni['Codice Provincia'] == 15)][['Codice Comune', 'Comune']]
        #
        #incidenti_all_comuni_pr_milano.set_index('Codice Comune', inplace=True)
        #comuni_pr_milano.set_index('Codice Comune', inplace=True)
        #
        ## Comuni nella provincia di Milano senza incidenti.
        #no_incidenti = comuni_pr_milano[~ comuni_pr_milano.isin(incidenti_all_comuni_pr_milano)]
        #no_incidenti.dropna(subset=['Comune'])
        self.__log.info("get_cities_without_incidents <<")
        return None

