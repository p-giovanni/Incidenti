# 
# File: RunIncidenti.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
# Project: DataScience
# 

import os
import sys
import json
import logging
import argparse

from src.logger_common import init_logger
from src.ClassIncidenti import Incidenti
from src.ClassAnagraficaComuni import AnagraficaComuni
from src.ClassSupportDecodeDataframes import SupportDecodeDataframes
from src.ClassPrintIncidentsCharts import PrintIncidentsCharts
from src.ClassIncidentsDataframeAggregator import IncidentsDataframeAggregator

# ----------------------------------------
# chart_male_female
# ----------------------------------------
def chart_male_female(aggregator, config):
    """
 
    :param aggregator:
    :param config:
    :return:
    """
    log = logging.getLogger('RunIncidenti')
    log.info("chart_male_female >>")

    base_path  = config["grafici"]["base_path"]
    chart_name = config["grafici"]["male_female_pie_chart"]
    path = os.path.join(base_path, chart_name)

    df_all_a = aggregator.get_male_female_by_area("nation_wide")
    df_prov_milano_a = aggregator.get_male_female_by_area("province", name="Milano")
    df_segrate_a = aggregator.get_male_female_by_area("city", name="Segrate")

    charts = PrintIncidentsCharts()
    charts.print_male_felmale_pie_chart(df_prov_milano_a, df_segrate_a, file_path=path)

    log.info("chart_male_female <<")

# ----------------------------------------
# chart_incident_typology
# ----------------------------------------
def chart_incident_typology(aggregator, config):
    log = logging.getLogger('RunIncidenti')
    log.info("chart_incident_typology >>")

    charts = PrintIncidentsCharts()

    base_dir = config["grafici"]["base_path"]
    base_name = config["grafici"]["incident_tipology_chart"]

    incident_type = aggregator.get_count_of_incidents_by_typology("nation_wide")
    path = os.path.join(base_dir, base_name+"nation_wide")
    charts.print_incidents_typology_chart(incident_type, path)

    province = "Milano"
    incident_type = aggregator.get_count_of_incidents_by_typology("province", province)
    path = os.path.join(base_dir, base_name+"province_"+province)
    charts.print_incidents_typology_chart(incident_type, path)

    city = "Segrate"
    incident_type = aggregator.get_count_of_incidents_by_typology("city", city)
    path = os.path.join(base_dir, base_name+"city_"+city)
    charts.print_incidents_typology_chart(incident_type, path)

    log.info("chart_incident_typology <<")

# ----------------------------------------
# passengers_outcome
# ----------------------------------------
def passengers_outcome(area, city, aggregator, incidenti, config):
    log = logging.getLogger('RunIncidenti')
    log.info("passengers_outcome >>")
    rv = False

    try:

        df_milano = aggregator.get_veicle_passengers_outcome("province", "Milano")
        df_segrate = aggregator.get_veicle_passengers_outcome("city", "Segrate")

    except Exception as ex:
        log.error("Exception - {ex}".format(ex=str(ex)))

    log.info("passengers_outcome <<")
    return rv

# ----------------------------------------
# chart_hourly_incidents
# ----------------------------------------
def chart_hourly_incidents(aggregator, config):
    log = logging.getLogger('RunIncidenti')
    log.info("chart_hourly_incidents >>")

    base_dir = config["grafici"]["base_path"]
    picture_name = config["grafici"]["hourly_distribution_bar_chart"]

    charts = PrintIncidentsCharts()

    titles = ["Incidenti provincia di Milano", "Incidenti citta' di Segrate"]
    df_list = [None] * 2
    df_list[0] = aggregator.get_number_of_incidents_by_hour("province", "Milano")
    df_list[1] = aggregator.get_number_of_incidents_by_hour("city", "Segrate")
    rv_chart = charts.print_incidents_hourly_chart(df_list, os.path.join(base_dir, picture_name), titles=titles)

    rv = df_list[0] is not None and df_list[1] is not None and rv_chart
    if rv == False:
        log.error("An error occurred in calculating the hourly incidents distribution.")
    log.info("chart_hourly_incidents ({ret_val}) <<".format(ret_val=rv))
    return rv

# ----------------------------------------
# article_artifacts_maker
# ----------------------------------------
def article_artifacts_maker(aggregator, incidenti, config):
    log = logging.getLogger('RunIncidenti')
    log.info("article_artifacts_maker >>")
    rv_01 = chart_hourly_incidents(aggregator, config)
    rv_02 = chart_male_female(aggregator, config)

    rv = rv_01 and rv_02
    log.info("article_artifacts_maker ({ret_val}) <<".format(ret_val=rv))
    return rv

# ----------------------------------------
# main
# ----------------------------------------
def main(args):
    log = logging.getLogger('RunIncidenti')
    log.info("************> INIZIO <************")
    rv = False
    try:
        if os.path.isfile(args.config_file) == False:
            log.error("Invalid config file parameter: {fi}".format(fi=args.config_file))
            return rv
        with open(args.config_file) as fconfig:
            config = json.load(fconfig)
            base_dir     = config['data_files']['data_path']
            incidenti_fn = config['data_files']['file_incidenti']
            com_anagr_fn = config['data_files']['anagrafica_comuni']

            anagrafica_comuni = AnagraficaComuni(file_comuni_anagrafica = os.path.join(base_dir, com_anagr_fn))
            support_dataframes = SupportDecodeDataframes()

            incidenti = Incidenti(file_incidenti = os.path.join(base_dir, incidenti_fn),
                                  anagrafica_comuni = anagrafica_comuni,
                                  support_dataframes = support_dataframes,
                                  data_file_configurations=config["data_structure"])
            rv = incidenti.load_data_files()

            aggregator = IncidentsDataframeAggregator(incidenti)

            if rv == False:
                log.error("Fallita lettura dei datafile.")
            else:
                if args.operation == "article":
                    rv = article_artifacts_maker(aggregator, incidenti, config)
                elif args.operation == "debug":
                    aggregator.get_incidents_outcome_by_typology('nation_wide')
                    aggregator.calculate_total_incident_outcome()
                    chart_male_female(aggregator, config)
                    chart_incident_typology(aggregator, config)
                    chart_hourly_incidents(aggregator, config)
                    passengers_outcome("province", "Milano", aggregator, incidenti, config)

    except Exception as ex:
        log.error("The job returned an error - {ex}".format(ex=str(ex)))

    if rv == False:
        log.error("Operation failed - calculated data is not valid.")

    log.info("************> FINE <************")
    return rv

# ----------------------------------------
#
# ----------------------------------------
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Statistiche sugli incidenti stradali in Italia.")
    parser.add_argument("config_file", help="Config file.")
    log_level_choices = ['debug','info','warn',"error","fatal","critical"]
    parser.add_argument("--operation", "-op", choices=["debug", "article"], type=str.lower, help="Operation to be performed: article, debug.")
    parser.add_argument("--log_level_file", "-llf"
                        ,default="debug"
                        ,choices=log_level_choices
                        ,type=str.lower
                        ,help="Set the logging level for the logging file.")
    parser.add_argument("--log_level_stdout", "-lls"
                        ,default="debug"
                        ,choices=log_level_choices
                        ,type=str.lower
                        ,help="Set the logging level for the stdout.")

    args = parser.parse_args()

    log_level = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warn': logging.WARN,
        'error': logging.ERROR,
        'fatal': logging.FATAL,
        'critical': logging.CRITICAL,
    }
    init_logger('/tmp', log_level=log_level[args.log_level_file], std_out_log_level=log_level[args.log_level_stdout])
    rv = main(args=args)
    sys.exit(0 if rv == True else 1)
