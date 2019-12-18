#
# File: JupyterNotebookWrapper.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
# Project: DataScience
#

import os
import sys
import logging

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from src.logger_common import init_logger
from src.ClassIncidenti import Incidenti
from src.ClassAnagraficaComuni import AnagraficaComuni
from src.ClassSupportDecodeDataframes import SupportDecodeDataframes
from src.ClassIncidentsDataframeAggregator import IncidentsDataframeAggregator

config = None 
with open(config_file) as fconfig:
    config = json.load(fconfig)

init_logger('/tmp', log_level=logging.FATAL, std_out_log_level=logging.FATAL)

base_dir = config['data_files']['data_path']
print(base_dir)
incidenti_fn = config['data_files']['file_incidenti']
incidenti_cols = config['data_structure']['columns']
cols_to_be_converted = config["data_structure"]["convert_to_number"]

anagrafica_comuni = AnagraficaComuni(file_comuni_anagrafica=os.path.join(data_dir, istat_comuni_file))
support_dataframes = SupportDecodeDataframes()

incidenti = Incidenti(file_incidenti=os.path.join(data_dir, incidenti_fn),
                      anagrafica_comuni=anagrafica_comuni,
                      support_dataframes=support_dataframes,
                      data_file_configurations=config["data_structure"])
incidenti.load_data_files()
aggregator = IncidentsDataframeAggregator(incidenti)

("Vers. Incidenti:", Incidenti.get_version(),
 "Vers. Aggregator:", IncidentsDataframeAggregator.get_version(),
 "Numerosita' df:", incidenti.df_incidenti.shape[0])