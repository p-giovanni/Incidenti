# 
# File: ClassSupportDecodeDataframes.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
# Project: DataScience
# 

import logging

import numpy as np
import pandas as pd

class SupportDecodeDataframes(object):

    def __init__(self):
        self.__log = logging.getLogger('SupportDecodeDataframes')
        self.__loaded = False
        self.organo_di_rilevazione = None
        self.localizzazione_incidente = None
        self.intersezione_o_non_intersezione = None
        self.fondo_stradale = None

    def is_loaded(self):
        return self.__loaded

    def load_dataframes(self):
        self.__log.info("load_dataframes >>")
        if self.__loaded == False:
            # Creo dei dataframe di decodifica per avere le descrizioni dei campi presenti come codice nel file dati.
            labels = ['codice', 'descrizione']

            organo_di_rilevazione = [
                    (1,'Agente di Polizia stradale'),
                    (2,'Carabiniere'),
                    (3,'Agente di Pubblica sicurezza'),
                    (4,'Agente di Polizia municipale'),
                    (5,'Altri'),
                    (6,'Agente di Polizia provinciale')
            ]
            self.organo_di_rilevazione = pd.DataFrame.from_records(organo_di_rilevazione, columns=labels)

            localizzazione_incidente = [
                (1, 'Strada urbana'),
                (2, 'Strada provinciale nell\'abitato'),
                (3, 'Strada statale nell\'abitato'),
                (0, 'Strada regionale entro l\'abitato'),
                (4, 'Strada comunale extraurbana'),
                (5, 'Strada provinciale fuori dell\'abitato'),
                (6, 'Strada statale fuori dell\'abitato'),
                (7, 'Autostrada'),
                (8, 'Altra strada'),
                (9, 'Strada regionale fuori l\'abitato')
            ]
            self.localizzazione_incidente = pd.DataFrame.from_records(localizzazione_incidente, columns=labels)

            intersezione_o_non_intersezione = [
                ('01' ,'Incrocio'),
                ('02' ,'Rotatoria'),
                ('03' ,'Intersezione segnalata'),
                ('04' ,'Intersezione con semaforo o vigile'),
                ('05' ,'Intersezione non segnalata'),
                ('06' ,'Passaggio a livello'),
                ('07' ,'Rettilineo'),
                ('08' ,'Curva '),
                ('09' ,'Dosso, strettoia'),
                ('10' ,'Pendenza'),
                ('11' ,'Galleria illuminata'),
                ('12' ,'Galleria  non illuminata'),
            ]
            self.intersezione_o_non_intersezione = pd.DataFrame.from_records(intersezione_o_non_intersezione, columns=labels)

            fondo_stradale = [
                (1 ,'Asciutto'),
                (2 ,'Bagnato'),
                (3 ,'Sdrucciolevole'),
                (4 ,'Ghiacciato'),
                (5 ,'Innevato')
            ]
            self.fondo_stradale = pd.DataFrame.from_records(fondo_stradale, columns=labels)

            segnaletica = [
                (1, "Assente"),
                (2, "Verticale"),
                (3, "Orizzontale"),
                (4, "Verticale e orizzontale"),
                (5, "Temporanea di cantiere")
            ]
            self.segnaletica = pd.DataFrame.from_records(segnaletica, columns=labels)

            condizioni_meteorologiche = [
                (1, "Sereno"),
                (2, "Nebbia"),
                (3, "Pioggia"),
                (4, "Grandine"),
                (5, "Neve"),
                (6, "Vento forte"),
                (7, "Altro")
            ]
            self.condizioni_meteorologiche = pd.DataFrame.from_records(condizioni_meteorologiche, columns=labels)

            natura_incidente_columns = labels + ["Commento"] 
            natura_incidente = [
                ("01", "Scontro frontale",                                 "Debbono essere coinvolti almeno due veicoli"),
                ("02", "Scontro frontale laterale",                        "Debbono essere coinvolti almeno due veicoli"),
                ("03", "Scontro laterale",                                 "Debbono essere coinvolti almeno due veicoli"),
                ("04", "Tamponamento",                                     "Debbono essere coinvolti almeno due veicoli"),
                ("05", "Investimento di pedone",                           "Deve essere coinvolto un solo veicolo"),
                ("06", "Urto con veicolo in momentanea fermata o arresto", "Debbono essere coinvolti almeno due veicoli"),
                ("07", "Urto con veicolo in sosta",                        "Deve essere coinvolto un solo veicolo"),
                ("08", "Urto con ostacolo accidentale",                    "Deve essere coinvolto un solo veicolo"),
                ("09", "Urto con treno",                                   "Deve essere coinvolto un solo veicolo"),
                ("10", "Fuoriuscita",                                      "Deve essere coinvolto un solo veicolo"),
                ("11", "Frenata improvvisa",                               "Deve essere coinvolto un solo veicolo"),
                ("12", "Caduta da veicolo",                                "Deve essere coinvolto un solo veicolo")
            ]
            self.natura_incidente = pd.DataFrame.from_records(natura_incidente, columns=natura_incidente_columns)

            tipo_veicolo = [
                ("01", "Autovettura_privata"),
                ("02", "Autovettura_con_rimorchio"),
                ("03", "Autovettura_pubblica"),
                ("04", "Autovettura_di_soccorso_o_di_polizia"),
                ("05", "Autobus_o_filobus in servizio_urbano"),
                ("06", "Autobus_di_linea_o_non_di_linea in extraurbana"),
                ("07", "Tram_"),
                ("08", "Autocarro"),
                ("09", "Autotreno_con_rimorchio"),
                ("10", "Autosnodato_o_autoarticolato"),
                ("11", "Veicolo_speciale"),
                ("12", "Trattore_stradale_o_motrice"),
                ("13", "Trattore_agricolo"),
                ("14", "Velocipede"),
                ("15", "Ciclomotore"),
                ("16", "Motociclo_a_solo"),
                ("17", "Motociclo_con_passeggero"),
                ("18", "Motocarro_o_motofurgone"),
                ("19", "Veicolo_a_trazione_animale_o_a_braccia"),
                ("20", "Veicolo_datosi_alla_fuga"),
                ("21", "Quadriciclo")
            ]
            self.tipo_veicolo = pd.DataFrame.from_records(tipo_veicolo, columns=labels)

            genere_persona = {
                "codice": [1, 2, 0],
                "descrizione": ["maschio", "femmina", "non dato"]
            }
            self.genere_persona = pd.DataFrame.from_records(genere_persona, columns=['codice','descrizione'])
            self.genere_persona['codice'].astype(np.int64, inplace=True)
            self.genere_persona.set_index('codice', inplace=True)

            self.__log.debug(self.genere_persona.head(5))
            self.__log.debug("Types of genere_persona : {ty}".format(ty=self.genere_persona.dtypes))

            esito_incidente = [
                (0, "Non dato"),
                (1, "Incolume"),
                (2, "Ferito"),
                (3, "Morto nelle 24 ore"),
                (4, "Morto entro il trentesimo giorno")
            ]
            self.esito_incidente = pd.DataFrame.from_records(esito_incidente, columns=labels)
            
            self.__loaded = True

        self.__log.info("load_dataframes <<")
        return 0

