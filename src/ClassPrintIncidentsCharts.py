# 
# File: ClassPrintIncidentsCharts.py
# Author(s): Ing. Giovanni Rizzardi - Summer 2019
# Project: DataScience
# 

import logging

import pandas.plotting._converter as pandacnv
import matplotlib.pyplot as plt
import numpy as np
import matplotlib as mpl


class PrintIncidentsCharts(object):

    def __init__(self):
        self.__log = logging.getLogger('Incidenti')

    # ----------------------------------------
    # print_incidents_outcome_by_typology 
    # ----------------------------------------
    def print_incidents_outcome_by_typology(self, df):
        fig, ax = plt.subplots(figsize=(10, 8))

        ind = np.arange(df.shape[0])
        width = 0.55

        color_incolumi = "#86e775"
        color_feriti = "#d3e144"
        color_morti = "#f54c08"

        incolumi_df = np.array(df['Incolumi'])
        feriti_df = np.array(df['Feriti'])
        morti_df = np.array(df['Morti'])

        p2 = ax.bar(ind, morti_df, width=width, color=color_morti, label='Morti')
        p1 = ax.bar(ind, incolumi_df, bottom=morti_df, width=width, color=color_incolumi, label='Incolumi')
        p3 = ax.bar(ind, feriti_df, bottom=incolumi_df, width=width, color=color_feriti, label='Feriti')

        ax.legend()
        ax.set_xticks(ind)
        ax.set_xticklabels(df['descrizione'].values, rotation=80)
        ax.spines['top'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.grid(False, color='#d4dadc', linestyle='-.', linewidth=0.2)

        plt.show()

    # ----------------------------------------
    # generic_bar_chart
    # ----------------------------------------
    def generic_bar_chart(self, ax, x, y, no_upper_numbers=True, title=None):
        """
        Creates a vertical bar chart usint the given axe.

        :param ax: axe to be used;
        :param x:
        :param y:
        :param no_upper_numbers:
        :return:
        """
        self.__log.info("generic_bar_chart >>")
        def autolabel(rects):
            """
            Attach a text label above each bar displaying its height
            """
            for rect in rects:
                height = rect.get_height()
                ax.text(rect.get_x() + rect.get_width() / 2., height + (height * 0.01),
                        '%d' % int(height),
                        ha='center', va='bottom')

        rv = False
        try:
            max_num = y.max() + (y.max() * 0.02)
            width = 0.6
            descriptions = x
            incidents_num = y.values
            idx = np.asarray([i for i in range(len(y.values))])

            rects = ax.bar(idx, incidents_num, width=width, color='#BEE2F0')

            if title is not None:
                ax.set_title(title, fontsize=18)
            ax.set_xticks(idx)
            ax.set_xticklabels(descriptions, rotation=65)
            ax.set_ylabel('# incidenti')
            ax.grid(False, color='#d4dadc', linestyle='-.', linewidth=0.2)
            ax.set_ylim([0, max_num])
            ax.spines['top'].set_visible(False)
            ax.spines['left'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_visible(False)

            if no_upper_numbers == False:
                autolabel(rects)
            rv = True
        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))
        self.__log.info("generic_bar_chart <<")
        return rv

    # ----------------------------------------
    # print_incidents_hourly_chart
    # ----------------------------------------
    def print_incidents_hourly_chart(self, df_list, file_path, titles):
        """
        This method calculate the chart of the hourly distribution
        of the incidents.

        :param df: dataframe (index: hour; Numero);
        :param file_path:
        :return: True for ok, False in case of error;
        """
        self.__log.info("print_incidents_hourly_chart >>")
        rv = False
        try:
            fig, ax_lst = plt.subplots(nrows=1, ncols=len(df_list), figsize=(15, 8),constrained_layout=True)
            for df, ax, title in zip(df_list, ax_lst, titles):
                rv = self.generic_bar_chart(y=df['Numero'], x=df.index.to_list(), ax=ax, title=title)
                if rv == False:
                    self.__log.error("The chart creation has returned an error.")
                    return False

            #fig.tight_layout()
            plt.savefig(file_path)
            rv = True

        except Exception as ex:
            self.__log.error("ERROR - {ex}".format(ex=str(ex)))

        self.__log.info("print_incidents_hourly_chart <<")
        return rv

    # ----------------------------------------
    # print_incidents_typology_chart
    # ----------------------------------------
    def print_incidents_typology_chart(self, df, file_path, orientation="vertical"):
        """
        Scrive su file png il grafico (bar chart) relativo alla tipologia di
        incidente (frontale, tamponamento, ecc.) con le numerosita'.

        L'orientamento puo' essere sia orizzontale che verticale.

        :param df: dataframe (colonne Numero e descrizine);
        :param file_path: percorso ove salvare il file;
        :param orientation: orizzontale o verticale (default);
        :return: True per OK, False in caso d'errore;
        """
        self.__log.info("print_incidents_typology_chart ({fi}) >>".format(fi=file_path))
        rv = False
        try:
            if orientation == "vertical":
                def autolabel(rects):
                    """
                    Attach a text label above each bar displaying its height
                    """
                    for rect in rects:
                        height = rect.get_height()
                        ax.text(rect.get_x() + rect.get_width() / 2., height + (height * 0.01),
                                '%d' % int(height),
                                ha='center', va='bottom')

                fig, ax = plt.subplots(figsize=(10, 8))

                max_num = df['Numero'].max() + (df['Numero'].max() * 0.02)
                width = 0.8

                descriptions = df['descrizione'].values
                incidents_num = df['Numero'].values
                idx = np.asarray([i for i in range(len(df['Numero'].values))])

                rects = ax.bar(idx, incidents_num, width=width, color='#BEE2F0')

                ax.set_xticks(idx)
                ax.set_xticklabels(descriptions, rotation=65)
                ax.set_ylabel('# incidenti')
                ax.grid(False, color='#d4dadc', linestyle='-.', linewidth=0.2)
                #ax.grid(True)
                ax.set_ylim([0, max_num])

                ax.spines['top'].set_visible(False)
                ax.spines['left'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['bottom'].set_visible(False)

                autolabel(rects)
                fig.tight_layout()
                plt.savefig(file_path)

                rv = True

            elif orientation == "horizontal":
                fig, ax = plt.subplots()

                descriptions = df['descrizione']
                y_pos = np.arange(len(descriptions))
                incidents_num = df['Numero']

                width = 0.3
                ax.barh(y_pos, incidents_num, align='center', color='#BEE2F0')
                ax.set_yticks(y_pos)
                ax.set_yticklabels(descriptions)

                ax.grid(color='#d4dadc', linestyle='-.', linewidth=0.2)
                ax.grid(True)

                ax.spines['top'].set_visible(False)
                ax.spines['left'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['bottom'].set_visible(False)

                ax.invert_yaxis()  # labels read top-to-bottom
                ax.set_xlabel('# incidenti')
                plt.savefig(file_path)
                rv = True

            else:
                self.__log.error("Invalid parameter {pa}.".format(pa=orientation))

        except Exception as ex:
            self.__log.error("{ex}".format(ex=str(ex)))

        self.__log.info("print_incidents_typology_chart ({rv}) <<".format(rv=rv))
        return rv

    def print_male_felmale_pie_chart(self, df_mv_province, df_mf_city, file_path):
        """
        Crea il file contenente i diagrammi a torta con le percentuai di incindenti
        in cui sono coinvolti maschi / femmine (al momento solo veicolo A).
        :param df_mv_province:
        :param df_mf_city:
        :param file_path:
        :return:
        """
        self.__log.info("print_male_felmale_pie_chart ({fi}) >>".format(fi=file_path))
        rv = False
        try:
            #pandacnv.register()

            # Create a list of colors (from iWantHue)
            colors = ["#BEE2F0", "#F4CCCC", "#81898c"]

            fig, ax = plt.subplots(nrows=1, ncols=2, constrained_layout=True)

            # Grandezza dell'immagine.
            value_height = 15
            value_width = 24
            fig.set_figheight(value_height)
            fig.set_figwidth(value_width)

            # Create a pie chart
            ax[0].set_titlmere("Incidenti uomini/donne - provincia Milano", fontsize=24)
            ax[0].pie(
                df_mv_province['Numero'],
                labels=df_mv_province['Sesso conducente'],
                shadow=False,
                colors=colors,
                startangle=90,
                autopct='%1.1f%%',
                textprops={'fontsize': 24}
            )
            ax[1].set_title("Incidenti uomini/donne - Segrate", fontsize=24)
            ax[1].pie(
                df_mf_city['Numero'],
                labels=df_mf_city['Sesso conducente'],
                shadow=False,
                colors=colors,
                startangle=90,
                autopct='%1.1f%%',
                textprops={'fontsize': 24}
            )
            plt.savefig(file_path)
            rv = True

        except Exception as ex:
            self.__log.error("{ex}".format(ex=str(ex)))

        self.__log.info("print_male_felmale_pie_chart ({rv}) <<".format(rv=rv))
        return rv
