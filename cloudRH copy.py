import os
from re import VERBOSE
from time import perf_counter

# from datetime import date
from datetime import datetime
import colorama
import pandas
from colorama import Fore, Back, Style
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm


class Open_File:
    def __init__(self, filename, mode):
        self.filename = filename
        self.mode = mode

    def __enter__(self):
        self.file = open(self.filename, self.mode)
        return self.file

    def __exit__(self, exc_type, exc_val, traceback):
        self.file.close()


def wlog(string, verbose=False):
    """ """
    with Open_File(f"OUTPUT\log.txt", "a") as fichierlog:
        fichierlog.write(f"{string}\n")

    if verbose == True:
        print(f"writing {Fore.GREEN}{string}{Fore.RESET} to log.txt")


def new_start(verbose=False):
    """
    Création d'une entête pour le log
    """
    now = datetime.now()
    now_string = now.strftime("%d/%m/%Y %H:%M:%S")

    if verbose == True:
        print("_" * 50 + "\n")
        print(f"Lancement du script: {Fore.CYAN}{now_string}{Fore.RESET}")
        print("_" * 50 + "\n")

    with Open_File(f"OUTPUT\log.txt", "a") as log:
        log.write("_" * 50 + "\n")
        log.write(f"Lancement du script: {now_string}\n")
        log.write("_" * 50 + "\n")


def moon_walk(verbose=False):
    """
    Scan complet d'un répertoire
    retourne une liste des fichiers *.csv
    les fichiers sont formatter avec le chemin donné en entrée
    """

    path_to_dir = os.getcwd().replace("\\", "/") + "/CSV/"

    if verbose == True:
        print(f"Start Scan in '{path_to_dir}':")

    tps_start = perf_counter()
    liste_fichiers_csv = []
    for root, dirs, files in os.walk(path_to_dir, topdown=False):
        for name in files:
            if ".csv" in name:
                temporary_string = os.path.join(root, name)
                temporary_string = temporary_string.replace("\\", "/")
                liste_fichiers_csv.append(temporary_string)
    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)
    if verbose == True:
        print(
            f"Le scan MJ a trouvé {len(liste_fichiers_csv)} fichier(s) en {Fore.MAGENTA}{tps}{Fore.RESET} secondes\n\n"
        )

    wlog(f"Fuction moon_walk found: {len(liste_fichiers_csv)} files in {tps} Sec")
    return liste_fichiers_csv


def loading_a_dataframe(string_to_the_csv, verbose=False):
    """
    import d'un csv
    traitement date, nom de colonne
    rtourne le dataframe formater
    """

    tps_start = perf_counter()
    wlog(f"Loading: {string_to_the_csv}")

    loaded_dataframe = pandas.read_csv(string_to_the_csv, sep=";")
    loaded_dataframe = loaded_dataframe.drop("RawData", axis=1)
    loaded_dataframe.Date = pandas.to_datetime(loaded_dataframe.Date)
    loaded_dataframe.rename(columns={"Date": "DATE"}, inplace=True)
    loaded_dataframe = loaded_dataframe.sort_values(by=["DATE"])
    loaded_dataframe.set_index("DATE", inplace=True)
    loaded_dataframe = loaded_dataframe[~loaded_dataframe.index.duplicated()]
    loaded_dataframe.rename(columns={"TEMPERATURE": "Temperatuur", "HUMIDITY": "Relatieve_vochtigheid"}, inplace=True)

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)

    if verbose == True:
        print(loaded_dataframe.head())
        print(
            f"\nLe dataframe a été chargé en mémoire en {Fore.MAGENTA}{tps}{Fore.RESET} secondes\n\n"
        )

    wlog(f"Function loading_a_dataframe tooks {tps} Sec")

    return loaded_dataframe


def get_basics_info(df_file, verbose=False):
    """ """

    tps_start = perf_counter()

    if verbose == True:
        print(f"Fonction {Fore.MAGENTA}get_basics_info{Fore.RESET} called")

    cover_range_df_file = df_file.index.max() - df_file.index.min()
    name_of_df_file = f"{df_file.SITE.unique()[0]}-{df_file.ROOM.unique()[0]}"

    if verbose == True:
        print(
            f"The data cover an range of {Fore.CYAN}{cover_range_df_file}{Fore.RESET}\nFrom {Fore.CYAN}{df_file.index.min()}{Fore.RESET} to {Fore.CYAN}{df_file.index.max()}{Fore.RESET}\n"
        )
        print(name_of_df_file)

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)
    if verbose == True:
        print(f"The function run in {Fore.MAGENTA}{tps}{Fore.RESET} secondes\n")

    wlog(
        f"get_basics_info found: {name_of_df_file} | {cover_range_df_file} in {tps} Sec"
    )

    return cover_range_df_file, name_of_df_file


def get_seasons(verbose=False):
    """ """

    tps_start = perf_counter()

    liste_seasons = [
        ("Winter", "2017-12-21 00:00:00", "2018-03-19 23:59:59"),
        ("Lente", "2018-03-20 00:00:00", "2018-06-20 23:59:59"),
        ("Zomer", "2018-06-21 00:00:00", "2018-09-20 23:59:59"),
        ("Herfst", "2018-09-21 00:00:00", "2018-12-20 23:59:59"),
        ("Winter", "2018-12-21 00:00:00", "2019-03-19 23:59:59"),
        ("Lente", "2019-03-20 00:00:00", "2019-06-20 23:59:59"),
        ("Zomer", "2019-06-21 00:00:00", "2019-09-20 23:59:59"),
        ("Herfst", "2019-09-21 00:00:00", "2019-12-20 23:59:59"),
        ("Winter", "2019-12-21 00:00:00", "2020-03-19 23:59:59"),
        ("Lente", "2020-03-20 00:00:00", "2020-06-19 23:59:59"),
        ("Zomer", "2020-06-20 00:00:00", "2020-09-21 23:59:59"),
        ("Herfst", "2020-09-22 00:00:00", "2020-12-20 23:59:59"),
        ("Winter", "2020-12-21 00:00:00", "2021-03-19 23:59:59"),
        ("Lente", "2021-03-20 00:00:00", "2021-06-20 23:59:59"),
        ("Zomer", "2021-06-21 00:00:00", "2021-09-20 23:59:59"),
        ("Herfst", "2021-09-21 00:00:00", "2021-12-20 23:59:59"),
        ("Winter", "2021-12-21 00:00:00", "2022-03-19 23:59:59"),
    ]

    if verbose == True:
        for _ in range(len(liste_seasons)):
            season = liste_seasons[_][0]
            start = liste_seasons[_][1]
            end = liste_seasons[_][2]

            print(
                f"la saison {Fore.LIGHTGREEN_EX}{season}{Fore.RESET} commence le {Fore.CYAN}{start}{Fore.RESET} et fini le {Fore.CYAN}{end}{Fore.RESET}."
            )

    print("\n")

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)

    wlog(f"Function loading_a_dataframe tooks {tps} Sec")

    return liste_seasons


def create_column_seasons(df_file, liste_seasons, verbose=False):
    """
    Génère une colonne avec le nom de la saison
    en fonction de l'index (Timeseries) du dataframe en entrée.
    renvois le df avec la colonne
    """

    tps_start = perf_counter()

    liste_datum = df_file.index.to_list()
    liste_column_season = []

    if verbose == True:
        print(f"Création de la colonne {Fore.YELLOW}SEASONS{Fore.RESET}")

        for cpt_date in tqdm(range(len(liste_datum))):
            # parcours les dates reprise dans le dataframe
            datum = liste_datum[cpt_date]

            for _ in range(len(liste_seasons)):
                # parcours les saison pour trouver la bonne
                season = liste_seasons[_][0]
                start = liste_seasons[_][1]
                end = liste_seasons[_][2]

                start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
                end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

                if (datum >= start) & (datum <= end):
                    liste_column_season.append(season)

    else:
        for datum in liste_datum:
            # parcours les dates reprise dans le dataframe
            for _ in range(len(liste_seasons)):
                # parcours les saison pour trouver la bonne
                season = liste_seasons[_][0]
                start = liste_seasons[_][1]
                end = liste_seasons[_][2]

                start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
                end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

                if (datum >= start) & (datum <= end):
                    liste_column_season.append(season)

    df_file["Seizoen"] = liste_column_season

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)

    if verbose == True:
        print(df_file.head())
        print(
            f"\nla colonne a été générée en {Fore.MAGENTA}{tps}{Fore.RESET} secondes\n\n"
        )

    wlog(f"Function create_column_seasons tooks {tps} Sec")

    return df_file


def draw_bivariate(name_of_df_file, df_file, custom_palette, switch_sub=False, verbose=False):
    """
    Génère le nuage de point à la façon Roel
    """
    # creation d'un chaine vide pour récupérer les stats incrémentativement

    custom_palette = custom_palette

    tps_start = perf_counter()
    statistique = ""
    if verbose == True:
        print(f"Fonction {Fore.MAGENTA}bivariate{Fore.RESET} started...")

    # cover range du dataset
    if verbose == True:
        print("Gettings cover Range")
    x_start = df_file.index.min()
    x_end = df_file.index.max()
    statistique += (
        f"\n[Data cover Range]\n{x_end-x_start}\nStart: {x_start}\nEnd: {x_end}\n"
    )

    if verbose == True:
        print("calcul stats")
    # Calcul des statistique descriptive du jeux de données 'Humidité'
    describe_HUM = df_file.Relatieve_vochtigheid.describe()
    liste_index = describe_HUM.index.tolist()
    liste_values = describe_HUM.values.tolist()
    statistique += "\n[Relatieve vochtigheid]\n"
    for cpt_describe in range(len(liste_index)):
        if cpt_describe == 0:
            statistique += f"{str(liste_index[cpt_describe])}: {str(int(liste_values[cpt_describe]))} Values\n"
        else:
            statistique += f"{str(liste_index[cpt_describe])}: {str(round(liste_values[cpt_describe], 0))} %\n"

    # Calcul des statistique descriptive du jeux de données 'Termperature'
    describe_TMP = df_file.Temperatuur.describe()
    liste_index = describe_TMP.index.tolist()
    liste_values = describe_TMP.values.tolist()
    statistique += "\n[Temperatuur]\n"
    for cpt_describe in range(len(liste_index)):
        if cpt_describe == 0:
            statistique += f"{str(liste_index[cpt_describe])}: {str(int(liste_values[cpt_describe]))} Values\n"
        else:
            statistique += f"{str(liste_index[cpt_describe])}: {str(round(liste_values[cpt_describe], 2))} degC\n"

    # Mise en place du graphique
    if verbose == True:
        print("Tracer du graphique... ", end="")

    sns.set(style="darkgrid")
    
 
    # sns.set_palette(custom_palette)
    sns.set()
    figure, graph = plt.subplots(figsize=(7, 8))
    graph = sns.relplot(
        data=df_file,
        x="Temperatuur",
        y="Relatieve_vochtigheid",
        hue="Seizoen",
        marker=",",
        s=2,
        legend="auto",
        palette=custom_palette
    )

    plt.xticks([5, 10, 15, 20, 25, 30, 35])
    plt.yticks([20, 30, 40, 50, 60, 70, 80])
    plt.xlim(5, 35)
    plt.ylim(20, 80)

    plt.ylabel("Relatieve vochtigheid [%]")
    plt.xlabel("Temperatuur [°C]")
    plt.title(f"{name_of_df_file}")

    if switch_sub == True:
        plt.savefig(
            f"OUTPUT\{df_file.SITE.unique()[0]}-{df_file.ROOM.unique()[0]}\{name_of_df_file}_{x_start}_bivariate.png",
            bbox_inches="tight",
        )
        plt.close("all")
        with Open_File(
            f"OUTPUT\{df_file.SITE.unique()[0]}-{df_file.ROOM.unique()[0]}\{name_of_df_file}_{x_start}sub_stats.txt",
            "a",
        ) as f:
            f.write(f"[NAME]\n{name_of_df_file}\n")
            f.write(statistique)
    else:
        plt.savefig(
            f"OUTPUT\{name_of_df_file}_bivariate_stats.png", bbox_inches="tight"
        )
        plt.close("all")
        with Open_File(f"OUTPUT\{name_of_df_file}_overall_stats.txt", "a") as f:
            f.write(f"[NAME]\n{name_of_df_file}\n")
            f.write(statistique)

    if verbose == True:
        print("Done")

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)

    if verbose == True:
        print(statistique)
        print(f"\nLe nuage a été généré en {Fore.MAGENTA}{tps}{Fore.RESET} secondes")

    wlog(f"Function draw_bivariate tooks {tps} Sec")


def draw_bivariate_per_seasons(name_of_df_file, df_file, custom_palette, verbose=False):
    """ """

    if verbose == True:
        print(
            f"Fonction {Fore.MAGENTA}bivariate_per_seasons{Fore.RESET}_split called",
            end="",
        )
    tps_start = perf_counter()

    sns.set(style="darkgrid")
    figure, ax = plt.subplots(figsize=(7, 8))

    custom_palette = custom_palette

    ax = sns.relplot(
        data=df_file,
        x="Temperatuur",
        y="Relatieve_vochtigheid",
        hue="Seizoen",
        col="Seizoen",
        marker=",",
        s=2,
        palette=custom_palette
    )

    plt.xticks([5, 10, 15, 20, 25, 30, 35])
    plt.yticks([20, 30, 40, 50, 60, 70, 80])
    plt.xlim(5, 35)
    plt.ylim(20, 80)

    ax.set_axis_labels(x_var="Temperatuur [°C]", y_var="Relatieve vochtigheid [%]")

    plt.savefig(f"OUTPUT\{name_of_df_file}_bivariate_per_seasons.png")
    plt.close("all")

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)
    if verbose == True:
        print(f"it tooks {Fore.MAGENTA}{tps}{Fore.RESET} secondes")

    wlog(f"Function draw_bivariate_per_seasons tooks {tps} Sec")


def draw_dispersion(df_file, name_of_df_file, custom_palette, verbose=False):
    """ """
    tps_start = perf_counter()

    # print(df_file.columns)
    # df_file.rename(columns={"TEMPERATURE": "Temperatuur", "HUMIDITY": "Relatieve vochtigheid","SEASONS": "Seizoen"}, inplace=True)
    # df.rename(columns={'oldName1': 'newName1', 'oldName2': 'newName2'}, inplace=True)

    if verbose == True:
        print(f"Fonction {Fore.MAGENTA}draw_dispersion{Fore.RESET}", end="")
    tps_start = perf_counter()

    sns_plot = sns.jointplot(data=df_file, x="Temperatuur", y="Relatieve_vochtigheid", marker=".",hue="Seizoen", palette=custom_palette)
    # plt.xticks([0, 5, 10, 15, 20, 25, 30, 35])
    # plt.yticks([0, 20, 30, 40, 50, 60, 70, 80])
    # plt.xlim(5, 35)
    # plt.ylim(20, 80)

    sns_plot.savefig(f"OUTPUT\{name_of_df_file}_dispersion.png")

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)
    if verbose == True:
        print(f"tooks {Fore.MAGENTA}{tps}{Fore.RESET} secondes\n\n")

    wlog(f"draw_dispersion tooks {tps} Sec")


def draw_season_layout(liste_seasons, name_of_df_file, df_file, verbose=False):
    if verbose == True:
        print(f"Fonction {Fore.MAGENTA}season_layout{Fore.RESET} called")

    tps_start = perf_counter()

    # Check whether the specified path exists or not
    isExist = os.path.exists(f"OUTPUT\{name_of_df_file}")
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(f"OUTPUT\{name_of_df_file}")
        wlog(f"The directory {name_of_df_file} is created!")

    switch_sub = True

    if verbose == True:
        for _ in tqdm(range(len(liste_seasons))):
            # parcours les saison pour trouver la bonne
            season = liste_seasons[_][0]
            start = liste_seasons[_][1]
            end = liste_seasons[_][2]

            start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

            filtre_seasons = (df_file.index >= start) & (df_file.index <= end)
            df_seasons = df_file[filtre_seasons].copy()
            season_name = f"{name_of_df_file}_{season}"
            try:
                draw_bivariate(season_name, df_seasons, switch_sub, False)
            except:
                wlog(
                    f"\ndraw_season_layout failed for \n{liste_seasons[_]}\n{season_name}\ntaille du sous df: {df_seasons.shape}"
                )

    else:
        for _ in range(len(liste_seasons)):
            # parcours les saison pour trouver la bonne
            season = liste_seasons[_][0]
            start = liste_seasons[_][1]
            end = liste_seasons[_][2]

            start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

            filtre_seasons = (df_file.index >= start) & (df_file.index <= end)
            df_seasons = df_file[filtre_seasons].copy()
            season_name = f"{name_of_df_file}_{season}_{start}"
            try:
                draw_bivariate(season_name, df_seasons, switch_sub, False)
            except:
                wlog(
                    f"\ndraw_season_layout failed for \n{liste_seasons[_]}\n{season_name}\ntaille du sous df: {df_seasons.shape}"
                )

    tps_stop = perf_counter()
    tps = round(tps_stop - tps_start, 3)
    if verbose == True:
        print(f"Functions tooks {Fore.MAGENTA}{tps}{Fore.RESET} secondes\n\n")

    wlog(f"season_layout tooks {tps} Sec")


def main(verbose=False):

    tps_tot_start = perf_counter()
    # génération de l'entête du log
    new_start(verbose)

    # scan du dossier csv
    liste_fichiers_csv = moon_walk(verbose)
    # liste_fichiers_csv = liste_fichiers_csv[1]

    # ------> Boucle for sur liste fichier
    for cpt_file in range(len(liste_fichiers_csv)):
        print(
            f"{Back.WHITE}{Fore.BLACK}File: {cpt_file+1}/{len(liste_fichiers_csv)}{Style.RESET_ALL}"
        )

        # Sélection d'un fichier
        file = liste_fichiers_csv[cpt_file]
        df_file = loading_a_dataframe(file, verbose)

        # liste_subsampling = ["1H", "2H", "3H", "4H"]
        subsampling = "4H"

        # info de couverture du dataframe
        cover_range_df_file, name_of_df_file = get_basics_info(df_file, verbose)


        # for sub_sampling_interval in liste_subsampling:

        df_mean = df_file.resample("4H").mean()
        # Génération d'une liste d'éphémérides
        liste_seasons = get_seasons(verbose)

        # création de la colonnes SEASONS
        df_mean = create_column_seasons(df_mean, liste_seasons, verbose)

        # Génération des graphiques
        switch_sub = False
        custom_palette = ["orange", "blue", "green", "red"] 

        draw_bivariate(name_of_df_file+f"-{subsampling}", df_mean, custom_palette, switch_sub, verbose)
        draw_bivariate_per_seasons(name_of_df_file+f"-{subsampling}", df_mean, custom_palette, verbose)
        draw_dispersion(df_mean, name_of_df_file+f"-{subsampling}", custom_palette, verbose)
        # draw_season_layout(liste_seasons, name_of_df_file, df_file, verbose)
        wlog("_" * 50 + "\n")
        print(
            f"{Back.WHITE}{Fore.BLACK}End of script for this iteration{Style.RESET_ALL}"
        )

    tps_tot_stop = perf_counter()
    tps = round(tps_tot_stop - tps_tot_start, 1)
    print(f"The scripts tooks {Fore.MAGENTA}{tps}{Fore.RESET} secondes\n\n")


if __name__ == "__main__":
    main(True)
