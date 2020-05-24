import json
import os
from urllib.parse import urljoin
import sqlite3
import requests
from bs4 import BeautifulSoup
import urllib3
import pandas as pd
import gc
import multiprocessing
import numpy as np
from retrying import retry
from datetime import datetime
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.expand_frame_repr = False
pd.options.mode.chained_assignment = None


def retry_if_connection_error(exception):
    print(f"Retrying after error: {exception}")
    return isinstance(exception, requests.exceptions.ConnectionError)


def create_row_for_each_dataframe_list_element(df, list_column):
    return pd.DataFrame({
        col: np.repeat(df[col].values, df[list_column].str.len())
        for col in df.columns.drop(list_column)}
    ).assign(**{list_column: np.concatenate(df[list_column].values)})[df.columns]


def create_column_for_each_dataframe_dictionary_element(df, dictionary_column):
    return pd.concat(
        [
            df.drop([dictionary_column], axis=1),
            df[dictionary_column].apply(pd.Series)
        ],
        axis=1
    )


class VikingsDB:
    def __init__(self):
        self.DB_FILE = os.path.join(os.path.dirname(__file__), "vikings.db")
        self.LANG = ["ru", "en", "de", "es", "fr", "it", "tr", "ja", "ko"]
        # self.LANG = ["ru", "en"]
        self.MASTER_URL = "https://vikings.help/"
        self.MATERIAL_URL = urljoin(self.MASTER_URL, "resources/materials/")
        self.STONE_URL = urljoin(self.MASTER_URL, "resources/gems/")
        self.RUNE_URL = urljoin(self.MASTER_URL, "resources/runes/")
        self.MONSTER_URL = urljoin(self.MASTER_URL, "resources/monsters/")
        self.EQUIPMENT_URL = urljoin(self.MASTER_URL, "resources/equipment/")
        self.CITY_SKINS = urljoin(self.MASTER_URL, "/resources/city_skins/")
        self.ACHIEVEMENTS = urljoin(self.MASTER_URL, "/resources/achievements/")
        self.DROP_TYPES = {
            "material": {
                "url": self.MATERIAL_URL
            },
            "stone": {
                "url": self.STONE_URL
            },
            "rune": {
                "url": self.RUNE_URL
            }
        }

    @staticmethod
    def on_error(e):
        if type(e) is Exception:
            pool.terminate()

    @property
    def get_config(self):
        config_file = os.path.join(os.path.dirname(__file__), "config.json")
        with open(config_file, "r") as config:
            config_data = config.read()
        return json.loads(config_data)

    @retry(retry_on_exception=retry_if_connection_error)
    def get_soup(self, url):
        page = requests.get(url, verify=False)
        return BeautifulSoup(page.content, "lxml")

    def get_lang_url(self, url, language):
        return url.replace(self.MASTER_URL, urljoin(self.MASTER_URL, f"{language}/"))

    def drop_table(self, table_name):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        drop_table = f"""
        DROP TABLE IF EXISTS [{table_name}];
        """

        print(drop_table)
        cursor.execute(drop_table)

        cursor.close()
        sqlite_connection.close()

    def drop_view(self, view_name):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        drop_view = f"""
        DROP VIEW IF EXISTS [{view_name}];
        """

        print(drop_view)
        cursor.execute(drop_view)
        sqlite_connection.commit()

        cursor.close()
        sqlite_connection.close()

    def create_dimension_table(self, table_name, extra_columns=None):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        create_table = f"""
        CREATE TABLE [{table_name}](
            [{table_name}_id] INTEGER PRIMARY KEY AUTOINCREMENT,
            [{table_name}_href] TEXT,"""

        for language in self.LANG:
            create_table += f"""
            [{table_name}_name_{language}] TEXT,"""

        create_table = create_table[:-1]

        if extra_columns:
            create_table += ","

            for extra_column in extra_columns:
                for extra_column_name in extra_column:
                    extra_column_type = extra_column[extra_column_name]
                    if extra_column_type.upper() == "TEXT":
                        for language in self.LANG:
                            create_table += f"""
                            [{extra_column_name}_{language}] {extra_column_type},"""
                    else:
                        create_table += f"""
                        [{extra_column_name}] {extra_column_type},"""

            create_table = create_table[:-1]

        create_table += """
        )"""

        self.drop_table(table_name)
        print(create_table)
        cursor.execute(create_table)

        cursor.close()
        sqlite_connection.close()

    def create_bridge_table(self, table_name):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        create_table = f"""
        CREATE TABLE [{table_name}]("""

        for column in table_name.split("_"):
            create_table += f"""
            [{column}_id] INTEGER,"""

        create_table = create_table[:-1]

        create_table += """
        )"""

        self.drop_table(table_name)
        print(create_table)
        cursor.execute(create_table)

        cursor.close()
        sqlite_connection.close()

    def create_fact_table(self, table_name, number_of_levels):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        create_table = f"""
        CREATE TABLE [{table_name}]("""

        for column in table_name[:-1].split("_"):
            create_table += f"""
            [{column}_id] INTEGER,"""

        for level_number in range(1, number_of_levels + 1):
            create_table += f"""
            [level_{level_number}] TEXT,"""

        create_table = create_table[:-1]

        create_table += """
        )"""

        # self.drop_table(table_name)
        print(create_table)
        # cursor.execute(create_table)

        cursor.close()
        sqlite_connection.close()

    def create_custom_table(self, table_name, columns):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        create_table = f"""
        CREATE TABLE [{table_name}]("""

        for column in columns:
            for column_name in column:
                column_type = column[column_name]
                if column_type.upper() == "TEXT":
                    for language in self.LANG:
                        create_table += f"""
                        [{column_name}_{language}] {column_type},"""
                else:
                    create_table += f"""
                    [{column_name}] {column_type},"""

        create_table = create_table[:-1]

        create_table += """
        )"""

        self.drop_table(table_name)
        print(create_table)
        cursor.execute(create_table)

        cursor.close()
        sqlite_connection.close()

    def create_view(self, view_name, source_table, columns=None, data_filter=None):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        create_view = f"""
        CREATE VIEW [{view_name}]
        AS
        SELECT"""

        if columns:
            get_source_columns_query = f"""
                select *
                from pragma_table_info('{source_table}')
                where"""
            for column in columns:
                get_source_columns_query += f""" or name like '{column}'"""

            get_source_columns_query = get_source_columns_query.replace("where or", "where")

            cursor.execute(get_source_columns_query)
            table_column_names = cursor.fetchall()

            source_column_names = []

            for name in table_column_names:
                source_column_names.append(name[1])

            for source_column_name in source_column_names:
                view_column_name = source_column_name.replace(source_table, view_name)
                view_column = f"{source_column_name} as {view_column_name},"

                create_view += f"""
                {view_column}"""

            create_view = create_view[:-1]
        else:
            create_view += """
            *"""

        create_view += f"""
        FROM [{source_table}]"""

        if data_filter:
            create_view += f"""
            WHERE {data_filter}"""

        self.drop_view(view_name)
        print(create_view)
        cursor.execute(create_view)

        cursor.close()
        sqlite_connection.close()

    def create_dimension_table_all(self, config):
        for table_name in config["tables"]["dimension"]:
            extra_columns = config["extra_columns"].get(table_name)
            self.create_dimension_table(table_name, extra_columns)

    def create_bridge_table_all(self, config):
        for table_name in config["tables"]["bridge"]:
            self.create_bridge_table(table_name)

    def create_fact_table_all(self, config):
        for table in config["tables"]["fact"]:
            table_name = table["name"]
            number_of_levels = table["levels"]
            self.create_fact_table(table_name, number_of_levels)

    def create_custom_table_all(self, config):
        for table in config["tables"].get("custom"):
            table_name = table["name"]
            columns = table["columns"]
            self.create_custom_table(
                table_name=table_name,
                columns=columns
            )

    def create_view_all(self, config):
        for view in config["views"]:
            view_name = list(view.keys())[0]
            source_table = view[view_name]["source_table"]
            columns = view[view_name].get("columns")
            data_filter = view[view_name].get("data_filter")
            self.create_view(
                view_name=view_name,
                source_table=source_table,
                columns=columns,
                data_filter=data_filter
            )

    def update_dimension(self, dimension_name, data):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        table_column_names = 'PRAGMA table_info([' + dimension_name + ']);'
        cursor.execute(table_column_names)
        table_column_names = cursor.fetchall()

        column_names = []

        for name in table_column_names:
            column_names.append(name[1])

        column_names.remove(f"{dimension_name}_id")

        df = data[column_names]
        df.drop_duplicates(inplace=True)

        df.to_sql(name=dimension_name, con=sqlite_connection, if_exists='append', index=False)

        cursor.close()
        sqlite_connection.close()

    def update_bridge(self, bridge_name, data, use_column=None):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        table_column_names = 'PRAGMA table_info([' + bridge_name + ']);'
        cursor.execute(table_column_names)
        table_column_names = cursor.fetchall()

        column_names = []

        for name in table_column_names:
            column_names.append(name[1])

        for bridge_id, dimension in enumerate(bridge_name.split("_")):
            query = f"SELECT [{dimension}_id], [{dimension}_href] FROM [{dimension}]"
            df_0 = pd.read_sql_query(query, sqlite_connection)

            left_on = f"{dimension}_href"
            right_on = f"{dimension}_href"

            if use_column and bridge_id > 0:
                left_on = use_column

            data = pd.merge(data, df_0, how="left", left_on=left_on, right_on=right_on)

        df = data[column_names]

        df.to_sql(name=bridge_name, con=sqlite_connection, if_exists='append', index=False)

        cursor.close()
        sqlite_connection.close()

    def update_fact(self, fact_name, data):
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        table_column_names = 'PRAGMA table_info([' + fact_name + ']);'
        cursor.execute(table_column_names)
        table_column_names = cursor.fetchall()
        cursor.close()

        column_names = []

        for name in table_column_names:
            column_names.append(name[1])

        for dimension in fact_name[:-1].split("_"):
            query = f"SELECT [{dimension}_id], [{dimension}_href] FROM [{dimension}]"
            df_0 = pd.read_sql_query(query, sqlite_connection)

            left_on = f"{dimension}_href"
            right_on = f"{dimension}_href"

            data = pd.merge(data, df_0, how="left", left_on=left_on, right_on=right_on)

        df = data[column_names]

        df.to_sql(name=fact_name, con=sqlite_connection, if_exists='append', index=False)

        sqlite_connection.close()

    def get_boost_details(self, boost_href):
        boost_details = {}
        for language in self.LANG:
            url = self.get_lang_url(boost_href, language)
            soup = self.get_soup(url)

            list_boost = soup.find("select", class_="boostsList").find_all("option")
            for boost in list_boost:
                boost_id = int(boost.get("value"))
                boost_name = boost.text
                print(" " * 3, language, boost_name)

                if boost_details.get(boost_id):
                    boost_details[boost_id].update({f"boost_name_{language}": boost.text})
                else:
                    boost_details.update({boost_id: {f"boost_name_{language}": boost_name}})

        return boost_details

    def get_drop_details(self, drop_href, drop_type):
        drop_details = {}
        for language in self.LANG:
            url = urljoin(self.MASTER_URL, f"{language}/{drop_href}")
            soup = self.get_soup(url)

            drop_name = soup.find("h1").text
            print(" " * 3, language, drop_name)

            drop_details.update({f"drop_name_{language}": drop_name})

        drop_details["drop_href"] = drop_href
        drop_details["is_material"] = 0
        drop_details["is_stone"] = 0
        drop_details["is_rune"] = 0
        drop_details.update({f"is_{drop_type}": 1})

        if drop_type != "material":
            url = urljoin(self.MASTER_URL, drop_href)
            soup = self.get_soup(url)

            list_boosts = []
            for a in soup.find_all("div", class_="table-responsive gemDetailDiv")[1].find_all("a"):
                if a.has_attr("href"):
                    boost_href = str(int(a.get("href").split("/")[-2]))
                    boost_info = {"boost_href": boost_href}

                    for div in a.parent.find_next_siblings():
                        level_key = f"level_{div.get('val')}"
                        level_value = div.text
                        boost_info.update({level_key: level_value})
                    list_boosts.append(boost_info)

            drop_details["drop_boosts"] = list_boosts

        return drop_details

    def get_monster_details(self, monster_href):
        monster_details = {}
        for language in self.LANG:
            url = urljoin(self.MASTER_URL, f"{language}/{monster_href}")
            soup = self.get_soup(url)

            monster_name = soup.find("h1").text
            print(" " * 3, language, monster_name)

            monster_details.update({f"monster_name_{language}": monster_name})

        monster_details["monster_href"] = monster_href

        url = urljoin(self.MASTER_URL, monster_href)
        soup = self.get_soup(url)

        table_general_info = soup.find("table", class_="gemMainDetail lines")

        is_uber = 0
        is_shaman = 0
        if table_general_info.find("tr"):
            general_info = pd.read_html(str(table_general_info))[0]
            if general_info.iloc[0, 0] == "Убер Захватчик":
                is_uber = 1
            elif general_info.iloc[0, 0] == "Дух":
                is_shaman = 1

        monster_details["is_uber"] = is_uber
        monster_details["is_shaman"] = is_shaman

        drops = []
        for div in soup.find_all("div", class_="name"):
            drops.append(div.find_parent("a").get("href"))

        monster_details["drop_href"] = drops

        return monster_details

    def get_equipment_details(self, equipment_href):
        equipment_details = {}
        for language in self.LANG:
            url = urljoin(self.MASTER_URL, f"{language}/{equipment_href}")
            soup = self.get_soup(url)

            equipment_name = soup.find("h1").text
            print(" " * 3, language, equipment_name)

            general_info = pd.read_html(str(soup.find("table", class_="gemMainDetail lines")))[0]

            if len(general_info) > 5:
                slot = general_info.iloc[4, 1]
                equipment_types = general_info.iloc[5, 1]
            else:
                slot = general_info.iloc[1, 1]
                equipment_types = general_info.iloc[2, 1]

            equipment_details.update({f"equipment_name_{language}": equipment_name})
            equipment_details.update({f"slot_{language}": slot})
            equipment_details.update({f"equipment_type_{language}": equipment_types})

            table_equipment_statistic = soup.find("div", class_="table-responsive eqInfoDiv").find("table")
            df_equipment_statistic = pd.read_html(str(table_equipment_statistic))[0]

            df_equipment_statistic.columns = ["boost", "level_1", "level_2", "level_3", "level_4", "level_5", "level_6"]

            list_equipment_statistic = df_equipment_statistic.to_dict("records")

            if equipment_details.get("equipment_boosts"):
                equipment_details["equipment_boosts"].update({language: list_equipment_statistic})
            else:
                equipment_details.update({"equipment_boosts": {language: list_equipment_statistic}})

        equipment_details["equipment_href"] = equipment_href

        url = urljoin(self.MASTER_URL, equipment_href)
        soup = self.get_soup(url)

        materials_table = soup.find("table", class_="gemMainDetail gemList")
        if materials_table:
            materials_table.find("tr").decompose()
            materials = []
            for a in materials_table.find_all("a"):
                materials.append(a.get("href"))

            equipment_details["material_subequipment_href"] = materials
        else:
            equipment_details["material_subequipment_href"] = [None]

        return equipment_details

    def update_boost(self):
        function_start_time = datetime.now()
        print(f">>> Update boost start time:   {str(function_start_time)}")

        print(f"Collect boosts urls list")
        list_url = [
            self.EQUIPMENT_URL,
            self.STONE_URL,
            self.RUNE_URL,
            self.CITY_SKINS,
            self.ACHIEVEMENTS
        ]
        boost_details = {}
        for url in list_url:
            print(f"Collect boosts details dictionary from {url}")
            boost_details.update(self.get_boost_details(url))

        df = pd.DataFrame.from_dict(boost_details, orient="index")
        df["boost_href"] = df.index
        df_boost_details = df.sort_index()

        print("Update boost dimension")
        self.update_dimension("boost", df_boost_details)

        print(f"Boost data refreshed")

        function_end_time = datetime.now()
        function_duration = function_end_time - function_start_time
        print(f">>> Update boost end time:     {str(function_end_time)}")
        print(f">>> Update boost duration:     {str(function_duration)}")
        print("-" * 100)

    def update_drop(self, drop_type):
        function_start_time = datetime.now()
        print(f">>> Update {drop_type} start time:   {str(function_start_time)}")

        if drop_type not in self.DROP_TYPES:
            raise ValueError(f"results: drop_type must be one of {self.DROP_TYPES.keys()}")

        print(f"Collect {drop_type}s list")
        url = self.DROP_TYPES[drop_type]["url"]
        soup = self.get_soup(url)

        list_drop_href = []
        for div in soup.find_all("div", class_="name"):
            list_drop_href.append(div.find_parent("a").get("href"))

        global pool
        pool = multiprocessing.Pool(20)
        print(f"Collect {drop_type}s details list")
        list_pool_drop_details = [
            pool.apply_async(self.get_drop_details, (drop_href, drop_type,), error_callback=self.on_error)
            for drop_href in list_drop_href]
        pool.close()
        pool.join()

        print("Parse results into dataframe")
        list_drop_details = [drop_details.get() for drop_details in list_pool_drop_details]

        df_drop_details = pd.DataFrame(list_drop_details)

        print("Update drop dimension")
        self.update_dimension("drop", df_drop_details)

        if "drop_boosts" in df_drop_details.columns:
            print("Update drop boost")
            df_drop_boosts_raw = df_drop_details[["drop_href", "drop_boosts"]]

            df_drop_boosts_01 = create_row_for_each_dataframe_list_element(df_drop_boosts_raw, "drop_boosts")
            df_drop_boosts = create_column_for_each_dataframe_dictionary_element(df_drop_boosts_01, "drop_boosts")

            self.update_fact("drop_boosts", df_drop_boosts)

        print(f"{drop_type.title()}s data refreshed")

        function_end_time = datetime.now()
        function_duration = function_end_time - function_start_time
        print(f">>> Update {drop_type} end time:     {str(function_end_time)}")
        print(f">>> Update {drop_type} duration:     {str(function_duration)}")
        print("-" * 100)

        gc.collect()

    def update_monster(self):
        function_start_time = datetime.now()
        print(f">>> Update monster start time:   {str(function_start_time)}")

        print("Collect monsters list")
        url = self.MONSTER_URL
        soup = self.get_soup(url)

        list_monster_href = []
        for div in soup.find_all("div", class_="name"):
            list_monster_href.append(div.find_parent("a").get("href"))

        global pool
        pool = multiprocessing.Pool(4)
        print("Collect monsters details list")
        list_pool_monster_details = [
            pool.apply_async(self.get_monster_details, (monster_href,), error_callback=self.on_error)
            for monster_href in list_monster_href]
        pool.close()
        pool.join()

        print("Parse results into dataframe")
        list_monster_details = [monster_details.get() for monster_details in list_pool_monster_details]

        df = pd.DataFrame(list_monster_details)

        df_monster_details = create_row_for_each_dataframe_list_element(df, "drop_href")

        print("Update monster dimension")
        self.update_dimension("monster", df_monster_details)

        print("Update monster_drop bridge")
        self.update_bridge("monster_drop", df_monster_details)

        print("Monsters data refreshed")

        function_end_time = datetime.now()
        function_duration = function_end_time - function_start_time
        print(f">>> Update monster end time:     {str(function_end_time)}")
        print(f">>> Update monster duration:     {str(function_duration)}")
        print("-" * 100)

        gc.collect()

    def update_equipment(self):
        function_start_time = datetime.now()
        print(f">>> Update equipment start time:   {str(function_start_time)}")

        print("Collect equipments list")
        url = self.EQUIPMENT_URL
        soup = self.get_soup(url)

        list_equipment_href = []
        for div in soup.find_all("div", class_="name"):
            list_equipment_href.append(div.find_parent("a").get("href"))

        global pool
        pool = multiprocessing.Pool(4)
        print("Collect equipments details list")
        list_pool_equipment_details = [
            pool.apply_async(self.get_equipment_details, (equipment_href,), error_callback=self.on_error)
            for equipment_href in list_equipment_href]
        pool.close()
        pool.join()

        print("Parse results into dataframe")
        list_equipment_details = [equipment_details.get() for equipment_details in list_pool_equipment_details]

        df = pd.DataFrame(list_equipment_details)

        df_equipment_details = create_row_for_each_dataframe_list_element(df, "material_subequipment_href")

        print("Update equipment dimension")
        self.update_dimension("equipment", df_equipment_details)

        print("Update equipment_drop bridge")
        self.update_bridge("equipment_material_subequipment", df_equipment_details, "material_subequipment_href")

        print("Update equipment boost")
        df_equipment_boosts_data = df[["equipment_href", "equipment_boosts"]]

        df_data_split_by_language = create_column_for_each_dataframe_dictionary_element(
                                        df_equipment_boosts_data,
                                        "equipment_boosts"
                                    )

        data_first_language = pd.DataFrame()
        for language in self.LANG:
            df_language_data = df_data_split_by_language[["equipment_href", language]]

            df_language_data_split_by_fact_line = create_row_for_each_dataframe_list_element(
                                                    df_language_data,
                                                    language
                                                )

            df_language_boost = create_column_for_each_dataframe_dictionary_element(
                                df_language_data_split_by_fact_line,
                                language
                            )

            query = f"SELECT [boost_href], [boost_name_{language}] FROM [boost]"
            df_0 = pd.read_sql_query(query, sqlite3.connect(self.DB_FILE))

            data = pd.merge(df_language_boost, df_0, how="left", left_on="boost", right_on=f"boost_name_{language}")
            data.drop(columns=["boost", f"boost_name_{language}"], inplace=True)

            if len(data_first_language) == 0:
                data_first_language = data_first_language.append(data)
            else:
                if not data.equals(data_first_language):
                    df_first_except_current = data_first_language[~(data_first_language.isin(data).all(axis=1))]
                    df_current_except_first = data[~(data.isin(data_first_language).all(axis=1))]
                    error_message = f"Boost data for {language} language does not match with {self.LANG[0]} language."
                    error_message += "\n"
                    error_message += f"Boost data for {self.LANG[0]} language:"
                    error_message += "\n"
                    error_message += f"{df_first_except_current}"
                    error_message += "\n"
                    error_message += "*" * 100
                    error_message += "\n"
                    error_message += f"Boost data for {language} language:"
                    error_message += "\n"
                    error_message += f"{df_current_except_first}"
                    raise ValueError(error_message)

        self.update_fact("equipment_boosts", data_first_language)

        print("Equipments data refreshed")

        function_end_time = datetime.now()
        function_duration = function_end_time - function_start_time
        print(f">>> Update equipment end time:     {str(function_end_time)}")
        print(f">>> Update equipment duration:     {str(function_duration)}")
        print("-" * 100)

    def update_equipment_materials(self, equipment_id, parent_equipment_id=None,
                                   equipment_0_id=None,
                                   equipment_1_id=None,
                                   equipment_2_id=None,
                                   equipment_3_id=None,
                                   equipment_4_id=None,
                                   equipment_5_id=None,
                                   equipment_6_id=None,
                                   equipment_7_id=None,
                                   equipment_8_id=None,
                                   equipment_9_id=None,
                                   ):

        sqlite_connection = sqlite3.connect(self.DB_FILE)

        select = f"""
        SELECT
            equipment_id,
            material_id,
            subequipment_id
        FROM
            equipment_material_subequipment
        WHERE
            equipment_id = '{equipment_id}'
        """

        df = pd.read_sql_query(select, sqlite_connection)
        sqlite_connection.close()

        if not parent_equipment_id:
            equipment_0_id = equipment_id
        elif parent_equipment_id == equipment_8_id:
            equipment_9_id = equipment_id
        elif parent_equipment_id == equipment_7_id:
            equipment_8_id = equipment_id
        elif parent_equipment_id == equipment_6_id:
            equipment_7_id = equipment_id
        elif parent_equipment_id == equipment_5_id:
            equipment_6_id = equipment_id
        elif parent_equipment_id == equipment_4_id:
            equipment_5_id = equipment_id
        elif parent_equipment_id == equipment_3_id:
            equipment_4_id = equipment_id
        elif parent_equipment_id == equipment_2_id:
            equipment_3_id = equipment_id
        elif parent_equipment_id == equipment_1_id:
            equipment_2_id = equipment_id
        elif parent_equipment_id == equipment_0_id:
            equipment_1_id = equipment_id

        for index, row in df.iterrows():
            equipment_id = row["equipment_id"]
            material_id = row["material_id"]
            subequipment_id = row["subequipment_id"]
            if subequipment_id:
                self.update_equipment_materials(subequipment_id, equipment_id,
                                                equipment_0_id=equipment_0_id,
                                                equipment_1_id=equipment_1_id,
                                                equipment_2_id=equipment_2_id,
                                                equipment_3_id=equipment_3_id,
                                                equipment_4_id=equipment_4_id,
                                                equipment_5_id=equipment_5_id,
                                                equipment_6_id=equipment_6_id,
                                                equipment_7_id=equipment_7_id,
                                                equipment_8_id=equipment_8_id,
                                                equipment_9_id=equipment_9_id
                                                )
            else:
                if not equipment_1_id:
                    equipment_1_id = "NULL"
                if not equipment_2_id:
                    equipment_2_id = "NULL"
                if not equipment_3_id:
                    equipment_3_id = "NULL"
                if not equipment_4_id:
                    equipment_4_id = "NULL"
                if not equipment_5_id:
                    equipment_5_id = "NULL"
                if not equipment_6_id:
                    equipment_6_id = "NULL"
                if not equipment_7_id:
                    equipment_7_id = "NULL"
                if not equipment_8_id:
                    equipment_8_id = "NULL"
                if not equipment_9_id:
                    equipment_9_id = "NULL"
                if not material_id:
                    material_id = "NULL"

                sqlite_connection_2 = sqlite3.connect(self.DB_FILE)
                cursor_2 = sqlite_connection_2.cursor()

                insert = f"""
                INSERT INTO equipment_materials(
                     equipment_0_id
                    ,equipment_1_id
                    ,equipment_2_id
                    ,equipment_3_id
                    ,equipment_4_id
                    ,equipment_5_id
                    ,equipment_6_id
                    ,equipment_7_id
                    ,equipment_8_id
                    ,equipment_9_id
                    ,material_id
                    ,invader_id
                    ,uber_invader_id
                )
                SELECT
                     {equipment_0_id}
                    ,{equipment_1_id}
                    ,{equipment_2_id}
                    ,{equipment_3_id}
                    ,{equipment_4_id}
                    ,{equipment_5_id}
                    ,{equipment_6_id}
                    ,{equipment_7_id}
                    ,{equipment_8_id}
                    ,{equipment_9_id}
                    ,{material_id}
                    ,(
                        select
                            invader_id
                        from
                            invader
                            join monster_drop
                                on monster_id = invader_id
                        where
                            drop_id = {material_id}
                     )
                    ,(
                        select
                            uber_invader_id
                        from
                            uber_invader
                            join monster_drop
                                on monster_id = uber_invader_id
                        where
                            drop_id = {material_id}

                     )
                """
                # print(insert)
                cursor_2.execute(insert)
                sqlite_connection_2.commit()

                cursor_2.close()
                sqlite_connection_2.close()

    def update_equipment_materials_all(self):
        function_start_time = datetime.now()
        print(f">>> Update all equipment materials start time:   {str(function_start_time)}")

        print("Update all equipment materials")
        sqlite_connection = sqlite3.connect(self.DB_FILE)
        cursor = sqlite_connection.cursor()

        select = f"""
        SELECT DISTINCT
            equipment_id
        FROM
            equipment_material_subequipment
        """

        cursor.execute(select)
        equipment_ids = cursor.fetchall()

        cursor.close()
        sqlite_connection.close()

        for equipment in equipment_ids:
            equipment_id = equipment[0]

            self.update_equipment_materials(equipment_id)
        print("All equipment materials updated")

        function_end_time = datetime.now()
        function_duration = function_end_time - function_start_time
        print(f">>> Update all equipment materials end time:     {str(function_end_time)}")
        print(f">>> Update all equipment materials duration:     {str(function_duration)}")
        print("-" * 100)

    def create_db(self):
        config = self.get_config

        self.create_dimension_table_all(config)
        self.create_bridge_table_all(config)
        self.create_fact_table_all(config)
        self.create_custom_table_all(config)
        self.create_view_all(config)
        print("-" * 100)

    def update_db(self):
        function_start_time = datetime.now()
        print(f">>> Update DB start time:   {str(function_start_time)}")

        self.update_boost()
        self.update_drop("material")
        self.update_drop("stone")
        self.update_drop("rune")
        self.update_monster()
        self.update_equipment()
        self.update_equipment_materials_all()

        function_end_time = datetime.now()
        function_duration = function_end_time - function_start_time
        print(f">>> Update DB end time:     {str(function_end_time)}")
        print(f">>> Update DB duration:     {str(function_duration)}")

    def init_db(self):
        self.create_db()
        self.update_db()

    def run_select(self, select):
        if not select.startswith("SELECT"):
            print("WRONG SELECT!!!")
        else:
            sqlite_connection = sqlite3.connect(self.DB_FILE)

            df = pd.read_sql_query(select, sqlite_connection)

            sqlite_connection.close()

            return df


if __name__ == "__main__":
    start = datetime.now()

    VikingsDB().init_db()
    VikingsDB().update_db()

    end = datetime.now()
    duration = end - start
    print(">>>>>>>>>>>>> Duration: ", str(duration))
