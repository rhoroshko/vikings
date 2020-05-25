from .vikings_db import VikingsDB


vdb = VikingsDB()


class Vikings:
    def __init__(self, output_folder, language="ru"):
        self.output_folder = output_folder
        self.language = language

    def get_materials(self):
        query = f"""SELECT material_name_{self.language} FROM material"""
        df = vdb.run_select(query)

        return df

    def get_set(self, set_name):
        query = f"""SELECT
             e0.equipment_name_{self.language}      as equipment_0
            ,e1.equipment_name_{self.language}      as equipment_1
            ,e2.equipment_name_{self.language}      as equipment_2
            ,e3.equipment_name_{self.language}      as equipment_3
            ,e4.equipment_name_{self.language}      as equipment_4
            ,e5.equipment_name_{self.language}      as equipment_5
            ,e6.equipment_name_{self.language}      as equipment_6
            ,e7.equipment_name_{self.language}      as equipment_7
            ,e8.equipment_name_{self.language}      as equipment_8
            ,e9.equipment_name_{self.language}      as equipment_9
            ,m.material_name_{self.language}        as material
            ,i.invader_name_{self.language}         as invader
            ,ui.uber_invader_name_{self.language}   as uber_invader
        FROM
            equipment_materials em
            LEFT JOIN equipment_boosts eb
                ON eb.equipment_id = em.equipment_0_id
            LEFT JOIN boost b
                ON b.boost_id = eb.boost_id
            LEFT JOIN equipment e0
                ON e0.equipment_id = em.equipment_0_id
            LEFT JOIN equipment e1
                ON e1.equipment_id = em.equipment_1_id
            LEFT JOIN equipment e2
                ON e2.equipment_id = em.equipment_2_id
            LEFT JOIN equipment e3
                ON e3.equipment_id = em.equipment_3_id
            LEFT JOIN equipment e4
                ON e4.equipment_id = em.equipment_4_id
            LEFT JOIN equipment e5
                ON e5.equipment_id = em.equipment_5_id
            LEFT JOIN equipment e6
                ON e6.equipment_id = em.equipment_6_id
            LEFT JOIN equipment e7
                ON e7.equipment_id = em.equipment_7_id
            LEFT JOIN equipment e8
                ON e8.equipment_id = em.equipment_8_id
            LEFT JOIN equipment e9
                ON e9.equipment_id = em.equipment_9_id
            LEFT JOIN material m
                ON m.material_id = em.material_id
            LEFT JOIN invader i
                ON i.invader_id = em.invader_id
            LEFT JOIN uber_invader ui
                ON ui.uber_invader_id = em.uber_invader_id
        WHERE
            b.boost_name_{self.language} = '{set_name.replace("'", "''")}'
        ORDER BY
            CAST(REPLACE(eb.level_6, '%', '') AS DECIMAL) desc
        """
        df = vdb.run_select(query)

        df.dropna(axis=1, how="all", inplace=True)

        return df
