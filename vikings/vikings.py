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

    def get_set_materials(self, set_name):
        query = f"""WITH cte AS (
        SELECT
            bbe.equipment_id
        FROM
            best_boost_equipment bbe
            JOIN boost b
                ON b.boost_id = bbe.boost_id
        WHERE
            b.boost_name_{self.language} = '{set_name.replace("'", "''")}'
        UNION ALL
        SELECT
             bbe.equipment_id
        FROM
            best_boost_equipment bbe
            JOIN boost b
                ON b.boost_id = bbe.boost_id
            JOIN equipment_slot es
                ON es.equipment_slot_id = bbe.equipment_slot_id
        WHERE
            b.boost_name_{self.language} = '{set_name.replace("'", "''")}'
            AND es.equipment_slot_href = '5'
        )
        SELECT
             e0.equipment_name_{self.language}      AS equipment_0
            ,e1.equipment_name_{self.language}      AS equipment_1
            ,e2.equipment_name_{self.language}      AS equipment_2
            ,e3.equipment_name_{self.language}      AS equipment_3
            ,e4.equipment_name_{self.language}      AS equipment_4
            ,e5.equipment_name_{self.language}      AS equipment_5
            ,e6.equipment_name_{self.language}      AS equipment_6
            ,e7.equipment_name_{self.language}      AS equipment_7
            ,e8.equipment_name_{self.language}      AS equipment_8
            ,e9.equipment_name_{self.language}      AS equipment_9
            ,m.material_name_{self.language}        AS material
            ,i.invader_name_{self.language}         AS invader
            ,ui.uber_invader_name_{self.language}   AS uber_invader
            ,et.equipment_type_name_{self.language} AS type
        FROM
            equipment_materials em
            JOIN cte
                ON cte.equipment_id = em.equipment_0_id
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
            LEFT JOIN equipment_slot es
                ON es.equipment_slot_id = e0.equipment_slot_id
            LEFT JOIN equipment_type et
                ON et.equipment_type_id = e0.equipment_type_id
        ORDER BY
             et.equipment_type_href
            ,es.equipment_slot_href
        """
        df = vdb.run_select(query)

        df.dropna(axis=1, how="all", inplace=True)

        return df

    def get_set_summary(self, set_name):
        query = f"""WITH cte AS (
        SELECT
            bbe.*
        FROM
            best_boost_equipment bbe
            JOIN boost b
                ON b.boost_id = bbe.boost_id
        WHERE
            b.boost_name_{self.language} = '{set_name.replace("'", "''")}'
        UNION ALL
        SELECT
             bbe.*
        FROM
            best_boost_equipment bbe
            JOIN boost b
                ON b.boost_id = bbe.boost_id
            JOIN equipment_slot es
                ON es.equipment_slot_id = bbe.equipment_slot_id
        WHERE
            b.boost_name_{self.language} = '{set_name.replace("'", "''")}'
            AND es.equipment_slot_href = '5'
        )
        SELECT
             e.equipment_name_{self.language}           AS equipment
            ,cte.level_1
            ,cte.level_2
            ,cte.level_3
            ,cte.level_4
            ,cte.level_5
            ,cte.level_6
            ,es.equipment_slot_name_{self.language}     AS slot
            ,et.equipment_type_name_{self.language}     AS type
        FROM
            cte
            JOIN equipment e
                ON e.equipment_id = cte.equipment_id
            JOIN equipment_slot es
                ON es.equipment_slot_id = cte.equipment_slot_id
            JOIN equipment_type et
                ON et.equipment_type_id = cte.equipment_type_id
        ORDER BY
             et.equipment_type_href
            ,es.equipment_slot_href
        """

        df = vdb.run_select(query)

        return df
