import unittest
from pathlib import Path

import duckdb
from ibge.brasil import IBGE_DB


class TestIBGEDB(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect(IBGE_DB)

    def tearDown(self):
        self.db.close()

    def test_ibge_db_exists(self):
        self.assertTrue(Path(IBGE_DB).exists())

    def test_list_all_tables(self):
        df = self.db.sql("SHOW ALL TABLES").fetchdf()
        expected_tables = [
            "cities",
            "macroregions",
            "mesoregions",
            "microregions",
            "states",
        ]
        self.assertEqual(list(df["name"]).sort(), expected_tables.sort())

    def test_macroregions_column_types(self):
        macroregions_desc = self.db.sql("DESCRIBE macroregions").fetchall()

        expected_columns = {
            "id": "INTEGER",
            "name": "VARCHAR",
        }

        columns = {row[0]: row[1] for row in macroregions_desc}
        self.assertEqual(columns, expected_columns)

    def test_states_column_types(self):
        states_desc = self.db.sql("DESCRIBE states").fetchall()

        expected_columns = {
            "id": "INTEGER",
            "name": "VARCHAR",
            "macroregion": "INTEGER",
            "uf": "VARCHAR",
        }

        columns = {row[0]: row[1] for row in states_desc}
        self.assertEqual(columns, expected_columns)

    def test_mesoregions_column_types(self):
        mesoregions_desc = self.db.sql("DESCRIBE mesoregions").fetchall()

        expected_columns = {
            "name": "VARCHAR",
            "state": "INTEGER",
            "geographic_id": "INTEGER",
        }

        columns = {row[0]: row[1] for row in mesoregions_desc}
        self.assertEqual(columns, expected_columns)

    def test_microregions_column_types(self):
        microregions_desc = self.db.sql("DESCRIBE microregions").fetchall()

        expected_columns = {
            "id": "INTEGER",
            "name": "VARCHAR",
            "mesoregion": "VARCHAR",
            "geographic_id": "INTEGER",
        }

        columns = {row[0]: row[1] for row in microregions_desc}
        self.assertEqual(columns, expected_columns)

    def test_cities_column_types(self):
        cities_desc = self.db.sql("DESCRIBE cities").fetchall()

        expected_columns = {
            "id": "INTEGER",
            "name": "VARCHAR",
            "microregion": "INTEGER",
            "latitude": "FLOAT",
            "longitude": "FLOAT",
            "timezone": "VARCHAR",
        }

        columns = {row[0]: row[1] for row in cities_desc}
        self.assertEqual(columns, expected_columns)

    def test_macroregions_amount(self):
        macroregions_count = self.db.sql(
            "SELECT COUNT(*) FROM macroregions"
        ).fetchone()
        expected_count = 5
        self.assertEqual(macroregions_count[0], expected_count)

    def test_states_amount(self):
        states_count = self.db.sql(
            "SELECT COUNT(*) FROM states"
        ).fetchone()
        expected_count = 27
        self.assertEqual(states_count[0], expected_count)

    def test_mesoregions_amount(self):
        mesoregions_count = self.db.sql(
            "SELECT COUNT(*) FROM mesoregions"
        ).fetchone()
        expected_count = 137
        self.assertEqual(mesoregions_count[0], expected_count)

    def test_microregions_amount(self):
        microregions_count = self.db.sql(
            "SELECT COUNT(*) FROM microregions"
        ).fetchone()
        expected_count = 558
        self.assertEqual(microregions_count[0], expected_count)

    def test_cities_amount(self):
        cities_count = self.db.sql("SELECT COUNT(*) FROM cities").fetchone()
        expected_count = 5570
        self.assertEqual(cities_count[0], expected_count)
