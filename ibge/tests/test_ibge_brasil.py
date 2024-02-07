import unittest

from ibge.brasil import Macrorregiao, Estado, Mesorregiao, Microrregiao, Municipio


class TestIBGEBrasil(unittest.TestCase):
    def setUp(self):
        self.rio_de_janeiro = Municipio(3304557)

    def tearDown(self):
        del self.rio_de_janeiro

    def test_rio_de_janeiro_municipio(self):
        expected_info = {
            "latitude": -22.9129,
            "longitude": -43.2003,
            "fuso_horario": "America/Sao_Paulo",
        }
        self.assertEqual(str(self.rio_de_janeiro.info), str(expected_info))
        self.assertEqual(str(self.rio_de_janeiro), "Rio de Janeiro")
        self.assertEqual(Municipio, type(self.rio_de_janeiro))

    def test_rio_de_janeiro_microregion(self):
        rio_de_janeiro_microregion = self.rio_de_janeiro.microrregiao
        self.assertEqual(str(rio_de_janeiro_microregion), "Rio de Janeiro")
        self.assertEqual(Microrregiao, type(rio_de_janeiro_microregion))

        expected_rio_de_janeiro_microregion_cities = [
            "Belford Roxo",
            "Duque de Caxias",
            "Guapimirim",
            "Itaboraí",
            "Japeri",
            "Magé",
            "Maricá",
            "Mesquita",
            "Nilópolis",
            "Niterói",
            "Nova Iguaçu",
            "Queimados",
            "Rio de Janeiro",
            "São Gonçalo",
            "São João de Meriti",
            "Tanguá",
        ]

        self.assertEqual(
            list(map(str, rio_de_janeiro_microregion.municipios)),
            expected_rio_de_janeiro_microregion_cities,
        )

    def test_rio_de_janeiro_mesoregion(self):
        rio_de_janeiro_mesoregion = self.rio_de_janeiro.mesorregiao
        self.assertEqual(
            str(rio_de_janeiro_mesoregion), "Metropolitana do Rio de Janeiro"
        )
        self.assertEqual(Mesorregiao, type(rio_de_janeiro_mesoregion))

        expected_rio_de_janeiro_mesoregion_microregions = [
            "Rio de Janeiro",
            "Macacu-Caceribu",
            "Vassouras",
            "Itaguaí",
            "Serrana",
        ]

        self.assertEqual(
            list(map(str, rio_de_janeiro_mesoregion.microrregioes)),
            expected_rio_de_janeiro_mesoregion_microregions,
        )

        self.assertEqual(len(rio_de_janeiro_mesoregion.municipios), 30)

    def test_rio_de_janeiro_state(self):
        rio_de_janeiro_state = self.rio_de_janeiro.estado
        self.assertEqual(str(rio_de_janeiro_state), "Rio de Janeiro")
        self.assertEqual(Estado, type(rio_de_janeiro_state))

        expected_rio_de_janeiro_state_mesoregions = [
            "Sul Fluminense",
            "Noroeste Fluminense",
            "Baixadas",
            "Centro Fluminense",
            "Metropolitana do Rio de Janeiro",
            "Norte Fluminense",
        ]

        self.assertEqual(
            list(map(str, rio_de_janeiro_state.mesorregioes)),
            expected_rio_de_janeiro_state_mesoregions,
        )

        expected_rio_de_janeiro_state_microregions = [
            "Baía da Ilha Grande",
            "Barra do Piraí",
            "Vale do Paraíba Fluminense",
            "Santo Antônio de Pádua",
            "Itaperuna",
            "Lagos",
            "Bacia de São João",
            "Três Rios",
            "Nova Friburgo",
            "Cantagalo-Cordeiro",
            "Santa Maria Madalena",
            "Rio de Janeiro",
            "Macacu-Caceribu",
            "Vassouras",
            "Itaguaí",
            "Serrana",
            "Campos dos Goytacazes",
            "Macaé",
        ]

        self.assertEqual(
            list(map(str, rio_de_janeiro_state.microrregioes)),
            expected_rio_de_janeiro_state_microregions,
        )

        self.assertEqual(len(rio_de_janeiro_state.municipios), 92)

    def test_rio_de_janeiro_macroregion(self):
        rio_de_janeiro_macroregion = self.rio_de_janeiro.macrorregiao
        self.assertEqual(str(rio_de_janeiro_macroregion), "Sudeste")
        self.assertEqual(Macrorregiao, type(rio_de_janeiro_macroregion))

        expected_rio_de_janeiro_macroregion_states = [
            "Minas Gerais",
            "Espírito Santo",
            "Rio de Janeiro",
            "São Paulo",
        ]

        self.assertEqual(
            list(map(str, rio_de_janeiro_macroregion.estados)),
            expected_rio_de_janeiro_macroregion_states,
        )

        expected_rio_de_janeiro_macroregion_mesoregions = [
            "Triângulo Mineiro/Alto Paranaíba",
            "Central Mineira",
            "Zona da Mata",
            "Vale do Rio Doce",
            "Oeste de Minas",
            "Vale do Mucuri",
            "Norte de Minas",
            "Sul/Sudoeste de Minas",
            "Campo das Vertentes",
            "Jequitinhonha",
            "Metropolitana de Belo Horizonte",
            "Noroeste de Minas",
            "Central Espírito-santense",
            "Noroeste Espírito-santense",
            "Sul Espírito-santense",
            "Litoral Norte Espírito-santense",
            "Sul Fluminense",
            "Noroeste Fluminense",
            "Baixadas",
            "Centro Fluminense",
            "Metropolitana do Rio de Janeiro",
            "Norte Fluminense",
            "Presidente Prudente",
            "São José do Rio Preto",
            "Campinas",
            "Bauru",
            "Piracicaba",
            "Itapetininga",
            "Ribeirão Preto",
            "Araçatuba",
            "Macro Metropolitana Paulista",
            "Marília",
            "Araraquara",
            "Vale do Paraíba Paulista",
            "Metropolitana de São Paulo",
            "Assis",
            "Litoral Sul Paulista",
        ]

        self.assertEqual(
            list(map(str, rio_de_janeiro_macroregion.mesorregioes)),
            expected_rio_de_janeiro_macroregion_mesoregions,
        )

        self.assertEqual(len(rio_de_janeiro_macroregion.microrregioes), 160)

        # self.assertEqual(len(rio_de_janeiro_macroregion.municipios), 1668)
