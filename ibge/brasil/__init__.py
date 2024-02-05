from typing import Union, Optional, List, ForwardRef, Self
from pathlib import Path

import duckdb


IBGE_DB = str(
    (Path(__file__).parent.parent / "data" / "ibge.duckdb").absolute()
)


class Macrorregiao:
    geocodigo: int
    nome: str
    __states__: List[ForwardRef('Estado')]

    _macroregions = {
        1: "Norte",
        2: "Nordeste",
        3: "Centro-Oeste",
        4: "Sudeste",
        5: "Sul"
    }

    def __init__(
        self,
        nome: Optional[str] = None,
        geocodigo: Optional[Union[int, str]] = None
    ):
        if nome and geocodigo:
            raise ValueError(
                "Utilize `nome` ou `geocodigo` para instanciar Macrorregião"
            )

        if geocodigo:
            self.geocodigo = int(geocodigo)

            if self.geocodigo not in [1, 2, 3, 4, 5]:
                raise ValueError(
                    "Macrorregião não encontrada. Opções: 1, 2, 3, 4 ou 5 "
                    "(Norte, Nordeste, Centro-Oeste, Sudeste ou Sul, "
                    "respectivamente)"
                )

            self.nome = self._macroregions[self.geocodigo]

        if nome:
            rev_macroregions = {v: k for k, v in self._macroregions.items()}
            self.nome = nome.capitalize()

            if self.nome not in self._macroregions.values():
                raise ValueError(
                    "Macrorregião não encontrada. Opções: Norte, Nordeste, "
                    "Centro-Oeste, Sudeste ou Sul"
                )

            self.geocodigo = rev_macroregions[self.nome]

    def __str__(self) -> str:
        return self.nome

    def __repr__(self) -> str:
        return self.nome

    def __hash__(self) -> int:
        return self.geocodigo

    def __eq__(self, other: Self) -> bool:
        return self.geocodigo == other.geocodigo

    @property
    def estados(self) -> List[ForwardRef('Estado')]:
        if not self.__states__:
            self.load_states()
        return self.__states__

    def load_states(self) -> None:
        self.__states__ = get_states_from_macroregion(self.geocodigo)


class Estado:
    geocodigo: int
    nome: str
    uf: str
    macrorregiao: Macrorregiao
    mesorregioes: List[ForwardRef('Mesorregiao')]
    microrregioes: List[ForwardRef('Microrregiao')]
    municipios: List[ForwardRef('Municipio')]

    def __init__(
        self,
        geocodigo: Optional[Union[int, str]] = None,
        uf: Optional[str] = None,
    ):
        if uf and geocodigo:
            raise ValueError(
                "Utilize `UF` ou `geocodigo` para instanciar Estados"
            )

        try:
            db = duckdb.connect(IBGE_DB)
            if geocodigo:
                state_df = db.sql(
                    f"SELECT * FROM states WHERE id = {geocodigo}"
                ).fetchdf()
            if uf:
                state_df = db.sql(
                    f"SELECT * FROM states WHERE uf = '{uf.upper()}'"
                ).fetchdf()
        finally:
            db.close()

        if state_df.empty:
            raise ValueError(
                f"Geocódigo `{geocodigo}` não encontrado. "
                "Exemplo: `11` (Rondônia)"
            )

        self.geocodigo = state_df["id"].to_list()[0]
        self.nome = state_df["name"].to_list()[0]
        self.uf = state_df["uf"].to_list()[0]
        self.macrorregiao = (  # Macrorregiao( #TODO
            state_df["macroregion"].to_list()[0]
        )

    def __str__(self) -> str:
        return self.nome

    def __repr__(self) -> str:
        return self.nome


class Mesorregiao:
    nome: str
    id_geografico: int
    macrorregiao: Macrorregiao
    estado: Estado
    microrregioes: List[ForwardRef('Microrregiao')]
    municipios: List[ForwardRef('Municipio')]


class Microrregiao:
    nome: str
    id_geografico: int
    macrorregiao: Macrorregiao
    estado: Estado
    mesorregiao: Mesorregiao
    municipios: List[ForwardRef('Municipio')]


class Municipio:
    geocodigo: int
    nome: str
    latitude: float
    longitude: float
    fuso_horario: str
    macrorregiao: Macrorregiao
    estado: Estado
    mesorregiao: Mesorregiao
    microrregiao: Microrregiao

    def __init__(self, geocodigo: Union[int, str]):
        self._check_geocode(str(geocodigo))
        self.geocodigo = int(geocodigo)

    def _check_geocode(self, geocodigo: str) -> None:
        if not geocodigo.isdigit():
            raise ValueError(
                "O Geocódigo do Município deve conter apenas dígitos"
            )

        if len(geocodigo) != 7:
            raise ValueError(
                "O Geocódigo do Município deve estar no formato do IBGE. "
                "E.g: 3304557"
            )


def get_states_from_macroregion(macroregion: int) -> List[Estado]:
    try:
        db = duckdb.connect(IBGE_DB)
        states_df = db.sql(
            f"SELECT * FROM states WHERE macroregion = {macroregion}"
        ).fetchdf()
    finally:
        db.close()

    return [Estado(geocodigo=id) for id in list(states_df["id"])]


def get_mesoregions(state: int):
    try:
        db = duckdb.connect(IBGE_DB)
        mesoregion_df = db.sql(
            f"SELECT * FROM mesoregions WHERE state = {state}"
        ).fetchdf()
    finally:
        db.close()
