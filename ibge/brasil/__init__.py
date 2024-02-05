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
    __mesoregions__: List[ForwardRef('Mesorregiao')]
    __microregions__: List[ForwardRef('Microrregiao')]
    __cities__: List[ForwardRef('Municipio')]

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

        self.__states__ = []
        self.__mesoregions__ = []
        self.__microregions__ = []
        self.__cities__ = []

    def __str__(self) -> str:
        return self.nome

    def __repr__(self) -> str:
        return self.nome

    def __hash__(self) -> int:
        return self.geocodigo

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, Macrorregiao):
            return ValueError("Not a Macrorregiao")
        return self.geocodigo == other.geocodigo

    @property
    def estados(self) -> List[ForwardRef('Estado')]:
        if not self.__states__:
            self._load_states()
        return self.__states__

    @property
    def mesorregioes(self) -> List[ForwardRef('Mesorregiao')]:
        if not self.__states__:
            self._load_states()

        if not self.__mesoregions__:
            mesoregions = []
            for state in self.__states__:
                mesoregions.extend(state.mesorregioes)
            self.__mesoregions__ = mesoregions

        return self.__mesoregions__

    @property
    def microrregioes(self) -> List[ForwardRef('Microrregiao')]:
        if not self.__states__:
            self._load_states()

        if not self.__microregions__:
            microregions = []
            for state in self.__states__:
                for mesoregion in state.mesorregioes:
                    microregions.extend(mesoregion.microrregioes)
            self.__microregions__ = microregions

        return self.__microregions__

    def _load_states(self) -> None:
        self.__states__ = get_states_from_macroregion(self)


class Estado:
    geocodigo: int
    nome: str
    uf: str
    macrorregiao: Macrorregiao
    __mesoregions__: List[ForwardRef('Mesorregiao')]
    __microregions__: List[ForwardRef('Microrregiao')]
    __cities__: List[ForwardRef('Municipio')]

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
                "Geocódigo ou UF não encontrado. Exemplo: "
                "`Estado(geocodigo=11)` ou `Estado(uf='RO')` (Rondônia)"
            )

        self.geocodigo = state_df["id"].to_list()[0]
        self.nome = state_df["name"].to_list()[0]
        self.uf = state_df["uf"].to_list()[0]
        self.macrorregiao = (
            Macrorregiao(geocodigo=state_df["macroregion"].to_list()[0])
        )
        self.__mesoregions__ = []
        self.__microregions__ = []
        self.__cities__ = []

    def __str__(self) -> str:
        return self.nome

    def __repr__(self) -> str:
        return self.nome

    def __hash__(self) -> int:
        return self.geocodigo

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, Estado):
            return ValueError("Not an Estado")
        return self.geocodigo == other.geocodigo

    @property
    def mesorregioes(self) -> List[ForwardRef('Mesorregiao')]:
        if not self.__mesoregions__:
            self._load_mesoregions()
        return self.__mesoregions__

    @property
    def microrregioes(self) -> List[ForwardRef('Microrregiao')]:
        if not self.__mesoregions__:
            self._load_mesoregions()

        if not self.__microregions__:
            microregions = []
            for mesoregion in self.__mesoregions__:
                microregions.extend(mesoregion.microrregioes)
            self.__microregions__ = microregions

        return self.__microregions__

    def _load_mesoregions(self) -> None:
        self.__mesoregions__ = get_mesoregions_from_state(self)


class Mesorregiao:
    nome: str
    id_geografico: int
    macrorregiao: Macrorregiao
    estado: Estado
    __microregions__: List[ForwardRef('Microrregiao')]
    __cities__: List[ForwardRef('Municipio')]

    def __init__(self, nome: str):
        try:
            db = duckdb.connect(IBGE_DB)
            mesoregion_df = db.sql(
                f"SELECT * FROM mesoregions WHERE LOWER(name) = '{nome.lower()}'"
            ).fetchdf()
        finally:
            db.close()

        if mesoregion_df.empty:
            raise ValueError(
                f"Mesorregião `{nome}` não encontrada. Por favor, verifique a "
                "acentuação. Exemplo: `Mesorregiao('Vale do Itajaí')`"
            )

        self.nome = nome
        self.id_geografico = mesoregion_df["geographic_id"][0]
        self.estado = Estado(geocodigo=mesoregion_df["state"][0])
        self.macrorregiao = self.estado.macrorregiao
        self.__microregions__ = []
        self.__cities__ = []

    def __str__(self) -> str:
        return self.nome

    def __repr__(self) -> str:
        return self.nome

    def __hash__(self) -> int:
        return self.nome

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, Mesorregiao):
            return ValueError("Not a Mesorregiao")
        return self.nome == other.nome

    @property
    def microrregioes(self) -> List[ForwardRef('Microrregiao')]:
        if not self.__microregions__:
            self._load_microregions()
        return self.__microregions__

    def _load_microregions(self) -> None:
        self.__microregions__ = get_microregions_from_mesoregion(self)


class Microrregiao:
    nome: str
    id_geografico: int
    macrorregiao: Macrorregiao
    estado: Estado
    mesorregiao: Mesorregiao
    municipios: List[ForwardRef('Municipio')]

    def __init__(self, nome: str, mesorregiao: Optional[str] = None):
        try:
            db = duckdb.connect(IBGE_DB)
            if "'" in nome:
                nome = nome.replace("'", r"''")

            if mesorregiao:
                microregion_df = db.sql(
                    "SELECT * FROM microregions WHERE LOWER(name) = '"
                    f"{nome.lower()}"
                    f"' AND LOWER(mesoregion) = '{mesorregiao.lower()}'"
                ).fetchdf()
            else:
                microregion_df = db.sql(
                    "SELECT * FROM microregions WHERE LOWER(name) = "
                    f"'{nome.lower()}'"
                ).fetchdf()

            if "''" in nome:
                nome = nome.replace("''", "'")
        finally:
            db.close()

        if microregion_df.empty:
            raise ValueError(
                f"Microrregião `{nome}` não encontrada. Por favor, verifique a "
                "acentuação. Exemplo: `Microrregiao('Vale do Itajaí')`"
            )

        if len(microregion_df) > 1:
            raise ValueError(f"""
A Microrregião {nome} é encontrada em diferentes Mesorregiões, 
por favor passe uma das opções: {list(microregion_df['mesoregion'])}.
Exemplo: Microrregiao(nome='{nome}', mesorregiao='{list(microregion_df['mesoregion'])[0]}')
            """)

        self.nome = nome
        self.id_geografico = microregion_df["geographic_id"][0]
        self.mesorregiao = Mesorregiao(microregion_df["mesoregion"][0])
        self.estado = self.mesorregiao.estado
        self.macrorregiao = self.estado.macrorregiao
        self.__cities__ = []

    def __str__(self) -> str:
        return self.nome

    def __repr__(self) -> str:
        return self.nome

    def __hash__(self) -> int:
        return self.nome

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, Microrregiao):
            return ValueError("Not a Microrregiao")
        return self.nome == other.nome


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


def get_states_from_macroregion(macroregion: Macrorregiao) -> List[Estado]:
    try:
        db = duckdb.connect(IBGE_DB)
        states_df = db.sql(
            f"SELECT * FROM states WHERE macroregion = {macroregion.geocodigo}"
        ).fetchdf()
    finally:
        db.close()

    return [Estado(geocodigo=id) for id in list(states_df["id"])]


def get_mesoregions_from_state(state: Estado) -> List[Mesorregiao]:
    try:
        db = duckdb.connect(IBGE_DB)
        mesoregions_df = db.sql(
            f"SELECT * FROM mesoregions WHERE state = {state.geocodigo}"
        ).fetchdf()
    finally:
        db.close()

    return [Mesorregiao(nome=name) for name in list(mesoregions_df["name"])]


def get_microregions_from_mesoregion(
    mesoregion: Mesorregiao
) -> List[Microrregiao]:
    try:
        db = duckdb.connect(IBGE_DB)
        microregions_df = db.sql(
            f"SELECT * FROM microregions WHERE mesoregion = '{mesoregion.nome}'"
        ).fetchdf()
    finally:
        db.close()

    return [
        Microrregiao(nome=name, mesorregiao=mesoregion.nome)
        for name in list(microregions_df["name"])
    ]


def get_mesoregions(state: int):
    try:
        db = duckdb.connect(IBGE_DB)
        mesoregion_df = db.sql(
            f"SELECT * FROM mesoregions WHERE state = {state}"
        ).fetchdf()
    finally:
        db.close()
