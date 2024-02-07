from typing import Union, Optional, List, ForwardRef, Self
from pathlib import Path

import duckdb


IBGE_DB = str((Path(__file__).parent.parent / "data" / "ibge.duckdb").absolute())


class Macrorregiao:
    geocodigo: int
    nome: str
    __states__: List[ForwardRef("Estado")]
    __mesoregions__: List[ForwardRef("Mesorregiao")]
    __microregions__: List[ForwardRef("Microrregiao")]
    __cities__: List[ForwardRef("Municipio")]

    _macroregions = {
        1: "Norte",
        2: "Nordeste",
        3: "Centro-Oeste",
        4: "Sudeste",
        5: "Sul",
    }

    def __init__(
        self, nome: Optional[str] = None, geocodigo: Optional[Union[int, str]] = None
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
    def estados(self) -> List[ForwardRef("Estado")]:
        if not self.__states__:
            self._load_states()
        return self.__states__

    @property
    def mesorregioes(self) -> List[ForwardRef("Mesorregiao")]:
        if not self.__states__:
            self._load_states()

        if not self.__mesoregions__:
            self.__mesoregions__ = get_mesoregions_from_macroregion(self)

        return self.__mesoregions__

    @property
    def microrregioes(self) -> List[ForwardRef("Microrregiao")]:
        if not self.__states__:
            self._load_states()

        if not self.__microregions__:
            self.__microregions__ = get_microregion_from_macroregion(self)

        return self.__microregions__

    @property
    def municipios(self) -> List[ForwardRef("Municipio")]:
        if not self.__states__:
            self._load_states()

        if not self.__cities__:
            self.__cities__ = get_cities_from_macroregion(self)

        return self.__cities__

    def _load_states(self) -> None:
        self.__states__ = get_states_from_macroregion(self)


class Estado:
    geocodigo: int
    nome: str
    uf: str
    macrorregiao: Macrorregiao
    __mesoregions__: List[ForwardRef("Mesorregiao")]
    __microregions__: List[ForwardRef("Microrregiao")]
    __cities__: List[ForwardRef("Municipio")]

    def __init__(
        self,
        geocodigo: Optional[Union[int, str]] = None,
        uf: Optional[str] = None,
    ):
        if uf and geocodigo:
            raise ValueError("Utilize `UF` ou `geocodigo` para instanciar Estados")

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
        self.macrorregiao = Macrorregiao(geocodigo=state_df["macroregion"].to_list()[0])
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
    def mesorregioes(self) -> List[ForwardRef("Mesorregiao")]:
        if not self.__mesoregions__:
            self._load_mesoregions()
        return self.__mesoregions__

    @property
    def microrregioes(self) -> List[ForwardRef("Microrregiao")]:
        if not self.__mesoregions__:
            self._load_mesoregions()

        if not self.__microregions__:
            microregions = []
            for mesoregion in self.__mesoregions__:
                microregions.extend(mesoregion.microrregioes)
            self.__microregions__ = microregions

        return self.__microregions__

    @property
    def municipios(self) -> List[ForwardRef("Municipio")]:
        if not self.__mesoregions__:
            self._load_mesoregions()

        if not self.__cities__:
            cities = []
            for microregion in self.microrregioes:
                cities.extend(microregion.municipios)
            self.__cities__ = cities

        return self.__cities__

    def _load_mesoregions(self) -> None:
        self.__mesoregions__ = get_mesoregions_from_state(self)


class Mesorregiao:
    nome: str
    id_geografico: int
    macrorregiao: Macrorregiao
    estado: Estado
    __microregions__: List[ForwardRef("Microrregiao")]
    __cities__: List[ForwardRef("Municipio")]

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
    def microrregioes(self) -> List[ForwardRef("Microrregiao")]:
        if not self.__microregions__:
            self._load_microregions()
        return self.__microregions__

    @property
    def municipios(self) -> List[ForwardRef("Municipio")]:
        if not self.__microregions__:
            self._load_microregions()

        if not self.__cities__:
            cities = []
            for microregion in self.__microregions__:
                cities.extend(microregion.municipios)
            self.__cities__ = cities

        return self.__cities__

    def _load_microregions(self) -> None:
        self.__microregions__ = get_microregions_from_mesoregion(self)


class Microrregiao:
    __id__: int
    nome: str
    id_geografico: int
    macrorregiao: Macrorregiao
    estado: Estado
    mesorregiao: Mesorregiao
    municipios: List[ForwardRef("Municipio")]

    def __init__(
        self,
        nome: Optional[str] = None,
        mesorregiao: Optional[str] = None,
        __id__: Optional[int] = None,
    ):
        try:
            db = duckdb.connect(IBGE_DB)

            if nome:
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

            if __id__ is not None:
                __id__ = int(__id__)
                microregion_df = db.sql(
                    f"SELECT * FROM microregions WHERE id = {__id__}"
                ).fetchdf()

        finally:
            db.close()

        if microregion_df.empty:
            raise ValueError(
                f"Microrregião `{nome}` não encontrada. Por favor, verifique a "
                "acentuação. Exemplo: `Microrregiao('Vale do Itajaí')`"
            )

        if len(microregion_df) > 1:
            raise ValueError(
                f"""
A Microrregião {nome} é encontrada em diferentes Mesorregiões, 
por favor passe uma das opções: {list(microregion_df['mesoregion'])}.
Exemplo: Microrregiao(nome='{nome}', mesorregiao='{list(microregion_df['mesoregion'])[0]}')
            """
            )

        if nome:
            self.__id__ = microregion_df["id"][0]
            self.nome = nome

        if __id__ is not None:
            self.__id__ = __id__
            self.nome = microregion_df["name"][0]

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

    @property
    def municipios(self) -> List[ForwardRef("Municipio")]:
        if not self.__cities__:
            self._load_cities()
        return self.__cities__

    def _load_cities(self) -> None:
        self.__cities__ = get_cities_from_microregion(self)


class Municipio:
    geocodigo: int
    nome: str
    macrorregiao: Macrorregiao
    estado: Estado
    mesorregiao: Mesorregiao
    microrregiao: Microrregiao
    info: dict

    def __init__(self, geocodigo: Union[int, str]):
        self._check_geocode(str(geocodigo))
        self.geocodigo = int(geocodigo)

        try:
            db = duckdb.connect(IBGE_DB)
            city_df = db.sql(f"SELECT * FROM cities WHERE id = {geocodigo}").fetchdf()
        finally:
            db.close()

        if city_df.empty:
            raise ValueError("Município não encontrado. Exemplo: `Municipio(3304557)`")

        self.nome = city_df["name"][0]
        self.microrregiao = Microrregiao(__id__=int(city_df["microregion"][0]))
        self.mesorregiao = self.microrregiao.mesorregiao
        self.estado = self.mesorregiao.estado
        self.macrorregiao = self.estado.macrorregiao

        self.info = {}
        self.info["latitude"] = city_df["latitude"][0]
        self.info["longitude"] = city_df["longitude"][0]
        self.info["fuso_horario"] = city_df["timezone"][0]

    def __str__(self) -> str:
        return self.nome

    def __repr__(self) -> str:
        return self.nome

    def __hash__(self) -> int:
        return self.geocodigo

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, Municipio):
            return ValueError("Not a Municipio")
        return self.geocodigo == other.geocodigo

    def _check_geocode(self, geocodigo: str) -> None:
        if not geocodigo.isdigit():
            raise ValueError("O Geocódigo do Município deve conter apenas dígitos")

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


def get_mesoregions_from_macroregion(macroregion: Macrorregiao) -> List[Municipio]:
    try:
        db = duckdb.connect(IBGE_DB)
        mesoregions_df = db.sql(
            "SELECT mesoregions.name AS name "
            "FROM mesoregions "
            "JOIN states ON mesoregions.state = states.id "
            "JOIN macroregions ON states.macroregion = macroregions.id "
            f"WHERE macroregions.id = {macroregion.geocodigo}"
        ).fetchdf()
    finally:
        db.close()

    return [Mesorregiao(nome=name) for name in list(mesoregions_df["name"])]


def get_microregion_from_macroregion(macroregion: Macrorregiao) -> List[Municipio]:
    try:
        db = duckdb.connect(IBGE_DB)
        microregions_df = db.sql(
            "SELECT microregions.id AS id "
            "FROM microregions "
            "JOIN mesoregions ON microregions.mesoregion = mesoregions.name "
            "JOIN states ON mesoregions.state = states.id "
            "JOIN macroregions ON states.macroregion = macroregions.id "
            f"WHERE macroregions.id = {macroregion.geocodigo}"
        ).fetchdf()
    finally:
        db.close()

    return [Microrregiao(__id__=id) for id in list(microregions_df["id"])]


def get_cities_from_macroregion(
    macroregion: Macrorregiao, raw: bool = False
) -> List[Municipio]:
    try:
        db = duckdb.connect(IBGE_DB)
        cities_df = db.sql(
            "SELECT cities.id AS geocodigo "
            "FROM cities "
            "JOIN microregions ON cities.microregion = microregions.id "
            "JOIN mesoregions ON microregions.mesoregion = mesoregions.name "
            "JOIN states ON mesoregions.state = states.id "
            "JOIN macroregions ON states.macroregion = macroregions.id "
            f"WHERE macroregions.id = {macroregion.geocodigo}"
        ).fetchdf()
    finally:
        db.close()

    if raw:
        return list(cities_df["geocodigo"])

    return [Municipio(geocode) for geocode in list(cities_df["geocodigo"])]


def get_mesoregions_from_state(state: Estado) -> List[Mesorregiao]:
    try:
        db = duckdb.connect(IBGE_DB)
        mesoregions_df = db.sql(
            f"SELECT * FROM mesoregions WHERE state = {state.geocodigo}"
        ).fetchdf()
    finally:
        db.close()

    return [Mesorregiao(nome=name) for name in list(mesoregions_df["name"])]


def get_microregions_from_mesoregion(mesoregion: Mesorregiao) -> List[Microrregiao]:
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


def get_cities_from_microregion(microregion: Microrregiao) -> List[Municipio]:
    try:
        db = duckdb.connect(IBGE_DB)
        cities_df = db.sql(
            f"SELECT * FROM cities WHERE microregion = {microregion.__id__}"
        ).fetchdf()
    finally:
        db.close()

    return [Municipio(geocode) for geocode in list(cities_df["id"])]
