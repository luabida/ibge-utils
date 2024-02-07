# IBGE-Utils

[ibge-utils](https://pypi.org/project/ibge-utils) é uma API Python com funções utilitárias para manipulação e extração de dados georeferenciados, de acordo com os padrões do IBGE.

## Dados
Os dados da API foram extraídos das [Tabelas da Divisão Territorial Brasileira 2022](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/23701-divisao-territorial-brasileira.html) do IBGE, e estão distribuídos nas tabelas do arquivo [ibge.duckdb](https://github.com/luabida/ibge-utils/blob/main/ibge/data/ibge.duckdb) no seguinte esquema:

![tables](https://tinyurl.com/23axbv2n)


## Macrorregiões

```py
from ibge.brasil import Macrorregiao

norte = Macrorregiao(geocodigo=1) # OU
norte = Macrorregiao(nome="norte")

norte.estados # Estados do Norte
# Ouput:
# [Rondônia, Acre, Amazonas, Roraima, Pará, Amapá, Tocantins]
norte.mesorregioes # Mesorregiões do Norte
norte.microrregioes # Microrregiões do Norte
norte.municipios # Todos os municípios do Norte
```

## Estados

```py
from ibge.brasil import Estado

rondonia = Estado(geocodigo=11) # OU
rondonia = Estado(uf="RO")

rondonia.macrorregiao # Norte
rondonia.mesorregioes # [Leste Rondoniense, Madeira-Guaporé]
rondonia.microrregioes # Microrregiões de Rondônia
rondonia.municipios # Todos os municípios de Rondônia
```

## Municípios
```py
from ibge.brasil import Municipio

cerejeiras = Municipio(geocodigo=1100056)

cerejeiras.macrorregiao # Norte
cerejeiras.estado # Rondônia
cerejeiras.mesorregiao # Leste Rondoniense
cerejeiras.microrregiao # Colorado do Oeste
cerejeiras.info
# Output:
# {'latitude': -13.187,
# 'longitude': -60.8168,
# 'fuso_horario': 'America/Porto_Velho'}
```
