"""
db.sql('''
SELECT states.id, states.name AS state, macroregions.name AS macroregion
FROM states
JOIN macroregions ON states.macroregion = macroregions.id;
''')

db.sql('''
SELECT m.name AS mesoregion, s.uf AS uf, s.name AS state
FROM mesoregions AS m
JOIN states as s ON m.state = s.id;
''')


"""
