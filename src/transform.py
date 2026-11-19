"""Transformación de mensajes del taller al contrato del modelo entrenado."""
import math
import pandas as pd

PAISES = {
    'Hong Kong S.A.R., China': 'Hong Kong', 'Taiwan Province of China': 'Taiwan',
    'Trinidad & Tobago': 'Trinidad and Tobago', 'North Cyprus': 'Northern Cyprus',
    'Macedonia': 'North Macedonia', 'Somaliland region': 'Somaliland Region',
}
ALIAS = {
    'gdp': ['Economy (GDP per Capita)', 'Economy..GDP.per.Capita.', 'GDP per capita'],
    'social_support': ['Family', 'Social support'],
    'life_expectancy': ['Health (Life Expectancy)', 'Health..Life.Expectancy.', 'Healthy life expectancy'],
    'freedom': ['Freedom', 'Freedom to make life choices'],
    'corruption': ['Trust (Government Corruption)', 'Trust..Government.Corruption.', 'Perceptions of corruption'],
    'generosity': ['Generosity'],
    'score': ['Happiness Score', 'Happiness.Score', 'Score'],
}

def presente(valor):
    return valor is not None and not pd.isna(valor) and str(valor).strip() != ''

def leer(datos, opciones):
    valores = [datos[n] for n in opciones if n in datos and presente(datos[n])]
    if len(valores) > 1:
        raise ValueError(f'Campos simultáneos ambiguos: {opciones}')
    return valores[0] if valores else None

def transformar(payload, artefacto):
    if not isinstance(payload, dict) or not isinstance(payload.get('metadata'), dict) or not isinstance(payload.get('data'), dict):
        raise ValueError('Se requieren metadata y data como objetos')
    meta, data = payload['metadata'], payload['data']
    pais = leer(data, ['Country', 'Country or region'])
    if pais is None: raise ValueError('Falta el país')
    pais = PAISES.get(str(pais).strip(), str(pais).strip())
    try:
        numero_anio = float(meta['year'])
        if not math.isfinite(numero_anio) or not numero_anio.is_integer(): raise ValueError()
        anio = int(numero_anio)
    except (KeyError, TypeError, ValueError): raise ValueError('Año ausente o inválido')
    if anio not in (2015, 2016, 2017, 2018, 2019): raise ValueError(f'Año no contemplado por el modelo: {anio}')
    region = leer(data, ['Region']) or artefacto['region_map'].get(pais)
    if not region: raise ValueError(f'Región desconocida para {pais}; revisar antes de predecir')
    valores = {'year': anio, 'region': region}
    for nombre, opciones in ALIAS.items():
        valor = leer(data, opciones)
        if valor is None:
            valores[nombre] = float('nan')
        else:
            try: valores[nombre] = float(valor)
            except (ValueError, TypeError): raise ValueError(f'Valor no numérico en {nombre}')
            if not math.isfinite(valores[nombre]): raise ValueError(f'Valor no finito en {nombre}')
    # Solo corruption tiene un nulo conocido en el conjunto suministrado.
    faltantes = [x for x in artefacto['num'] if math.isnan(valores[x]) and x != 'corruption']
    if faltantes: raise ValueError(f'Faltan variables necesarias: {faltantes}')
    rid = meta.get('record_id')
    if not presente(rid): raise ValueError('Falta record_id')
    return {'record_id': str(rid), 'country': pais, 'year': anio,
            'region': region, 'score_real': valores['score'],
            'features': {k: valores[k] for k in artefacto['features']}}
