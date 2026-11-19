"""Carga idempotente de predicciones en SQLite desde Kafka o CSV local."""
import argparse
import json
import sqlite3
from pathlib import Path
import joblib
import pandas as pd
from src.transform import transformar

ROOT = Path(__file__).resolve().parent.parent

def abrir_db(ruta):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ruta)
    conn.executescript('''
    CREATE TABLE IF NOT EXISTS predicciones (
        source TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
        record_id TEXT NOT NULL, country TEXT NOT NULL, year INTEGER NOT NULL,
        region TEXT NOT NULL, score_real REAL, score_predicho REAL NOT NULL,
        modelo TEXT NOT NULL, PRIMARY KEY(source, partition_id, offset_id));
    CREATE TABLE IF NOT EXISTS errores (
        source TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
        detalle TEXT NOT NULL, PRIMARY KEY(source, partition_id, offset_id));
    ''')
    return conn

def procesar(conn, artefacto, payload, source, partition_id, offset_id):
    clave = (source, int(partition_id), int(offset_id))
    try:
        fila = transformar(payload, artefacto)
        X = pd.DataFrame([fila['features']], columns=artefacto['features'])
        pred = float(artefacto['pipeline'].predict(X)[0])
        score = fila['score_real'] if pd.notna(fila['score_real']) else None
        with conn:
            conn.execute('INSERT OR IGNORE INTO predicciones VALUES (?,?,?,?,?,?,?,?,?,?)',
                (*clave, fila['record_id'], fila['country'], fila['year'], fila['region'], score, pred, artefacto['modelo']))
            conn.execute('DELETE FROM errores WHERE source=? AND partition_id=? AND offset_id=?', clave)
        return True
    except (ValueError, KeyError, TypeError) as e:
        with conn:
            conn.execute('INSERT OR REPLACE INTO errores VALUES (?,?,?,?)', (*clave, str(e)))
        return False

def desde_csv(conn, artefacto, ruta):
    tabla = pd.read_csv(ruta)
    meta = ['source_file', 'year', 'record_id']
    if not set(meta + ['_partition', '_offset']).issubset(tabla): raise ValueError('CSV sin metadatos necesarios')
    cols = [x for x in tabla.columns if x not in meta + ['_partition', '_offset']]
    ok = mal = 0
    for _, row in tabla.iterrows():
        payload = {'metadata': {k: row[k] for k in meta}, 'data': {k: row[k] for k in cols if pd.notna(row[k])}}
        if procesar(conn, artefacto, payload, 'csv-local', row['_partition'], row['_offset']): ok += 1
        else: mal += 1
    return ok, mal

def desde_kafka(conn, artefacto, timeout):
    # Se usa la configuración existente sin modificar los offsets del grupo.
    from confluent_kafka import Consumer, TopicPartition, KafkaException
    from src.config import kafka_conf, TOPIC
    conf = kafka_conf(); conf['enable.auto.commit'] = False
    consumer = Consumer(conf)
    ok = mal = 0
    try:
        md = consumer.list_topics(TOPIC, timeout=15)
        topic = md.topics[TOPIC]
        if topic.error: raise KafkaException(topic.error)
        particiones = []
        limites = {}
        for pid in topic.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, pid), timeout=10)
            particiones.append(TopicPartition(TOPIC, pid, low)); limites[pid] = high
            print(f'Partición {pid}: inicio={low}, límite inicial={high}')
        # assign evita depender de offsets previamente confirmados y no hace commit.
        consumer.assign(particiones)
        pendientes = {p.partition for p in particiones if p.offset < limites[p.partition]}
        while pendientes:
            msg = consumer.poll(timeout)
            if msg is None:
                print('Tiempo de espera agotado; la lectura puede estar incompleta.')
                break
            if msg.error(): raise KafkaException(msg.error())
            pid, off = msg.partition(), msg.offset()
            if off >= limites[pid]:
                pendientes.discard(pid); continue
            try: payload = json.loads(msg.value().decode('utf-8'))
            except (ValueError, UnicodeDecodeError, AttributeError): payload = None
            if procesar(conn, artefacto, payload, 'kafka:' + TOPIC, pid, off): ok += 1
            else: mal += 1
            if off + 1 >= limites[pid]: pendientes.discard(pid)
        if pendientes: print('ADVERTENCIA: particiones sin completar:', sorted(pendientes))
        return ok, mal, not pendientes
    finally: consumer.close()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    origen = p.add_mutually_exclusive_group(required=True)
    origen.add_argument('--csv', type=Path, help='Archivo local para prueba sin Kafka')
    origen.add_argument('--kafka', action='store_true', help='Leer una instantánea del tópico desde el inicio, sin hacer commit')
    p.add_argument('--modelo', type=Path, default=ROOT/'models/model.joblib')
    p.add_argument('--db', type=Path, default=ROOT/'data/predicciones.sqlite')
    p.add_argument('--timeout', type=float, default=20)
    args = p.parse_args()
    if not args.modelo.is_file(): p.error(f'No existe {args.modelo}; ejecuta primero los dos notebooks')
    artefacto = joblib.load(args.modelo)
    conn = abrir_db(args.db)
    try:
        if args.csv: ok, mal = desde_csv(conn, artefacto, args.csv); completo = True
        else: ok, mal, completo = desde_kafka(conn, artefacto, args.timeout)
        print(f'Procesados correctamente en esta ejecución: {ok}; errores: {mal}')
        for tabla in ('predicciones', 'errores'):
            print(f'Filas en {tabla}: {conn.execute("SELECT COUNT(*) FROM " + tabla).fetchone()[0]}')
        print('Base de datos:', args.db)
        if mal or not completo: raise SystemExit('Revisa errores o lectura incompleta antes de presentar resultados.')
    finally: conn.close()

if __name__ == '__main__': main()
