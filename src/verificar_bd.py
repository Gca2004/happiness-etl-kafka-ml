"""Resumen verificable de los resultados guardados en SQLite."""
import argparse
import sqlite3
from pathlib import Path

def main():
    p = argparse.ArgumentParser(); p.add_argument('--db', type=Path, default=Path('data/predicciones.sqlite'))
    a = p.parse_args()
    if not a.db.is_file(): p.error('No existe la base de datos. Ejecuta primero stream_predict.')
    con = sqlite3.connect(a.db)
    try:
        for title, query in [
            ('Predicciones por origen', 'SELECT source, COUNT(*) FROM predicciones GROUP BY source'),
            ('Registros por año', 'SELECT source, year, COUNT(*) FROM predicciones GROUP BY source, year ORDER BY source, year'),
            ('Record ID duplicados por origen', 'SELECT source, COUNT(*)-COUNT(DISTINCT record_id) FROM predicciones GROUP BY source'),
            ('Errores por origen', 'SELECT source, COUNT(*) FROM errores GROUP BY source'),
            ('Primeras cinco predicciones', 'SELECT source, country, year, score_real, ROUND(score_predicho, 3) FROM predicciones LIMIT 5')]:
            print('\n' + title)
            for fila in con.execute(query): print(fila)
    finally: con.close()
if __name__ == '__main__': main()
