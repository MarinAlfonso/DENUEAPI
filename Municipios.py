#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera un listado de claves municipales (CVEGEO) a partir de un archivo AGEEML.
Cumple con PEP 8.
"""

import csv

INPUT_FILE = 'AGEEML_2025414144304.txt'
OUTPUT_FILE = 'municipios.txt'


def main():
    """Extrae claves municipales de un TSV y las escribe en un TXT."""
    count = 0

    with open(
        INPUT_FILE,
        encoding='latin-1',
    ) as src, open(
        OUTPUT_FILE,
        mode='w',
        encoding='utf-8',
    ) as dst:
        reader = csv.DictReader(src, delimiter='\t')

        for row in reader:
            clave = row.get('CVEGEO', '').strip().strip('"')
            if len(clave) == 5 and clave.isdigit():
                dst.write(f'{clave}\n')
                count += 1

    print(f"Generado '{OUTPUT_FILE}' con {count} municipios.")


if __name__ == '__main__':
    main()
