#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DENUE Cuantificador: Script para cuantificar actividades económicas
por área geográfica y estrato, y generar un CSV con los resultados.

Requisitos:
    - Python 3.7+
    - requests

Uso:
    python denue_cuantificar.py [-h] [-r RAMOS] [-a AREA] [-e ESTRATOS]
                               [-o OUTPUT] [-w WORKERS] [-t TOKENS]

Ejemplo:
    python denue_cuantificar.py \
       -r 0 \
       -a municipios.txt \
       -e 1,2,3,4,5,6,7 \
       -t token1,token2,token3 \
       -o denue_municipal.csv \
       -w 50  # concurrencia recomendada igual a tokens * 2
"""

import argparse
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, Union
import threading
import queue

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------------

BASE_URL = "https://www.inegi.org.mx/app/api/denue/v1/consulta/Cuantificar"

# Tokens por defecto (puedes pasar otros vía -t)
TOKEN1 = ""
TOKEN2 = ""
TOKEN3 = ""
DEFAULT_TOKENS = [TOKEN1, TOKEN2, TOKEN3]

MAX_RETRIES_PER_TOKEN = 3
TOKEN_TIMEOUT = 60.0

ESTRATOS_FIJOS = [1,2,3,4,5,6,7]
DEFAULT_WORKERS = 50

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Validaciones y utilidades
# -----------------------------------------------------------------------------

def default_estratos() -> str:
    return ','.join(str(e) for e in ESTRATOS_FIJOS)


def validate_actividad(actividad: str) -> bool:
    return actividad == '0' or (actividad.isdigit() and len(actividad) == 2)


def validate_area(area: str) -> bool:
    return area == '0' or (area.isdigit() and len(area) == 5)


def validate_estrato(estrato: Union[int,str], allow_zero: bool=False) -> bool:
    try:
        e = int(estrato)
    except ValueError:
        return False
    if allow_zero and e == 0:
        return True
    return 1 <= e <= 7


def pad_areas(areas: List[str]) -> List[str]:
    out = []
    for a in areas:
        a = a.strip()
        if a == '0':
            out.append('0')
        elif a.isdigit() and len(a) <= 5:
            out.append(a.zfill(5))
        else:
            logger.warning("Formato inválido de área, se ignora: %s", a)
    return list(dict.fromkeys(out))

# -----------------------------------------------------------------------------
# Sesión HTTP con retry
# -----------------------------------------------------------------------------

def create_retry_session(
    total_retries: int=5,
    backoff_factor: float=0.5,
    status_forcelist: Tuple[int,...]=(429,500,502,503,504)
) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# -----------------------------------------------------------------------------
# Rotación de tokens thread-safe
# -----------------------------------------------------------------------------

class TokenManager:
    def __init__(self, tokens: List[str]):
        self.queue = queue.Queue()
        self.errors = {t: 0 for t in tokens}
        for t in tokens:
            self.queue.put(t)

    def get_token(self) -> str:
        return self.queue.get(block=True)

    def release(self, token: str, success: bool=True) -> None:
        if not success:
            self.errors[token] += 1
            if self.errors[token] >= MAX_RETRIES_PER_TOKEN:
                logger.warning("Token %s deshabilitado.", token)
                return
        self.queue.put(token)

# -----------------------------------------------------------------------------
# Cliente DENUE
# -----------------------------------------------------------------------------

class DENUEClient:
    def __init__(self, tokens: List[str]):
        self.token_mgr = TokenManager(tokens)
        self.session = create_retry_session()

    def fetch(
        self,
        actividad: str,
        area: str,
        estrato: int,
        timeout: float=TOKEN_TIMEOUT,
        allow_zero_estrato: bool=False
    ) -> List[Union[List[Any], Dict[str,Any]]]:
        if not (validate_actividad(actividad) and validate_area(area) and validate_estrato(estrato, allow_zero_estrato)):
            logger.error("Parámetros inválidos: %s, %s, %s", actividad, area, estrato)
            return []
        token = self.token_mgr.get_token()
        url = f"{BASE_URL}/{actividad}/{area}/{estrato}/{token}"
        try:
            r = self.session.get(url, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            self.token_mgr.release(token, True)
            return data if isinstance(data, list) else []
        except requests.HTTPError as e:
            code = e.response.status_code
            logger.warning("HTTP %s en %s", code, url)
            self.token_mgr.release(token, success=(code not in (401,403)))
        except Exception as e:
            logger.warning("Error en %s: %s", url, e)
            self.token_mgr.release(token, True)
        return []

    def extract_id(self, record) -> Optional[str]:
        if isinstance(record, (list,tuple)) and record:
            return str(record[0])
        if isinstance(record, dict):
            for k in ('AE','IdActividad','idActividad','IDE_ACTIVIDAD_ECONOMICA','actividad','Id','id'):
                if k in record:
                    return str(record[k])
        return None

    def quantify(self, actividad: str, area: str, estrato: int) -> int:
        data = self.fetch(actividad, area, estrato)
        total = 0
        for item in data:
            try:
                if isinstance(item, (list,tuple)) and len(item) >= 3:
                    total += int(item[2])
                elif isinstance(item, dict):
                    val = item.get('Total') or item.get('total')
                    if val is not None:
                        total += int(val)
            except:
                continue
        return total

    def get_activities(self, ramos_arg: str) -> List[str]:
        """Obtiene lista de códigos de rama (2 dígitos) si se pide '0', o lista dada."""
        ramos_arg = ramos_arg.strip()
        if ramos_arg == '0':
            logger.info("Obteniendo lista de sectores (2 dígitos) desde API...")
            # Se usa actividad='0' y área='0' para obtener catálogo completo
            raw = self.fetch('0', '0', 0, timeout=30.0, allow_zero_estrato=True)
            ids = [self.extract_id(r) for r in raw if self.extract_id(r)]
            sectors = sorted({i for i in ids if i and len(i) == 2}, key=int)
            logger.info("Encontrados %d sectores de 2 dígitos.", len(sectors))
            return sectors
        # Lista explícita de sectores (2 dígitos)
        return [p for p in (x.strip() for x in ramos_arg.split(',')) if validate_actividad(p)]

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Generar CSV con progreso
# -----------------------------------------------------------------------------
def generate_csv(
    client: DENUEClient,
    ramos: List[str],
    estratos: List[int],
    areas: List[str],
    output: str,
    max_workers: int=DEFAULT_WORKERS
) -> None:
    """
    Genera CSV desglosando por ramo, estrato y municipio.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor, \
         open(output, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['ramo', 'estrato', 'total', 'area'])
        # Preparar tareas
        futures = {}
        for ramo in ramos:
            for area in areas:
                for estrato in estratos:
                    future = executor.submit(client.quantify, ramo, area, estrato)
                    futures[future] = (ramo, estrato, area)
        total_tasks = len(futures)
        logger.info("Total tareas a procesar: %d", total_tasks)
        # Procesar resultados conforme completan
        processed = 0
        for future in as_completed(futures):
            ramo, estrato, area = futures[future]
            total = future.result()
            writer.writerow([ramo, estrato, total, area])
            processed += 1
            if processed % 100 == 0 or processed == total_tasks:
                logger.info("Procesadas %d/%d tareas...", processed, total_tasks)
    logger.info("CSV generado en: %s", output)

# -----------------------------------------------------------------------------
# Entrada principal
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Quantifica actividades económicas por municipio y estrato."
    )
    parser.add_argument('-r', '--ramos',
        default='0',
        help="'0' para total agregado o lista coma-separada '11,21,...' para desglose por sector"
    )
    parser.add_argument('-a', '--area',
        default='municipios.txt',
        help="Ruta a archivo municipios.txt (5 dígitos uno por línea) o lista '01001,09009'"
    )
    parser.add_argument('-e', '--estratos',
        default=default_estratos(),
        help="Estratos (1-7) coma-separados"
    )
    parser.add_argument('-t', '--tokens',
        default=','.join(DEFAULT_TOKENS),
        help="Tokens API coma-separados"
    )
    parser.add_argument('-w', '--workers',
        type=int,
        default=DEFAULT_WORKERS,
        help="Hilos simultáneos"
    )
    parser.add_argument('-o', '--output',
        default='denue_municipal.csv',
        help="Archivo CSV de salida"
    )
    args = parser.parse_args()

    tokens = [t.strip() for t in args.tokens.split(',') if t.strip()]
    client = DENUEClient(tokens)

    actividades = client.get_activities(args.ramos)
    if not actividades:
        logger.error("No hay actividades para procesar. Saliendo.")
        exit(1)

    estratos = [int(e) for e in args.estratos.split(',') if validate_estrato(e)] or ESTRATOS_FIJOS

    if args.area.lower().endswith('.txt'):
        with open(args.area, encoding='utf-8') as f:
            raw_areas = [l.strip() for l in f if l.strip()]
    else:
        raw_areas = [x.strip() for x in args.area.split(',') if x.strip()]
    areas = pad_areas(raw_areas)
    if not areas:
        logger.error("No hay áreas municipales válidas. Saliendo.")
        exit(1)
    logger.info("Áreas a procesar: %s", areas)

    generate_csv(client, actividades, estratos, areas, args.output, args.workers)
