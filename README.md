# DENUE Cuantificador

Este repositorio contiene un script en Python para cuantificar actividades económicas por municipio y estrato, utilizando la API de DENUE (Directorio Estadístico Nacional de Unidades Económicas) proporcionada por INEGI.

---

## Características

* Obtiene códigos de actividad económica (2 dígitos) o usa una lista específica.
* Permite filtrar por municipios (códigos de 5 dígitos) y estratos (1–7).
* Maneja múltiples tokens con rotación y retry ante errores HTTP.
* Ejecuta las consultas en paralelo con `ThreadPoolExecutor` para acelerar el proceso.
* Genera un archivo CSV con el total de unidades económicas por ramo, estrato y área.

---

## Requisitos

* Python 3.7 o superior
* Paquete `requests`

Puedes instalar la dependencia con:

```bash
pip install requests
```

---

## Estructura de archivos

```text
├── README.md
├── denue_cuantificar.py    # Script principal
├── Municipios.py           # Ejemplo de manejo de municipios (opcional)
├── municipios.txt          # Archivo con códigos de municipio (5 dígitos) uno por línea
├── ramos.txt               # Lista de códigos de sector de acuerdo a la clasificación de NAICS
├── denue_municipal.csv     # CSV de ejemplo generado
├── AGEEML_*.txt            # Municipios en desorden descargados de INEGI
└── requirements.txt        # dependencias
```

---

## Uso

```bash
python denue_cuantificar.py \
  -r 0 \
  -a municipios.txt \
  -e 1,2,3,4,5,6,7 \
  -t token1,token2,token3 \
  -w 50 \
  -o denue_municipal.csv
```

**Parámetros:**

* `-r, --ramos`   : `'0'` para total agregado o lista coma-separada de sectores (p.ej. `11,21,31`).
* `-a, --area`    : Ruta a archivo `.txt` con códigos de municipio o lista `01001,09009`.
* `-e, --estratos`: Estratos (1–7) separados por comas. Por defecto todos (`1,2,3,4,5,6,7`).
* `-t, --tokens`  : Tokens de API separados por comas. Se usan para rotación y retry.
* `-w, --workers` : Número de hilos simultáneos. Por defecto 50.
* `-o, --output`  : Nombre del archivo CSV de salida.

---

## Ejemplo

```bash
python denue_cuantificar.py \
  -r 0 \
  -a municipios.txt \
  -e 3,4 \
  -t uso de token propios \
  -w 20 \
  -o resultados.csv
```

Esto generará `resultados.csv` con el total de unidades económicas para los estratos 3 y 4 en cada municipio listado.

---

## Contribuciones

Si encuentras algún error o quieres mejorar el script, ¡las contribuciones son bienvenidas!

1. Haz un fork del repositorio.
2. Crea una rama para tu feature: `git checkout -b feature/nueva-funcionalidad`.
3. Realiza tus cambios y haz commit.
4. Envía un pull request describiendo tus modificaciones.

---

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.
