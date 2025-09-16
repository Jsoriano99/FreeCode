# DVAG Vermögensberater Scraper

Este repositorio contiene un script en Python pensado para descargar la información
pública de los asesores disponibles en el buscador oficial de DVAG
([https://www.dvag.de/dvag/unsere-vermoegensberater/vermoegensberater-finden.html](https://www.dvag.de/dvag/unsere-vermoegensberater/vermoegensberater-finden.html)).

El objetivo es generar un archivo de Excel con los siguientes campos para cada
asesor:

- Nombre
- Teléfono principal
- Teléfono secundario (si existe)
- Código postal
- Ciudad
- Calle
- Correo electrónico
- URL de la ficha pública (dato auxiliar para control de calidad)

> ⚠️ **Nota importante**: el entorno de ejecución de esta tarea no permite
> acceder a `dvag.de`, por lo que no fue posible ejecutar una extracción
> completa durante el desarrollo. El script está diseñado para funcionar en un
> entorno local o servidor con acceso directo al sitio web.

## Requisitos

1. Python 3.10 o superior.
2. Dependencias listadas en `requirements.txt`.

Instalación de dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uso

El script principal es `dvag_scraper.py`. Ejemplos de ejecución:

```bash
# Descarga completa con configuración por defecto
python dvag_scraper.py --output asesores_dvag.xlsx

# Prueba rápida descargando únicamente 50 perfiles
python dvag_scraper.py --limit 50 --output muestra.xlsx --log-level DEBUG
```

### Parámetros destacados

- `--sitemap`: permite indicar uno o varios sitemaps iniciales distintos al
  índice por defecto (`https://www.dvag.de/sitemap-index.xml`). Se puede repetir
  la opción para añadir múltiples URLs.
- `--limit`: restringe el número máximo de perfiles a procesar (útil para
  pruebas).
- `--max-workers`: número de hilos usados en paralelo.
- `--min-delay` / `--max-delay`: definen la ventana aleatoria de espera en cada
  petición, lo que ayuda a no sobrecargar el sitio.
- `--log-level`: ajusta la verbosidad de los mensajes de log.

El resultado es un archivo de Excel (`.xlsx`) con columnas ordenadas exactamente
como solicitó el cliente. Los valores vacíos se dejan en blanco para facilitar
el postprocesado.

## Funcionamiento interno

1. **Descubrimiento de URLs**: el script parte de uno o varios sitemaps, que se
   expanden recursivamente hasta localizar todas las URLs que contienen el
   fragmento `/vermoegensberater/`.
2. **Descarga y parseo**: cada ficha se descarga de forma paralela (usando un
   `ThreadPoolExecutor`) respetando pequeños retardos aleatorios. Los datos se
   extraen preferentemente de los bloques `JSON-LD` (schema.org). Si faltara
   algún campo, se recurre a los microdatos visibles en la página.
3. **Exportación**: los registros se almacenan en un `DataFrame` de `pandas` y se
   exportan a Excel.

## Buenas prácticas recomendadas

- Ejecutar el script en franjas horarias de baja carga y respetar los retrasos
  configurados.
- Guardar el archivo Excel resultante junto con la fecha de descarga para poder
  repetir la extracción en el futuro si fuera necesario.
- Si se detectan bloqueos por parte del sitio, reducir el número de hilos y
  aumentar los retardos entre peticiones.

## Limitaciones conocidas

- Si DVAG cambia la estructura de sus páginas o deja de exponer los datos en
  sitemaps, será necesario actualizar el script.
- Algunos asesores pueden no publicar correo electrónico o teléfono secundario;
  en ese caso, las celdas correspondientes aparecerán vacías.

## Licencia

Uso interno del cliente.
