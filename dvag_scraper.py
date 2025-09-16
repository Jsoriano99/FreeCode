#!/usr/bin/env python3
"""Herramienta para extraer fichas públicas de asesores en dvag.de."""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import random
import threading
import time
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional, Sequence, Set
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger("dvag_scraper")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"\
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
PROFILE_FRAGMENT = "/vermoegensberater/"
THREAD_LOCAL = threading.local()


@dataclass
class Profile:
    """Estructura de datos para almacenar la ficha de un asesor."""

    name: Optional[str] = None
    phone: Optional[str] = None
    phone2: Optional[str] = None
    zip_code: Optional[str] = None
    city: Optional[str] = None
    street: Optional[str] = None
    email: Optional[str] = None
    profile_url: Optional[str] = None

    def to_ordered_dict(self) -> Dict[str, Optional[str]]:
        """Devuelve los datos en el orden deseado para el Excel."""

        return {
            "Name": self.name,
            "Phone": self.phone,
            "Phone 2": self.phone2,
            "ZIP": self.zip_code,
            "City": self.city,
            "Street": self.street,
            "Email": self.email,
            "Profile URL": self.profile_url,
        }


def get_thread_session() -> requests.Session:
    """Obtiene (o crea) una sesión por hilo para las descargas."""

    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        THREAD_LOCAL.session = session
    return session


def fetch_response(url: str, timeout: int = 60) -> requests.Response:
    """Descarga una URL utilizando la sesión por hilo."""

    session = get_thread_session()
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response


def _read_sitemap_content(response: requests.Response) -> str:
    """Decodifica el contenido de un sitemap, manejando gzip si procede."""

    content = response.content
    if response.headers.get("Content-Encoding", "").lower() == "gzip" or response.url.endswith(".gz"):
        content = gzip.decompress(content)
    return content.decode(response.encoding or "utf-8", errors="replace")


def extract_loc_values(xml_text: str) -> List[str]:
    """Extrae los valores de <loc> de un XML de sitemap."""

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        LOGGER.warning("No se pudo interpretar el sitemap: %s", exc)
        return []

    loc_elements = root.findall(f".//{SITEMAP_NS}loc")
    if not loc_elements:
        loc_elements = root.findall(".//loc")

    locs: List[str] = []
    for element in loc_elements:
        if element.text:
            locs.append(element.text.strip())
    return locs


def expand_sitemap(url: str, seen: Optional[Set[str]] = None) -> List[str]:
    """Expande recursivamente un sitemap y devuelve URLs de perfiles."""

    seen = seen or set()
    if url in seen:
        return []
    seen.add(url)

    LOGGER.debug("Descargando sitemap: %s", url)
    response = fetch_response(url, timeout=90)
    xml_text = _read_sitemap_content(response)
    locs = extract_loc_values(xml_text)

    profile_urls: List[str] = []
    for loc in locs:
        if not loc:
            continue
        lowered = loc.lower()
        if lowered.endswith(".xml") or lowered.endswith(".xml.gz"):
            profile_urls.extend(expand_sitemap(loc, seen))
            continue
        if PROFILE_FRAGMENT in lowered:
            profile_urls.append(loc)
    return profile_urls


def collect_profile_urls(sitemap_urls: Sequence[str]) -> List[str]:
    """Agrupa y deduplica todas las URLs de perfil detectadas."""

    urls: Set[str] = set()
    for sitemap_url in sitemap_urls:
        try:
            for url in expand_sitemap(sitemap_url):
                urls.add(url)
        except Exception as exc:  # pragma: no cover - resiliencia frente a fallos remotos
            LOGGER.warning("Fallo al procesar %s: %s", sitemap_url, exc)
    ordered = sorted(urls)
    LOGGER.info("Total de perfiles detectados: %d", len(ordered))
    return ordered


def _ensure_list(value) -> List:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def extract_from_schema(item: Dict) -> Profile:
    """Genera un perfil a partir de un bloque JSON-LD."""

    result = Profile()

    result.name = _clean_text(item.get("name"))

    telephones: List[str] = []
    for phone in _ensure_list(item.get("telephone")):
        phone = _clean_text(phone)
        if phone and phone not in telephones:
            telephones.append(phone)

    for contact in _ensure_list(item.get("contactPoint")):
        if isinstance(contact, dict):
            phone = _clean_text(contact.get("telephone"))
            if phone and phone not in telephones:
                telephones.append(phone)
            email = _clean_text(contact.get("email"))
            if email and not result.email:
                result.email = email

    if telephones:
        result.phone = telephones[0]
        if len(telephones) > 1:
            result.phone2 = telephones[1]

    address = item.get("address")
    if isinstance(address, dict):
        result.street = _clean_text(address.get("streetAddress"))
        result.zip_code = _clean_text(address.get("postalCode"))
        result.city = _clean_text(address.get("addressLocality"))

    if not result.email:
        result.email = _clean_text(item.get("email"))

    return result


def _iter_schema_candidates(soup: BeautifulSoup) -> Iterable[Dict]:
    """Genera bloques JSON-LD potencialmente útiles."""

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string or script.get_text()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        items = _ensure_list(payload)
        for item in items:
            if not isinstance(item, dict):
                continue
            types = _ensure_list(item.get("@type"))
            types_lower = {str(t).lower() for t in types}
            if types_lower & {"person", "financialservice", "localbusiness", "professionalservice"}:
                yield item


def extract_microdata(soup: BeautifulSoup) -> Profile:
    """Extrae datos usando microdatos HTML como reserva."""

    profile = Profile()

    name_tag = soup.find(attrs={"itemprop": "name"})
    if name_tag:
        profile.name = _clean_text(name_tag.get_text())

    phones = []
    for phone_tag in soup.find_all(attrs={"itemprop": "telephone"}):
        text = _clean_text(phone_tag.get_text())
        if text and text not in phones:
            phones.append(text)
    if phones:
        profile.phone = phones[0]
        if len(phones) > 1:
            profile.phone2 = phones[1]

    email_tag = soup.find("a", href=lambda href: isinstance(href, str) and href.lower().startswith("mailto:"))
    if email_tag and email_tag.get("href"):
        profile.email = _clean_text(email_tag["href"].split(":", 1)[-1])

    street_tag = soup.find(attrs={"itemprop": "streetAddress"})
    if street_tag:
        profile.street = _clean_text(street_tag.get_text())

    zip_tag = soup.find(attrs={"itemprop": "postalCode"})
    if zip_tag:
        profile.zip_code = _clean_text(zip_tag.get_text())

    city_tag = soup.find(attrs={"itemprop": "addressLocality"})
    if city_tag:
        profile.city = _clean_text(city_tag.get_text())

    return profile


def merge_profiles(base: Profile, extra: Profile) -> Profile:
    """Combina dos perfiles dando prioridad a los datos existentes."""

    for field_name, value in asdict(extra).items():
        if value and not getattr(base, field_name):
            setattr(base, field_name, value)
    return base


def parse_profile_page(html: str, url: str) -> Profile:
    """Interpreta una ficha individual."""

    soup = BeautifulSoup(html, "html.parser")
    profile = Profile(profile_url=url)

    for schema in _iter_schema_candidates(soup):
        merge_profiles(profile, extract_from_schema(schema))

    if not profile.name or not profile.phone:
        merge_profiles(profile, extract_microdata(soup))

    return profile


def fetch_profile(url: str, delay_range: Sequence[float]) -> Optional[Profile]:
    """Descarga y procesa una ficha individual con control de errores."""

    min_delay, max_delay = delay_range
    if max_delay > 0:
        sleep_time = random.uniform(min_delay, max_delay)
        LOGGER.debug("Esperando %.2f s antes de solicitar %s", sleep_time, url)
        time.sleep(sleep_time)

    try:
        response = fetch_response(url)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response else "?"
        LOGGER.warning("Error HTTP %s en %s", status, url)
        return None
    except requests.RequestException as exc:
        LOGGER.warning("Fallo de red en %s: %s", url, exc)
        return None

    profile = parse_profile_page(response.text, url)
    if not any([profile.name, profile.phone, profile.email]):
        LOGGER.debug("Ficha vacía en %s", url)
    return profile


def export_profiles(profiles: List[Profile], output_path: str) -> None:
    """Genera el Excel con los perfiles."""

    rows = [profile.to_ordered_dict() for profile in profiles]
    df = pd.DataFrame(rows, columns=["Name", "Phone", "Phone 2", "ZIP", "City", "Street", "Email", "Profile URL"])
    df.to_excel(output_path, index=False)
    LOGGER.info("Archivo Excel generado: %s", output_path)


def positive_float(value: str) -> float:
    val = float(value)
    if val < 0:
        raise argparse.ArgumentTypeError("El valor debe ser positivo")
    return val


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Descarga todas las fichas públicas de asesores DVAG y "
            "genera un Excel con los datos clave (nombre, teléfonos, "
            "dirección y correo electrónico)."
        )
    )
    parser.add_argument(
        "--sitemap",
        action="append",
        dest="sitemaps",
        default=["https://www.dvag.de/sitemap-index.xml"],
        help="URL(s) de sitemap iniciales. Se puede repetir la opción para múltiples orígenes.",
    )
    parser.add_argument(
        "--output",
        default="dvag_vermoegensberater.xlsx",
        help="Ruta del archivo Excel de salida.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Número máximo de perfiles a procesar (útil para pruebas).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Número de hilos simultáneos para descargar fichas.",
    )
    parser.add_argument(
        "--min-delay",
        type=positive_float,
        default=0.3,
        help="Retraso mínimo (en segundos) entre descargas para cada hilo.",
    )
    parser.add_argument(
        "--max-delay",
        type=positive_float,
        default=0.8,
        help="Retraso máximo (en segundos) entre descargas para cada hilo.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Nivel de detalle en los mensajes de log.",
    )
    return parser


def main() -> None:
    parser = create_argument_parser()
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s - %(levelname)s - %(message)s")

    if args.min_delay > args.max_delay:
        parser.error("--min-delay no puede ser mayor que --max-delay")

    LOGGER.info("Sitemaps iniciales: %s", ", ".join(args.sitemaps))
    profiles_urls = collect_profile_urls(args.sitemaps)
    if args.limit is not None:
        profiles_urls = profiles_urls[: args.limit]
        LOGGER.info("Procesando solo %d perfiles por --limit", len(profiles_urls))

    if not profiles_urls:
        LOGGER.error("No se detectaron URLs de perfiles. Revise los sitemaps proporcionados.")
        return

    LOGGER.info("Comenzando la descarga de %d perfiles", len(profiles_urls))

    profiles: List[Profile] = []
    from concurrent.futures import ThreadPoolExecutor, as_completed

    delay_range = (args.min_delay, args.max_delay)
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(fetch_profile, url, delay_range): url for url in profiles_urls}
        for index, future in enumerate(as_completed(futures), 1):
            url = futures[future]
            try:
                profile = future.result()
            except Exception as exc:  # pragma: no cover - fallos inesperados
                LOGGER.warning("Error no controlado en %s: %s", url, exc)
                continue
            if profile:
                profiles.append(profile)
            if index % 100 == 0:
                LOGGER.info("Perfiles procesados: %d", index)

    if not profiles:
        LOGGER.error("No se pudo obtener ninguna ficha. Revise los logs para más detalles.")
        return

    export_profiles(profiles, args.output)


if __name__ == "__main__":
    main()
