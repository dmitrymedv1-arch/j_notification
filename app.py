import streamlit as st
import requests
import pandas as pd
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import json
import asyncio
import aiohttp
import time
import sqlite3
import os
from pathlib import Path
import hashlib
import joblib
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ratelimit import limits, sleep_and_retry
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from typing import List, Dict, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import io
from reportlab.lib.pagesizes import A4, letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch, cm
from reportlab.pdfgen import canvas
from reportlab.platypus import Image
from reportlab.platypus.flowables import Flowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import Image
from reportlab.platypus import KeepTogether
import xlsxwriter
from PIL import Image as PILImage
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройки приложения
st.set_page_config(
    page_title="Journal Article Analyzer Pro",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Кастомные стили
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.8rem;
    }
    
    .step-card {
        background: linear-gradient(135deg, #667eea15 0%, #764ba215 100%);
        border-radius: 12px;
        padding: 18px;
        border-left: 4px solid #667eea;
        margin-bottom: 15px;
        box-shadow: 0 3px 5px rgba(0, 0, 0, 0.04);
    }
    
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 3px 10px rgba(0, 0, 0, 0.06);
        border: 1px solid #e0e0e0;
        height: 100%;
        min-height: 90px;
    }
    
    .result-card {
        background: white;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 12px;
        border-left: 3px solid #4CAF50;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05);
    }
    
    .topic-card {
        background: white;
        border-radius: 8px;
        padding: 12px;
        margin-bottom: 8px;
        border: 1px solid #e0e0e0;
        cursor: pointer;
        transition: all 0.2s ease;
    }
    
    .filter-section {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 12px;
        padding: 15px;
        margin-bottom: 15px;
        border: 1px solid #dee2e6;
    }
    
    .language-selector {
        position: fixed;
        top: 10px;
        right: 20px;
        z-index: 1000;
        background: white;
        padding: 5px 10px;
        border-radius: 20px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# МУЛЬТИЯЗЫЧНАЯ ПОДДЕРЖКА
# ============================================================================

LANGUAGES = {
    'en': {
        'app_title': '📚 Journal Article Analyzer Pro',
        'app_subtitle': 'Analyze journal articles by research topics with citation metrics',
        'step1_title': 'Step 1: Journal Information',
        'step1_desc': 'Enter journal ISSN and upload logo (optional)',
        'issn_label': 'Journal ISSN',
        'issn_placeholder': 'Example: 1234-5678 or 12345678 or 1234 5678',
        'logo_label': 'Journal Logo (Optional)',
        'logo_help': 'Upload logo image (PNG, JPG) - will appear on PDF cover page',
        'next_btn': 'Next →',
        'step2_title': 'Step 2: Select Publication Years',
        'step2_desc': 'Choose year range for analysis',
        'years_label': 'Publication Years',
        'years_help': 'Format: 2021 or 2021,2023-2025 or 2023-2026',
        'analyze_btn': '🔍 Analyze Journal',
        'step3_title': 'Step 3: Analysis Results',
        'step3_desc': 'Articles grouped by research topics',
        'total_articles': 'Total Articles',
        'total_topics': 'Research Topics',
        'avg_citations': 'Avg Citations',
        'highly_cited': 'Highly Cited',
        'citations_badge': '🔥 Highly Cited',
        'citations_tooltip': '>10 total citations OR >5 citations per year',
        'export_btn': '📥 Export Reports',
        'new_analysis_btn': '🔄 New Analysis',
        'journal_not_found': '❌ Journal not found. Please check ISSN.',
        'no_articles': '❌ No articles found for selected period.',
        'loading_journal': 'Searching for journal...',
        'loading_articles': 'Loading articles from OpenAlex...',
        'analyzing': 'Grouping by research topics...',
        'topic': 'Topic',
        'articles_count': 'articles',
        'citations': 'Citations',
        'citations_per_year': 'per year',
        'authors': 'Authors',
        'title': 'Title',
        'journal': 'Journal',
        'year': 'Year',
        'volume': 'Volume',
        'issue': 'Issue',
        'pages': 'Pages',
        'doi': 'DOI',
        'view_article': 'View Article'
    },
    'ru': {
        'app_title': '📚 Анализатор статей журнала Pro',
        'app_subtitle': 'Анализ статей журнала по исследовательским темам с метриками цитирования',
        'step1_title': 'Шаг 1: Информация о журнале',
        'step1_desc': 'Введите ISSN журнала и загрузите логотип (опционально)',
        'issn_label': 'ISSN журнала',
        'issn_placeholder': 'Пример: 1234-5678 или 12345678 или 1234 5678',
        'logo_label': 'Логотип журнала (опционально)',
        'logo_help': 'Загрузите изображение логотипа (PNG, JPG) - появится на обложке PDF',
        'next_btn': 'Далее →',
        'step2_title': 'Шаг 2: Выбор годов публикации',
        'step2_desc': 'Выберите период для анализа',
        'years_label': 'Годы публикации',
        'years_help': 'Формат: 2021 или 2021,2023-2025 или 2023-2026',
        'analyze_btn': '🔍 Анализировать журнал',
        'step3_title': 'Шаг 3: Результаты анализа',
        'step3_desc': 'Статьи сгруппированы по исследовательским темам',
        'total_articles': 'Всего статей',
        'total_topics': 'Тем исследований',
        'avg_citations': 'Среднее цитирований',
        'highly_cited': 'Активно цитируемые',
        'citations_badge': '🔥 Активно цитируемая',
        'citations_tooltip': '>10 всего цитирований ИЛИ >5 цитирований в год',
        'export_btn': '📥 Экспорт отчетов',
        'new_analysis_btn': '🔄 Новый анализ',
        'journal_not_found': '❌ Журнал не найден. Проверьте ISSN.',
        'no_articles': '❌ Статьи не найдены за выбранный период.',
        'loading_journal': 'Поиск журнала...',
        'loading_articles': 'Загрузка статей из OpenAlex...',
        'analyzing': 'Группировка по исследовательским темам...',
        'topic': 'Тема',
        'articles_count': 'статей',
        'citations': 'Цитирований',
        'citations_per_year': 'в год',
        'authors': 'Авторы',
        'title': 'Название',
        'journal': 'Журнал',
        'year': 'Год',
        'volume': 'Том',
        'issue': 'Выпуск',
        'pages': 'Страницы',
        'doi': 'DOI',
        'view_article': 'Смотреть статью'
    }
}

# ============================================================================
# КОНФИГУРАЦИЯ OPENALEX API
# ============================================================================

OPENALEX_BASE_URL = "https://api.openalex.org"
MAILTO = "your-email@example.com"
POLITE_POOL_HEADER = {'User-Agent': f'JournalAnalyzer (mailto:{MAILTO})'}

RATE_LIMIT_PER_SECOND = 8
BATCH_SIZE = 50
CURSOR_PAGE_SIZE = 200
MAX_WORKERS_ASYNC = 3
MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 60

CACHE_DIR = Path("./cache")
CACHE_DB = CACHE_DIR / "openalex_cache.db"
CACHE_EXPIRY_DAYS = 30

CACHE_DIR.mkdir(exist_ok=True)

# ============================================================================
# КЭШИРОВАНИЕ SQLITE
# ============================================================================

def init_cache_db():
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS works_cache (
            doi TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sources_cache (
            issn TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS source_works_cache (
            source_id TEXT,
            year_filter TEXT,
            data TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME,
            PRIMARY KEY (source_id, year_filter)
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_works_expires ON works_cache(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sources_expires ON sources_cache(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_works_expires ON source_works_cache(expires_at)')
    
    conn.commit()
    conn.close()

def get_cache_connection():
    init_cache_db()
    return sqlite3.connect(CACHE_DB, check_same_thread=False)

def cache_work(doi: str, data: dict):
    conn = get_cache_connection()
    cursor = conn.cursor()
    expires_at = datetime.now() + timedelta(days=CACHE_EXPIRY_DAYS)
    cursor.execute('''
        INSERT OR REPLACE INTO works_cache (doi, data, expires_at)
        VALUES (?, ?, ?)
    ''', (doi, json.dumps(data), expires_at))
    conn.commit()
    conn.close()

def get_cached_work(doi: str) -> Optional[dict]:
    conn = get_cache_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT data FROM works_cache 
        WHERE doi = ? AND (expires_at IS NULL OR expires_at > ?)
    ''', (doi, datetime.now()))
    result = cursor.fetchone()
    conn.close()
    if result:
        return json.loads(result[0])
    return None

def cache_source(issn: str, data: dict):
    conn = get_cache_connection()
    cursor = conn.cursor()
    expires_at = datetime.now() + timedelta(days=30)
    cursor.execute('''
        INSERT OR REPLACE INTO sources_cache (issn, data, expires_at)
        VALUES (?, ?, ?)
    ''', (issn, json.dumps(data), expires_at))
    conn.commit()
    conn.close()

def get_cached_source(issn: str) -> Optional[dict]:
    conn = get_cache_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT data FROM sources_cache 
        WHERE issn = ? AND (expires_at IS NULL OR expires_at > ?)
    ''', (issn, datetime.now()))
    result = cursor.fetchone()
    conn.close()
    if result:
        return json.loads(result[0])
    return None

def cache_source_works(source_id: str, year_filter: str, data: dict):
    conn = get_cache_connection()
    cursor = conn.cursor()
    expires_at = datetime.now() + timedelta(days=7)
    cursor.execute('''
        INSERT OR REPLACE INTO source_works_cache (source_id, year_filter, data, expires_at)
        VALUES (?, ?, ?, ?)
    ''', (source_id, year_filter, json.dumps(data), expires_at))
    conn.commit()
    conn.close()

def get_cached_source_works(source_id: str, year_filter: str) -> Optional[dict]:
    conn = get_cache_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT data FROM source_works_cache 
        WHERE source_id = ? AND year_filter = ? 
        AND (expires_at IS NULL OR expires_at > ?)
    ''', (source_id, year_filter, datetime.now()))
    result = cursor.fetchone()
    conn.close()
    if result:
        return json.loads(result[0])
    return None

def clear_old_cache():
    conn = get_cache_connection()
    cursor = conn.cursor()
    now_str = datetime.now().isoformat(' ', 'seconds')
    cursor.execute('DELETE FROM works_cache WHERE expires_at <= ?', (now_str,))
    cursor.execute('DELETE FROM sources_cache WHERE expires_at <= ?', (now_str,))
    cursor.execute('DELETE FROM source_works_cache WHERE expires_at <= ?', (now_str,))
    conn.commit()
    conn.close()

# ============================================================================
# ПАРСИНГ ISSN
# ============================================================================

def parse_issn(issn_input: str) -> Optional[str]:
    """
    Парсит ISSN из различных форматов:
    - "1234-5678" -> "12345678"
    - "1234 5678" -> "12345678"
    - "12345678" -> "12345678"
    - "ISSN 1234-5678" -> "12345678"
    """
    if not issn_input:
        return None
    
    # Удаляем префикс ISSN если есть
    issn_clean = re.sub(r'^ISSN\s*', '', issn_input, flags=re.IGNORECASE)
    
    # Оставляем только цифры
    digits = re.sub(r'[^0-9]', '', issn_clean)
    
    # ISSN должен быть 8 цифр
    if len(digits) == 8:
        return digits
    elif len(digits) == 7:
        # Возможно пропущена контрольная цифра
        logger.warning(f"ISSN has 7 digits: {digits}")
        return None
    
    return None

# ============================================================================
# ПОИСК ЖУРНАЛА В OPENALEX
# ============================================================================

def get_journal_by_issn(issn: str) -> Optional[dict]:
    """
    Поиск журнала в OpenAlex по ISSN.
    """
    # Проверяем кэш
    cached = get_cached_source(issn)
    if cached:
        logger.info(f"Using cached journal data for ISSN {issn}")
        return cached
    
    # Приводим ISSN к формату XXXX-XXXX для OpenAlex
    issn_clean = re.sub(r'[^0-9X]', '', issn.upper())
    if len(issn_clean) == 8:
        issn_formatted = f"{issn_clean[:4]}-{issn_clean[4:]}"
    else:
        issn_formatted = issn
    
    logger.info(f"Searching for journal with ISSN {issn_formatted}")
    
    try:
        # OpenAlex использует ISSN-L или обычный ISSN
        url = f"{OPENALEX_BASE_URL}/sources"
        params = {
            "filter": f"issn:{issn_formatted}",
            "mailto": MAILTO
        }
        
        response = requests.get(url, params=params, headers=POLITE_POOL_HEADER, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            if results:
                source = results[0]
                cache_source(issn, source)
                logger.info(f"Found journal: {source.get('display_name')}")
                return source
            else:
                # Пробуем поискать через primary_location.source.issn в works
                logger.warning(f"No journal found for ISSN {issn_formatted}, trying alternative search...")
                alt_url = f"{OPENALEX_BASE_URL}/works"
                alt_params = {
                    "filter": f"primary_location.source.issn:{issn_formatted}",
                    "per-page": 1,
                    "mailto": MAILTO
                }
                alt_response = requests.get(alt_url, params=alt_params, headers=POLITE_POOL_HEADER, timeout=30)
                
                if alt_response.status_code == 200:
                    alt_data = alt_response.json()
                    if alt_data.get('results'):
                        # Извлекаем информацию о журнале из первой работы
                        first_work = alt_data['results'][0]
                        primary_location = first_work.get('primary_location', {})
                        source = primary_location.get('source', {})
                        if source:
                            cache_source(issn, source)
                            logger.info(f"Found journal via alternative method: {source.get('display_name')}")
                            return source
                
                logger.warning(f"No journal found for ISSN {issn_formatted}")
                return None
        else:
            logger.error(f"Error fetching journal: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error in get_journal_by_issn: {str(e)}")
        return None

# ============================================================================
# ЗАГРУЗКА СТАТЕЙ ЖУРНАЛА
# ============================================================================

def parse_year_filter(year_input: str) -> List[int]:
    """
    Парсит строку фильтра годов.
    Примеры:
    "2021" -> [2021]
    "2021,2023-2025" -> [2021, 2023, 2024, 2025]
    "2023-2026" -> [2023, 2024, 2025, 2026]
    """
    years = set()
    
    if not year_input or year_input.strip() == "":
        current_year = datetime.now().year
        return [current_year - 2, current_year - 1, current_year]
    
    parts = year_input.split(',')
    
    for part in parts:
        part = part.strip()
        if '-' in part:
            try:
                start, end = part.split('-')
                start_year = int(start.strip())
                end_year = int(end.strip())
                for year in range(start_year, end_year + 1):
                    if 1900 <= year <= 2100:
                        years.add(year)
            except ValueError:
                logger.warning(f"Could not parse range: {part}")
        else:
            try:
                year = int(part)
                if 1900 <= year <= 2100:
                    years.add(year)
            except ValueError:
                logger.warning(f"Could not parse year: {part}")
    
    return sorted(list(years))

def format_year_filter_for_filename(years: List[int]) -> str:
    """
    Форматирует список годов для имени файла.
    [2021, 2023, 2024, 2025] -> "2021,2023-2025"
    """
    if not years:
        return ""
    
    years.sort()
    ranges = []
    start = years[0]
    end = years[0]
    
    for i in range(1, len(years)):
        if years[i] == end + 1:
            end = years[i]
        else:
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = years[i]
            end = years[i]
    
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")
    
    return ",".join(ranges)

def fetch_articles_by_journal(source_id: str, years: List[int], progress_callback=None) -> List[dict]:
    """
    Загружает все статьи журнала за указанные годы.
    """
    year_filter_str = ",".join(map(str, years))
    cache_key = f"{source_id}_{year_filter_str}"
    
    # Проверяем кэш
    cached = get_cached_source_works(source_id, year_filter_str)
    if cached:
        logger.info(f"Using cached articles for {source_id}, years {years}")
        return cached.get('articles', [])
    
    logger.info(f"Fetching articles for source {source_id}, years {years}")
    
    all_articles = []
    cursor = "*"
    page_count = 0
    total_count = 0
    
    # Используем более надежный фильтр через primary_location.source.id
    years_str = "|".join(map(str, years))
    filter_str = f"primary_location.source.id:{source_id},publication_year:{years_str}"
    
    try:
        while True:
            page_count += 1
            
            params = {
                "filter": filter_str,
                "per-page": CURSOR_PAGE_SIZE,
                "cursor": cursor,
                "mailto": MAILTO,
                "sort": "publication_date:desc"
            }
            
            url = f"{OPENALEX_BASE_URL}/works"
            response = requests.get(url, params=params, headers=POLITE_POOL_HEADER, timeout=60)
            
            if response.status_code != 200:
                logger.error(f"Error fetching articles: {response.status_code}")
                break
            
            data = response.json()
            
            if page_count == 1:
                total_count = data.get('meta', {}).get('count', 0)
                logger.info(f"Total articles found: {total_count}")
                
                if total_count == 0:
                    return []
            
            articles = data.get('results', [])
            if not articles:
                break
            
            all_articles.extend(articles)
            
            if progress_callback and total_count > 0:
                progress = min(len(all_articles) / total_count, 1.0)
                progress_callback(progress, len(all_articles), page_count, total_count)
            
            logger.info(f"Page {page_count}: got {len(articles)} articles, total: {len(all_articles)}/{total_count}")
            
            next_cursor = data.get('meta', {}).get('next_cursor')
            if not next_cursor:
                break
            
            cursor = next_cursor
            time.sleep(0.1)
        
        # Сохраняем в кэш
        if all_articles:
            cache_data = {
                'articles': all_articles,
                'total_count': total_count,
                'years': years,
                'timestamp': datetime.now().isoformat()
            }
            cache_source_works(source_id, year_filter_str, cache_data)
        
        return all_articles
        
    except Exception as e:
        logger.error(f"Error in fetch_articles_by_journal: {str(e)}")
        return all_articles

# ============================================================================
# РАСЧЕТ МЕТРИК ЦИТИРОВАНИЯ
# ============================================================================

def calculate_citation_activity(work: dict, current_year: int = None) -> Tuple[int, float, bool]:
    """
    Рассчитывает метрики цитирования для статьи.
    
    Returns:
        Tuple[citations_total, citations_per_year, is_highly_cited]
    """
    citations_total = work.get('cited_by_count', 0)
    
    publication_year = work.get('publication_year', 0)
    if current_year is None:
        current_year = datetime.now().year
    
    # Возраст статьи в годах (минимум 1 год)
    age = max(1, current_year - publication_year) if publication_year > 0 else 1
    
    citations_per_year = citations_total / age
    
    # Активно-цитируемая: >10 всего ИЛИ >5 в год
    is_highly_cited = (citations_total > 10) or (citations_per_year > 5)
    
    return citations_total, citations_per_year, is_highly_cited

# ============================================================================
# ОБОГАЩЕНИЕ ДАННЫХ СТАТЬИ
# ============================================================================

def enrich_article_data(article: dict) -> dict:
    """
    Обогащает данные статьи полной информацией.
    """
    if not article:
        return {}
    
    doi_raw = article.get('doi')
    doi_clean = ''
    if doi_raw:
        doi_clean = str(doi_raw).replace('https://doi.org/', '')
    
    # Извлекаем информацию о публикации
    biblio = article.get('biblio', {})
    volume = biblio.get('volume', '')
    issue = biblio.get('issue', '')
    first_page = biblio.get('first_page', '')
    last_page = biblio.get('last_page', '')
    
    # Форматируем страницы
    pages_str = ''
    if first_page and last_page and first_page != last_page:
        pages_str = f"{first_page}-{last_page}"
    elif first_page:
        pages_str = first_page
    elif last_page:
        pages_str = last_page
    
    # Извлекаем авторов с правильной обработкой кириллицы
    authorships = article.get('authorships', [])
    authors = []
    
    for authorship in authorships[:10]:  # Максимум 10 авторов
        if authorship:
            # Пробуем несколько вариантов получения имени автора
            author_name = ''
            
            # Вариант 1: raw_author_name (оригинальное написание)
            if 'raw_author_name' in authorship:
                author_name = authorship.get('raw_author_name', '')
            
            # Вариант 2: через author.display_name
            if not author_name:
                author = authorship.get('author', {})
                if author:
                    author_name = author.get('display_name', '')
            
            # Вариант 3: напрямую из author
            if not author_name and 'author' in authorship:
                author_obj = authorship['author']
                if isinstance(author_obj, dict):
                    author_name = author_obj.get('display_name', '')
            
            if author_name:
                # Нормализуем Unicode
                import unicodedata
                author_name = unicodedata.normalize('NFC', str(author_name))
                
                # Очищаем от проблемных символов, но сохраняем кириллицу
                # Разрешенные символы: буквы (рус/англ), пробелы, точки, запятые, дефисы, скобки
                author_name = re.sub(r'[^a-zA-Zа-яА-ЯёЁ\s\.\,\-\'\(\)]', '', author_name)
                
                # Убираем лишние пробелы
                author_name = re.sub(r'\s+', ' ', author_name).strip()
                
                if author_name:
                    authors.append(author_name)
    
    authors_str = ', '.join(authors)
    if len(authorships) > 10:
        authors_str += f" et al. ({len(authorships)} authors total)"
    
    # Получаем тему
    topics = article.get('topics', [])
    primary_topic = ''
    topic_id = ''
    if topics:
        sorted_topics = sorted(topics, key=lambda x: x.get('score', 0) if x else 0, reverse=True)
        primary_topic_obj = sorted_topics[0] if sorted_topics else {}
        primary_topic = primary_topic_obj.get('display_name', '')
        topic_id_raw = primary_topic_obj.get('id', '')
        topic_id = topic_id_raw.split('/')[-1] if topic_id_raw else ''
    
    # Рассчитываем метрики цитирования
    citations_total, citations_per_year, is_highly_cited = calculate_citation_activity(article)
    
    # Получаем информацию об источнике (журнале)
    journal_name = ''
    primary_location = article.get('primary_location')
    if primary_location:
        source = primary_location.get('source', {})
        if source:
            journal_name = source.get('display_name', '')
            if not journal_name:
                host_venue = article.get('host_venue', {})
                journal_name = host_venue.get('display_name', '')
    
    enriched = {
        'doi': doi_clean,
        'doi_url': f"https://doi.org/{doi_clean}" if doi_clean else '',
        'title': article.get('title', ''),
        'publication_year': article.get('publication_year', 0),
        'publication_date': article.get('publication_date', ''),
        'cited_by_count': citations_total,
        'citations_per_year': round(citations_per_year, 1),
        'is_highly_cited': is_highly_cited,
        'authors': authors_str,
        'authors_list': authors,
        'journal_name': journal_name,
        'volume': volume,
        'issue': issue,
        'pages': pages_str,
        'primary_topic': primary_topic,
        'topic_id': topic_id,
        'type': article.get('type', ''),
        'is_oa': article.get('open_access', {}).get('is_oa', False) if article.get('open_access') else False
    }
    
    return enriched

# ============================================================================
# ГРУППИРОВКА СТАТЕЙ ПО ТЕМАМ
# ============================================================================

def group_articles_by_topic(articles: List[dict]) -> Dict[str, List[dict]]:
    """
    Группирует статьи по исследовательским темам.
    """
    grouped = defaultdict(list)
    
    for article in articles:
        enriched = enrich_article_data(article)
        topic = enriched.get('primary_topic', 'Other')
        if not topic:
            topic = 'Other'
        grouped[topic].append(enriched)
    
    # Сортируем темы по количеству статей (убывание)
    sorted_grouped = dict(sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True))
    
    return sorted_grouped

# ============================================================================
# ГЕНЕРАЦИЯ АББРЕВИАТУРЫ ЖУРНАЛА
# ============================================================================

def generate_journal_abbreviation(journal_name: str) -> str:
    """
    Генерирует аббревиатуру из названия журнала.
    Пример: "Journal of Power Sources" -> "JOPS"
    """
    if not journal_name:
        return "JOURNAL"
    
    # Слова для игнорирования
    stop_words = {'of', 'the', 'and', 'for', 'in', 'on', 'at', 'to', 'by', 'with', 'from'}
    
    # Разбиваем на слова
    words = re.findall(r'[A-Za-z]+', journal_name)
    
    # Берем первые буквы значимых слов
    abbreviation_parts = []
    for word in words:
        word_lower = word.lower()
        if word_lower not in stop_words and len(word) > 2:
            abbreviation_parts.append(word[0].upper())
        elif len(abbreviation_parts) == 0 and len(words) <= 3:
            # Если журнал короткий, берем первые буквы всех слов
            abbreviation_parts.append(word[0].upper())
    
    # Если аббревиатура получилась слишком короткой (меньше 3 букв)
    if len(abbreviation_parts) < 3 and len(words) > 0:
        # Берем первые 3-4 буквы первого значимого слова
        for word in words:
            if word.lower() not in stop_words:
                abbreviation_parts = [word[:4].upper()]
                break
    
    abbreviation = ''.join(abbreviation_parts)
    
    # Если все еще пусто, берем первые 4 буквы первого слова
    if not abbreviation and words:
        abbreviation = words[0][:4].upper()
    
    return abbreviation if abbreviation else "JOURNAL"

def generate_filename(journal_abbr: str, years: List[int], language: str, extension: str) -> str:
    """
    Генерирует имя файла в формате: JOPS_2024,2026_en.pdf
    """
    years_str = format_year_filter_for_filename(years)
    return f"{journal_abbr}_{years_str}_{language}.{extension}"

# ============================================================================
# ГЕНЕРАЦИЯ PDF ОТЧЕТА (РУССКИЙ)
# ============================================================================

def generate_pdf_ru(journal_name: str, journal_abbr: str, years: List[int], 
                    grouped_articles: Dict[str, List[dict]], logo_path: str = None) -> bytes:
    """Генерация PDF отчета на русском языке с поддержкой кириллицы"""

    import hashlib                    
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.fonts import addMapping
    
    # Регистрируем шрифт с поддержкой кириллицы
    import os
    
    font_found = False
    russian_font_name = 'Helvetica'  # fallback
    
    # Список возможных путей к шрифтам с кириллицей
    font_paths = [
        # Linux (Streamlit Cloud)
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
        '/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf',
        '/usr/share/fonts/truetype/freefont/FreeSans.ttf',
        '/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf',
        '/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf',
        '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
        # macOS
        '/System/Library/Fonts/Helvetica.ttc',
        '/System/Library/Fonts/Arial.ttf',
        '/Library/Fonts/Arial.ttf',
        # Windows
        'C:/Windows/Fonts/arial.ttf',
        'C:/Windows/Fonts/times.ttf',
    ]
    
    for font_path in font_paths:
        if os.path.exists(font_path):
            try:
                pdfmetrics.registerFont(TTFont('RussianFont', font_path))
                russian_font_name = 'RussianFont'
                font_found = True
                print(f"Registered Russian font from: {font_path}")
                break
            except Exception as e:
                print(f"Failed to register {font_path}: {e}")
                continue
    
    if not font_found:
        # Если шрифт не найден, используем стандартный шрифт ReportLab
        # Он не будет корректно отображать кириллицу, но не вызовет ошибку
        print("WARNING: No Cyrillic font found, text may not display correctly")
        russian_font_name = 'Helvetica'
    
    def clean_text(text):
        if not text:
            return ""
        if isinstance(text, bytes):
            text = text.decode('utf-8', 'ignore')
        import unicodedata
        text = unicodedata.normalize('NFC', str(text))
        # Не удаляем кириллицу! Только опасные символы для XML
        text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        return text
    
    buffer = io.BytesIO()
    
    doc = SimpleDocTemplate(
        buffer, 
        pagesize=A4,
        topMargin=1.5*cm,
        bottomMargin=1.5*cm,
        leftMargin=2*cm,
        rightMargin=2*cm
    )
    
    styles = getSampleStyleSheet()
    
    # Стили с поддержкой кириллицы
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Normal'],
        fontSize=22,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Normal'],
        fontSize=14,
        textColor=colors.HexColor('#34495E'),
        spaceAfter=8,
        alignment=TA_CENTER,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    topic_style = ParagraphStyle(
        'TopicStyle',
        parent=styles['Normal'],
        fontSize=16,
        textColor=colors.HexColor('#16A085'),
        spaceAfter=10,
        spaceBefore=15,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    article_title_style = ParagraphStyle(
        'ArticleTitle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=5,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    authors_style = ParagraphStyle(
        'AuthorsStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=3,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    meta_style = ParagraphStyle(
        'MetaStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#7F8C8D'),
        spaceAfter=3,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    citation_style = ParagraphStyle(
        'CitationStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#27AE60'),
        spaceAfter=3,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    toc_style = ParagraphStyle(
        'TOCStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=4,
        fontName=russian_font_name,
        underline=True,
        encoding='utf-8'
    )
    
    intro_style = ParagraphStyle(
        'IntroStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=20,
        alignment=TA_JUSTIFY,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    footer_style = ParagraphStyle(
        'FooterStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#95A5A6'),
        spaceBefore=15,
        alignment=TA_CENTER,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    separator_style = ParagraphStyle(
        'Separator',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#BDC3C7'),
        alignment=TA_CENTER,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    conclusion_style = ParagraphStyle(
        'ConclusionStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=20,
        alignment=TA_JUSTIFY,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    story = []
    
    # ========== ТИТУЛЬНАЯ СТРАНИЦА ==========
    story.append(Spacer(1, 2*cm))
    
    if logo_path and os.path.exists(logo_path):
        try:
            from PIL import Image as PILImage
            
            pil_img = PILImage.open(logo_path)
            original_width, original_height = pil_img.size
            pil_img.close()
            
            max_width = 180
            max_height = 100
            
            width_ratio = max_width / original_width
            height_ratio = max_height / original_height
            scale_ratio = min(width_ratio, height_ratio)
            
            new_width = original_width * scale_ratio
            new_height = original_height * scale_ratio
            
            logo = Image(logo_path, width=new_width, height=new_height)
            logo.hAlign = 'CENTER'
            story.append(logo)
            story.append(Spacer(1, 1*cm))
            
        except Exception as e:
            print(f"Could not load logo: {e}")
    
    story.append(Paragraph("Аналитический отчет", title_style))
    story.append(Paragraph(f"«{clean_text(journal_name)}»", subtitle_style))
    story.append(Spacer(1, 1*cm))
    
    years_str = format_year_filter_for_filename(years)
    story.append(Paragraph(f"Период публикации: {years_str}", subtitle_style))
    story.append(Spacer(1, 1.5*cm))
    
    intro_text = f"""
    <b>Уважаемые коллеги!</b><br/><br/>
    Представляем Вашему вниманию тематический обзор статей, опубликованных в журнале 
    «{clean_text(journal_name)}» за {years_str} год(ы). Каждая работа прошла строгий 
    peer-review и представляет собой завершенное научное исследование.<br/><br/>
    <b>Почему эти статьи заслуживают Вашего внимания и цитирования?</b><br/>
    • Они отражают актуальные направления современной науки<br/>
    • Содержат верифицированные данные и воспроизводимые методы<br/>
    • Могут стать фундаментом для Ваших будущих исследований<br/>
    • Цитирование этих работ укрепит научный диалог в Вашей области<br/><br/>
    Мы приглашаем Вас ознакомиться с подборкой и рассмотреть возможность включения 
    этих работ в Ваши научные труды. Каждая цитата — это не просто ссылка, 
    это признание вклада коллег и развитие научного сообщества.
    """
    
    story.append(Paragraph(intro_text, intro_style))
    
    total_articles = sum(len(articles) for articles in grouped_articles.values())
    total_topics = len(grouped_articles)
    highly_cited = sum(1 for articles in grouped_articles.values() 
                      for a in articles if a.get('is_highly_cited', False))
    
    story.append(Spacer(1, 1*cm))
    
    stats_data = [
        ["Показатель", "Значение"],
        ["Всего статей", str(total_articles)],
        ["Тем исследований", str(total_topics)],
        ["Активно цитируемые статьи", str(highly_cited)]
    ]
    
    stats_table = Table(stats_data, colWidths=[doc.width/2.5, doc.width/3])
    stats_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), russian_font_name),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F2F4F4')]),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    
    story.append(stats_table)
    story.append(PageBreak())
    
    # ========== ОГЛАВЛЕНИЕ С ГИПЕРССЫЛКАМИ ==========
    story.append(Paragraph("Содержание", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    import hashlib
    
    # Стиль для ссылок в оглавлении (с подчеркиванием и синим цветом)
    toc_link_style = ParagraphStyle(
        'TOCLinkStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=4,
        fontName=russian_font_name,
        underline=True,
        encoding='utf-8'
    )
    
    for i, (topic, articles) in enumerate(grouped_articles.items(), 1):
        # Создаем уникальный идентификатор для якоря
        anchor_id = f"topic_{i}_{hashlib.md5(topic.encode('utf-8')).hexdigest()[:8]}"
        
        # Создаем гиперссылку
        link_text = f'{i}. {clean_text(topic)} — {len(articles)} статей'
        # Используем тег <a href="#anchor_id"> для создания ссылки
        toc_link = Paragraph(f'<a href="#{anchor_id}">{link_text}</a>', toc_link_style)
        story.append(toc_link)
        story.append(Spacer(1, 0.2*cm))
    
    if len(grouped_articles) > 30:
        story.append(Paragraph(f"... и {len(grouped_articles)-30} других тем", meta_style))
    
    story.append(PageBreak())
    
    # ========== СТАТЬИ ПО ТЕМАМ С ЯКОРЯМИ ==========
    for i, (topic, articles) in enumerate(grouped_articles.items(), 1):
        # Создаем уникальный идентификатор для якоря (должен совпадать с тем, что в оглавлении)
        anchor_id = f"topic_{i}_{hashlib.md5(topic.encode('utf-8')).hexdigest()[:8]}"
        
        # Стиль для невидимого якоря
        anchor_style = ParagraphStyle(
            'AnchorStyle',
            parent=styles['Normal'],
            fontSize=1,
            textColor=colors.white,
            fontName=russian_font_name,
            encoding='utf-8'
        )
        
        # Добавляем невидимый якорь перед заголовком темы
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', anchor_style)
        story.append(anchor_para)
        
        # Заголовок темы
        story.append(Paragraph(clean_text(topic), topic_style))
        story.append(Spacer(1, 0.3*cm))
        
        for idx, article in enumerate(articles, 1):
            title = clean_text(article.get('title', 'Без названия'))
            story.append(Paragraph(f"{idx}. {title}", article_title_style))
            
            authors = clean_text(article.get('authors', 'Авторы не указаны'))
            story.append(Paragraph(f"<b>Авторы:</b> {authors}", authors_style))
            
            journal = clean_text(article.get('journal_name', journal_name))
            year = article.get('publication_year', '')
            volume = article.get('volume', '')
            issue = article.get('issue', '')
            pages = article.get('pages', '')
            
            meta_parts = [f"<b>{journal}</b>"]
            if year:
                meta_parts.append(str(year))
            if volume:
                meta_parts.append(f"Том {volume}")
            if issue:
                meta_parts.append(f"Вып. {issue}")
            if pages:
                meta_parts.append(f"С. {pages}")
            
            story.append(Paragraph(", ".join(meta_parts), meta_style))
            
            citations = article.get('cited_by_count', 0)
            citations_per_year = article.get('citations_per_year', 0)
            is_highly = article.get('is_highly_cited', False)
            
            citation_text = f"<b>Цитирований:</b> {citations} | <b>в год:</b> {citations_per_year}"
            if is_highly:
                citation_text += " 🔥 Активно цитируемая"
            
            story.append(Paragraph(citation_text, citation_style))
            
            doi_url = article.get('doi_url', '')
            if doi_url:
                story.append(Paragraph(f"<b>DOI:</b> <link href='{doi_url}'>{doi_url}</link>", meta_style))
            
            story.append(Spacer(1, 0.2*cm))
            
            if idx < len(articles):
                story.append(Paragraph("─" * 50, separator_style))
                story.append(Spacer(1, 0.2*cm))
        
        story.append(Spacer(1, 0.5*cm))
        story.append(PageBreak())
    
    # ========== ЗАКЛЮЧЕНИЕ ==========
    story.append(Paragraph("Заключение", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    conclusion_text = f"""
    Данный отчет содержит {total_articles} статей из журнала «{clean_text(journal_name)}», 
    сгруппированных по {total_topics} исследовательским темам. Из них {highly_cited} статей 
    являются активно цитируемыми, что делает их особенно ценными для включения в Ваши научные работы.<br/><br/>
    
    Рекомендуем обратить особое внимание на статьи с пометкой «Активно цитируемая» — 
    они демонстрируют высокий научный интерес и могут стать важной частью Вашего исследования.<br/><br/>
    
    Отчет сгенерирован автоматически с использованием данных OpenAlex API.
    """
    
    story.append(Paragraph(conclusion_text, conclusion_style))
    
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(f"© {clean_text(journal_name)} | {datetime.now().strftime('%d.%m.%Y')}", footer_style))
    story.append(Paragraph("Отчет подготовлен с использованием CTA Journal Analyzer Pro", footer_style))
    
    doc.build(story)
    
    return buffer.getvalue()

# ============================================================================
# ГЕНЕРАЦИЯ PDF ОТЧЕТА (АНГЛИЙСКИЙ)
# ============================================================================

def generate_pdf_en(journal_name: str, journal_abbr: str, years: List[int], 
                    grouped_articles: Dict[str, List[dict]], logo_path: str = None) -> bytes:
    """Генерация PDF отчета на английском языке с активным оглавлением"""
    
    def clean_text(text):
        if not text:
            return ""
        if isinstance(text, bytes):
            text = text.decode('utf-8', 'ignore')
        import unicodedata
        text = unicodedata.normalize('NFC', str(text))
        text = re.sub(r'<[^>]+>', '', text)
        text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        allowed_pattern = r'[^a-zA-Zа-яА-ЯёЁ\s\.\,\-\'\(\)\d]'
        text = re.sub(allowed_pattern, '', text)
        return text
    
    buffer = io.BytesIO()
    
    doc = SimpleDocTemplate(
        buffer, 
        pagesize=A4,
        topMargin=1.5*cm,
        bottomMargin=1.5*cm,
        leftMargin=2*cm,
        rightMargin=2*cm
    )
    
    styles = getSampleStyleSheet()
    
    # Кастомные стили
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=22,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#34495E'),
        spaceAfter=8,
        alignment=TA_CENTER,
        fontName='Helvetica'
    )
    
    topic_style = ParagraphStyle(
        'TopicStyle',
        parent=styles['Heading3'],
        fontSize=16,
        textColor=colors.HexColor('#16A085'),
        spaceAfter=10,
        spaceBefore=15,
        fontName='Helvetica-Bold'
    )
    
    article_title_style = ParagraphStyle(
        'ArticleTitle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=5,
        fontName='Helvetica-Bold'
    )
    
    authors_style = ParagraphStyle(
        'AuthorsStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=3,
        fontName='Helvetica'
    )
    
    meta_style = ParagraphStyle(
        'MetaStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#7F8C8D'),
        spaceAfter=3,
        fontName='Helvetica'
    )
    
    citation_style = ParagraphStyle(
        'CitationStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#27AE60'),
        spaceAfter=3,
        fontName='Helvetica-Bold'
    )
    
    toc_style = ParagraphStyle(
        'TOCStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=4,
        fontName='Helvetica',
        underline=True
    )
    
    footer_style = ParagraphStyle(
        'FooterStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#95A5A6'),
        spaceBefore=15,
        alignment=TA_CENTER,
        fontName='Helvetica-Oblique'
    )
    
    story = []
    
    # ========== COVER PAGE ==========
    story.append(Spacer(1, 2*cm))

    if logo_path and os.path.exists(logo_path):
        try:
            from PIL import Image as PILImage
            
            # Открываем изображение для получения исходных размеров
            pil_img = PILImage.open(logo_path)
            
            # Получаем исходные размеры
            original_width, original_height = pil_img.size
            
            # Закрываем изображение после получения размеров
            pil_img.close()
            
            # Определяем максимальную ширину для логотипа (например, 180 пикселей)
            max_width = 180
            max_height = 100
            
            # Рассчитываем масштаб с сохранением пропорций
            width_ratio = max_width / original_width
            height_ratio = max_height / original_height
            
            # Берем минимальный коэффициент, чтобы логотип поместился в оба ограничения
            scale_ratio = min(width_ratio, height_ratio)
            
            # Вычисляем новые размеры с сохранением пропорций
            new_width = original_width * scale_ratio
            new_height = original_height * scale_ratio
            
            # Создаем Image с рассчитанными размерами
            logo = Image(logo_path, width=new_width, height=new_height)
            logo.hAlign = 'CENTER'
            story.append(logo)
            story.append(Spacer(1, 1*cm))
            
        except Exception as e:
            logger.warning(f"Could not load logo: {e}")
    
    story.append(Paragraph("Analytical Report", title_style))
    story.append(Paragraph(f"«{clean_text(journal_name)}»", subtitle_style))
    story.append(Spacer(1, 1*cm))
    
    years_str = format_year_filter_for_filename(years)
    story.append(Paragraph(f"Publication period: {years_str}", subtitle_style))
    story.append(Spacer(1, 1.5*cm))
    
    intro_text = f"""
    <b>Dear Colleagues!</b><br/><br/>
    We are pleased to present a curated collection of articles published in 
    «{clean_text(journal_name)}» during {years_str}. Each paper has undergone rigorous 
    peer-review and represents a complete scientific investigation.<br/><br/>
    <b>Why these papers deserve your attention and citations?</b><br/>
    • They address cutting-edge directions in modern science<br/>
    • Contain validated data and reproducible methods<br/>
    • Can serve as a foundation for your future research<br/>
    • Citing these works strengthens scholarly dialogue in your field<br/><br/>
    We invite you to explore this selection and consider incorporating these works 
    into your research. Every citation is not merely a reference — it's an acknowledgment 
    of colleagues' contributions and a step forward for the scientific community.
    """
    
    story.append(Paragraph(intro_text, ParagraphStyle(
        'IntroStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=20,
        alignment=TA_JUSTIFY,
        fontName='Helvetica'
    )))
    
    total_articles = sum(len(articles) for articles in grouped_articles.values())
    total_topics = len(grouped_articles)
    highly_cited = sum(1 for articles in grouped_articles.values() 
                      for a in articles if a.get('is_highly_cited', False))
    
    story.append(Spacer(1, 1*cm))
    
    stats_data = [
        ["Metric", "Value"],
        ["Total Articles", str(total_articles)],
        ["Research Topics", str(total_topics)],
        ["Highly Cited Articles", str(highly_cited)]
    ]
    
    stats_table = Table(stats_data, colWidths=[doc.width/2.5, doc.width/3])
    stats_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F2F4F4')]),
    ]))
    
    story.append(stats_table)
    story.append(PageBreak())
    
    # ========== TABLE OF CONTENTS WITH HYPERLINKS ==========
    story.append(Paragraph("Table of Contents", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    # Временный список для хранения ссылок оглавления
    toc_links = []
    
    for i, (topic, articles) in enumerate(grouped_articles.items(), 1):
        # Создаем уникальный идентификатор для якоря
        anchor_id = f"topic_{i}_{hashlib.md5(topic.encode()).hexdigest()[:8]}"
        
        # Добавляем ссылку в оглавление
        link_text = f'{i}. {clean_text(topic)} — {len(articles)} articles'
        # Используем специальный тег <a> с именем якоря
        toc_link = Paragraph(f'<a href="#{anchor_id}">{link_text}</a>', toc_style)
        toc_links.append(toc_link)
        toc_links.append(Spacer(1, 0.2*cm))
    
    # Добавляем все ссылки в story
    story.extend(toc_links)
    
    if len(grouped_articles) > 30:
        story.append(Paragraph(f"... and {len(grouped_articles)-30} other topics", meta_style))
    
    story.append(PageBreak())
    
    # ========== ARTICLES BY TOPIC WITH ANCHORS ==========
    for i, (topic, articles) in enumerate(grouped_articles.items(), 1):
        anchor_id = f"topic_{i}_{hashlib.md5(topic.encode()).hexdigest()[:8]}"
        
        # Добавляем якорь перед заголовком темы
        # Используем невидимый элемент для ссылки
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', ParagraphStyle(
            'AnchorStyle',
            parent=styles['Normal'],
            fontSize=1,
            textColor=colors.white
        ))
        story.append(anchor_para)
        
        # Заголовок темы
        story.append(Paragraph(clean_text(topic), topic_style))
        story.append(Spacer(1, 0.3*cm))
        
        for idx, article in enumerate(articles, 1):
            # Заголовок статьи
            title = clean_text(article.get('title', 'No title'))
            story.append(Paragraph(f"{idx}. {title}", article_title_style))
            
            # Авторы
            authors = clean_text(article.get('authors', 'Authors not specified'))
            story.append(Paragraph(f"<b>Authors:</b> {authors}", authors_style))
            
            # Метаданные
            journal = clean_text(article.get('journal_name', journal_name))
            year = article.get('publication_year', '')
            volume = article.get('volume', '')
            issue = article.get('issue', '')
            pages = article.get('pages', '')
            
            meta_parts = [f"<b>{journal}</b>"]
            if year:
                meta_parts.append(str(year))
            if volume:
                meta_parts.append(f"Volume {volume}")
            if issue:
                meta_parts.append(f"Issue {issue}")
            if pages:
                meta_parts.append(f"pp. {pages}")
            
            story.append(Paragraph(", ".join(meta_parts), meta_style))
            
            # Цитирования
            citations = article.get('cited_by_count', 0)
            citations_per_year = article.get('citations_per_year', 0)
            is_highly = article.get('is_highly_cited', False)
            
            citation_text = f"<b>Citations:</b> {citations} | <b>per year:</b> {citations_per_year}"
            if is_highly:
                citation_text += " 🔥 Highly Cited"
            
            story.append(Paragraph(citation_text, citation_style))
            
            # DOI ссылка
            doi_url = article.get('doi_url', '')
            if doi_url:
                story.append(Paragraph(f"<b>DOI:</b> <link href='{doi_url}'>{doi_url}</link>", meta_style))
            
            story.append(Spacer(1, 0.2*cm))
            
            if idx < len(articles):
                story.append(Paragraph("─" * 50, ParagraphStyle(
                    'Separator',
                    parent=styles['Normal'],
                    fontSize=8,
                    textColor=colors.HexColor('#BDC3C7'),
                    alignment=TA_CENTER
                )))
                story.append(Spacer(1, 0.2*cm))
        
        story.append(Spacer(1, 0.5*cm))
        story.append(PageBreak())
    
    # ========== CONCLUSION ==========
    story.append(Paragraph("Conclusion", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    conclusion_text = f"""
    This report contains {total_articles} articles from «{clean_text(journal_name)}», 
    grouped into {total_topics} research topics. Among them, {highly_cited} articles 
    are highly cited, making them particularly valuable for inclusion in your research.<br/><br/>
    
    We recommend paying special attention to articles marked as "Highly Cited" — 
    they demonstrate significant scientific interest and can become an important part 
    of your research.<br/><br/>
    
    This report was automatically generated using OpenAlex API data.
    """
    
    story.append(Paragraph(conclusion_text, ParagraphStyle(
        'ConclusionStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=20,
        alignment=TA_JUSTIFY,
        fontName='Helvetica'
    )))
    
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(f"© {clean_text(journal_name)} | {datetime.now().strftime('%d.%m.%Y')}", footer_style))
    story.append(Paragraph("Report generated using CTA Journal Analyzer Pro", footer_style))
    
    doc.build(story)
    
    return buffer.getvalue()

# ============================================================================
# ГЕНЕРАЦИЯ TXT ОТЧЕТА (РУССКИЙ)
# ============================================================================

def generate_txt_ru(journal_name: str, years: List[int], grouped_articles: Dict[str, List[dict]]) -> str:
    """Генерация TXT отчета на русском языке"""
    
    output = []
    
    years_str = format_year_filter_for_filename(years)
    total_articles = sum(len(articles) for articles in grouped_articles.values())
    total_topics = len(grouped_articles)
    highly_cited = sum(1 for articles in grouped_articles.values() 
                      for a in articles if a.get('is_highly_cited', False))
    
    # Заголовок
    output.append("=" * 80)
    output.append(f"АНАЛИТИЧЕСКИЙ ОТЧЕТ")
    output.append(f"Журнал: {journal_name}")
    output.append(f"Период публикации: {years_str}")
    output.append("=" * 80)
    output.append("")
    
    # Вступительное обращение
    output.append("Уважаемые коллеги!")
    output.append("")
    output.append(f"Представляем Вашему вниманию тематический обзор статей, опубликованных в журнале")
    output.append(f"«{journal_name}» за {years_str} год(ы). Каждая работа прошла строгий peer-review")
    output.append("и представляет собой завершенное научное исследование.")
    output.append("")
    output.append("Почему эти статьи заслуживают Вашего внимания и цитирования?")
    output.append("• Они отражают актуальные направления современной науки")
    output.append("• Содержат верифицированные данные и воспроизводимые методы")
    output.append("• Могут стать фундаментом для Ваших будущих исследований")
    output.append("• Цитирование этих работ укрепит научный диалог в Вашей области")
    output.append("")
    output.append("Мы приглашаем Вас ознакомиться с подборкой и рассмотреть возможность включения")
    output.append("этих работ в Ваши научные труды. Каждая цитата — это не просто ссылка,")
    output.append("это признание вклада коллег и развитие научного сообщества.")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Статистика
    output.append("СТАТИСТИКА")
    output.append("-" * 40)
    output.append(f"Всего статей: {total_articles}")
    output.append(f"Тем исследований: {total_topics}")
    output.append(f"Активно цитируемые статьи: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Содержание
    output.append("СОДЕРЖАНИЕ")
    output.append("-" * 40)
    for i, (topic, articles) in enumerate(grouped_articles.items(), 1):
        output.append(f"{i}. {topic} — {len(articles)} статей")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Статьи по темам
    for topic, articles in grouped_articles.items():
        output.append("")
        output.append("█" * 60)
        output.append(f"ТЕМА: {topic}")
        output.append("█" * 60)
        output.append("")
        
        for idx, article in enumerate(articles, 1):
            output.append(f"{idx}. {article.get('title', 'Без названия')}")
            output.append(f"   Авторы: {article.get('authors', 'Авторы не указаны')}")
            
            # Метаданные
            meta_parts = [f"   {article.get('journal_name', journal_name)}"]
            if article.get('publication_year'):
                meta_parts.append(str(article.get('publication_year')))
            if article.get('volume'):
                meta_parts.append(f"Том {article.get('volume')}")
            if article.get('issue'):
                meta_parts.append(f"Вып. {article.get('issue')}")
            if article.get('pages'):
                meta_parts.append(f"С. {article.get('pages')}")
            
            output.append(", ".join(meta_parts))
            
            # Цитирования
            citations = article.get('cited_by_count', 0)
            citations_per_year = article.get('citations_per_year', 0)
            highly = " 🔥 АКТИВНО ЦИТИРУЕМАЯ" if article.get('is_highly_cited') else ""
            output.append(f"   Цитирований: {citations} | в год: {citations_per_year}{highly}")
            
            # DOI
            if article.get('doi_url'):
                output.append(f"   DOI: {article.get('doi_url')}")
            
            output.append("")
    
    # Заключение
    output.append("=" * 80)
    output.append("ЗАКЛЮЧЕНИЕ")
    output.append("=" * 80)
    output.append("")
    output.append(f"Данный отчет содержит {total_articles} статей из журнала «{journal_name}»,")
    output.append(f"сгруппированных по {total_topics} исследовательским темам. Из них {highly_cited} статей")
    output.append("являются активно цитируемыми, что делает их особенно ценными для включения")
    output.append("в Ваши научные работы.")
    output.append("")
    output.append("Рекомендуем обратить особое внимание на статьи с пометкой «Активно цитируемая» —")
    output.append("они демонстрируют высокий научный интерес и могут стать важной частью")
    output.append("Вашего исследования.")
    output.append("")
    output.append("=" * 80)
    output.append(f"Отчет сгенерирован: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    output.append(f"© {journal_name}")
    output.append("=" * 80)
    
    return "\n".join(output)

# ============================================================================
# ГЕНЕРАЦИЯ TXT ОТЧЕТА (АНГЛИЙСКИЙ)
# ============================================================================

def generate_txt_en(journal_name: str, years: List[int], grouped_articles: Dict[str, List[dict]]) -> str:
    """Генерация TXT отчета на английском языке"""
    
    output = []
    
    years_str = format_year_filter_for_filename(years)
    total_articles = sum(len(articles) for articles in grouped_articles.values())
    total_topics = len(grouped_articles)
    highly_cited = sum(1 for articles in grouped_articles.values() 
                      for a in articles if a.get('is_highly_cited', False))
    
    # Header
    output.append("=" * 80)
    output.append(f"ANALYTICAL REPORT")
    output.append(f"Journal: {journal_name}")
    output.append(f"Publication period: {years_str}")
    output.append("=" * 80)
    output.append("")
    
    # Introduction
    output.append("Dear Colleagues!")
    output.append("")
    output.append(f"We are pleased to present a curated collection of articles published in")
    output.append(f"«{journal_name}» during {years_str}. Each paper has undergone rigorous")
    output.append("peer-review and represents a complete scientific investigation.")
    output.append("")
    output.append("Why these papers deserve your attention and citations?")
    output.append("• They address cutting-edge directions in modern science")
    output.append("• Contain validated data and reproducible methods")
    output.append("• Can serve as a foundation for your future research")
    output.append("• Citing these works strengthens scholarly dialogue in your field")
    output.append("")
    output.append("We invite you to explore this selection and consider incorporating these works")
    output.append("into your research. Every citation is not merely a reference — it's an acknowledgment")
    output.append("of colleagues' contributions and a step forward for the scientific community.")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Statistics
    output.append("STATISTICS")
    output.append("-" * 40)
    output.append(f"Total Articles: {total_articles}")
    output.append(f"Research Topics: {total_topics}")
    output.append(f"Highly Cited Articles: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Table of Contents
    output.append("TABLE OF CONTENTS")
    output.append("-" * 40)
    for i, (topic, articles) in enumerate(grouped_articles.items(), 1):
        output.append(f"{i}. {topic} — {len(articles)} articles")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Articles by topic
    for topic, articles in grouped_articles.items():
        output.append("")
        output.append("█" * 60)
        output.append(f"TOPIC: {topic}")
        output.append("█" * 60)
        output.append("")
        
        for idx, article in enumerate(articles, 1):
            output.append(f"{idx}. {article.get('title', 'No title')}")
            output.append(f"   Authors: {article.get('authors', 'Authors not specified')}")
            
            # Metadata
            meta_parts = [f"   {article.get('journal_name', journal_name)}"]
            if article.get('publication_year'):
                meta_parts.append(str(article.get('publication_year')))
            if article.get('volume'):
                meta_parts.append(f"Volume {article.get('volume')}")
            if article.get('issue'):
                meta_parts.append(f"Issue {article.get('issue')}")
            if article.get('pages'):
                meta_parts.append(f"pp. {article.get('pages')}")
            
            output.append(", ".join(meta_parts))
            
            # Citations
            citations = article.get('cited_by_count', 0)
            citations_per_year = article.get('citations_per_year', 0)
            highly = " 🔥 HIGHLY CITED" if article.get('is_highly_cited') else ""
            output.append(f"   Citations: {citations} | per year: {citations_per_year}{highly}")
            
            # DOI
            if article.get('doi_url'):
                output.append(f"   DOI: {article.get('doi_url')}")
            
            output.append("")
    
    # Conclusion
    output.append("=" * 80)
    output.append("CONCLUSION")
    output.append("=" * 80)
    output.append("")
    output.append(f"This report contains {total_articles} articles from «{journal_name}»,")
    output.append(f"grouped into {total_topics} research topics. Among them, {highly_cited} articles")
    output.append("are highly cited, making them particularly valuable for inclusion in your research.")
    output.append("")
    output.append("We recommend paying special attention to articles marked as 'Highly Cited' —")
    output.append("they demonstrate significant scientific interest and can become an important part")
    output.append("of your research.")
    output.append("")
    output.append("=" * 80)
    output.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append(f"© {journal_name}")
    output.append("=" * 80)
    
    return "\n".join(output)

# ============================================================================
# ИНТЕРФЕЙС ПРИЛОЖЕНИЯ
# ============================================================================

def main():
    """Главная функция приложения"""
    
    # Переключатель языка
    col_lang1, col_lang2 = st.columns([6, 1])
    with col_lang2:
        language = st.selectbox("🌐", ["English", "Русский"], key="language_selector")
    
    lang = 'en' if language == "English" else 'ru'
    t = LANGUAGES[lang]
    
    # Инициализация состояния
    if 'step' not in st.session_state:
        st.session_state.step = 1
    if 'journal_info' not in st.session_state:
        st.session_state.journal_info = None
    if 'journal_logo' not in st.session_state:
        st.session_state.journal_logo = None
    if 'articles' not in st.session_state:
        st.session_state.articles = None
    if 'grouped_articles' not in st.session_state:
        st.session_state.grouped_articles = None
    if 'selected_years' not in st.session_state:
        st.session_state.selected_years = None
    if 'years_input' not in st.session_state:
        st.session_state.years_input = ""
    
    # Заголовок
    st.markdown(f"<h1 class='main-header'>{t['app_title']}</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='font-size: 1rem; color: #666; margin-bottom: 1.5rem;'>{t['app_subtitle']}</p>", unsafe_allow_html=True)
    
    # Очистка старого кэша
    clear_old_cache()
    
    # Шаг 1: Ввод ISSN и логотипа
    if st.session_state.step == 1:
        st.markdown(f"""
        <div class="step-card">
            <h3 style="margin: 0; font-size: 1.3rem;">{t['step1_title']}</h3>
            <p style="margin: 5px 0; font-size: 0.9rem;">{t['step1_desc']}</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            issn_input = st.text_input(
                t['issn_label'],
                placeholder=t['issn_placeholder'],
                key="issn_input"
            )
        
        with col2:
            logo_file = st.file_uploader(
                t['logo_label'],
                type=['png', 'jpg', 'jpeg'],
                help=t['logo_help'],
                key="logo_uploader"
            )
        
        if st.button(t['next_btn'], type="primary", use_container_width=True):
            if issn_input:
                issn_clean = parse_issn(issn_input)
                if issn_clean:
                    with st.spinner(t['loading_journal']):
                        journal = get_journal_by_issn(issn_clean)
                        if journal:
                            st.session_state.journal_info = journal
                            if logo_file:
                                # Сохраняем логотип временно
                                temp_logo_path = CACHE_DIR / f"logo_{issn_clean}.png"
                                with open(temp_logo_path, 'wb') as f:
                                    f.write(logo_file.getbuffer())
                                st.session_state.journal_logo = str(temp_logo_path)
                            st.session_state.step = 2
                            st.rerun()
                        else:
                            st.error(t['journal_not_found'])
                else:
                    st.error(t['journal_not_found'])
            else:
                st.error(t['journal_not_found'])
    
    # Шаг 2: Выбор годов
    elif st.session_state.step == 2:
        st.markdown(f"""
        <div class="step-card">
            <h3 style="margin: 0; font-size: 1.3rem;">{t['step2_title']}</h3>
            <p style="margin: 5px 0; font-size: 0.9rem;">{t['step2_desc']}</p>
        </div>
        """, unsafe_allow_html=True)
        
        journal_name = st.session_state.journal_info.get('display_name', 'Journal')
        st.info(f"**Journal found:** {journal_name}")
        
        years_input = st.text_input(
            t['years_label'],
            value=st.session_state.years_input,
            placeholder=t['years_help'],
            help=t['years_help'],
            key="years_input_widget"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("← " + t['next_btn'].replace('→', '←'), use_container_width=True):
                st.session_state.step = 1
                st.rerun()
        
        with col2:
            if st.button(t['analyze_btn'], type="primary", use_container_width=True):
                if years_input:
                    years = parse_year_filter(years_input)
                    if years:
                        st.session_state.selected_years = years
                        st.session_state.years_input = years_input
                        
                        with st.spinner(t['loading_articles']):
                            source_id = st.session_state.journal_info.get('id')
                            if source_id:
                                articles = fetch_articles_by_journal(source_id, years)
                                if articles:
                                    with st.spinner(t['analyzing']):
                                        grouped = group_articles_by_topic(articles)
                                        st.session_state.articles = articles
                                        st.session_state.grouped_articles = grouped
                                        st.session_state.step = 3
                                        st.rerun()
                                else:
                                    st.error(t['no_articles'])
                            else:
                                st.error(t['journal_not_found'])
                    else:
                        st.error(t['years_help'])
                else:
                    st.error(t['years_help'])
    
    # Шаг 3: Результаты
    elif st.session_state.step == 3:
        st.markdown(f"""
        <div class="step-card">
            <h3 style="margin: 0; font-size: 1.3rem;">{t['step3_title']}</h3>
            <p style="margin: 5px 0; font-size: 0.9rem;">{t['step3_desc']}</p>
        </div>
        """, unsafe_allow_html=True)
        
        journal_name = st.session_state.journal_info.get('display_name', 'Journal')
        grouped = st.session_state.grouped_articles
        years = st.session_state.selected_years
        
        total_articles = sum(len(articles) for articles in grouped.values())
        total_topics = len(grouped)
        highly_cited = sum(1 for articles in grouped.values() 
                          for a in articles if a.get('is_highly_cited', False))
        
        if total_articles > 0:
            # Метрики
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(t['total_articles'], f"{total_articles:,}")
            with col2:
                st.metric(t['total_topics'], f"{total_topics}")
            with col3:
                avg_citations = sum(a.get('cited_by_count', 0) for articles in grouped.values() for a in articles) / total_articles
                st.metric(t['avg_citations'], f"{avg_citations:.1f}")
            with col4:
                st.metric(t['highly_cited'], f"{highly_cited}")
            
            # Отображение тем и статей
            st.markdown("---")
            
            # Аккордеон для каждой темы
            for topic, articles in grouped.items():
                with st.expander(f"📚 {topic} ({len(articles)} {t['articles_count']})"):
                    for idx, article in enumerate(articles, 1):
                        st.markdown(f"""
                        <div style="padding: 10px; margin: 5px 0; background: #f8f9fa; border-radius: 8px;">
                            <b>{idx}. {article.get('title', 'No title')}</b><br>
                            <span style="color: #666; font-size: 0.9rem;">
                                👤 {article.get('authors', 'N/A')[:100]}<br>
                                📄 {article.get('journal_name', journal_name)} | {article.get('publication_year', 'N/A')}
                                {f' | Vol.{article.get("volume")}' if article.get('volume') else ''}
                                {f' | Iss.{article.get("issue")}' if article.get('issue') else ''}
                                {f' | pp.{article.get("pages")}' if article.get('pages') else ''}<br>
                                📊 {t['citations']}: {article.get('cited_by_count', 0)} 
                                ({t['citations_per_year']}: {article.get('citations_per_year', 0)})
                                {f' 🔥 {t["citations_badge"]}' if article.get('is_highly_cited') else ''}<br>
                                🔗 <a href="{article.get('doi_url', '#')}" target="_blank">{t['view_article']}</a>
                            </span>
                        </div>
                        """, unsafe_allow_html=True)
            
            # Экспорт
            st.markdown("---")
            st.markdown(f"### {t['export_btn']}")
            
            journal_abbr = generate_journal_abbreviation(journal_name)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**PDF Reports**")
                
                # PDF English
                pdf_en_data = generate_pdf_en(journal_name, journal_abbr, years, grouped, st.session_state.journal_logo)
                filename_en = generate_filename(journal_abbr, years, 'en', 'pdf')
                st.download_button(
                    label="📄 PDF (English)",
                    data=pdf_en_data,
                    file_name=filename_en,
                    mime="application/pdf",
                    use_container_width=True,
                    key="pdf_en"
                )
                
                # PDF Russian
                pdf_ru_data = generate_pdf_ru(journal_name, journal_abbr, years, grouped, st.session_state.journal_logo)
                filename_ru = generate_filename(journal_abbr, years, 'ru', 'pdf')
                st.download_button(
                    label="📄 PDF (Русский)",
                    data=pdf_ru_data,
                    file_name=filename_ru,
                    mime="application/pdf",
                    use_container_width=True,
                    key="pdf_ru"
                )
            
            with col2:
                st.markdown("**TXT Reports**")
                
                # TXT English
                txt_en_data = generate_txt_en(journal_name, years, grouped)
                filename_en_txt = generate_filename(journal_abbr, years, 'en', 'txt')
                st.download_button(
                    label="📝 TXT (English)",
                    data=txt_en_data,
                    file_name=filename_en_txt,
                    mime="text/plain",
                    use_container_width=True,
                    key="txt_en"
                )
                
                # TXT Russian
                txt_ru_data = generate_txt_ru(journal_name, years, grouped)
                filename_ru_txt = generate_filename(journal_abbr, years, 'ru', 'txt')
                st.download_button(
                    label="📝 TXT (Русский)",
                    data=txt_ru_data,
                    file_name=filename_ru_txt,
                    mime="text/plain",
                    use_container_width=True,
                    key="txt_ru"
                )
            
            # Кнопка нового анализа
            st.markdown("---")
            if st.button(t['new_analysis_btn'], use_container_width=True):
                # Очищаем состояние
                keys_to_clear = ['step', 'journal_info', 'journal_logo', 'articles', 
                                'grouped_articles', 'selected_years', 'years_input']
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                st.session_state.step = 1
                st.rerun()
        else:
            st.warning(t['no_articles'])
            if st.button("← " + t['next_btn'].replace('→', '←'), use_container_width=True):
                st.session_state.step = 2
                st.rerun()
    
    # Футер
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align: center; color: #888; font-size: 0.8rem; margin-top: 1rem;">
        <p>© CTA, https://chimicatechnoacta.ru / developed by daM©</p>
        <p style="font-size: 0.7rem; color: #aaa;">v3.0 - Journal Analyzer Pro</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
