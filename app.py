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
from typing import List, Dict, Tuple, Optional, Set, Any
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

# Загружаем логотип из корневой директории
import os
from PIL import Image

LOGO_PATH = "logo.png"
if os.path.exists(LOGO_PATH):
    try:
        logo_image = Image.open(LOGO_PATH)
        st.session_state.app_logo = logo_image
    except Exception as e:
        st.session_state.app_logo = None
        logger.warning(f"Could not load logo: {e}")
else:
    st.session_state.app_logo = None

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
        'view_article': 'View Article',
        'customize_message': 'Customize Message',
        'message_preview': 'Message Preview',
        'use_default': 'Reset to Default',
        'domain': 'Domain',
        'field': 'Field',
        'subfield': 'Subfield',
        'articles_count_label': 'articles',
        'citations_count_label': 'citations'
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
        'view_article': 'Смотреть статью',
        'customize_message': 'Настроить сообщение',
        'message_preview': 'Предпросмотр сообщения',
        'use_default': 'Сбросить на стандартное',
        'domain': 'Область',
        'field': 'Поле',
        'subfield': 'Подполе',
        'articles_count_label': 'статей',
        'citations_count_label': 'цитирований'
    }
}

# ============================================================================
# НАСТРАИВАЕМЫЙ ТЕКСТ ПО УМОЛЧАНИЮ
# ============================================================================

DEFAULT_MESSAGES = {
    'en': {
        'title': 'Dear Colleagues!',
        'body': """We are pleased to present a curated collection of articles published in «JOURNAL_NAME» during YEARS. Each paper has undergone rigorous peer-review and represents a complete scientific investigation.

Why these papers deserve your attention and citations?
• They address cutting-edge directions in modern science
• Contain validated data and reproducible methods
• Can serve as a foundation for your future research
• Citing these works strengthens scholarly dialogue in your field

We invite you to explore this selection and consider incorporating these works into your research. Every citation is not merely a reference — it's an acknowledgment of colleagues' contributions and a step forward for the scientific community."""
    },
    'ru': {
        'title': 'Уважаемые коллеги!',
        'body': """Представляем Вашему вниманию тематический обзор статей, опубликованных в журнале «JOURNAL_NAME» за YEARS. Каждая работа прошла строгий peer-review и представляет собой завершенное научное исследование.

Почему эти статьи заслуживают Вашего внимания и цитирования?
• Они отражают актуальные направления современной науки
• Содержат верифицированные данные и воспроизводимые методы
• Могут стать фундаментом для Ваших будущих исследований
• Цитирование этих работ укрепит научный диалог в Вашей области

Мы приглашаем Вас ознакомиться с подборкой и рассмотреть возможность включения этих работ в Ваши научные труды. Каждая цитата — это не просто ссылка, это признание вклада коллег и развитие научного сообщества."""
    }
}

# ============================================================================
# КАСТОМНЫЙ CSS ДИЗАЙН
# ============================================================================

st.markdown("""
<style>
    /* Основные стили */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    /* Градиентный фон для main */
    .stApp {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    
    /* Главный хедер с анимацией */
    .main-header {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
        animation: fadeInDown 0.8s ease-out;
        letter-spacing: -0.02em;
    }
    
    @keyframes fadeInDown {
        from {
            opacity: 0;
            transform: translateY(-30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    /* Карточки шагов с эффектом стекла */
    .step-card {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(10px);
        border-radius: 24px;
        padding: 24px;
        border: 1px solid rgba(255, 255, 255, 0.3);
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.04);
        margin-bottom: 20px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        animation: fadeInUp 0.6s ease-out;
    }
    
    .step-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 24px 48px rgba(0, 0, 0, 0.12);
    }
    
    /* Метрические карточки с градиентом */
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
        border-radius: 20px;
        padding: 20px;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.06);
        border: 1px solid rgba(102, 126, 234, 0.15);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #667eea, #764ba2, #f093fb);
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 16px 32px rgba(102, 126, 234, 0.15);
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea, #764ba2);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 5px;
    }
    
    .metric-label {
        font-size: 0.85rem;
        color: #6c757d;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Карточка результата */
    .result-card {
        background: white;
        border-radius: 16px;
        padding: 18px;
        margin-bottom: 12px;
        border-left: 4px solid #667eea;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
        transition: all 0.2s ease;
    }
    
    .result-card:hover {
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.1);
        transform: translateX(4px);
    }
    
    /* Фильтр секция */
    .filter-section {
        background: rgba(255, 255, 255, 0.9);
        backdrop-filter: blur(8px);
        border-radius: 20px;
        padding: 20px;
        margin-bottom: 20px;
        border: 1px solid rgba(102, 126, 234, 0.2);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.04);
    }
    
    /* Кастомные кнопки */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 10px 24px;
        font-weight: 600;
        font-size: 0.9rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4);
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    .stButton > button:active {
        transform: translateY(0px);
    }
    
    /* Кастомные expander */
    .streamlit-expanderHeader {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 12px;
        font-weight: 600;
        color: #2c3e50;
        transition: all 0.2s ease;
    }
    
    .streamlit-expanderHeader:hover {
        background: linear-gradient(135deg, #e9ecef 0%, #dee2e6 100%);
    }
    
    /* Инпуты с фокусом */
    .stTextInput > div > div > input {
        border-radius: 12px;
        border: 2px solid #e0e0e0;
        transition: all 0.3s ease;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
    }
    
    /* Селекторы */
    .stSelectbox > div > div {
        border-radius: 12px;
    }
    
    /* Прогресс бар */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea, #764ba2, #f093fb);
    }
    
    /* Инфо бокс */
    .stAlert {
        border-radius: 16px;
        border-left: 4px solid #667eea;
    }
    
    /* Скроллбар */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea, #764ba2);
        border-radius: 10px;
    }
    
    /* Анимация для загрузки */
    @keyframes pulse {
        0%, 100% {
            opacity: 1;
        }
        50% {
            opacity: 0.5;
        }
    }
    
    .loading-spinner {
        animation: pulse 1.5s ease-in-out infinite;
    }
    
    /* Badge для цитирований */
    .citation-badge {
        display: inline-block;
        background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        color: #d63031;
    }
    
    /* Разделитель с градиентом */
    .gradient-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, #667eea, #764ba2, #f093fb, transparent);
        margin: 20px 0;
    }
    
    /* Футер */
    .footer {
        text-align: center;
        padding: 20px;
        color: #6c757d;
        font-size: 0.8rem;
        border-top: 1px solid rgba(102, 126, 234, 0.2);
        margin-top: 40px;
    }
    
    /* Кастомный таб */
    .custom-tab {
        background: white;
        border-radius: 12px;
        padding: 8px 16px;
        cursor: pointer;
        transition: all 0.2s;
    }
    
    /* Стиль для текстовой области сообщения */
    .message-editor {
        background: white;
        border-radius: 16px;
        padding: 16px;
        border: 1px solid #e0e0e0;
        margin-bottom: 16px;
    }
    
    /* Анимированный градиент */
    @keyframes gradientShift {
        0% {
            background-position: 0% 50%;
        }
        50% {
            background-position: 100% 50%;
        }
        100% {
            background-position: 0% 50%;
        }
    }
</style>
""", unsafe_allow_html=True)

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

def extract_topic_hierarchy(article: dict) -> Tuple[str, str, str, str]:
    """
    Извлекает иерархию тем из primary_topic статьи.
    
    Returns:
        Tuple[domain, field, subfield, topic]
    """
    primary_topic = article.get('primary_topic', {})
    
    if not primary_topic:
        return ("Unidentified", "Unidentified", "Unidentified", "Unidentified")
    
    # Извлекаем Domain
    domain_obj = primary_topic.get('domain', {})
    domain = domain_obj.get('display_name', 'Unidentified') if domain_obj else 'Unidentified'
    
    # Извлекаем Field
    field_obj = primary_topic.get('field', {})
    field = field_obj.get('display_name', 'Unidentified') if field_obj else 'Unidentified'
    
    # Извлекаем Subfield
    subfield_obj = primary_topic.get('subfield', {})
    subfield = subfield_obj.get('display_name', 'Unidentified') if subfield_obj else 'Unidentified'
    
    # Извлекаем Topic
    topic = primary_topic.get('display_name', 'Unidentified')
    
    return (domain, field, subfield, topic)

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
    
    # Получаем иерархию тем
    domain, field, subfield, primary_topic = extract_topic_hierarchy(article)
    
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
        'domain': domain,
        'field': field,
        'subfield': subfield,
        'primary_topic': primary_topic,
        'type': article.get('type', ''),
        'is_oa': article.get('open_access', {}).get('is_oa', False) if article.get('open_access') else False
    }
    
    return enriched

# ============================================================================
# ИЕРАРХИЧЕСКАЯ ГРУППИРОВКА СТАТЕЙ
# ============================================================================

def group_articles_by_hierarchy(articles: List[dict]) -> Dict[str, Dict[str, Dict[str, Dict[str, List[dict]]]]]:
    """
    Группирует статьи по иерархии: Domain -> Field -> Subfield -> Topic
    
    Returns:
        {
            "Physical Sciences": {
                "Materials Science": {
                    "Materials Chemistry": {
                        "Advancements in SOFC": [article1, article2],
                        "Electronic Properties": [article3]
                    }
                }
            }
        }
    """
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    
    for article in articles:
        enriched = enrich_article_data(article)
        
        domain = enriched.get('domain', 'Unidentified')
        field = enriched.get('field', 'Unidentified')
        subfield = enriched.get('subfield', 'Unidentified')
        topic = enriched.get('primary_topic', 'Unidentified')
        
        hierarchy[domain][field][subfield][topic].append(enriched)
    
    # Преобразуем defaultdict в обычный dict для сериализации
    result = {}
    for domain, fields in hierarchy.items():
        result[domain] = {}
        for field, subfields in fields.items():
            result[domain][field] = {}
            for subfield, topics in subfields.items():
                result[domain][field][subfield] = dict(topics)
    
    return result

def calculate_hierarchy_statistics(hierarchy: Dict) -> Dict:
    """
    Рассчитывает статистику для каждого уровня иерархии.
    
    Returns:
        {
            "domain_name": {
                "articles": 100,
                "citations": 5000,
                "fields": {...}
            }
        }
    """
    stats = {}
    
    for domain, fields in hierarchy.items():
        domain_articles = 0
        domain_citations = 0
        field_stats = {}
        
        for field, subfields in fields.items():
            field_articles = 0
            field_citations = 0
            subfield_stats = {}
            
            for subfield, topics in subfields.items():
                subfield_articles = 0
                subfield_citations = 0
                topic_stats = {}
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    
                    topic_stats[topic] = {
                        'articles': topic_articles,
                        'citations': topic_citations,
                        'articles_list': articles
                    }
                    
                    subfield_articles += topic_articles
                    subfield_citations += topic_citations
                
                subfield_stats[subfield] = {
                    'articles': subfield_articles,
                    'citations': subfield_citations,
                    'topics': topic_stats
                }
                
                field_articles += subfield_articles
                field_citations += subfield_citations
            
            field_stats[field] = {
                'articles': field_articles,
                'citations': field_citations,
                'subfields': subfield_stats
            }
            
            domain_articles += field_articles
            domain_citations += field_citations
        
        stats[domain] = {
            'articles': domain_articles,
            'citations': domain_citations,
            'fields': field_stats
        }
    
    return stats

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

def format_message_with_variables(message: str, journal_name: str, years_str: str) -> str:
    """Заменяет переменные в сообщении на реальные значения"""
    message = message.replace('JOURNAL_NAME', journal_name)
    message = message.replace('YEARS', years_str)
    return message

# ============================================================================
# ГЕНЕРАЦИЯ PDF ОТЧЕТА (РУССКИЙ) С ИЕРАРХИЕЙ
# ============================================================================

def generate_pdf_ru(journal_name: str, journal_abbr: str, years: List[int], 
                    hierarchy: Dict, logo_path: str = None, custom_message: str = None,
                    app_logo_path: str = None) -> bytes:
    """Генерация PDF отчета на русском языке с иерархической группировкой"""

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
        print("WARNING: No Cyrillic font found, text may not display correctly")
        russian_font_name = 'Helvetica'
    
    def clean_text(text):
        if not text:
            return ""
        if isinstance(text, bytes):
            text = text.decode('utf-8', 'ignore')
        import unicodedata
        text = unicodedata.normalize('NFC', str(text))
        text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        return text
    
    # Рассчитываем статистику
    stats = calculate_hierarchy_statistics(hierarchy)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values())
    highly_cited = sum(1 for domain in hierarchy.values() 
                      for field in domain.values()
                      for subfield in field.values()
                      for topic in subfield.values()
                      for a in topic if a.get('is_highly_cited', False))
    
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
    
    domain_style = ParagraphStyle(
        'DomainStyle',
        parent=styles['Normal'],
        fontSize=18,
        textColor=colors.HexColor('#667eea'),
        spaceAfter=10,
        spaceBefore=20,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    field_style = ParagraphStyle(
        'FieldStyle',
        parent=styles['Normal'],
        fontSize=15,
        textColor=colors.HexColor('#764ba2'),
        spaceAfter=8,
        spaceBefore=12,
        leftIndent=20,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    subfield_style = ParagraphStyle(
        'SubfieldStyle',
        parent=styles['Normal'],
        fontSize=13,
        textColor=colors.HexColor('#16A085'),
        spaceAfter=8,
        spaceBefore=10,
        leftIndent=40,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    topic_style = ParagraphStyle(
        'TopicStyle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=8,
        spaceBefore=8,
        leftIndent=60,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    article_title_style = ParagraphStyle(
        'ArticleTitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=5,
        leftIndent=80,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    authors_style = ParagraphStyle(
        'AuthorsStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=3,
        leftIndent=80,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    meta_style = ParagraphStyle(
        'MetaStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#7F8C8D'),
        spaceAfter=3,
        leftIndent=80,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    citation_style = ParagraphStyle(
        'CitationStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#27AE60'),
        spaceAfter=3,
        leftIndent=80,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    toc_domain_style = ParagraphStyle(
        'TOCDomainStyle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.HexColor('#667eea'),
        spaceAfter=6,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    toc_field_style = ParagraphStyle(
        'TOCFieldStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#764ba2'),
        spaceAfter=4,
        leftIndent=15,
        fontName=russian_font_name,
        encoding='utf-8'
    )
    
    toc_subfield_style = ParagraphStyle(
        'TOCSubfieldStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#16A085'),
        spaceAfter=3,
        leftIndent=30,
        fontName=russian_font_name,
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
    
    # Настраиваемый текст или стандартный
    if custom_message:
        intro_text = format_message_with_variables(custom_message, clean_text(journal_name), years_str)
    else:
        default_msg = DEFAULT_MESSAGES['ru']['body']
        intro_text = format_message_with_variables(default_msg, clean_text(journal_name), years_str)
    
    # Сохраняем переносы строк
    intro_text_formatted = intro_text.replace('\n\n', '<br/><br/>').replace('\n', '<br/>')
    story.append(Paragraph(intro_text_formatted, intro_style))
    
    story.append(Spacer(1, 1*cm))
    
    stats_data = [
        ["Показатель", "Значение"],
        ["Всего статей", str(total_articles)],
        ["Областей науки", str(total_domains)],
        ["Всего цитирований", str(total_citations)]
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
    
    # ========== ОГЛАВЛЕНИЕ (Domain -> Field -> Subfield) ==========
    story.append(Paragraph("Содержание", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        
        anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
        story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} статей, {domain_citations} цитирований</a>', toc_domain_style))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
            story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} статей, {field_citations} цитирований', toc_field_style))
            
            for subfield in subfields.keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} статей, {subfield_citations} цитирований', toc_subfield_style))
        
        story.append(Spacer(1, 0.3*cm))
    
    story.append(PageBreak())
    
    # ========== СТАТЬИ ПО ИЕРАРХИИ С ЯКОРЯМИ ==========
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        
        anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
        story.append(anchor_para)
        
        story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} статей, {domain_citations} цитирований", domain_style))
        story.append(Spacer(1, 0.3*cm))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
            field_anchor_para = Paragraph(f'<a name="{field_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
            story.append(field_anchor_para)
            
            story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} статей, {field_citations} цитирований", field_style))
            story.append(Spacer(1, 0.2*cm))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                subfield_anchor_para = Paragraph(f'<a name="{subfield_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
                story.append(subfield_anchor_para)
                
                story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} статей, {subfield_citations} цитирований", subfield_style))
                story.append(Spacer(1, 0.2*cm))
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)} — {topic_articles} статей, {topic_citations} цитирований", topic_style))
                    story.append(Spacer(1, 0.2*cm))
                    
                    for idx, article in enumerate(articles, 1):
                        title = clean_text(article.get('title', 'Без названия'))
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{idx}. {title}", article_title_style))
                        
                        authors = clean_text(article.get('authors', 'Авторы не указаны'))
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>Авторы:</b> {authors}", authors_style))
                        
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
                        
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{', '.join(meta_parts)}", meta_style))
                        
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        is_highly = article.get('is_highly_cited', False)
                        
                        citation_text = f"<b>Цитирований:</b> {citations} | <b>в год:</b> {citations_per_year}"
                        if is_highly:
                            citation_text += " 🔥 Активно цитируемая"
                        
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{citation_text}", citation_style))
                        
                        doi_url = article.get('doi_url', '')
                        if doi_url:
                            story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>DOI:</b> <link href='{doi_url}'>{doi_url}</link>", meta_style))
                        
                        story.append(Spacer(1, 0.15*cm))
                        
                        if idx < len(articles):
                            story.append(Paragraph("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" + "─" * 40, separator_style))
                            story.append(Spacer(1, 0.1*cm))
                    
                    story.append(Spacer(1, 0.3*cm))
                
                story.append(Spacer(1, 0.2*cm))
            
            story.append(Spacer(1, 0.3*cm))
        
        story.append(PageBreak())
    
    # ========== ЗАКЛЮЧЕНИЕ ==========
    story.append(Paragraph("Заключение", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    conclusion_text = f"""
    Данный отчет содержит {total_articles} статей из журнала «{clean_text(journal_name)}», 
    сгруппированных по иерархической структуре: {total_domains} областей науки, 
    включающих множество полей и подполей. Из них {highly_cited} статей 
    являются активно цитируемыми, что делает их особенно ценными для включения в Ваши научные работы.<br/><br/>
    
    Рекомендуем обратить особое внимание на статьи с пометкой «Активно цитируемая» — 
    они демонстрируют высокий научный интерес и могут стать важной частью Вашего исследования.<br/><br/>
    
    Отчет сгенерирован автоматически с использованием данных OpenAlex API.
    """
    
    story.append(Paragraph(conclusion_text, conclusion_style))
    
    story.append(Spacer(1, 1*cm))
    
    # Добавляем логотип ПРИЛОЖЕНИЯ в футер
    if app_logo_path and os.path.exists(app_logo_path):
        try:
            from PIL import Image as PILImage
            pil_img = PILImage.open(app_logo_path)
            original_width, original_height = pil_img.size
            pil_img.close()
            
            max_width = 80
            max_height = 40
            width_ratio = max_width / original_width
            height_ratio = max_height / original_height
            scale_ratio = min(width_ratio, height_ratio)
            new_width = original_width * scale_ratio
            new_height = original_height * scale_ratio
            
            footer_logo = Image(app_logo_path, width=new_width, height=new_height)
            footer_logo.hAlign = 'CENTER'
            story.append(footer_logo)
            story.append(Spacer(1, 0.3*cm))
        except Exception as e:
            logger.warning(f"Could not load footer logo: {e}")
    
    story.append(Paragraph(f"© {clean_text(journal_name)} | {datetime.now().strftime('%d.%m.%Y')}", footer_style))
    story.append(Paragraph("Отчет подготовлен с использованием CTA Journal Analyzer Pro", footer_style))
    
    doc.build(story)
    
    return buffer.getvalue()

# ============================================================================
# ГЕНЕРАЦИЯ PDF ОТЧЕТА (АНГЛИЙСКИЙ) С ИЕРАРХИЕЙ
# ============================================================================

def generate_pdf_en(journal_name: str, journal_abbr: str, years: List[int], 
                    hierarchy: Dict, logo_path: str = None, custom_message: str = None,
                    app_logo_path: str = None) -> bytes:
    """Генерация PDF отчета на английском языке с иерархической группировкой"""
    
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
    
    # Рассчитываем статистику
    stats = calculate_hierarchy_statistics(hierarchy)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values())
    highly_cited = sum(1 for domain in hierarchy.values() 
                      for field in domain.values()
                      for subfield in field.values()
                      for topic in subfield.values()
                      for a in topic if a.get('is_highly_cited', False))
    
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
    
    domain_style = ParagraphStyle(
        'DomainStyle',
        parent=styles['Heading3'],
        fontSize=18,
        textColor=colors.HexColor('#667eea'),
        spaceAfter=10,
        spaceBefore=20,
        fontName='Helvetica-Bold'
    )
    
    field_style = ParagraphStyle(
        'FieldStyle',
        parent=styles['Normal'],
        fontSize=15,
        textColor=colors.HexColor('#764ba2'),
        spaceAfter=8,
        spaceBefore=12,
        leftIndent=20,
        fontName='Helvetica-Bold'
    )
    
    subfield_style = ParagraphStyle(
        'SubfieldStyle',
        parent=styles['Normal'],
        fontSize=13,
        textColor=colors.HexColor('#16A085'),
        spaceAfter=8,
        spaceBefore=10,
        leftIndent=40,
        fontName='Helvetica-Bold'
    )
    
    topic_style = ParagraphStyle(
        'TopicStyle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=8,
        spaceBefore=8,
        leftIndent=60,
        fontName='Helvetica-Bold'
    )
    
    article_title_style = ParagraphStyle(
        'ArticleTitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=5,
        leftIndent=80,
        fontName='Helvetica'
    )
    
    authors_style = ParagraphStyle(
        'AuthorsStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=3,
        leftIndent=80,
        fontName='Helvetica'
    )
    
    meta_style = ParagraphStyle(
        'MetaStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#7F8C8D'),
        spaceAfter=3,
        leftIndent=80,
        fontName='Helvetica'
    )
    
    citation_style = ParagraphStyle(
        'CitationStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#27AE60'),
        spaceAfter=3,
        leftIndent=80,
        fontName='Helvetica-Bold'
    )
    
    toc_domain_style = ParagraphStyle(
        'TOCDomainStyle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.HexColor('#667eea'),
        spaceAfter=6,
        fontName='Helvetica-Bold'
    )
    
    toc_field_style = ParagraphStyle(
        'TOCFieldStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#764ba2'),
        spaceAfter=4,
        leftIndent=15,
        fontName='Helvetica'
    )
    
    toc_subfield_style = ParagraphStyle(
        'TOCSubfieldStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#16A085'),
        spaceAfter=3,
        leftIndent=30,
        fontName='Helvetica'
    )
    
    intro_style = ParagraphStyle(
        'IntroStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=20,
        alignment=TA_JUSTIFY,
        fontName='Helvetica'
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
    
    separator_style = ParagraphStyle(
        'Separator',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#BDC3C7'),
        alignment=TA_CENTER,
        fontName='Helvetica'
    )
    
    conclusion_style = ParagraphStyle(
        'ConclusionStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=20,
        alignment=TA_JUSTIFY,
        fontName='Helvetica'
    )
    
    story = []
    
    # ========== COVER PAGE ==========
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
            logger.warning(f"Could not load logo: {e}")
    
    story.append(Paragraph("Analytical Report", title_style))
    story.append(Paragraph(f"«{clean_text(journal_name)}»", subtitle_style))
    story.append(Spacer(1, 1*cm))
    
    years_str = format_year_filter_for_filename(years)
    story.append(Paragraph(f"Publication period: {years_str}", subtitle_style))
    story.append(Spacer(1, 1.5*cm))
    
    # Настраиваемый текст или стандартный
    if custom_message:
        intro_text = format_message_with_variables(custom_message, clean_text(journal_name), years_str)
    else:
        default_msg = DEFAULT_MESSAGES['en']['body']
        intro_text = format_message_with_variables(default_msg, clean_text(journal_name), years_str)
    
    # Сохраняем переносы строк
    intro_text_formatted = intro_text.replace('\n\n', '<br/><br/>').replace('\n', '<br/>')
    story.append(Paragraph(intro_text_formatted, intro_style))
    
    story.append(Spacer(1, 1*cm))
    
    stats_data = [
        ["Metric", "Value"],
        ["Total Articles", str(total_articles)],
        ["Research Domains", str(total_domains)],
        ["Total Citations", str(total_citations)]
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
    
    # ========== TABLE OF CONTENTS (Domain -> Field -> Subfield) ==========
    story.append(Paragraph("Table of Contents", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        
        anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
        story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} articles, {domain_citations} citations</a>', toc_domain_style))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
            story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} articles, {field_citations} citations', toc_field_style))
            
            for subfield in subfields.keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} articles, {subfield_citations} citations', toc_subfield_style))
        
        story.append(Spacer(1, 0.3*cm))
    
    story.append(PageBreak())
    
    # ========== ARTICLES BY HIERARCHY WITH ANCHORS ==========
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        
        anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
        story.append(anchor_para)
        
        story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} articles, {domain_citations} citations", domain_style))
        story.append(Spacer(1, 0.3*cm))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
            field_anchor_para = Paragraph(f'<a name="{field_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
            story.append(field_anchor_para)
            
            story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} articles, {field_citations} citations", field_style))
            story.append(Spacer(1, 0.2*cm))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                subfield_anchor_para = Paragraph(f'<a name="{subfield_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
                story.append(subfield_anchor_para)
                
                story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} articles, {subfield_citations} citations", subfield_style))
                story.append(Spacer(1, 0.2*cm))
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)} — {topic_articles} articles, {topic_citations} citations", topic_style))
                    story.append(Spacer(1, 0.2*cm))
                    
                    for idx, article in enumerate(articles, 1):
                        title = clean_text(article.get('title', 'No title'))
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{idx}. {title}", article_title_style))
                        
                        authors = clean_text(article.get('authors', 'Authors not specified'))
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>Authors:</b> {authors}", authors_style))
                        
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
                        
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{', '.join(meta_parts)}", meta_style))
                        
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        is_highly = article.get('is_highly_cited', False)
                        
                        citation_text = f"<b>Citations:</b> {citations} | <b>per year:</b> {citations_per_year}"
                        if is_highly:
                            citation_text += " 🔥 Highly Cited"
                        
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{citation_text}", citation_style))
                        
                        doi_url = article.get('doi_url', '')
                        if doi_url:
                            story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>DOI:</b> <link href='{doi_url}'>{doi_url}</link>", meta_style))
                        
                        story.append(Spacer(1, 0.15*cm))
                        
                        if idx < len(articles):
                            story.append(Paragraph("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" + "─" * 40, separator_style))
                            story.append(Spacer(1, 0.1*cm))
                    
                    story.append(Spacer(1, 0.3*cm))
                
                story.append(Spacer(1, 0.2*cm))
            
            story.append(Spacer(1, 0.3*cm))
        
        story.append(PageBreak())
    
    # ========== CONCLUSION ==========
    story.append(Paragraph("Conclusion", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    conclusion_text = f"""
    This report contains {total_articles} articles from «{clean_text(journal_name)}», 
    grouped into a hierarchical structure: {total_domains} research domains, 
    encompassing multiple fields and subfields. Among them, {highly_cited} articles 
    are highly cited, making them particularly valuable for inclusion in your research.<br/><br/>
    
    We recommend paying special attention to articles marked as "Highly Cited" — 
    they demonstrate significant scientific interest and can become an important part 
    of your research.<br/><br/>
    
    This report was automatically generated using OpenAlex API data.
    """
    
    story.append(Paragraph(conclusion_text, conclusion_style))
    
    story.append(Spacer(1, 1*cm))
    
    # Добавляем логотип в футер, если он есть
    if app_logo_path and os.path.exists(app_logo_path):
        try:
            from PIL import Image as PILImage
            pil_img = PILImage.open(app_logo_path)
            original_width, original_height = pil_img.size
            pil_img.close()
            
            max_width = 80
            max_height = 40
            width_ratio = max_width / original_width
            height_ratio = max_height / original_height
            scale_ratio = min(width_ratio, height_ratio)
            new_width = original_width * scale_ratio
            new_height = original_height * scale_ratio
            
            footer_logo = Image(app_logo_path, width=new_width, height=new_height)
            footer_logo.hAlign = 'CENTER'
            story.append(footer_logo)
            story.append(Spacer(1, 0.3*cm))
        except Exception as e:
            logger.warning(f"Could not load footer logo: {e}")
    
    story.append(Paragraph(f"© {clean_text(journal_name)} | {datetime.now().strftime('%d.%m.%Y')}", footer_style))
    story.append(Paragraph("Report generated using CTA Journal Analyzer Pro", footer_style))
    
    doc.build(story)
    
    return buffer.getvalue()

# ============================================================================
# ГЕНЕРАЦИЯ TXT ОТЧЕТА (РУССКИЙ) С ИЕРАРХИЕЙ
# ============================================================================

def generate_txt_ru(journal_name: str, years: List[int], hierarchy: Dict, custom_message: str = None) -> str:
    """Генерация TXT отчета на русском языке с иерархической группировкой"""
    
    output = []
    
    years_str = format_year_filter_for_filename(years)
    
    # Рассчитываем статистику
    stats = calculate_hierarchy_statistics(hierarchy)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values())
    highly_cited = sum(1 for domain in hierarchy.values() 
                      for field in domain.values()
                      for subfield in field.values()
                      for topic in subfield.values()
                      for a in topic if a.get('is_highly_cited', False))
    
    # Заголовок
    output.append("=" * 80)
    output.append(f"АНАЛИТИЧЕСКИЙ ОТЧЕТ")
    output.append(f"Журнал: {journal_name}")
    output.append(f"Период публикации: {years_str}")
    output.append("=" * 80)
    output.append("")
    
    # Вступительное обращение (настраиваемое)
    if custom_message:
        intro_text = format_message_with_variables(custom_message, journal_name, years_str)
    else:
        intro_text = format_message_with_variables(DEFAULT_MESSAGES['ru']['body'], journal_name, years_str)
    
    output.append(intro_text)
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Статистика
    output.append("СТАТИСТИКА")
    output.append("-" * 40)
    output.append(f"Всего статей: {total_articles}")
    output.append(f"Областей науки: {total_domains}")
    output.append(f"Всего цитирований: {total_citations}")
    output.append(f"Активно цитируемые статьи: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Содержание (оглавление по иерархии)
    output.append("СОДЕРЖАНИЕ")
    output.append("-" * 40)
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        output.append(f"{domain} — {domain_articles} статей, {domain_citations} цитирований")
        
        for field in fields.keys():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            output.append(f"  └── {field} — {field_articles} статей, {field_citations} цитирований")
            
            for subfield in fields[field].keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                output.append(f"      └── {subfield} — {subfield_articles} статей, {subfield_citations} цитирований")
    
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Статьи по иерархии
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        
        output.append("")
        output.append("█" * 80)
        output.append(f"ОБЛАСТЬ: {domain} — {domain_articles} статей, {domain_citations} цитирований")
        output.append("█" * 80)
        output.append("")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            
            output.append(f"▓▓▓ ПОЛЕ: {field} — {field_articles} статей, {field_citations} цитирований ▓▓▓")
            output.append("")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                
                output.append(f"▒▒▒ ПОДПОЛЕ: {subfield} — {subfield_articles} статей, {subfield_citations} цитирований ▒▒▒")
                output.append("")
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    
                    output.append(f"  ● ТЕМА: {topic} — {topic_articles} статей, {topic_citations} цитирований")
                    output.append("")
                    
                    for idx, article in enumerate(articles, 1):
                        output.append(f"    {idx}. {article.get('title', 'Без названия')}")
                        output.append(f"       Авторы: {article.get('authors', 'Авторы не указаны')}")
                        
                        meta_parts = [f"       {article.get('journal_name', journal_name)}"]
                        if article.get('publication_year'):
                            meta_parts.append(str(article.get('publication_year')))
                        if article.get('volume'):
                            meta_parts.append(f"Том {article.get('volume')}")
                        if article.get('issue'):
                            meta_parts.append(f"Вып. {article.get('issue')}")
                        if article.get('pages'):
                            meta_parts.append(f"С. {article.get('pages')}")
                        
                        output.append(", ".join(meta_parts))
                        
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        highly = " 🔥 АКТИВНО ЦИТИРУЕМАЯ" if article.get('is_highly_cited') else ""
                        output.append(f"       Цитирований: {citations} | в год: {citations_per_year}{highly}")
                        
                        if article.get('doi_url'):
                            output.append(f"       DOI: {article.get('doi_url')}")
                        
                        output.append("")
                    
                    output.append("")
                
                output.append("")
            
            output.append("")
        
        output.append("")
    
    # Заключение
    output.append("=" * 80)
    output.append("ЗАКЛЮЧЕНИЕ")
    output.append("=" * 80)
    output.append("")
    output.append(f"Данный отчет содержит {total_articles} статей из журнала «{journal_name}»,")
    output.append(f"сгруппированных по иерархической структуре: {total_domains} областей науки,")
    output.append(f"включающих множество полей и подполей. Из них {highly_cited} статей")
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
# ГЕНЕРАЦИЯ TXT ОТЧЕТА (АНГЛИЙСКИЙ) С ИЕРАРХИЕЙ
# ============================================================================

def generate_txt_en(journal_name: str, years: List[int], hierarchy: Dict, custom_message: str = None) -> str:
    """Генерация TXT отчета на английском языке с иерархической группировкой"""
    
    output = []
    
    years_str = format_year_filter_for_filename(years)
    
    # Рассчитываем статистику
    stats = calculate_hierarchy_statistics(hierarchy)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values())
    highly_cited = sum(1 for domain in hierarchy.values() 
                      for field in domain.values()
                      for subfield in field.values()
                      for topic in subfield.values()
                      for a in topic if a.get('is_highly_cited', False))
    
    # Header
    output.append("=" * 80)
    output.append(f"ANALYTICAL REPORT")
    output.append(f"Journal: {journal_name}")
    output.append(f"Publication period: {years_str}")
    output.append("=" * 80)
    output.append("")
    
    # Introduction (customizable)
    if custom_message:
        intro_text = format_message_with_variables(custom_message, journal_name, years_str)
    else:
        intro_text = format_message_with_variables(DEFAULT_MESSAGES['en']['body'], journal_name, years_str)
    
    output.append(intro_text)
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Statistics
    output.append("STATISTICS")
    output.append("-" * 40)
    output.append(f"Total Articles: {total_articles}")
    output.append(f"Research Domains: {total_domains}")
    output.append(f"Total Citations: {total_citations}")
    output.append(f"Highly Cited Articles: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Table of Contents
    output.append("TABLE OF CONTENTS")
    output.append("-" * 40)
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        output.append(f"{domain} — {domain_articles} articles, {domain_citations} citations")
        
        for field in fields.keys():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            output.append(f"  └── {field} — {field_articles} articles, {field_citations} citations")
            
            for subfield in fields[field].keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                output.append(f"      └── {subfield} — {subfield_articles} articles, {subfield_citations} citations")
    
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Articles by hierarchy
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        domain_citations = domain_stats.get('citations', 0)
        
        output.append("")
        output.append("█" * 80)
        output.append(f"DOMAIN: {domain} — {domain_articles} articles, {domain_citations} citations")
        output.append("█" * 80)
        output.append("")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            field_citations = field_stats.get('citations', 0)
            
            output.append(f"▓▓▓ FIELD: {field} — {field_articles} articles, {field_citations} citations ▓▓▓")
            output.append("")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                subfield_citations = subfield_stats.get('citations', 0)
                
                output.append(f"▒▒▒ SUBFIELD: {subfield} — {subfield_articles} articles, {subfield_citations} citations ▒▒▒")
                output.append("")
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    
                    output.append(f"  ● TOPIC: {topic} — {topic_articles} articles, {topic_citations} citations")
                    output.append("")
                    
                    for idx, article in enumerate(articles, 1):
                        output.append(f"    {idx}. {article.get('title', 'No title')}")
                        output.append(f"       Authors: {article.get('authors', 'Authors not specified')}")
                        
                        meta_parts = [f"       {article.get('journal_name', journal_name)}"]
                        if article.get('publication_year'):
                            meta_parts.append(str(article.get('publication_year')))
                        if article.get('volume'):
                            meta_parts.append(f"Volume {article.get('volume')}")
                        if article.get('issue'):
                            meta_parts.append(f"Issue {article.get('issue')}")
                        if article.get('pages'):
                            meta_parts.append(f"pp. {article.get('pages')}")
                        
                        output.append(", ".join(meta_parts))
                        
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        highly = " 🔥 HIGHLY CITED" if article.get('is_highly_cited') else ""
                        output.append(f"       Citations: {citations} | per year: {citations_per_year}{highly}")
                        
                        if article.get('doi_url'):
                            output.append(f"       DOI: {article.get('doi_url')}")
                        
                        output.append("")
                    
                    output.append("")
                
                output.append("")
            
            output.append("")
        
        output.append("")
    
    # Conclusion
    output.append("=" * 80)
    output.append("CONCLUSION")
    output.append("=" * 80)
    output.append("")
    output.append(f"This report contains {total_articles} articles from «{journal_name}»,")
    output.append(f"grouped into a hierarchical structure: {total_domains} research domains,")
    output.append(f"encompassing multiple fields and subfields. Among them, {highly_cited} articles")
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
    if 'hierarchy' not in st.session_state:
        st.session_state.hierarchy = None
    if 'selected_years' not in st.session_state:
        st.session_state.selected_years = None
    if 'years_input' not in st.session_state:
        st.session_state.years_input = ""
    if 'custom_message_en' not in st.session_state:
        st.session_state.custom_message_en = DEFAULT_MESSAGES['en']['body']
    if 'custom_message_ru' not in st.session_state:
        st.session_state.custom_message_ru = DEFAULT_MESSAGES['ru']['body']
    
    # Заголовок
    if hasattr(st.session_state, 'app_logo') and st.session_state.app_logo is not None:
        # Центрируем логотип
        col1, col2, col3 = st.columns([3, 3, 10])
        with col2:
            st.image(st.session_state.app_logo, use_container_width=True)
    else:
        # Если логотипа нет, показываем заголовок как раньше
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
                                        hierarchy = group_articles_by_hierarchy(articles)
                                        st.session_state.articles = articles
                                        st.session_state.hierarchy = hierarchy
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
        hierarchy = st.session_state.hierarchy
        years = st.session_state.selected_years
        
        # Рассчитываем статистику для отображения
        stats = calculate_hierarchy_statistics(hierarchy)
        total_articles = sum(s['articles'] for s in stats.values())
        total_domains = len(hierarchy)
        total_citations = sum(s['citations'] for s in stats.values())
        highly_cited = sum(1 for domain in hierarchy.values() 
                          for field in domain.values()
                          for subfield in field.values()
                          for topic in subfield.values()
                          for a in topic if a.get('is_highly_cited', False))
        
        if total_articles > 0:
            # Метрики в красивых карточках
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{total_articles:,}</div>
                    <div class="metric-label">{t['total_articles']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{total_domains}</div>
                    <div class="metric-label">{t['total_topics']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col3:
                avg_citations = total_citations / total_articles if total_articles > 0 else 0
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{avg_citations:.1f}</div>
                    <div class="metric-label">{t['avg_citations']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col4:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{highly_cited}</div>
                    <div class="metric-label">{t['highly_cited']}</div>
                </div>
                """, unsafe_allow_html=True)
            
            # Раздел настройки сообщения
            st.markdown("---")
            st.markdown(f"### ✏️ {t['customize_message']}")
            
            with st.expander(f"📝 {t['customize_message']} ({language})"):
                if language == "English":
                    custom_msg = st.text_area(
                        t['message_preview'],
                        value=st.session_state.custom_message_en,
                        height=300,
                        key="msg_editor_en"
                    )
                    if st.button(t['use_default'], key="reset_en"):
                        st.session_state.custom_message_en = DEFAULT_MESSAGES['en']['body']
                        st.rerun()
                else:
                    custom_msg = st.text_area(
                        t['message_preview'],
                        value=st.session_state.custom_message_ru,
                        height=300,
                        key="msg_editor_ru"
                    )
                    if st.button(t['use_default'], key="reset_ru"):
                        st.session_state.custom_message_ru = DEFAULT_MESSAGES['ru']['body']
                        st.rerun()
                
                # Сохраняем сообщение в session_state
                if language == "English":
                    st.session_state.custom_message_en = custom_msg
                else:
                    st.session_state.custom_message_ru = custom_msg
            
            # Отображение иерархии в UI
            st.markdown("---")
            st.markdown("### 📊 Research Hierarchy")
            
            for domain, fields in hierarchy.items():
                domain_stats = stats.get(domain, {})
                domain_articles = domain_stats.get('articles', 0)
                domain_citations = domain_stats.get('citations', 0)
                
                with st.expander(f"🌍 {domain} — {domain_articles} {t['articles_count']}, {domain_citations} {t['citations']}"):
                    for field, subfields in fields.items():
                        field_stats = domain_stats.get('fields', {}).get(field, {})
                        field_articles = field_stats.get('articles', 0)
                        field_citations = field_stats.get('citations', 0)
                        
                        st.markdown(f"**📁 {field}** — {field_articles} {t['articles_count']}, {field_citations} {t['citations']}")
                        
                        for subfield, topics in subfields.items():
                            subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                            subfield_articles = subfield_stats.get('articles', 0)
                            subfield_citations = subfield_stats.get('citations', 0)
                            
                            st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;**📂 {subfield}** — {subfield_articles} {t['articles_count']}, {subfield_citations} {t['citations']}")
                            
                            for topic, articles in topics.items():
                                topic_articles = len(articles)
                                topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                                
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**🔬 {topic}** — {topic_articles} {t['articles_count']}, {topic_citations} {t['citations']}")
                                
                                for idx, article in enumerate(articles[:5]):  # Показываем первые 5 статей для компактности
                                    st.markdown(f"""
                                    <div style="padding: 8px; margin: 4px 0 4px 60px; background: #f8f9fa; border-radius: 8px; font-size: 0.85rem;">
                                        <b>{idx+1}. {article.get('title', 'No title')[:80]}{'...' if len(article.get('title', '')) > 80 else ''}</b><br>
                                        👤 {article.get('authors', 'N/A')[:80]}<br>
                                        📊 {t['citations']}: {article.get('cited_by_count', 0)} ({t['citations_per_year']}: {article.get('citations_per_year', 0)})
                                        {f' 🔥' if article.get('is_highly_cited') else ''}<br>
                                        🔗 <a href="{article.get('doi_url', '#')}" target="_blank">{t['view_article']}</a>
                                    </div>
                                    """, unsafe_allow_html=True)
                                
                                if len(articles) > 5:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;*... и {len(articles) - 5} других статей*")
            
            # Экспорт
            st.markdown("---")
            st.markdown(f"### {t['export_btn']}")
            
            journal_abbr = generate_journal_abbreviation(journal_name)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**PDF Reports**")
                
                # PDF English
                pdf_en_data = generate_pdf_en(journal_name, journal_abbr, years, hierarchy, 
                                              st.session_state.journal_logo, 
                                              st.session_state.custom_message_en,
                                              app_logo_path="logo.png")
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
                pdf_ru_data = generate_pdf_ru(journal_name, journal_abbr, years, hierarchy, 
                                              st.session_state.journal_logo,
                                              st.session_state.custom_message_ru,
                                              app_logo_path="logo.png")
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
                txt_en_data = generate_txt_en(journal_name, years, hierarchy, st.session_state.custom_message_en)
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
                txt_ru_data = generate_txt_ru(journal_name, years, hierarchy, st.session_state.custom_message_ru)
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
                                'hierarchy', 'selected_years', 'years_input']
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
    st.markdown("""
    <div class="footer">
        <p>© CTA, https://chimicatechnoacta.ru / developed by daM©</p>
        <p style="font-size: 0.7rem; color: #aaa;">v3.0 - Journal Analyzer Pro</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
