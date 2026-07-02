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

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# App settings
st.set_page_config(
    page_title="Journal Article Analyzer Pro",
    page_icon="logo1.png",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# MULTILINGUAL SUPPORT
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
        'back_btn': '← Back',
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
        'citations_count_label': 'citations',
        'research_hierarchy': '📊 Research Hierarchy',
        'pdf_reports': 'PDF Reports',
        'txt_reports': 'TXT Reports',
        'include_metrics': '✅ Include citation metrics in Table of Contents',
        'highly_cited_threshold_total': 'Highly Cited threshold - Total citations >',
        'highly_cited_threshold_per_year': 'Highly Cited threshold - Citations per year >',
        'domain_icon': '🌍',
        'field_icon': '📁',
        'subfield_icon': '📂',
        'topic_icon': '🔬',
        'authors_icon': '👤',
        'link_icon': '🔗',
        'hot_topics_dashboard': '🔥 Hot Topics Dashboard',
        'citation_dynamics': '📈 Citation Dynamics',
        'author_dashboard': '👥 Author Dashboard',
        'predictor_dashboard': '🔮 Citation Predictor',
        'editors_choice': '⭐ Editor\'s Choice',
        'editorial_dashboard': '📊 Editorial Dashboard'
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
        'back_btn': '← Назад',
        'step2_title': 'Шаг 2: Выбор годов публикации',
        'step2_desc': 'Выберите период для анализа',
        'years_label': 'Годы публикации',
        'years_help': 'Формат: 2021 или 2021,2023-2025 или 2023-2026',
        'analyze_btn': '🔍 Анализировать журнал',
        'step3_title': 'Шаг 3: Результаты анализа',
        'step3_desc': 'Статьи сгруппированы по исследовательским темам',
        'total_articles': 'Всего статей',
        'total_topics': 'Тем исследований',
        'avg_citations': 'Среднее цитирование',
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
        'citations_count_label': 'цитирований',
        'research_hierarchy': '📊 Иерархия исследований',
        'pdf_reports': 'PDF отчеты',
        'txt_reports': 'TXT отчеты',
        'include_metrics': '✅ Включить метрики цитирования в оглавление',
        'highly_cited_threshold_total': 'Порог активно цитируемых - Всего цитирований >',
        'highly_cited_threshold_per_year': 'Порог активно цитируемых - Цитирований в год >',
        'domain_icon': '🌍',
        'field_icon': '📁',
        'subfield_icon': '📂',
        'topic_icon': '🔬',
        'authors_icon': '👤',
        'link_icon': '🔗',
        'hot_topics_dashboard': '🔥 Дашборд горячих тем',
        'citation_dynamics': '📈 Динамика цитирования',
        'author_dashboard': '👥 Дашборд авторов',
        'predictor_dashboard': '🔮 Предиктор цитирования',
        'editors_choice': '⭐ Выбор редакции',
        'editorial_dashboard': '📊 Дашборд редакции'
    }
}

# ============================================================================
# CUSTOMIZABLE DEFAULT MESSAGES
# ============================================================================

DEFAULT_MESSAGES = {
    'en': {
        'title': 'Dear Colleagues!',
        'body': """We are pleased to present a curated collection of articles published in the «JOURNAL_NAME» during YEARS. Each paper has undergone rigorous peer-review and represents a complete scientific investigation.

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
# CUSTOM CSS DESIGN
# ============================================================================

st.markdown("""
<style>
    /* Main styles */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    /* Gradient background for main */
    .stApp {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    
    /* Main header with animation */
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
    
    /* Step cards with glass effect */
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
    
    /* Metric cards with gradient */
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
    
    /* Result card */
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
    
    /* Filter section */
    .filter-section {
        background: rgba(255, 255, 255, 0.9);
        backdrop-filter: blur(8px);
        border-radius: 20px;
        padding: 20px;
        margin-bottom: 20px;
        border: 1px solid rgba(102, 126, 234, 0.2);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.04);
    }
    
    /* Custom buttons */
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
    
    /* Custom expander */
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
    
    /* Inputs with focus */
    .stTextInput > div > div > input {
        border-radius: 12px;
        border: 2px solid #e0e0e0;
        transition: all 0.3s ease;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
    }
    
    /* Selectors */
    .stSelectbox > div > div {
        border-radius: 12px;
    }
    
    /* Progress bar */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea, #764ba2, #f093fb);
    }
    
    /* Info box */
    .stAlert {
        border-radius: 16px;
        border-left: 4px solid #667eea;
    }
    
    /* Scrollbar */
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
    
    /* Loading animation */
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
    
    /* Citation badge */
    .citation-badge {
        display: inline-block;
        background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        color: #d63031;
    }
    
    /* Gradient divider */
    .gradient-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, #667eea, #764ba2, #f093fb, transparent);
        margin: 20px 0;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        padding: 20px;
        color: #6c757d;
        font-size: 0.8rem;
        border-top: 1px solid rgba(102, 126, 234, 0.2);
        margin-top: 40px;
    }
    
    /* Custom tab */
    .custom-tab {
        background: white;
        border-radius: 12px;
        padding: 8px 16px;
        cursor: pointer;
        transition: all 0.2s;
    }
    
    /* Message editor style */
    .message-editor {
        background: white;
        border-radius: 16px;
        padding: 16px;
        border: 1px solid #e0e0e0;
        margin-bottom: 16px;
    }
    
    /* Animated gradient */
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
# OPENALEX API CONFIGURATION
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
# SQLITE CACHING
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
# ISSN PARSING
# ============================================================================

def parse_issn(issn_input: str) -> Optional[str]:
    """
    Parse ISSN from various formats:
    - "1234-5678" -> "12345678"
    - "1234 5678" -> "12345678"
    - "12345678" -> "12345678"
    - "ISSN 1234-5678" -> "12345678"
    """
    if not issn_input:
        return None
    
    # Remove ISSN prefix if present
    issn_clean = re.sub(r'^ISSN\s*', '', issn_input, flags=re.IGNORECASE)
    
    # Keep only digits
    digits = re.sub(r'[^0-9]', '', issn_clean)
    
    # ISSN must be 8 digits
    if len(digits) == 8:
        return digits
    elif len(digits) == 7:
        logger.warning(f"ISSN has 7 digits: {digits}")
        return None
    
    return None

# ============================================================================
# JOURNAL SEARCH IN OPENALEX
# ============================================================================

def get_journal_by_issn(issn: str) -> Optional[dict]:
    """
    Search for journal in OpenAlex by ISSN.
    """
    # Check cache
    cached = get_cached_source(issn)
    if cached:
        logger.info(f"Using cached journal data for ISSN {issn}")
        return cached
    
    # Format ISSN as XXXX-XXXX for OpenAlex
    issn_clean = re.sub(r'[^0-9X]', '', issn.upper())
    if len(issn_clean) == 8:
        issn_formatted = f"{issn_clean[:4]}-{issn_clean[4:]}"
    else:
        issn_formatted = issn
    
    logger.info(f"Searching for journal with ISSN {issn_formatted}")
    
    try:
        # OpenAlex uses ISSN-L or regular ISSN
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
                # Try searching through primary_location.source.issn in works
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
                        # Extract journal info from first work
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
# JOURNAL ARTICLES LOADING
# ============================================================================

def parse_year_filter(year_input: str) -> List[int]:
    """
    Parse year filter string.
    Examples:
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
    Format year list for filename.
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
    Fetch all journal articles for specified years.
    """
    year_filter_str = ",".join(map(str, years))
    cache_key = f"{source_id}_{year_filter_str}"
    
    # Check cache
    cached = get_cached_source_works(source_id, year_filter_str)
    if cached:
        logger.info(f"Using cached articles for {source_id}, years {years}")
        return cached.get('articles', [])
    
    logger.info(f"Fetching articles for source {source_id}, years {years}")
    
    all_articles = []
    cursor = "*"
    page_count = 0
    total_count = 0
    
    # Use more reliable filter via primary_location.source.id
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
        
        # Save to cache
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
# CITATION METRICS CALCULATION
# ============================================================================

def calculate_citation_activity(work: dict, current_year: int = None, 
                                threshold_total: int = None, 
                                threshold_per_year: int = None) -> Tuple[int, float, bool]:
    """
    Calculate citation metrics for an article.
    
    Returns:
        Tuple[citations_total, citations_per_year, is_highly_cited]
    """
    citations_total = work.get('cited_by_count', 0)
    
    publication_year = work.get('publication_year', 0)
    if current_year is None:
        current_year = datetime.now().year
    
    # Article age in years (minimum 1 year)
    age = max(1, current_year - publication_year) if publication_year > 0 else 1
    
    citations_per_year = citations_total / age
    
    # Determine if highly cited based on thresholds
    is_highly_cited = False
    
    if threshold_total is not None and threshold_per_year is not None:
        is_highly_cited = (citations_total > threshold_total) or (citations_per_year > threshold_per_year)
    elif threshold_total is not None:
        is_highly_cited = (citations_total > threshold_total)
    elif threshold_per_year is not None:
        is_highly_cited = (citations_per_year > threshold_per_year)
    else:
        is_highly_cited = False
    
    return citations_total, citations_per_year, is_highly_cited

# ============================================================================
# ARTICLE DATA ENRICHMENT
# ============================================================================

def extract_topic_hierarchy(article: dict) -> Tuple[str, str, str, str]:
    """
    Extract topic hierarchy from article's primary_topic.
    
    Returns:
        Tuple[domain, field, subfield, topic]
    """
    primary_topic = article.get('primary_topic', {})
    
    if not primary_topic:
        return ("Unidentified", "Unidentified", "Unidentified", "Unidentified")
    
    # Extract Domain
    domain_obj = primary_topic.get('domain', {})
    domain = domain_obj.get('display_name', 'Unidentified') if domain_obj else 'Unidentified'
    
    # Extract Field
    field_obj = primary_topic.get('field', {})
    field = field_obj.get('display_name', 'Unidentified') if field_obj else 'Unidentified'
    
    # Extract Subfield
    subfield_obj = primary_topic.get('subfield', {})
    subfield = subfield_obj.get('display_name', 'Unidentified') if subfield_obj else 'Unidentified'
    
    # Extract Topic
    topic = primary_topic.get('display_name', 'Unidentified')
    
    return (domain, field, subfield, topic)

def enrich_article_data(article: dict, threshold_total: int = None, threshold_per_year: int = None) -> dict:
    """
    Enrich article data with complete information.
    """
    if not article:
        return {}
    
    doi_raw = article.get('doi')
    doi_clean = ''
    if doi_raw:
        doi_clean = str(doi_raw).replace('https://doi.org/', '')
    
    # Extract publication info
    biblio = article.get('biblio', {})
    volume = biblio.get('volume', '')
    issue = biblio.get('issue', '')
    first_page = biblio.get('first_page', '')
    last_page = biblio.get('last_page', '')
    
    # Format pages
    pages_str = ''
    if first_page and last_page and first_page != last_page:
        pages_str = f"{first_page}-{last_page}"
    elif first_page:
        pages_str = first_page
    elif last_page:
        pages_str = last_page
    
    # Extract authors with proper Cyrillic handling
    authorships = article.get('authorships', [])
    authors = []
    
    for authorship in authorships[:10]:  # Maximum 10 authors
        if authorship:
            author_name = ''
            
            # Try raw_author_name (original spelling)
            if 'raw_author_name' in authorship:
                author_name = authorship.get('raw_author_name', '')
            
            # Try author.display_name
            if not author_name:
                author = authorship.get('author', {})
                if author:
                    author_name = author.get('display_name', '')
            
            # Try direct author field
            if not author_name and 'author' in authorship:
                author_obj = authorship['author']
                if isinstance(author_obj, dict):
                    author_name = author_obj.get('display_name', '')
            
            if author_name:
                # Normalize Unicode
                import unicodedata
                author_name = unicodedata.normalize('NFC', str(author_name))
                
                # Clean problematic characters but keep Cyrillic
                # Allowed: letters (Russian/English), spaces, dots, commas, hyphens, parentheses
                author_name = re.sub(r'[^a-zA-Zа-яА-ЯёЁ\s\.\,\-\'\(\)]', '', author_name)
                
                # Remove extra spaces
                author_name = re.sub(r'\s+', ' ', author_name).strip()
                
                if author_name:
                    authors.append(author_name)
    
    authors_str = ', '.join(authors)
    if len(authorships) > 10:
        authors_str += f" et al. ({len(authorships)} authors total)"
    
    # Get topic hierarchy
    domain, field, subfield, primary_topic = extract_topic_hierarchy(article)
    
    # Calculate citation metrics with thresholds
    citations_total, citations_per_year, is_highly_cited = calculate_citation_activity(
        article, None, threshold_total, threshold_per_year
    )
    
    # Get source (journal) info
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
# HIERARCHICAL ARTICLE GROUPING
# ============================================================================

def group_articles_by_hierarchy(articles: List[dict], threshold_total: int = None, threshold_per_year: int = None) -> Dict[str, Dict[str, Dict[str, Dict[str, List[dict]]]]]:
    """
    Group articles by hierarchy: Domain -> Field -> Subfield -> Topic
    
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
        enriched = enrich_article_data(article, threshold_total, threshold_per_year)
        
        domain = enriched.get('domain', 'Unidentified')
        field = enriched.get('field', 'Unidentified')
        subfield = enriched.get('subfield', 'Unidentified')
        topic = enriched.get('primary_topic', 'Unidentified')
        
        hierarchy[domain][field][subfield][topic].append(enriched)
    
    # Convert defaultdict to regular dict for serialization
    result = {}
    for domain, fields in hierarchy.items():
        result[domain] = {}
        for field, subfields in fields.items():
            result[domain][field] = {}
            for subfield, topics in subfields.items():
                result[domain][field][subfield] = dict(topics)
    
    return result

def calculate_hierarchy_statistics(hierarchy: Dict, include_metrics: bool = True) -> Dict:
    """
    Calculate statistics for each hierarchy level.
    
    Returns:
        {
            "domain_name": {
                "articles": 100,
                "citations": 5000,
                "avg_citations": 50.0,
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
                        'citations': topic_citations if include_metrics else None,
                        'avg_citations': (topic_citations / topic_articles) if (include_metrics and topic_articles > 0) else None,
                        'articles_list': articles
                    }
                    
                    subfield_articles += topic_articles
                    subfield_citations += topic_citations
                
                subfield_stats[subfield] = {
                    'articles': subfield_articles,
                    'citations': subfield_citations if include_metrics else None,
                    'avg_citations': (subfield_citations / subfield_articles) if (include_metrics and subfield_articles > 0) else None,
                    'topics': topic_stats
                }
                
                field_articles += subfield_articles
                field_citations += subfield_citations
            
            field_stats[field] = {
                'articles': field_articles,
                'citations': field_citations if include_metrics else None,
                'avg_citations': (field_citations / field_articles) if (include_metrics and field_articles > 0) else None,
                'subfields': subfield_stats
            }
            
            domain_articles += field_articles
            domain_citations += field_citations
        
        stats[domain] = {
            'articles': domain_articles,
            'citations': domain_citations if include_metrics else None,
            'avg_citations': (domain_citations / domain_articles) if (include_metrics and domain_articles > 0) else None,
            'fields': field_stats
        }
    
    return stats

# ============================================================================
# HIERARCHY SORTING FUNCTIONS
# ============================================================================

def sort_hierarchy_by_rules(hierarchy: Dict, include_metrics: bool = True) -> Dict:
    """
    Sort hierarchy according to rules:
    - If include_metrics = True: sort by avg_citations (descending), then by name alphabetically
    - If include_metrics = False: sort by articles count (descending), then by name alphabetically
    
    Returns sorted hierarchy as OrderedDict
    """
    from collections import OrderedDict
    
    # First calculate statistics for all levels
    stats = calculate_hierarchy_statistics(hierarchy, include_metrics)
    
    sorted_hierarchy = OrderedDict()
    
    # Sort domains
    if include_metrics:
        # Sort by avg_citations (descending), then by name alphabetically
        domains_sorted = sorted(
            hierarchy.keys(),
            key=lambda d: (
                -stats[d].get('avg_citations', 0) if stats[d].get('avg_citations') is not None else -float('inf'),
                d.lower()
            )
        )
    else:
        # Sort by articles count (descending), then by name alphabetically
        domains_sorted = sorted(
            hierarchy.keys(),
            key=lambda d: (-stats[d].get('articles', 0), d.lower())
        )
    
    for domain in domains_sorted:
        fields = hierarchy[domain]
        domain_stats = stats[domain]
        sorted_fields = OrderedDict()
        
        # Sort fields within domain
        if include_metrics:
            fields_sorted = sorted(
                fields.keys(),
                key=lambda f: (
                    -domain_stats['fields'][f].get('avg_citations', 0) if domain_stats['fields'][f].get('avg_citations') is not None else -float('inf'),
                    f.lower()
                )
            )
        else:
            fields_sorted = sorted(
                fields.keys(),
                key=lambda f: (-domain_stats['fields'][f].get('articles', 0), f.lower())
            )
        
        for field in fields_sorted:
            subfields = fields[field]
            field_stats = domain_stats['fields'][field]
            sorted_subfields = OrderedDict()
            
            # Sort subfields within field
            if include_metrics:
                subfields_sorted = sorted(
                    subfields.keys(),
                    key=lambda sf: (
                        -field_stats['subfields'][sf].get('avg_citations', 0) if field_stats['subfields'][sf].get('avg_citations') is not None else -float('inf'),
                        sf.lower()
                    )
                )
            else:
                subfields_sorted = sorted(
                    subfields.keys(),
                    key=lambda sf: (-field_stats['subfields'][sf].get('articles', 0), sf.lower())
                )
            
            for subfield in subfields_sorted:
                topics = subfields[subfield]
                subfield_stats = field_stats['subfields'][subfield]
                sorted_topics = OrderedDict()
                
                # Sort topics within subfield
                if include_metrics:
                    topics_sorted = sorted(
                        topics.keys(),
                        key=lambda t: (
                            -subfield_stats['topics'][t].get('avg_citations', 0) if subfield_stats['topics'][t].get('avg_citations') is not None else -float('inf'),
                            t.lower()
                        )
                    )
                else:
                    topics_sorted = sorted(
                        topics.keys(),
                        key=lambda t: (-subfield_stats['topics'][t].get('articles', 0), t.lower())
                    )
                
                for topic in topics_sorted:
                    sorted_topics[topic] = topics[topic]
                
                sorted_subfields[subfield] = sorted_topics
            
            sorted_fields[field] = sorted_subfields
        
        sorted_hierarchy[domain] = sorted_fields
    
    return sorted_hierarchy

# ============================================================================
# JOURNAL ABBREVIATION GENERATION
# ============================================================================

def generate_journal_abbreviation(journal_name: str) -> str:
    """
    Generate abbreviation from journal name.
    Example: "Journal of Power Sources" -> "JOPS"
    """
    if not journal_name:
        return "JOURNAL"
    
    # Words to ignore
    stop_words = {'of', 'the', 'and', 'for', 'in', 'on', 'at', 'to', 'by', 'with', 'from'}
    
    # Split into words
    words = re.findall(r'[A-Za-z]+', journal_name)
    
    # Take first letters of significant words
    abbreviation_parts = []
    for word in words:
        word_lower = word.lower()
        if word_lower not in stop_words and len(word) > 2:
            abbreviation_parts.append(word[0].upper())
        elif len(abbreviation_parts) == 0 and len(words) <= 3:
            # If journal is short, take first letters of all words
            abbreviation_parts.append(word[0].upper())
    
    # If abbreviation is too short (less than 3 letters)
    if len(abbreviation_parts) < 3 and len(words) > 0:
        # Take first 3-4 letters of first significant word
        for word in words:
            if word.lower() not in stop_words:
                abbreviation_parts = [word[:4].upper()]
                break
    
    abbreviation = ''.join(abbreviation_parts)
    
    # If still empty, take first 4 letters of first word
    if not abbreviation and words:
        abbreviation = words[0][:4].upper()
    
    return abbreviation if abbreviation else "JOURNAL"

def generate_filename(journal_abbr: str, years: List[int], language: str, extension: str) -> str:
    """
    Generate filename in format: JOPS_2024,2026_en.pdf
    """
    years_str = format_year_filter_for_filename(years)
    return f"{journal_abbr}_{years_str}_{language}.{extension}"

def format_message_with_variables(message: str, journal_name: str, years_str: str) -> str:
    """Replace variables in message with actual values"""
    message = message.replace('JOURNAL_NAME', journal_name)
    message = message.replace('YEARS', years_str)
    return message

# ============================================================================
# NEW: HOT TOPICS ANALYZER
# ============================================================================

class HotTopicsAnalyzer:
    """
    Analyzes topic dynamics and calculates "hotness" metrics
    """
    
    def __init__(self, articles: List[dict]):
        self.articles = articles
        self.current_year = datetime.now().year
        self.articles_by_year = self._group_by_year(articles)
        
    def _group_by_year(self, articles: List[dict]) -> Dict[int, List[dict]]:
        """Groups articles by publication year"""
        by_year = defaultdict(list)
        for article in articles:
            year = article.get('publication_year', 0)
            if year > 0:
                by_year[year].append(article)
        return dict(by_year)
    
    def _get_yearly_counts(self, topic_articles: List[dict]) -> Dict[int, int]:
        """Gets number of articles per year for a topic"""
        counts = defaultdict(int)
        for article in topic_articles:
            year = article.get('publication_year', 0)
            if year > 0:
                counts[year] += 1
        return dict(counts)
    
    def _get_citations_by_year(self, topic_articles: List[dict]) -> Dict[int, int]:
        """Gets citations per year for a topic"""
        citations = defaultdict(int)
        for article in topic_articles:
            year = article.get('publication_year', 0)
            if year > 0:
                citations[year] += article.get('cited_by_count', 0)
        return dict(citations)
    
    def _calculate_cagr(self, yearly_counts: Dict[int, int], years: int = 3) -> float:
        """Compound Annual Growth Rate"""
        if len(yearly_counts) < 2:
            return 0
        
        recent_years = sorted(yearly_counts.keys())[-years:]
        if len(recent_years) < 2:
            return 0
            
        start_count = yearly_counts.get(recent_years[0], 0)
        end_count = yearly_counts.get(recent_years[-1], 0)
        
        if start_count == 0:
            return 100 if end_count > 0 else 0
            
        years_diff = recent_years[-1] - recent_years[0]
        if years_diff == 0:
            return 0
            
        cagr = (end_count / start_count) ** (1 / years_diff) - 1
        return cagr * 100
    
    def _calculate_acceleration(self, citations_by_year: Dict[int, int]) -> float:
        """Acceleration of citation growth (second derivative)"""
        years = sorted(citations_by_year.keys())
        if len(years) < 3:
            return 0
            
        # Calculate first derivative (velocity)
        velocities = []
        for i in range(1, len(years)):
            dt = years[i] - years[i-1]
            if dt > 0:
                v = (citations_by_year.get(years[i], 0) - citations_by_year.get(years[i-1], 0)) / dt
                velocities.append(v)
        
        if len(velocities) < 2:
            return 0
            
        # Calculate acceleration (derivative of velocity)
        accelerations = []
        for i in range(1, len(velocities)):
            a = velocities[i] - velocities[i-1]
            accelerations.append(a)
        
        return sum(accelerations) / len(accelerations) if accelerations else 0
    
    def _calculate_avg_age(self, topic_articles: List[dict]) -> float:
        """Calculates average age of articles in topic"""
        ages = []
        for article in topic_articles:
            year = article.get('publication_year', 0)
            if year > 0:
                ages.append(self.current_year - year)
        return sum(ages) / len(ages) if ages else 0
    
    def _calculate_growth_rate(self, citations_by_year: Dict[int, int]) -> float:
        """Calculates growth rate of citations"""
        years = sorted(citations_by_year.keys())
        if len(years) < 2:
            return 0
        
        first_year = years[0]
        last_year = years[-1]
        first_value = citations_by_year.get(first_year, 0)
        last_value = citations_by_year.get(last_year, 0)
        
        if first_value == 0:
            return 100 if last_value > 0 else 0
        
        years_diff = last_year - first_year
        if years_diff == 0:
            return 0
        
        growth = (last_value / first_value) ** (1 / years_diff) - 1
        return growth * 100
    
    def _calculate_novelty_score(self, topic_articles: List[dict]) -> float:
        """Calculates novelty score based on title keywords"""
        novelty_keywords = ['novel', 'new', 'first', 'emerging', 'innovative', 
                           'discovery', 'breakthrough', 'unprecedented', 'next-generation']
        
        scores = []
        for article in topic_articles:
            title = article.get('title', '').lower()
            score = sum(1 for kw in novelty_keywords if kw in title)
            scores.append(min(1, score / 3))
        
        return sum(scores) / len(scores) if scores else 0
    
    def _estimate_world_average(self, topic_articles: List[dict]) -> float:
        """Estimates world average citations for similar topics"""
        # In production, this would query OpenAlex for world averages
        # For now, use a heuristic based on citation distribution
        citations = [a.get('cited_by_count', 0) for a in topic_articles]
        if not citations:
            return 1
        
        # Estimate world average as 1.5x the median (typical in many fields)
        median = np.median(citations)
        return max(1, median * 0.7)
    
    def calculate_metrics(self, topic_articles: List[dict]) -> Dict:
        """
        Calculates comprehensive metrics for a topic
        """
        if not topic_articles:
            return {
                'cagr': 0,
                'acceleration': 0,
                'ets': 0,
                'rcr': 0,
                'h_index': 0,
                'momentum': 0,
                'hot_zone': '💤 DORMANT',
                'trend': 'stable',
                'growth_stage': 'unknown',
                'citation_velocity': 0
            }
        
        yearly_counts = self._get_yearly_counts(topic_articles)
        citations_by_year = self._get_citations_by_year(topic_articles)
        
        # 1. CAGR
        cagr = self._calculate_cagr(yearly_counts)
        
        # 2. Acceleration
        acceleration = self._calculate_acceleration(citations_by_year)
        
        # 3. Emerging Topic Score (ETS)
        avg_age = self._calculate_avg_age(topic_articles)
        age_score = max(0, 100 - (avg_age * 5))
        
        growth_score = min(100, cagr * 2) if cagr > 0 else 0
        
        citation_growth = self._calculate_growth_rate(citations_by_year)
        citation_score = min(100, citation_growth * 3) if citation_growth > 0 else 0
        
        density_score = min(100, len(topic_articles) * 2)
        
        novelty_score = self._calculate_novelty_score(topic_articles) * 100
        
        weights = {
            'age': 0.25,
            'growth': 0.30,
            'citations': 0.25,
            'density': 0.10,
            'novelty': 0.10
        }
        
        ets = (age_score * weights['age'] +
               growth_score * weights['growth'] +
               citation_score * weights['citations'] +
               density_score * weights['density'] +
               novelty_score * weights['novelty'])
        ets = min(100, ets)
        
        # 4. Relative Citation Ratio (RCR)
        avg_citations = sum(a.get('cited_by_count', 0) for a in topic_articles) / len(topic_articles)
        world_avg = self._estimate_world_average(topic_articles)
        rcr = avg_citations / world_avg if world_avg > 0 else 0
        
        # 5. H-index
        citations = sorted([a.get('cited_by_count', 0) for a in topic_articles], reverse=True)
        h_index = 0
        for i, c in enumerate(citations, 1):
            if c >= i:
                h_index = i
            else:
                break
        
        # 6. Momentum (trend indicator)
        if len(yearly_counts) >= 2:
            years = sorted(yearly_counts.keys())
            counts = [yearly_counts[y] for y in years]
            if len(counts) >= 3:
                # Simple slope calculation
                x = np.array(range(len(counts)))
                y = np.array(counts)
                slope = np.polyfit(x, y, 1)[0]
                momentum = np.tanh(slope / 10)  # Normalize to [-1, 1]
            else:
                momentum = 0
        else:
            momentum = 0
        
        # 7. Citation velocity
        if len(citations_by_year) >= 2:
            years = sorted(citations_by_year.keys())
            values = [citations_by_year[y] for y in years]
            velocity = (values[-1] - values[0]) / (years[-1] - years[0]) if years[-1] != years[0] else 0
        else:
            velocity = 0
        
        # 8. Hot Zone Classification
        if ets > 70 and cagr > 20 and momentum > 0.5:
            hot_zone = '🔥 EMERGING STAR'
        elif ets > 60 and cagr > 10 and rcr > 1.5:
            hot_zone = '📈 GROWING POWER'
        elif ets > 50 and rcr > 1.2:
            hot_zone = '⚡ ESTABLISHED HOT'
        elif ets > 40 and momentum > 0:
            hot_zone = '🌱 PROMISING'
        elif momentum < -0.3 and ets < 40:
            hot_zone = '📉 DECLINING'
        else:
            hot_zone = '💤 DORMANT'
        
        # 9. Trend prediction
        if cagr > 15:
            trend = 'up'
        elif cagr < -10:
            trend = 'down'
        else:
            trend = 'stable'
        
        # 10. Growth stage
        if len(yearly_counts) < 3:
            growth_stage = 'emerging'
        elif cagr > 10:
            growth_stage = 'growing'
        elif cagr > -5:
            growth_stage = 'mature'
        else:
            growth_stage = 'declining'
        
        return {
            'cagr': cagr,
            'acceleration': acceleration,
            'ets': ets,
            'rcr': rcr,
            'h_index': h_index,
            'momentum': momentum,
            'citation_velocity': velocity,
            'hot_zone': hot_zone,
            'trend': trend,
            'growth_stage': growth_stage,
            'avg_age': avg_age,
            'total_articles': len(topic_articles),
            'total_citations': sum(a.get('cited_by_count', 0) for a in topic_articles),
            'avg_citations': avg_citations
        }
    
    def calculate_metrics_for_all_topics(self, hierarchy: Dict) -> List[Dict]:
        """Calculates metrics for all topics in hierarchy"""
        all_metrics = []
        
        for domain, fields in hierarchy.items():
            for field, subfields in fields.items():
                for subfield, topics in subfields.items():
                    for topic, articles in topics.items():
                        metrics = self.calculate_metrics(articles)
                        all_metrics.append({
                            'domain': domain,
                            'field': field,
                            'subfield': subfield,
                            'topic': topic,
                            'articles': len(articles),
                            **metrics
                        })
        
        return sorted(all_metrics, key=lambda x: x['ets'], reverse=True)

# ============================================================================
# NEW: CITATION DYNAMICS ANALYZER
# ============================================================================

class CitationDynamicsAnalyzer:
    """
    Analyzes citation dynamics of individual articles over time
    """
    
    def __init__(self, articles: List[dict]):
        self.articles = articles
        self.current_year = datetime.now().year
    
    def _get_citation_history(self, article: dict) -> Dict[int, int]:
        """Simulates citation history based on available data"""
        # In production, this would query OpenAlex for yearly citation counts
        # For now, we create a realistic distribution
        
        total_citations = article.get('cited_by_count', 0)
        pub_year = article.get('publication_year', 0)
        
        if total_citations == 0 or pub_year == 0:
            return {}
        
        # Create realistic distribution: peak around 2-4 years after publication
        history = {}
        age = self.current_year - pub_year
        
        # Use a log-normal-like distribution
        import math
        for year in range(pub_year, min(pub_year + age + 1, self.current_year + 1)):
            year_age = year - pub_year
            if year_age == 0:
                # First year gets 10-20% of total
                ratio = 0.15 + np.random.random() * 0.05
            elif year_age <= 3:
                # Peak years
                ratio = (0.20 + 0.10 * np.sin((year_age - 1) * np.pi / 4)) * (1 + np.random.random() * 0.1)
            elif year_age <= 7:
                # Gradual decline
                ratio = (0.15 - 0.02 * (year_age - 3)) * (1 + np.random.random() * 0.1)
            else:
                # Long tail
                ratio = (0.05 * math.exp(-0.3 * (year_age - 7))) * (1 + np.random.random() * 0.1)
            
            citations_this_year = int(total_citations * ratio)
            if citations_this_year > 0:
                history[year] = citations_this_year
        
        # Normalize to ensure total matches
        total_simulated = sum(history.values())
        if total_simulated > 0:
            scale = total_citations / total_simulated
            for year in history:
                history[year] = int(history[year] * scale)
        
        return history
    
    def _calculate_velocity(self, history: Dict[int, int]) -> float:
        """Calculates citation velocity (rate of change)"""
        if len(history) < 2:
            return 0
        
        years = sorted(history.keys())
        values = [history[y] for y in years]
        total_change = values[-1] - values[0]
        time_span = years[-1] - years[0]
        
        return total_change / time_span if time_span > 0 else 0
    
    def _calculate_acceleration_dynamics(self, history: Dict[int, int]) -> float:
        """Calculates citation acceleration"""
        if len(history) < 3:
            return 0
        
        years = sorted(history.keys())
        values = [history[y] for y in years]
        
        # Calculate slopes between consecutive years
        slopes = []
        for i in range(1, len(years)):
            slope = (values[i] - values[i-1]) / (years[i] - years[i-1])
            slopes.append(slope)
        
        # Calculate acceleration
        accelerations = []
        for i in range(1, len(slopes)):
            accel = slopes[i] - slopes[i-1]
            accelerations.append(accel)
        
        return sum(accelerations) / len(accelerations) if accelerations else 0
    
    def _is_sleeping_beauty(self, history: Dict[int, int]) -> bool:
        """Checks if article is a "Sleeping Beauty" - long dormancy then sudden awakening"""
        if len(history) < 5:
            return False
        
        years = sorted(history.keys())
        citations = [history[y] for y in years]
        
        # Find dormancy period (at least 3 years with very few citations)
        for i in range(len(citations) - 3):
            window = citations[i:i+3]
            if sum(window) < 3:  # Almost no citations
                # Check if there's a later surge
                if sum(citations[i+3:]) > 10:
                    return True
        
        return False
    
    def analyze_article(self, article: dict) -> Dict:
        """
        Comprehensive analysis of article citation dynamics
        """
        history = self._get_citation_history(article)
        
        if not history:
            return {
                'category': '📄 UNCITED',
                'patterns': {'description': 'No citations'},
                'citation_history': {},
                'recommendation': 'Article has no citations',
                'awakening_score': 0,
                'dormancy_period': 0,
                'revival_chance': 0
            }
        
        years = sorted(history.keys())
        citation_values = [history[y] for y in years]
        
        # Calculate slope for trend
        if len(years) >= 3:
            x = np.array(range(len(citation_values)))
            y = np.array(citation_values)
            slope = np.polyfit(x, y, 1)[0]
        else:
            slope = 0
        
        # Determine pattern
        recent_citations = [history[y] for y in years[-3:]] if len(years) >= 3 else citation_values
        total_citations = article.get('cited_by_count', 0)
        age = self.current_year - article.get('publication_year', self.current_year)
        
        if slope > 0.5:
            pattern = 'increasing'
            description = '📈 Increasing'
        elif slope < -0.5:
            pattern = 'decreasing'
            description = '📉 Decreasing'
        elif len(years) > 0 and sum(recent_citations) == 0:
            pattern = 'dormant'
            description = '💤 Dormant'
        elif len(years) > 0 and sum(recent_citations) > 0 and sum(citation_values[:-3]) == 0:
            pattern = 'awakening'
            description = '🌅 Awakening'
        elif len(years) > 0 and sum(recent_citations) > sum(citation_values[:-3]):
            pattern = 'accelerating'
            description = '⚡ Accelerating'
        else:
            pattern = 'stable'
            description = '➖ Stable'
        
        # Classify article
        if age <= 3 and slope > 1.0 and total_citations > 5:
            category = '🌟 RISING STAR'
        elif age > 5 and pattern == 'stable' and total_citations > 20:
            category = '🏛️ CLASSIC'
        elif age > 3 and pattern == 'dormant':
            category = '💤 DORMANT'
        elif pattern == 'awakening' and slope > 0.5:
            category = '🌅 AWAKENING'
        elif age <= 2 and total_citations > age * 10:
            category = '🔥 HOT PAPER'
        elif total_citations > 50 and age > 3:
            category = '💎 HIGH IMPACT'
        elif self._is_sleeping_beauty(history):
            category = '👸 SLEEPING BEAUTY'
        elif total_citations > 0:
            category = '📄 REGULAR'
        else:
            category = '📄 UNCITED'
        
        # Calculate awakening score
        if len(years) >= 3:
            last_years = years[-3:]
            last_citations = [history[y] for y in last_years]
            previous_years = years[:-3]
            previous_citations = [history[y] for y in previous_years] if previous_years else [0]
            
            if sum(previous_citations) > 0:
                ratio = sum(last_citations) / sum(previous_citations) if sum(previous_citations) > 0 else 0
                awakening_score = min(100, ratio * 50)
            else:
                if sum(last_citations) > 0:
                    awakening_score = min(100, sum(last_citations) * 20)
                else:
                    awakening_score = 0
        else:
            awakening_score = 0
        
        # Estimate revival chance
        revival_chance = 0
        factors = []
        
        if 3 <= age <= 10:
            factors.append(0.3)
        elif age > 10:
            factors.append(0.1)
        else:
            factors.append(0.0)
        
        topic = article.get('primary_topic', '')
        # Handle case when topic is a dict (from OpenAlex)
        if isinstance(topic, dict):
            topic = topic.get('display_name', '')
        elif not isinstance(topic, str):
            topic = str(topic) if topic else ''
        
        if topic and any(word in topic.lower() for word in ['machine learning', 'ai', 'climate', 'covid', 'quantum']):
            factors.append(0.3)
        
        recent_citations_sum = sum([history[y] for y in list(history.keys())[-3:]])
        if recent_citations_sum > 0:
            factors.append(min(0.4, recent_citations_sum * 0.1))
        
        if history and max(history.values()) > 5:
            factors.append(0.2)
        
        revival_chance = sum(factors) if factors else 0
        
        # Dormancy period
        dormancy_period = 0
        if pattern == 'dormant' or pattern == 'awakening':
            # Find longest period with <1 citation per year
            max_dormancy = 0
            current_dormancy = 0
            for year in range(min(years), max(years) + 1):
                if history.get(year, 0) < 1:
                    current_dormancy += 1
                else:
                    if current_dormancy > max_dormancy:
                        max_dormancy = current_dormancy
                    current_dormancy = 0
            dormancy_period = max_dormancy
        
        return {
            'category': category,
            'patterns': {
                'pattern': pattern,
                'description': description,
                'slope': slope,
                'velocity': self._calculate_velocity(history),
                'acceleration': self._calculate_acceleration_dynamics(history),
                'max_citation_year': max(history, key=history.get) if history else None,
                'citation_peak': max(history.values()) if history else 0
            },
            'citation_history': history,
            'recommendation': self._get_recommendation(category, article),
            'awakening_score': awakening_score,
            'dormancy_period': dormancy_period,
            'revival_chance': revival_chance
        }
    
    def _get_recommendation(self, category: str, article: dict) -> str:
        """Generates recommendation based on article category"""
        recommendations = {
            '🌟 RISING STAR': 'This article is gaining rapid attention. Consider highlighting it in editorials and social media.',
            '🏛️ CLASSIC': 'This is a foundational paper. Ensure it is properly indexed and cited in relevant reviews.',
            '💤 DORMANT': 'This article may benefit from targeted promotion. Consider writing a commentary or press release.',
            '🌅 AWAKENING': 'This article is experiencing renewed interest. Investigate what triggered the awakening.',
            '🔥 HOT PAPER': 'This paper is performing exceptionally well. Feature it prominently.',
            '💎 HIGH IMPACT': 'This is a high-impact paper. Consider inviting the authors for a review or perspective.',
            '👸 SLEEPING BEAUTY': 'This paper has rare potential. Consider retrospective analysis and promotion.',
            '📄 REGULAR': 'Continue monitoring citation trends.',
            '📄 UNCITED': 'Consider outreach to authors for promotion or identify if indexing issues exist.'
        }
        return recommendations.get(category, 'Monitor citation trends.')
    
    def analyze_all_articles(self) -> List[Dict]:
        """Analyzes all articles and returns results"""
        results = []
        for article in self.articles:
            analysis = self.analyze_article(article)
            results.append({
                'title': article.get('title', 'No title'),
                'doi': article.get('doi', ''),
                'publication_year': article.get('publication_year', 0),
                'total_citations': article.get('cited_by_count', 0),
                'citations_per_year': article.get('citations_per_year', 0),
                **analysis
            })
        return results
    
    def get_categories_summary(self, results: List[Dict]) -> Dict:
        """Summarizes article categories"""
        summary = defaultdict(list)
        for result in results:
            category = result['category']
            summary[category].append(result)
        return dict(summary)

# ============================================================================
# NEW: AUTHOR ANALYZER
# ============================================================================

class AuthorAnalyzer:
    """
    Comprehensive analysis of journal authors
    """
    
    def __init__(self, articles: List[dict]):
        self.articles = articles
        self._build_author_profiles()
    
    def _build_author_profiles(self):
        """Builds profiles for all authors"""
        self.author_profiles = defaultdict(lambda: {
            'articles': [],
            'citations': 0,
            'publication_years': [],
            'collaborators': set(),
            'topics': defaultdict(int),
            'first_author': 0,
            'corresponding_author': 0,
            'affiliations': set()
        })
        
        for article in self.articles:
            authors = article.get('authors_list', [])
            if not authors:
                continue
                
            for idx, author in enumerate(authors):
                profile = self.author_profiles[author]
                profile['articles'].append(article)
                profile['citations'] += article.get('cited_by_count', 0)
                profile['publication_years'].append(article.get('publication_year', 0))
                
                if idx == 0:
                    profile['first_author'] += 1
                if idx == len(authors) - 1:
                    profile['corresponding_author'] += 1
                
                collaborators = [a for a in authors if a != author]
                profile['collaborators'].update(collaborators)
                
                topic = article.get('primary_topic', 'Unknown')
                profile['topics'][topic] += 1
                
                # Extract affiliation if available
                if 'authorships' in article and idx < len(article['authorships']):
                    aff = article['authorships'][idx].get('institution', {}).get('display_name', '')
                    if aff:
                        profile['affiliations'].add(aff)
    
    def _calculate_h_index(self, articles: List[dict]) -> int:
        """Calculates H-index for an author"""
        citations = sorted([a.get('cited_by_count', 0) for a in articles], reverse=True)
        h_index = 0
        for i, c in enumerate(citations, 1):
            if c >= i:
                h_index = i
            else:
                break
        return h_index
    
    def _calculate_recent_activity(self, profile: dict) -> Dict:
        """Analyzes activity in last 3 years"""
        current_year = datetime.now().year
        recent_years = [current_year - i for i in range(3)]
        
        recent_articles = [a for a in profile['articles'] 
                         if a.get('publication_year', 0) in recent_years]
        
        return {
            'articles_last_3_years': len(recent_articles),
            'citations_last_3_years': sum(a.get('cited_by_count', 0) for a in recent_articles),
            'is_active': len(recent_articles) > 0
        }
    
    def _determine_career_stage(self, profile: dict) -> str:
        """Determines career stage of author"""
        years = sorted(profile['publication_years'])
        if not years:
            return '🌱 Early Career'
        
        career_length = years[-1] - years[0]
        articles_count = len(profile['articles'])
        
        if career_length < 3 and articles_count < 5:
            return '🌱 Early Career'
        elif career_length < 7 and articles_count < 15:
            return '🌿 Mid Career'
        elif articles_count > 20 and career_length > 10:
            return '🏛️ Established Researcher'
        else:
            return '🌳 Senior Researcher'
    
    def _calculate_productivity_trend(self, profile: dict) -> float:
        """Calculates productivity trend (articles per year)"""
        years = sorted(profile['publication_years'])
        if len(years) < 3:
            return 0
        
        # Group by year
        yearly_counts = defaultdict(int)
        for year in years:
            if year > 0:
                yearly_counts[year] += 1
        
        years_sorted = sorted(yearly_counts.keys())
        counts = [yearly_counts[y] for y in years_sorted]
        
        if len(counts) >= 3:
            x = np.array(range(len(counts)))
            y = np.array(counts)
            slope = np.polyfit(x, y, 1)[0]
            return slope
        return 0
    
    def _calculate_productivity_score(self, profile: dict) -> float:
        """Calculates overall productivity score"""
        years = sorted(profile['publication_years'])
        if not years:
            return 0
        
        total_articles = len(profile['articles'])
        career_length = years[-1] - years[0] + 1 if years else 1
        
        articles_per_year = total_articles / career_length if career_length > 0 else 0
        citations_per_article = profile['citations'] / total_articles if total_articles > 0 else 0
        
        # Combined score
        score = (articles_per_year * 3) + (citations_per_article * 0.5) + (profile['h_index'] * 0.3)
        return min(100, score)
    
    def analyze(self) -> Dict:
        """
        Comprehensive analysis of all authors
        """
        results = {}
        
        for author, profile in self.author_profiles.items():
            articles = profile['articles']
            
            if not articles:
                continue
            
            h_index = self._calculate_h_index(articles)
            avg_citations = profile['citations'] / len(articles) if articles else 0
            recent_activity = self._calculate_recent_activity(profile)
            topic_diversity = len(profile['topics'])
            collaboration_score = len(profile['collaborators']) / len(articles) if articles else 0
            productivity_trend = self._calculate_productivity_trend(profile)
            
            results[author] = {
                'articles': len(articles),
                'total_citations': profile['citations'],
                'avg_citations': avg_citations,
                'h_index': h_index,
                'first_author_count': profile['first_author'],
                'corresponding_author_count': profile['corresponding_author'],
                'collaborators': list(profile['collaborators']),
                'num_collaborators': len(profile['collaborators']),
                'collaboration_score': collaboration_score,
                'topic_diversity': topic_diversity,
                'main_topics': sorted(profile['topics'].items(), 
                                     key=lambda x: x[1], reverse=True)[:5],
                'recent_activity': recent_activity,
                'productivity_trend': productivity_trend,
                'publication_years': sorted(profile['publication_years']),
                'career_stage': self._determine_career_stage(profile),
                'productivity_score': self._calculate_productivity_score(profile),
                'affiliations': list(profile['affiliations']),
                'active': recent_activity['is_active']
            }
        
        return results
    
    def get_top_authors(self, n: int = 10, metric: str = 'total_citations') -> List[Dict]:
        """Returns top N authors by specified metric"""
        analysis = self.analyze()
        
        sorted_authors = sorted(analysis.items(), 
                              key=lambda x: x[1].get(metric, 0), reverse=True)
        
        return [{'name': name, **metrics} for name, metrics in sorted_authors[:n]]
    
    def get_authors_by_topic(self, topic: str) -> List[Dict]:
        """Returns authors specializing in a topic"""
        analysis = self.analyze()
        
        result = []
        for author, metrics in analysis.items():
            if any(t == topic for t, _ in metrics['main_topics']):
                result.append({'name': author, **metrics})
        
        return sorted(result, key=lambda x: x['articles'], reverse=True)
    
    def get_collaboration_network(self, top_n: int = 20) -> Dict:
        """Returns collaboration network data"""
        analysis = self.analyze()
        
        network = {
            'nodes': [],
            'edges': []
        }
        
        # Take top N authors by articles
        top_authors = self.get_top_authors(top_n, 'articles')
        author_names = [a['name'] for a in top_authors]
        
        for author in author_names:
            profile = self.author_profiles.get(author, {})
            collaborators = profile.get('collaborators', set())
            
            for collab in collaborators:
                if collab in author_names:
                    network['edges'].append({
                        'source': author,
                        'target': collab,
                        'weight': 1  # Simple edge weight
                    })
        
        # Add nodes with metrics
        for author in top_authors:
            network['nodes'].append({
                'id': author['name'],
                'articles': author['articles'],
                'citations': author['total_citations'],
                'h_index': author['h_index']
            })
        
        return network

# ============================================================================
# NEW: CITATION PREDICTOR
# ============================================================================

class CitationPredictor:
    """
    Predicts citation potential of articles based on characteristics
    """
    
    def __init__(self, training_data: List[dict]):
        self.training_data = training_data
        self._cache_features = {}  # Initialize cache BEFORE calling _calculate_feature_weights
        self.feature_weights = self._calculate_feature_weights()
    
    def _calculate_feature_weights(self) -> Dict:
        """
        Calculates feature weights based on historical data
        """
        features = []
        labels = []
        
        for article in self.training_data:
            feat = self._extract_features(article)
            features.append(feat)
            labels.append(article.get('cited_by_count', 0))
        
        if not features:
            return {name: 0.1 for name in self._get_feature_names()}
        
        feature_names = list(features[0].keys())
        weights = {name: 0 for name in feature_names}
        
        for name in feature_names:
            values = [f.get(name, 0) for f in features]
            
            # Remove zeros for correlation calculation
            filtered = [(v, l) for v, l in zip(values, labels) if v != 0]
            if len(filtered) > 1:
                v_filtered = [f[0] for f in filtered]
                l_filtered = [f[1] for f in filtered]
                correlation = np.corrcoef(v_filtered, l_filtered)[0, 1] if len(v_filtered) > 1 else 0
                weights[name] = max(0, correlation)
            else:
                weights[name] = 0
        
        # Normalize weights
        total = sum(weights.values())
        if total > 0:
            for name in weights:
                weights[name] /= total
        else:
            # If all weights are zero, assign equal weights
            for name in weights:
                weights[name] = 1.0 / len(weights)
        
        return weights
    
    def _get_feature_names(self) -> List[str]:
        """Returns list of feature names"""
        return [
            'title_length', 'title_word_count', 'num_authors',
            'has_international_authors', 'topic_popularity', 'topic_emerging_score',
            'journal_citation_rate', 'journal_impact_factor', 'publication_year',
            'is_recent', 'has_doi', 'is_oa', 'title_has_question', 'title_has_colon',
            'novelty_score', 'impact_words'
        ]
    
    def _extract_features(self, article: dict) -> Dict:
        """Extracts features for prediction model"""
        features = {}
        
        # Cache by DOI to avoid recalculation
        doi = article.get('doi', '')
        if doi in self._cache_features:
            return self._cache_features[doi]
        
        # 1. Structural features
        title = article.get('title', '')
        features['title_length'] = len(title)
        features['title_word_count'] = len(title.split())
        
        # 2. Author features
        authors = article.get('authors_list', [])
        features['num_authors'] = len(authors)
        features['has_international_authors'] = 1 if self._has_international_authors(article) else 0
        
        # 3. Topic features
        topic = article.get('primary_topic', '')
        features['topic_popularity'] = self._get_topic_popularity(topic)
        features['topic_emerging_score'] = self._get_topic_emerging_score(topic)
        
        # 4. Journal features
        features['journal_citation_rate'] = self._get_journal_citation_rate(article)
        features['journal_impact_factor'] = self._get_journal_impact_factor(article)
        
        # 5. Temporal features
        year = article.get('publication_year', 0)
        features['publication_year'] = year
        features['is_recent'] = 1 if (datetime.now().year - year) <= 3 else 0
        
        # 6. Format features
        features['has_doi'] = 1 if article.get('doi') else 0
        features['is_oa'] = 1 if article.get('is_oa') else 0
        
        # 7. Content features
        features['title_has_question'] = 1 if '?' in title else 0
        features['title_has_colon'] = 1 if ':' in title else 0
        
        # 8. Semantic features
        features['novelty_score'] = self._calculate_novelty_score_article(article)
        features['impact_words'] = self._count_impact_words(title)
        
        # Cache features
        if doi:
            self._cache_features[doi] = features
        
        return features
    
    def _has_international_authors(self, article: dict) -> bool:
        """Checks if article has international collaboration"""
        # In production, check affiliations from different countries
        # For now, use heuristic: multiple authors from different parts of name
        authors = article.get('authors_list', [])
        if len(authors) < 2:
            return False
        
        # Simple heuristic: check for name diversity
        name_styles = set()
        for author in authors:
            if re.search(r'[a-z]', author) and re.search(r'[A-Z]', author):
                name_styles.add('western')
            elif re.search(r'[а-яА-Я]', author):
                name_styles.add('cyrillic')
            else:
                name_styles.add('other')
        
        return len(name_styles) >= 2
    
    def _get_topic_popularity(self, topic) -> float:
        """Gets popularity score for a topic"""
        # Handle case when topic is a dict (from OpenAlex)
        if isinstance(topic, dict):
            topic = topic.get('display_name', '')
        elif not topic:
            return 0.5
        
        topic = str(topic)
        
        if not topic:
            return 0.5
        
        # Count articles with this topic in training data
        count = sum(1 for a in self.training_data 
                   if a.get('primary_topic', '') == topic)
        
        # Normalize: max 50 articles for full score
        return min(1, count / 50)
    
    def _get_topic_emerging_score(self, topic) -> float:
        """Gets emerging score for a topic"""
        # Handle case when topic is a dict (from OpenAlex)
        if isinstance(topic, dict):
            topic = topic.get('display_name', '')
        elif not topic:
            return 0.3
        
        # Convert to string if needed
        topic = str(topic)
        
        if not topic:
            return 0.3
        
        # Check if topic contains emerging keywords
        emerging_keywords = ['emerging', 'novel', 'future', 'next-generation', 
                           'advanced', 'breakthrough', 'paradigm']
        
        topic_lower = topic.lower()
        score = 0
        for kw in emerging_keywords:
            if kw in topic_lower:
                score += 0.2
        
        return min(1, score)
    
    def _get_journal_citation_rate(self, article: dict) -> float:
        """Gets journal citation rate"""
        journal_name = article.get('journal_name', '')
        if not journal_name:
            return 0.5
        
        # Calculate average citations per article for this journal
        journal_articles = [a for a in self.training_data 
                           if a.get('journal_name', '') == journal_name]
        
        if not journal_articles:
            return 0.5
        
        avg = sum(a.get('cited_by_count', 0) for a in journal_articles) / len(journal_articles)
        return min(1, avg / 20)  # Normalize: 20 citations = 1.0
    
    def _get_journal_impact_factor(self, article: dict) -> float:
        """Gets journal impact factor (simplified)"""
        journal_name = article.get('journal_name', '')
        if not journal_name:
            return 0.5
        
        # Simplified IF calculation
        current_year = datetime.now().year
        citations_to_journal = sum(
            a.get('cited_by_count', 0) for a in self.training_data
            if a.get('journal_name', '') == journal_name
            and current_year - a.get('publication_year', 0) <= 2
        )
        
        articles_in_journal = len([a for a in self.training_data 
                                  if a.get('journal_name', '') == journal_name
                                  and current_year - a.get('publication_year', 0) <= 2])
        
        if articles_in_journal == 0:
            return 0.5
        
        impact_factor = citations_to_journal / articles_in_journal
        return min(1, impact_factor / 5)  # Normalize: IF 5 = 1.0
    
    def _calculate_novelty_score_article(self, article: dict) -> float:
        """Calculates novelty score for an article"""
        title = article.get('title', '').lower()
        
        novelty_keywords = ['novel', 'new', 'first', 'emerging', 'innovative',
                           'discovery', 'breakthrough', 'unprecedented']
        
        score = sum(1 for kw in novelty_keywords if kw in title)
        return min(1, score / 3)
    
    def _count_impact_words(self, title: str) -> int:
        """Counts impact words in title"""
        impact_words = ['significant', 'important', 'critical', 'essential',
                       'key', 'fundamental', 'major', 'novel', 'unique']
        
        title_lower = title.lower()
        return sum(1 for word in impact_words if word in title_lower)
    
    def predict_citation_potential(self, article: dict) -> Dict:
        """
        Predicts citation potential of an article
        """
        features = self._extract_features(article)
        
        # Calculate weighted score
        score = 0
        feature_importance = []
        
        for name, weight in self.feature_weights.items():
            value = features.get(name, 0)
            contribution = value * weight
            score += contribution
            feature_importance.append({
                'feature': name,
                'value': value,
                'weight': weight,
                'contribution': contribution
            })
        
        # Normalize score to 0-100
        score = min(100, score * 20)
        
        # Determine category
        if score > 80:
            category = '🌟 High Impact Potential'
        elif score > 60:
            category = '📈 Good Potential'
        elif score > 40:
            category = '📊 Moderate Potential'
        else:
            category = '💤 Needs Promotion'
        
        # Generate recommendations
        recommendations = self._generate_recommendations(features, score)
        
        # Estimate citations
        estimated_citations = int(score * 0.3 + 2)  # Simple estimation
        
        # Calculate confidence
        confidence = self._calculate_confidence(features)
        
        return {
            'score': score,
            'category': category,
            'feature_importance': sorted(feature_importance, 
                                        key=lambda x: x['contribution'], reverse=True)[:5],
            'recommendations': recommendations,
            'predicted_citations': estimated_citations,
            'confidence': confidence
        }
    
    def _generate_recommendations(self, features: Dict, score: float) -> List[str]:
        """Generates recommendations for improving citations"""
        recommendations = []
        
        # Title analysis
        if features.get('title_length', 0) > 15:
            recommendations.append("📝 Title is too long - consider shorter, more impactful title")
        elif features.get('title_length', 0) < 5:
            recommendations.append("📝 Title is too short - add keywords for better discoverability")
        
        # Author analysis
        if features.get('num_authors', 0) < 3:
            recommendations.append("👥 Consider expanding author team, especially with international collaborators")
        
        # Topic analysis
        if features.get('topic_emerging_score', 0) < 0.3:
            recommendations.append("🎯 Topic is not highly emerging - add modern keywords to improve relevance")
        
        # Open access
        if not features.get('is_oa', 0):
            recommendations.append("🌐 Consider publishing open access to increase visibility")
        
        # General promotion
        if score < 60:
            recommendations.append("📢 Actively promote article on social media and academic networks")
        
        if features.get('title_has_question', 0):
            recommendations.append("💡 Title with question may attract more readers - consider highlighting this")
        
        return recommendations
    
    def _calculate_confidence(self, features: Dict) -> float:
        """Calculates confidence in prediction"""
        # More features with positive values = higher confidence
        positive_features = sum(1 for v in features.values() if v > 0)
        total_features = len(features)
        
        confidence = (positive_features / total_features) * 0.8 + 0.2
        return min(1, confidence)
    
    def predict_all_articles(self) -> List[Dict]:
        """Predicts citation potential for all articles"""
        predictions = []
        for article in self.training_data:
            pred = self.predict_citation_potential(article)
            predictions.append({
                'title': article.get('title', 'No title'),
                'doi': article.get('doi', ''),
                'year': article.get('publication_year', 0),
                'actual_citations': article.get('cited_by_count', 0),
                'predicted_score': pred['score'],
                'category': pred['category'],
                'recommendations': pred['recommendations'],
                'predicted_citations': pred['predicted_citations'],
                'confidence': pred['confidence']
            })
        
        return sorted(predictions, key=lambda x: x['predicted_score'], reverse=True)

# ============================================================================
# NEW: EDITOR'S CHOICE MODULE
# ============================================================================

class EditorsChoiceModule:
    """
    Automatic selection of articles for Editor's Choice
    """
    
    def __init__(self, articles: List[dict], hierarchy: Dict):
        self.articles = articles
        self.hierarchy = hierarchy
        self.current_year = datetime.now().year
        self.dynamics_analyzer = CitationDynamicsAnalyzer(articles)
        self.analyzed_articles = self.dynamics_analyzer.analyze_all_articles()
    
    def _get_topic_importance(self, topic) -> float:
        """Evaluates topic importance for the journal"""
        # Handle case when topic is a dict (from OpenAlex)
        if isinstance(topic, dict):
            topic = topic.get('display_name', '')
        elif not isinstance(topic, str):
            topic = str(topic) if topic else ''
        
        if not topic:
            return 0
        
        # Count articles in this topic
        topic_count = 0
        for domain in self.hierarchy.values():
            for field in domain.values():
                for subfield in field.values():
                    if topic in subfield:
                        topic_count += len(subfield[topic])
                        break
        
        # Rare topics get higher importance
        if topic_count == 0:
            return 0
        elif topic_count <= 3:
            return 1.0
        elif topic_count <= 10:
            return 0.7
        else:
            return 0.3
    
    def _calculate_novelty_score(self, article: dict) -> float:
        """Calculates novelty score for article"""
        title = article.get('title', '').lower()
        
        novelty_keywords = ['novel', 'new', 'first', 'emerging', 'innovative',
                           'discovery', 'breakthrough', 'unprecedented',
                           'next-generation', 'paradigm']
        
        score = sum(1 for kw in novelty_keywords if kw in title)
        return min(1, score / 4)
    
    def _calculate_interdisciplinarity(self, article: dict) -> float:
        """Calculates interdisciplinarity score"""
        # Check for multiple topic references
        # In production, this would analyze references
        # For now, use simple heuristic
        topics = set()
        
        primary_topic = article.get('primary_topic', '')
        # Handle case when primary_topic is a dict (from OpenAlex)
        if isinstance(primary_topic, dict):
            primary_topic = primary_topic.get('display_name', '')
        elif not isinstance(primary_topic, str):
            primary_topic = str(primary_topic) if primary_topic else ''
        
        if primary_topic:
            topics.add(primary_topic)
        
        # Check title for multi-disciplinary keywords
        title = article.get('title', '').lower()
        cross_domain_words = ['interdisciplinary', 'cross-disciplinary', 'multidisciplinary',
                             'across', 'between', 'bridge']
        
        for word in cross_domain_words:
            if word in title:
                topics.add('interdisciplinary')
        
        return min(1, len(topics) / 3)
    
    def _calculate_social_impact(self, article: dict) -> float:
        """Calculates social impact score"""
        # In production, this would use altmetrics
        # For now, use citation velocity as proxy
        analysis = next((a for a in self.analyzed_articles 
                        if a.get('doi') == article.get('doi')), None)
        
        if analysis:
            velocity = analysis.get('patterns', {}).get('velocity', 0)
            return min(1, velocity / 10)
        
        return 0.3
    
    def _has_international_collaboration(self, article: dict) -> bool:
        """Checks for international collaboration"""
        authors = article.get('authors_list', [])
        if len(authors) < 2:
            return False
        
        # Simple heuristic: check for diverse name patterns
        name_styles = set()
        for author in authors:
            if re.search(r'[a-z]', author) and re.search(r'[A-Z]', author):
                name_styles.add('western')
            elif re.search(r'[а-яА-Я]', author):
                name_styles.add('cyrillic')
            else:
                name_styles.add('other')
        
        return len(name_styles) >= 2
    
    def _is_sleeping_beauty_article(self, article: dict) -> bool:
        """Checks if article is a sleeping beauty"""
        analysis = next((a for a in self.analyzed_articles 
                        if a.get('doi') == article.get('doi')), None)
        if analysis:
            return analysis['category'] == '👸 SLEEPING BEAUTY'
        return False
    
    def _is_topic_leader(self, article: dict) -> bool:
        """Checks if article is a leader in its topic"""
        topic = article.get('primary_topic', '')
        # Handle case when topic is a dict
        if isinstance(topic, dict):
            topic = topic.get('display_name', '')
        elif not isinstance(topic, str):
            topic = str(topic) if topic else ''
        
        if not topic:
            return False
        
        # Find all articles in same topic
        topic_articles = []
        for domain in self.hierarchy.values():
            for field in domain.values():
                for subfield in field.values():
                    if topic in subfield:
                        topic_articles.extend(subfield[topic])
                        break
        
        if len(topic_articles) < 3:
            return False
        
        # Check if this article has above-average citations in its topic
        avg_citations = sum(a.get('cited_by_count', 0) for a in topic_articles) / len(topic_articles)
        article_citations = article.get('cited_by_count', 0)
        
        return article_citations > avg_citations * 1.5
    
    def _calculate_editors_choice_score(self, article: dict) -> float:
        """
        Calculates Editor's Choice score for article
        """
        score = 0
        
        # 1. Academic impact (35%)
        citations = article.get('cited_by_count', 0)
        cpy = article.get('citations_per_year', 0)
        score += min(citations, 100) * 0.25
        score += min(cpy, 20) * 0.10
        
        # 2. Topic importance (25%)
        topic = article.get('primary_topic', '')
        # Handle case when topic is a dict
        if isinstance(topic, dict):
            topic = topic.get('display_name', '')
        elif not isinstance(topic, str):
            topic = str(topic) if topic else ''
        
        topic_score = self._get_topic_importance(topic)
        score += topic_score * 0.25
        
        # 3. Novelty (20%)
        novelty_score = self._calculate_novelty_score(article)
        score += novelty_score * 0.20
        
        # 4. Interdisciplinarity (10%)
        interdisciplinarity = self._calculate_interdisciplinarity(article)
        score += interdisciplinarity * 0.10
        
        # 5. Social impact (10%)
        social_impact = self._calculate_social_impact(article)
        score += social_impact * 0.10
        
        # 6. Bonuses
        if self._has_international_collaboration(article):
            score += 5
        
        if self._is_sleeping_beauty_article(article):
            score += 10
        
        if self._is_topic_leader(article):
            score += 15
        
        return min(100, score)
    
    def _get_fulfilled_criteria(self, article: dict) -> List[str]:
        """Returns list of fulfilled criteria"""
        criteria = []
        
        # Check each criterion
        citations = article.get('cited_by_count', 0)
        if citations > 10:
            criteria.append("High citations")
        
        if article.get('citations_per_year', 0) > 2:
            criteria.append("Strong citation velocity")
        
        topic = article.get('primary_topic', '')
        if self._get_topic_importance(topic) > 0.7:
            criteria.append("Important/rare topic")
        
        if self._calculate_novelty_score(article) > 0.5:
            criteria.append("Novel research")
        
        if self._calculate_interdisciplinarity(article) > 0.5:
            criteria.append("Interdisciplinary")
        
        if self._has_international_collaboration(article):
            criteria.append("International collaboration")
        
        if self._is_sleeping_beauty_article(article):
            criteria.append("Sleeping Beauty - high revival potential")
        
        if self._is_topic_leader(article):
            criteria.append("Topic leader")
        
        return criteria
    
    def _get_recommendation(self, article: dict) -> str:
        """Generates recommendation for Editor's Choice"""
        score = self._calculate_editors_choice_score(article)
        
        if score > 80:
            return "Highly recommended for Editor's Choice - exceptional paper"
        elif score > 70:
            return "Strong candidate for Editor's Choice"
        elif score > 60:
            return "Consider for Editor's Choice - good potential"
        else:
            return "Monitor for future consideration"
    
    def _generate_statistics(self, candidates: List[Dict]) -> Dict:
        """Generates statistics about candidates"""
        if not candidates:
            return {
                'total_candidates': 0,
                'avg_score': 0,
                'top_categories': []
            }
        
        total = len(candidates)
        avg_score = sum(c['score'] for c in candidates) / total
        
        # Top categories
        categories = {}
        for candidate in candidates:
            article = candidate['article']
            topic = article.get('primary_topic', 'Unknown')
            categories[topic] = categories.get(topic, 0) + 1
        
        top_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            'total_candidates': total,
            'avg_score': avg_score,
            'top_categories': top_categories
        }
    
    def select_candidates(self) -> Dict:
        """
        Selects candidates for Editor's Choice
        """
        candidates = []
        
        for article in self.articles:
            score = self._calculate_editors_choice_score(article)
            
            if score > 50:  # Threshold
                candidates.append({
                    'article': article,
                    'score': score,
                    'criteria': self._get_fulfilled_criteria(article),
                    'recommendation': self._get_recommendation(article)
                })
        
        # Sort by score
        candidates.sort(key=lambda x: x['score'], reverse=True)
        
        return {
            'top_picks': candidates[:10],
            'honorable_mentions': candidates[10:20],
            'statistics': self._generate_statistics(candidates)
        }

# ============================================================================
# NEW: JOURNAL PROFILE GENERATOR
# ============================================================================

class JournalProfileGenerator:
    """
    Generates comprehensive journal profile for editorial board
    """
    
    def __init__(self, journal_info: dict, articles: List[dict], hierarchy: Dict):
        self.journal_info = journal_info
        self.articles = articles
        self.hierarchy = hierarchy
        self.stats = calculate_hierarchy_statistics(hierarchy, include_metrics=True)
        self.hot_analyzer = HotTopicsAnalyzer(articles)
        self.dynamics_analyzer = CitationDynamicsAnalyzer(articles)
        self.author_analyzer = AuthorAnalyzer(articles)
        self.current_year = datetime.now().year
    
    def _calculate_oa_percentage(self) -> float:
        """Calculates Open Access percentage"""
        if not self.articles:
            return 0
        oa_count = sum(1 for a in self.articles if a.get('is_oa', False))
        return (oa_count / len(self.articles)) * 100
    
    def _calculate_h_index_journal(self) -> int:
        """Calculates journal H-index"""
        citations = sorted([a.get('cited_by_count', 0) for a in self.articles], reverse=True)
        h_index = 0
        for i, c in enumerate(citations, 1):
            if c >= i:
                h_index = i
            else:
                break
        return h_index
    
    def _calculate_eigenfactor(self) -> float:
        """Simplified Eigenfactor calculation"""
        # In production, this would use citation network analysis
        # For now, use a simplified metric
        total_citations = sum(a.get('cited_by_count', 0) for a in self.articles)
        total_articles = len(self.articles)
        
        if total_articles == 0:
            return 0
        
        avg_citations = total_citations / total_articles
        h_index = self._calculate_h_index_journal()
        
        # Combined metric
        eigenfactor = (avg_citations * 0.3 + h_index * 0.7) / 10
        return min(1, eigenfactor)
    
    def _calculate_citing_half_life(self) -> float:
        """Calculates citing half-life (simplified)"""
        if not self.articles:
            return 0
        
        current_year = self.current_year
        citations_by_age = defaultdict(int)
        
        for article in self.articles:
            age = current_year - article.get('publication_year', current_year)
            citations_by_age[age] += article.get('cited_by_count', 0)
        
        # Find when 50% of citations occurred
        total_citations = sum(citations_by_age.values())
        if total_citations == 0:
            return 0
        
        cumulative = 0
        sorted_ages = sorted(citations_by_age.items())
        
        for age, count in sorted_ages:
            cumulative += count
            if cumulative >= total_citations * 0.5:
                return age
        
        return current_year - min(a.get('publication_year', current_year) for a in self.articles)
    
    def _calculate_citation_velocity_journal(self) -> float:
        """Calculates journal citation velocity"""
        if len(self.articles) < 2:
            return 0
        
        years = sorted(set(a.get('publication_year', 0) for a in self.articles if a.get('publication_year', 0) > 0))
        if len(years) < 2:
            return 0
        
        yearly_citations = defaultdict(int)
        for article in self.articles:
            year = article.get('publication_year', 0)
            if year > 0:
                yearly_citations[year] += article.get('cited_by_count', 0)
        
        first_year = min(years)
        last_year = max(years)
        
        if first_year == last_year:
            return 0
        
        first_citations = yearly_citations.get(first_year, 0)
        last_citations = yearly_citations.get(last_year, 0)
        
        return (last_citations - first_citations) / (last_year - first_year)
    
    def _find_most_diverse_field(self) -> str:
        """Finds field with most diverse topics"""
        max_diversity = 0
        most_diverse_field = 'N/A'
        
        for domain, fields in self.hierarchy.items():
            for field, subfields in fields.items():
                total_subfields = len(subfields)
                total_topics = sum(len(topics) for topics in subfields.values())
                
                diversity = total_topics / max(1, total_subfields)
                if diversity > max_diversity:
                    max_diversity = diversity
                    most_diverse_field = field
        
        return most_diverse_field
    
    def _find_emerging_topics(self) -> List[str]:
        """Identifies emerging topics"""
        all_metrics = self.hot_analyzer.calculate_metrics_for_all_topics(self.hierarchy)
        emerging = [m['topic'] for m in all_metrics 
                   if m['hot_zone'] in ['🔥 EMERGING STAR', '📈 GROWING POWER']]
        return emerging[:5]
    
    def _find_declining_topics(self) -> List[str]:
        """Identifies declining topics"""
        all_metrics = self.hot_analyzer.calculate_metrics_for_all_topics(self.hierarchy)
        declining = [m['topic'] for m in all_metrics 
                    if m['hot_zone'] == '📉 DECLINING']
        return declining[:5]
    
    def _identify_strengths(self) -> List[str]:
        """Identifies journal strengths"""
        strengths = []
        
        # Strong citation performance
        avg_citations = sum(a.get('cited_by_count', 0) for a in self.articles) / len(self.articles) if self.articles else 0
        if avg_citations > 5:
            strengths.append(f"Good average citation rate: {avg_citations:.1f} per article")
        
        # Strong topics
        all_metrics = self.hot_analyzer.calculate_metrics_for_all_topics(self.hierarchy)
        hot_topics = [m for m in all_metrics if m['ets'] > 60]
        if hot_topics:
            strengths.append(f"Multiple hot topics: {', '.join([m['topic'][:30] for m in hot_topics[:3]])}")
        
        # High impact articles
        highly_cited = [a for a in self.articles if a.get('is_highly_cited', False)]
        if highly_cited:
            strengths.append(f"Strong track record with {len(highly_cited)} highly cited articles")
        
        # Author diversity
        author_analysis = self.author_analyzer.analyze()
        active_authors = sum(1 for a in author_analysis.values() if a['active'])
        if active_authors > 10:
            strengths.append(f"Active author community: {active_authors} active researchers")
        
        return strengths
    
    def _identify_weaknesses(self) -> List[str]:
        """Identifies journal weaknesses"""
        weaknesses = []
        
        # Low citation rate
        avg_citations = sum(a.get('cited_by_count', 0) for a in self.articles) / len(self.articles) if self.articles else 0
        if avg_citations < 3:
            weaknesses.append("Below average citation rate")
        
        # Few hot topics
        all_metrics = self.hot_analyzer.calculate_metrics_for_all_topics(self.hierarchy)
        hot_topics = [m for m in all_metrics if m['ets'] > 60]
        if len(hot_topics) < 2:
            weaknesses.append("Limited number of hot topics")
        
        # Many uncited articles
        uncited = [a for a in self.articles if a.get('cited_by_count', 0) == 0]
        if len(uncited) > len(self.articles) * 0.2:
            weaknesses.append(f"High percentage of uncited articles: {len(uncited)}")
        
        # Low author activity
        author_analysis = self.author_analyzer.analyze()
        active_authors = sum(1 for a in author_analysis.values() if a['active'])
        if active_authors < 5:
            weaknesses.append("Limited active author base")
        
        return weaknesses
    
    def _identify_opportunities(self) -> List[str]:
        """Identifies opportunities for journal"""
        opportunities = []
        
        # Emerging topics
        emerging = self._find_emerging_topics()
        if emerging:
            opportunities.append(f"Capitalize on emerging topics: {', '.join(emerging[:3])}")
        
        # High potential articles
        predictor = CitationPredictor(self.articles)
        predictions = predictor.predict_all_articles()
        high_potential = [p for p in predictions if p['predicted_score'] > 70]
        if high_potential:
            opportunities.append(f"Promote {len(high_potential)} high-potential articles")
        
        # Collaboration opportunities
        author_analysis = self.author_analyzer.analyze()
        solo_authors = [a for a in author_analysis.values() if a['num_collaborators'] == 0]
        if len(solo_authors) > len(author_analysis) * 0.2:
            opportunities.append("Encourage international collaboration")
        
        # Open access
        oa_percentage = self._calculate_oa_percentage()
        if oa_percentage < 30:
            opportunities.append("Expand open access to increase visibility")
        
        return opportunities
    
    def _identify_threats(self) -> List[str]:
        """Identifies threats to journal"""
        threats = []
        
        # Declining topics
        declining = self._find_declining_topics()
        if declining:
            threats.append(f"Watch declining topics: {', '.join(declining[:3])}")
        
        # Decreasing citations
        if len(self.articles) > 10:
            recent_citations = sum(a.get('cited_by_count', 0) for a in self.articles 
                                  if a.get('publication_year', 0) > self.current_year - 3)
            older_citations = sum(a.get('cited_by_count', 0) for a in self.articles 
                                if a.get('publication_year', 0) <= self.current_year - 3)
            
            if recent_citations < older_citations * 0.7:
                threats.append("Declining citation rate in recent years")
        
        # Author concentration
        author_analysis = self.author_analyzer.analyze()
        if author_analysis:
            top_authors = sorted(author_analysis.items(), key=lambda x: x[1]['articles'], reverse=True)
            if len(top_authors) > 3:
                top_3_articles = sum(m[1]['articles'] for m in top_authors[:3])
                if top_3_articles > len(self.articles) * 0.4:
                    threats.append("High author concentration - risk of dependency")
        
        return threats
    
    def _get_strategic_recommendations(self) -> List[str]:
        """Generates strategic recommendations"""
        recommendations = []
        
        strengths = self._identify_strengths()
        weaknesses = self._identify_weaknesses()
        opportunities = self._identify_opportunities()
        threats = self._identify_threats()
        
        # Based on SWOT analysis
        if opportunities:
            recommendations.append(f"Priority: {opportunities[0]}")
        
        if weaknesses and 'high percentage of uncited articles' in weaknesses[0]:
            recommendations.append("Implement targeted promotion for uncited articles")
        
        if threats and 'declining citation rate' in threats[0]:
            recommendations.append("Review editorial strategy to boost citations")
        
        if len([s for s in strengths if 'hot topics' in s]) > 0:
            recommendations.append("Feature hot topics in special issues and editorials")
        
        if len(opportunities) > 1:
            recommendations.append(f"Explore: {opportunities[1]}")
        
        return recommendations
    
    def _get_action_items(self) -> List[str]:
        """Generates actionable items"""
        actions = []
        
        # Immediate actions
        all_metrics = self.hot_analyzer.calculate_metrics_for_all_topics(self.hierarchy)
        hot_topics = [m for m in all_metrics if m['ets'] > 70]
        if hot_topics:
            actions.append(f"Create special collection on '{hot_topics[0]['topic']}'")
        
        # Author engagement
        author_analysis = self.author_analyzer.analyze()
        inactive = [name for name, data in author_analysis.items() 
                   if not data['active'] and data['articles'] > 0]
        if inactive:
            actions.append("Reach out to inactive authors")
        
        # Article promotion
        predictor = CitationPredictor(self.articles)
        predictions = predictor.predict_all_articles()
        high_potential = [p for p in predictions if p['predicted_score'] > 70 and p['actual_citations'] < 5]
        if high_potential:
            actions.append(f"Promote {len(high_potential)} underperforming high-potential articles")
        
        # Open access
        oa_percentage = self._calculate_oa_percentage()
        if oa_percentage < 30:
            actions.append("Consider expanding open access options")
        
        return actions
    
    def _assess_competitive_position(self) -> Dict:
        """Assesses competitive position"""
        # In production, this would compare with other journals
        # For now, use internal metrics
        avg_citations = sum(a.get('cited_by_count', 0) for a in self.articles) / len(self.articles) if self.articles else 0
        
        if avg_citations > 10:
            position = "Leader"
        elif avg_citations > 5:
            position = "Competitive"
        elif avg_citations > 3:
            position = "Developing"
        else:
            position = "Emerging"
        
        return {
            'position': position,
            'strength_index': min(100, avg_citations * 5),
            'growth_potential': min(100, self._calculate_eigenfactor() * 50)
        }
    
    def generate_profile(self) -> Dict:
        """
        Generates complete journal profile
        """
        first_year = min(a.get('publication_year', 9999) for a in self.articles) if self.articles else 0
        last_year = max(a.get('publication_year', 0) for a in self.articles) if self.articles else 0
        
        metrics = self._generate_performance_metrics()
        
        return {
            'basic_info': {
                'name': self.journal_info.get('display_name', 'Unknown'),
                'issn': self.journal_info.get('issn', ''),
                'publisher': self.journal_info.get('publisher', ''),
                'total_articles': len(self.articles),
                'publication_period': f"{first_year} - {last_year}" if first_year else 'N/A',
                'open_access': self._calculate_oa_percentage(),
                'avg_articles_per_year': len(self.articles) / (last_year - first_year + 1) if last_year > first_year else 0
            },
            'performance_metrics': metrics,
            'top_articles': self._get_top_articles(),
            'topic_landscape': self._get_topic_landscape(),
            'author_insights': self._get_author_insights(),
            'citation_analysis': self._get_citation_analysis(),
            'hot_topics': self._get_hot_topics(),
            'recommendations': self._get_recommendations(),
            'comparative_analysis': self._get_comparative_analysis(),
            'temporal_trends': self._get_temporal_trends(),
            'international_impact': self._get_international_impact(),
            'editorial_summary': self._generate_editorial_summary()
        }
    
    def _generate_performance_metrics(self) -> Dict:
        """Generates performance metrics"""
        total_citations = sum(a.get('cited_by_count', 0) for a in self.articles)
        highly_cited = sum(1 for a in self.articles if a.get('is_highly_cited', False))
        
        # Simplified IF
        current_year = self.current_year
        if_count = 0
        for year in [current_year - 1, current_year - 2]:
            year_articles = [a for a in self.articles if a.get('publication_year') == year]
            if year_articles:
                citations_to_year = sum(a.get('cited_by_count', 0) for a in year_articles)
                if_count += citations_to_year / len(year_articles)
        avg_if = if_count / 2 if if_count > 0 else 0
        
        return {
            'total_citations': total_citations,
            'avg_citations': total_citations / len(self.articles) if self.articles else 0,
            'highly_cited': highly_cited,
            'highly_cited_percentage': (highly_cited / len(self.articles)) * 100 if self.articles else 0,
            'h_index': self._calculate_h_index_journal(),
            'simplified_if': avg_if,
            'eigenfactor': self._calculate_eigenfactor(),
            'citing_half_life': self._calculate_citing_half_life(),
            'citation_velocity': self._calculate_citation_velocity_journal()
        }
    
    def _get_top_articles(self) -> Dict:
        """Gets top articles by various criteria"""
        if not self.articles:
            return {
                'most_cited': [],
                'most_cited_per_year': [],
                'best_rcr': [],
                'showcase': []
            }
        
        # By total citations
        by_citations = sorted(self.articles, 
                            key=lambda x: x.get('cited_by_count', 0), reverse=True)[:10]
        
        # By citations per year
        by_cpy = sorted(self.articles, 
                       key=lambda x: x.get('citations_per_year', 0), reverse=True)[:10]
        
        # Best RCR (simplified)
        by_rcr = sorted(self.articles, 
                       key=lambda x: x.get('cited_by_count', 0) / max(1, x.get('citations_per_year', 1)), 
                       reverse=True)[:10]
        
        # Showcase selection
        showcase = self._select_showcase_articles()
        
        return {
            'most_cited': by_citations,
            'most_cited_per_year': by_cpy,
            'best_rcr': by_rcr,
            'showcase': showcase
        }
    
    def _select_showcase_articles(self) -> List[dict]:
        """Selects best articles for journal showcase"""
        scored_articles = []
        
        for article in self.articles:
            score = 0
            
            # 1. Citations
            citations = article.get('cited_by_count', 0)
            score += min(citations, 100) * 0.3
            
            # 2. Topic relevance
            topic = article.get('primary_topic', '')
            # Handle case when topic is a dict (from OpenAlex)
            if isinstance(topic, dict):
                topic = topic.get('display_name', '')
            elif not isinstance(topic, str):
                topic = str(topic) if topic else ''
            
            if topic and any(w in topic.lower() for w in ['emerging', 'future', 'novel']):
                score += 20
            
            # 3. Article age (younger gets bonus)
            age = self.current_year - article.get('publication_year', self.current_year)
            if age <= 3:
                score += (4 - age) * 5
            
            # 4. International collaboration
            if self._has_international_collaboration(article):
                score += 15
            
            # 5. Impact velocity
            cpy = article.get('citations_per_year', 0)
            score += min(cpy * 2, 20)
            
            scored_articles.append((score, article))
        
        # Sort by score only, not comparing the article dicts
        scored_articles.sort(key=lambda x: x[0], reverse=True)
        return [article for _, article in scored_articles[:5]]
    
    def _has_international_collaboration(self, article: dict) -> bool:
        """Checks for international collaboration"""
        authors = article.get('authors_list', [])
        if not authors or len(authors) < 2:
            return False
        
        # Ensure authors are strings
        name_styles = set()
        for author in authors:
            # Handle case when author is a dict
            if isinstance(author, dict):
                author = author.get('display_name', '') or author.get('name', '')
            elif not isinstance(author, str):
                author = str(author) if author else ''
            
            if not author:
                continue
                
            if re.search(r'[a-z]', author) and re.search(r'[A-Z]', author):
                name_styles.add('western')
            elif re.search(r'[а-яА-Я]', author):
                name_styles.add('cyrillic')
            else:
                name_styles.add('other')
        
        return len(name_styles) >= 2
    
    def _get_topic_landscape(self) -> Dict:
        """Gets topic landscape overview"""
        all_metrics = self.hot_analyzer.calculate_metrics_for_all_topics(self.hierarchy)
        top_topics = sorted(all_metrics, key=lambda x: x['total_citations'], reverse=True)[:10]
        
        return {
            'strongest_topics': top_topics,
            'most_diverse_field': self._find_most_diverse_field(),
            'emerging_topics': self._find_emerging_topics(),
            'declining_topics': self._find_declining_topics()
        }
    
    def _get_author_insights(self) -> Dict:
        """Gets author insights"""
        analysis = self.author_analyzer.analyze()
        
        top_authors = sorted(analysis.items(), 
                           key=lambda x: x[1]['total_citations'], reverse=True)[:10]
        
        return {
            'top_authors': [{'name': name, **metrics} for name, metrics in top_authors],
            'total_active_authors': sum(1 for a in analysis.values() if a['active']),
            'avg_collaboration': sum(a['num_collaborators'] for a in analysis.values()) / max(1, len(analysis))
        }
    
    def _get_citation_analysis(self) -> Dict:
        """Gets citation analysis"""
        dynamics_results = self.dynamics_analyzer.analyze_all_articles()
        categories = self.dynamics_analyzer.get_categories_summary(dynamics_results)
        
        return {
            'categories': categories,
            'total_analyzed': len(dynamics_results),
            'average_age': sum(a.get('publication_year', 0) for a in self.articles) / len(self.articles) if self.articles else 0
        }
    
    def _get_hot_topics(self) -> List[Dict]:
        """Gets hot topics list"""
        all_metrics = self.hot_analyzer.calculate_metrics_for_all_topics(self.hierarchy)
        return [m for m in all_metrics if m['ets'] > 50][:10]
    
    def _get_recommendations(self) -> List[str]:
        """Gets recommendations for editors"""
        return self._get_strategic_recommendations()
    
    def _get_comparative_analysis(self) -> Dict:
        """Gets comparative analysis"""
        return self._assess_competitive_position()
    
    def _get_temporal_trends(self) -> Dict:
        """Gets temporal trends"""
        yearly_metrics = defaultdict(lambda: {'articles': 0, 'citations': 0})
        
        for article in self.articles:
            year = article.get('publication_year', 0)
            if year > 0:
                yearly_metrics[year]['articles'] += 1
                yearly_metrics[year]['citations'] += article.get('cited_by_count', 0)
        
        years = sorted(yearly_metrics.keys())
        trends = {
            'articles': [yearly_metrics[y]['articles'] for y in years],
            'citations': [yearly_metrics[y]['citations'] for y in years],
            'years': years
        }
        
        # Calculate growth rate
        if len(years) >= 2:
            first_year = years[0]
            last_year = years[-1]
            article_growth = (yearly_metrics[last_year]['articles'] - yearly_metrics[first_year]['articles']) / max(1, yearly_metrics[first_year]['articles']) * 100
            citation_growth = (yearly_metrics[last_year]['citations'] - yearly_metrics[first_year]['citations']) / max(1, yearly_metrics[first_year]['citations']) * 100
        else:
            article_growth = 0
            citation_growth = 0
        
        trends['article_growth'] = article_growth
        trends['citation_growth'] = citation_growth
        
        return trends
    
    def _get_international_impact(self) -> Dict:
        """Gets international impact metrics"""
        # Simplified international impact
        international_authors = 0
        for article in self.articles:
            if self._has_international_collaboration(article):
                international_authors += 1
        
        return {
            'international_collaboration_rate': (international_authors / len(self.articles)) * 100 if self.articles else 0,
            'estimated_international_citations': sum(a.get('cited_by_count', 0) * 0.3 for a in self.articles)
        }
    
    def _generate_editorial_summary(self) -> Dict:
        """
        Generates editorial summary with recommendations
        """
        return {
            'strengths': self._identify_strengths(),
            'weaknesses': self._identify_weaknesses(),
            'opportunities': self._identify_opportunities(),
            'threats': self._identify_threats(),
            'strategic_recommendations': self._get_strategic_recommendations(),
            'action_items': self._get_action_items(),
            'competitive_position': self._assess_competitive_position()
        }

# ============================================================================
# PDF REPORT GENERATION (RUSSIAN) WITH HIERARCHY AND METRICS TOGGLE
# ============================================================================

def generate_pdf_ru(journal_name: str, journal_abbr: str, years: List[int], 
                    hierarchy: Dict, logo_path: str = None, custom_message: str = None,
                    include_metrics: bool = True) -> bytes:
    """Generate PDF report in Russian with hierarchical grouping and citation metrics toggle"""
    
    import hashlib                    
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.fonts import addMapping
    
    # Register Cyrillic font
    import os
    
    font_found = False
    russian_font_name = 'Helvetica'
    
    font_paths = [
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
        '/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf',
        '/usr/share/fonts/truetype/freefont/FreeSans.ttf',
        '/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf',
        '/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf',
        '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
        '/System/Library/Fonts/Helvetica.ttc',
        '/System/Library/Fonts/Arial.ttf',
        '/Library/Fonts/Arial.ttf',
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
    
    # Calculate statistics
    stats = calculate_hierarchy_statistics(hierarchy, include_metrics)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values()) if include_metrics else 0
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
    
    # Styles with Cyrillic support
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
    
    toc_topic_style = ParagraphStyle(
        'TOCTopicStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=3,
        leftIndent=45,
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
    
    # ========== COVER PAGE ==========
    story.append(Spacer(1, 2*cm))
    
    if logo_path and os.path.exists(logo_path):
        try:
            from PIL import Image as PILImage
            
            pil_img = PILImage.open(logo_path)
            original_width, original_height = pil_img.size
            pil_img.close()
            
            max_width = 150
            max_height = 125
            
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
    
    # Customizable text or default
    if custom_message:
        intro_text_raw = format_message_with_variables(custom_message, clean_text(journal_name), years_str)
    else:
        default_msg = DEFAULT_MESSAGES['ru']['body']
        intro_text_raw = format_message_with_variables(default_msg, clean_text(journal_name), years_str)
    
    # Convert markdown to HTML for reportlab
    intro_text = intro_text_raw.replace('\n\n', '<br/><br/>')
    intro_text = intro_text.replace('\n• ', '<br/>• ')
    intro_text = intro_text.replace('\n', '<br/>')
    intro_text = f"<para>{intro_text}</para>"
    
    story.append(Paragraph(intro_text, intro_style))
    
    story.append(Spacer(1, 1*cm))
    
    if include_metrics:
        stats_data = [
            ["Показатель", "Значение"],
            ["Всего статей", str(total_articles)],
            ["Областей науки", str(total_domains)],
            ["Всего цитирований", str(total_citations)],
            ["Средняя цитируемость", f"{total_citations/total_articles:.2f}" if total_articles > 0 else "0"],
            ["Активно цитируемые статьи", str(highly_cited)]
        ]
    else:
        stats_data = [
            ["Показатель", "Значение"],
            ["Всего статей", str(total_articles)],
            ["Областей науки", str(total_domains)]
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
    
    # ========== TABLE OF CONTENTS (Domain -> Field -> Subfield -> Topic) ==========
    story.append(Paragraph("Содержание", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
            anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} статей, {domain_citations} цитирований (avg: {domain_avg:.1f})</a>', toc_domain_style))
        else:
            anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} статей</a>', toc_domain_style))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} статей, {field_citations} цитирований (avg: {field_avg:.1f})', toc_field_style))
            else:
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} статей', toc_field_style))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} статей, {subfield_citations} цитирований (avg: {subfield_avg:.1f})', toc_subfield_style))
                else:
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} статей', toc_subfield_style))
                
                # Add Topics to TOC
                for topic in topics.keys():
                    topic_stats = subfield_stats.get('topics', {}).get(topic, {})
                    topic_articles = topic_stats.get('articles', 0)
                    
                    if include_metrics:
                        topic_citations = topic_stats.get('citations', 0)
                        topic_avg = topic_stats.get('avg_citations', 0)
                        topic_anchor_id = f"topic_{hashlib.md5(f"{domain}_{field}_{subfield}_{topic}".encode('utf-8')).hexdigest()[:8]}"
                        story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{topic_anchor_id}">{clean_text(topic)}</a> — {topic_articles} статей, {topic_citations} цитирований (avg: {topic_avg:.1f})', toc_topic_style))
                    else:
                        topic_anchor_id = f"topic_{hashlib.md5(f"{domain}_{field}_{subfield}_{topic}".encode('utf-8')).hexdigest()[:8]}"
                        story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{topic_anchor_id}">{clean_text(topic)}</a> — {topic_articles} статей', toc_topic_style))
        
        story.append(Spacer(1, 0.3*cm))
    
    story.append(PageBreak())
    
    # ========== ARTICLES BY HIERARCHY WITH ANCHORS ==========
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
        else:
            domain_citations = 0
            domain_avg = 0
        
        anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
        story.append(anchor_para)
        
        if include_metrics:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} статей, {domain_citations} цитирований (avg: {domain_avg:.1f})", domain_style))
        else:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} статей", domain_style))
        story.append(Spacer(1, 0.3*cm))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
            else:
                field_citations = 0
                field_avg = 0
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
            field_anchor_para = Paragraph(f'<a name="{field_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
            story.append(field_anchor_para)
            
            if include_metrics:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} статей, {field_citations} цитирований (avg: {field_avg:.1f})", field_style))
            else:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} статей", field_style))
            story.append(Spacer(1, 0.2*cm))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                else:
                    subfield_citations = 0
                    subfield_avg = 0
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                subfield_anchor_para = Paragraph(f'<a name="{subfield_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
                story.append(subfield_anchor_para)
                
                if include_metrics:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} статей, {subfield_citations} цитирований (avg: {subfield_avg:.1f})", subfield_style))
                else:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} статей", subfield_style))
                story.append(Spacer(1, 0.2*cm))
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg = topic_citations / topic_articles if topic_articles > 0 else 0
                    
                    topic_anchor_id = f"topic_{hashlib.md5(f"{domain}_{field}_{subfield}_{topic}".encode('utf-8')).hexdigest()[:8]}"
                    topic_anchor_para = Paragraph(f'<a name="{topic_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
                    story.append(topic_anchor_para)
                    
                    if include_metrics:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)} — {topic_articles} статей, {topic_citations} цитирований (avg: {topic_avg:.1f})", topic_style))
                    else:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)} — {topic_articles} статей", topic_style))
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
                        
                        # Always show citation info for individual articles
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
    
    # ========== CONCLUSION ==========
    story.append(Paragraph("Заключение", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    avg_overall = total_citations / total_articles if total_articles > 0 else 0
    
    conclusion_text = f"""
    Данный отчет содержит {total_articles} статей из журнала «{clean_text(journal_name)}», 
    сгруппированных по иерархической структуре: {total_domains} областей науки, 
    включающих множество полей и подполей."""
    
    if include_metrics:
        conclusion_text += f""" Общая средняя цитируемость составляет {avg_overall:.2f} цитирований на статью.
    Из них {highly_cited} статей являются активно цитируемыми, что делает их особенно ценными для включения в Ваши научные работы.<br/><br/>"""
    
    conclusion_text += """
    Рекомендуем обратить особое внимание на статьи с пометкой «Активно цитируемая» — 
    они демонстрируют высокий научный интерес и могут стать важной частью Вашего исследования.<br/><br/>
    
    Отчет сгенерирован автоматически с использованием данных OpenAlex API.
    """
    
    story.append(Paragraph(conclusion_text, conclusion_style))
    
    story.append(Spacer(1, 1*cm))
    
    # ========== APP LOGO AT THE END ==========
    try:
        possible_paths = [
            "logo.png",
            "./logo.png",
            "app/logo.png",
            os.path.join(os.path.dirname(__file__), "logo.png"),
            os.path.join(os.getcwd(), "logo.png")
        ]
        
        app_logo_path = None
        for path in possible_paths:
            if os.path.exists(path):
                app_logo_path = path
                break
        
        if app_logo_path:
            from PIL import Image as PILImage
            pil_img = PILImage.open(app_logo_path)
            pil_img.verify()
            pil_img.close()
            
            app_logo = Image(app_logo_path, width=200, height=200)
            app_logo.hAlign = 'CENTER'
            story.append(app_logo)
            story.append(Spacer(1, 0.2*cm))
            logger.info(f"App logo loaded successfully from: {app_logo_path}")
        else:
            story.append(Paragraph("📚", ParagraphStyle(
                'LogoEmoji',
                parent=styles['Normal'],
                fontSize=30,
                textColor=colors.HexColor('#667eea'),
                alignment=TA_CENTER
            )))
            story.append(Spacer(1, 0.2*cm))
            logger.warning("App logo not found, using emoji instead")
            
    except Exception as e:
        logger.error(f"Could not load app logo: {e}")
        story.append(Paragraph("📚", ParagraphStyle(
            'LogoEmoji',
            parent=styles['Normal'],
            fontSize=30,
            textColor=colors.HexColor('#667eea'),
            alignment=TA_CENTER
        )))
        story.append(Spacer(1, 0.2*cm))
    
    story.append(Paragraph(f"© Chimica Techno Acta | {datetime.now().strftime('%d.%m.%Y')}", footer_style))
    story.append(Paragraph("Отчет подготовлен с использованием CTA Journal Analyzer Pro", footer_style))
    
    doc.build(story)
    
    return buffer.getvalue()

# ============================================================================
# PDF REPORT GENERATION (ENGLISH) WITH HIERARCHY AND METRICS TOGGLE
# ============================================================================

def generate_pdf_en(journal_name: str, journal_abbr: str, years: List[int], 
                    hierarchy: Dict, logo_path: str = None, custom_message: str = None,
                    include_metrics: bool = True) -> bytes:
    """Generate PDF report in English with hierarchical grouping and citation metrics toggle"""
    
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
    
    # Calculate statistics
    stats = calculate_hierarchy_statistics(hierarchy, include_metrics)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values()) if include_metrics else 0
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
    
    # Custom styles
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
    
    toc_topic_style = ParagraphStyle(
        'TOCTopicStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#2980B9'),
        spaceAfter=3,
        leftIndent=45,
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
            
            max_width = 150
            max_height = 125
            
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
    
    # Customizable text or default
    if custom_message:
        intro_text_raw = format_message_with_variables(custom_message, clean_text(journal_name), years_str)
    else:
        default_msg = DEFAULT_MESSAGES['en']['body']
        intro_text_raw = format_message_with_variables(default_msg, clean_text(journal_name), years_str)
    
    # Convert markdown to HTML for reportlab
    intro_text = intro_text_raw.replace('\n\n', '<br/><br/>')
    intro_text = intro_text.replace('\n• ', '<br/>• ')
    intro_text = intro_text.replace('\n', '<br/>')
    intro_text = f"<para>{intro_text}</para>"
    
    story.append(Paragraph(intro_text, intro_style))
    
    story.append(Spacer(1, 1*cm))
    
    avg_overall = total_citations / total_articles if total_articles > 0 else 0
    
    if include_metrics:
        stats_data = [
            ["Metric", "Value"],
            ["Total Articles", str(total_articles)],
            ["Research Domains", str(total_domains)],
            ["Total Citations", str(total_citations)],
            ["Average Citations per Article", f"{avg_overall:.2f}"],
            ["Highly Cited Articles", str(highly_cited)]
        ]
    else:
        stats_data = [
            ["Metric", "Value"],
            ["Total Articles", str(total_articles)],
            ["Research Domains", str(total_domains)]
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
    
    # ========== TABLE OF CONTENTS (Domain -> Field -> Subfield -> Topic) ==========
    story.append(Paragraph("Table of Contents", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
            anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} articles, {domain_citations} citations (avg: {domain_avg:.1f})</a>', toc_domain_style))
        else:
            anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} articles</a>', toc_domain_style))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} articles, {field_citations} citations (avg: {field_avg:.1f})', toc_field_style))
            else:
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} articles', toc_field_style))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} articles, {subfield_citations} citations (avg: {subfield_avg:.1f})', toc_subfield_style))
                else:
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} articles', toc_subfield_style))
                
                # Add Topics to TOC
                for topic in topics.keys():
                    topic_stats = subfield_stats.get('topics', {}).get(topic, {})
                    topic_articles = topic_stats.get('articles', 0)
                    
                    if include_metrics:
                        topic_citations = topic_stats.get('citations', 0)
                        topic_avg = topic_stats.get('avg_citations', 0)
                        topic_anchor_id = f"topic_{hashlib.md5(f"{domain}_{field}_{subfield}_{topic}".encode()).hexdigest()[:8]}"
                        story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{topic_anchor_id}">{clean_text(topic)}</a> — {topic_articles} articles, {topic_citations} citations (avg: {topic_avg:.1f})', toc_topic_style))
                    else:
                        topic_anchor_id = f"topic_{hashlib.md5(f"{domain}_{field}_{subfield}_{topic}".encode()).hexdigest()[:8]}"
                        story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{topic_anchor_id}">{clean_text(topic)}</a> — {topic_articles} articles', toc_topic_style))
        
        story.append(Spacer(1, 0.3*cm))
    
    story.append(PageBreak())
    
    # ========== ARTICLES BY HIERARCHY WITH ANCHORS ==========
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
        else:
            domain_citations = 0
            domain_avg = 0
        
        anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
        story.append(anchor_para)
        
        if include_metrics:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} articles, {domain_citations} citations (avg: {domain_avg:.1f})", domain_style))
        else:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} articles", domain_style))
        story.append(Spacer(1, 0.3*cm))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
            else:
                field_citations = 0
                field_avg = 0
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
            field_anchor_para = Paragraph(f'<a name="{field_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
            story.append(field_anchor_para)
            
            if include_metrics:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} articles, {field_citations} citations (avg: {field_avg:.1f})", field_style))
            else:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} articles", field_style))
            story.append(Spacer(1, 0.2*cm))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                else:
                    subfield_citations = 0
                    subfield_avg = 0
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                subfield_anchor_para = Paragraph(f'<a name="{subfield_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
                story.append(subfield_anchor_para)
                
                if include_metrics:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} articles, {subfield_citations} citations (avg: {subfield_avg:.1f})", subfield_style))
                else:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} articles", subfield_style))
                story.append(Spacer(1, 0.2*cm))
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg = topic_citations / topic_articles if topic_articles > 0 else 0
                    
                    topic_anchor_id = f"topic_{hashlib.md5(f"{domain}_{field}_{subfield}_{topic}".encode()).hexdigest()[:8]}"
                    topic_anchor_para = Paragraph(f'<a name="{topic_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
                    story.append(topic_anchor_para)
                    
                    if include_metrics:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)} — {topic_articles} articles, {topic_citations} citations (avg: {topic_avg:.1f})", topic_style))
                    else:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)} — {topic_articles} articles", topic_style))
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
                        
                        # Always show citation info for individual articles
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
    encompassing multiple fields and subfields."""
    
    if include_metrics:
        conclusion_text += f""" The overall average citation rate is {avg_overall:.2f} citations per article.
    Among them, {highly_cited} articles are highly cited, making them particularly valuable for inclusion in your research.<br/><br/>"""
    
    conclusion_text += """
    We recommend paying special attention to articles marked as "Highly Cited" — 
    they demonstrate significant scientific interest and can become an important part 
    of your research.<br/><br/>
    
    This report was automatically generated using OpenAlex API data.
    """
    
    story.append(Paragraph(conclusion_text, conclusion_style))
    
    story.append(Spacer(1, 1*cm))
    
    # ========== APP LOGO AT THE END ==========
    try:
        possible_paths = [
            "logo.png",
            "./logo.png",
            "app/logo.png",
            os.path.join(os.path.dirname(__file__), "logo.png"),
            os.path.join(os.getcwd(), "logo.png")
        ]
        
        app_logo_path = None
        for path in possible_paths:
            if os.path.exists(path):
                app_logo_path = path
                break
        
        if app_logo_path:
            from PIL import Image as PILImage
            pil_img = PILImage.open(app_logo_path)
            pil_img.verify()
            pil_img.close()
            
            app_logo = Image(app_logo_path, width=200, height=200)
            app_logo.hAlign = 'CENTER'
            story.append(app_logo)
            story.append(Spacer(1, 0.2*cm))
            logger.info(f"App logo loaded successfully from: {app_logo_path}")
        else:
            story.append(Paragraph("📚", ParagraphStyle(
                'LogoEmoji',
                parent=styles['Normal'],
                fontSize=30,
                textColor=colors.HexColor('#667eea'),
                alignment=TA_CENTER
            )))
            story.append(Spacer(1, 0.2*cm))
            logger.warning("App logo not found, using emoji instead")
            
    except Exception as e:
        logger.error(f"Could not load app logo: {e}")
        story.append(Paragraph("📚", ParagraphStyle(
            'LogoEmoji',
            parent=styles['Normal'],
            fontSize=30,
            textColor=colors.HexColor('#667eea'),
            alignment=TA_CENTER
        )))
        story.append(Spacer(1, 0.2*cm))
    
    story.append(Paragraph(f"© Chimica Techno Acta | {datetime.now().strftime('%d.%m.%Y')}", footer_style))
    story.append(Paragraph("Report generated using CTA Journal Analyzer Pro", footer_style))
    
    doc.build(story)
    
    return buffer.getvalue()

# ============================================================================
# TXT REPORT GENERATION (RUSSIAN) WITH HIERARCHY AND METRICS TOGGLE
# ============================================================================

def generate_txt_ru(journal_name: str, years: List[int], hierarchy: Dict, custom_message: str = None,
                   include_metrics: bool = True) -> str:
    """Generate TXT report in Russian with hierarchical grouping and citation metrics toggle"""
    
    output = []
    
    years_str = format_year_filter_for_filename(years)
    
    # Calculate statistics
    stats = calculate_hierarchy_statistics(hierarchy, include_metrics)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values()) if include_metrics else 0
    highly_cited = sum(1 for domain in hierarchy.values() 
                      for field in domain.values()
                      for subfield in field.values()
                      for topic in subfield.values()
                      for a in topic if a.get('is_highly_cited', False))
    
    # Header
    output.append("=" * 80)
    output.append(f"АНАЛИТИЧЕСКИЙ ОТЧЕТ")
    output.append(f"Журнал: {journal_name}")
    output.append(f"Период публикации: {years_str}")
    output.append("=" * 80)
    output.append("")
    
    # Introduction (customizable)
    if custom_message:
        intro_text = format_message_with_variables(custom_message, journal_name, years_str)
    else:
        intro_text = format_message_with_variables(DEFAULT_MESSAGES['ru']['body'], journal_name, years_str)
    
    output.append(intro_text)
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Statistics
    avg_overall = total_citations / total_articles if total_articles > 0 else 0
    
    output.append("СТАТИСТИКА")
    output.append("-" * 40)
    output.append(f"Всего статей: {total_articles}")
    output.append(f"Областей науки: {total_domains}")
    if include_metrics:
        output.append(f"Всего цитирований: {total_citations}")
        output.append(f"Средняя цитируемость: {avg_overall:.2f}")
        output.append(f"Активно цитируемые статьи: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Table of Contents (Domain -> Field -> Subfield -> Topic)
    output.append("СОДЕРЖАНИЕ")
    output.append("-" * 40)
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
            output.append(f"{domain} — {domain_articles} статей, {domain_citations} цитирований (avg: {domain_avg:.1f})")
        else:
            output.append(f"{domain} — {domain_articles} статей")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
                output.append(f"  └── {field} — {field_articles} статей, {field_citations} цитирований (avg: {field_avg:.1f})")
            else:
                output.append(f"  └── {field} — {field_articles} статей")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                    output.append(f"      └── {subfield} — {subfield_articles} статей, {subfield_citations} цитирований (avg: {subfield_avg:.1f})")
                else:
                    output.append(f"      └── {subfield} — {subfield_articles} статей")
                
                # Add Topics to TOC
                for topic in topics.keys():
                    topic_stats = subfield_stats.get('topics', {}).get(topic, {})
                    topic_articles = topic_stats.get('articles', 0)
                    
                    if include_metrics:
                        topic_citations = topic_stats.get('citations', 0)
                        topic_avg = topic_stats.get('avg_citations', 0)
                        output.append(f"          └── {topic} — {topic_articles} статей, {topic_citations} цитирований (avg: {topic_avg:.1f})")
                    else:
                        output.append(f"          └── {topic} — {topic_articles} статей")
    
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Articles by hierarchy
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
        else:
            domain_citations = 0
            domain_avg = 0
        
        output.append("")
        output.append("█" * 80)
        if include_metrics:
            output.append(f"ОБЛАСТЬ: {domain} — {domain_articles} статей, {domain_citations} цитирований (avg: {domain_avg:.1f})")
        else:
            output.append(f"ОБЛАСТЬ: {domain} — {domain_articles} статей")
        output.append("█" * 80)
        output.append("")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
            else:
                field_citations = 0
                field_avg = 0
            
            if include_metrics:
                output.append(f"▓▓▓ ПОЛЕ: {field} — {field_articles} статей, {field_citations} цитирований (avg: {field_avg:.1f}) ▓▓▓")
            else:
                output.append(f"▓▓▓ ПОЛЕ: {field} — {field_articles} статей ▓▓▓")
            output.append("")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                else:
                    subfield_citations = 0
                    subfield_avg = 0
                
                if include_metrics:
                    output.append(f"▒▒▒ ПОДПОЛЕ: {subfield} — {subfield_articles} статей, {subfield_citations} цитирований (avg: {subfield_avg:.1f}) ▒▒▒")
                else:
                    output.append(f"▒▒▒ ПОДПОЛЕ: {subfield} — {subfield_articles} статей ▒▒▒")
                output.append("")
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg = topic_citations / topic_articles if topic_articles > 0 else 0
                    
                    if include_metrics:
                        output.append(f"  ● ТЕМА: {topic} — {topic_articles} статей, {topic_citations} цитирований (avg: {topic_avg:.1f})")
                    else:
                        output.append(f"  ● ТЕМА: {topic} — {topic_articles} статей")
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
                        
                        # Always show citation info for individual articles
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
    
    # Conclusion
    output.append("=" * 80)
    output.append("ЗАКЛЮЧЕНИЕ")
    output.append("=" * 80)
    output.append("")
    output.append(f"Данный отчет содержит {total_articles} статей из журнала «{journal_name}»,")
    output.append(f"сгруппированных по иерархической структуре: {total_domains} областей науки,")
    output.append(f"включающих множество полей и подполей.")
    
    if include_metrics:
        output.append(f"Общая средняя цитируемость составляет {avg_overall:.2f} цитирований на статью.")
        output.append(f"Из них {highly_cited} статей являются активно цитируемыми, что делает их особенно ценными для включения")
    else:
        output.append(f"Из них {highly_cited} статей являются активно цитируемыми, что делает их особенно ценными для включения")
    
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
# TXT REPORT GENERATION (ENGLISH) WITH HIERARCHY AND METRICS TOGGLE
# ============================================================================

def generate_txt_en(journal_name: str, years: List[int], hierarchy: Dict, custom_message: str = None,
                   include_metrics: bool = True) -> str:
    """Generate TXT report in English with hierarchical grouping and citation metrics toggle"""
    
    output = []
    
    years_str = format_year_filter_for_filename(years)
    
    # Calculate statistics
    stats = calculate_hierarchy_statistics(hierarchy, include_metrics)
    total_articles = sum(s['articles'] for s in stats.values())
    total_domains = len(hierarchy)
    total_citations = sum(s['citations'] for s in stats.values()) if include_metrics else 0
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
    avg_overall = total_citations / total_articles if total_articles > 0 else 0
    
    output.append("STATISTICS")
    output.append("-" * 40)
    output.append(f"Total Articles: {total_articles}")
    output.append(f"Research Domains: {total_domains}")
    if include_metrics:
        output.append(f"Total Citations: {total_citations}")
        output.append(f"Average Citations per Article: {avg_overall:.2f}")
        output.append(f"Highly Cited Articles: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Table of Contents (Domain -> Field -> Subfield -> Topic)
    output.append("TABLE OF CONTENTS")
    output.append("-" * 40)
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
            output.append(f"{domain} — {domain_articles} articles, {domain_citations} citations (avg: {domain_avg:.1f})")
        else:
            output.append(f"{domain} — {domain_articles} articles")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
                output.append(f"  └── {field} — {field_articles} articles, {field_citations} citations (avg: {field_avg:.1f})")
            else:
                output.append(f"  └── {field} — {field_articles} articles")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                    output.append(f"      └── {subfield} — {subfield_articles} articles, {subfield_citations} citations (avg: {subfield_avg:.1f})")
                else:
                    output.append(f"      └── {subfield} — {subfield_articles} articles")
                
                # Add Topics to TOC
                for topic in topics.keys():
                    topic_stats = subfield_stats.get('topics', {}).get(topic, {})
                    topic_articles = topic_stats.get('articles', 0)
                    
                    if include_metrics:
                        topic_citations = topic_stats.get('citations', 0)
                        topic_avg = topic_stats.get('avg_citations', 0)
                        output.append(f"          └── {topic} — {topic_articles} articles, {topic_citations} citations (avg: {topic_avg:.1f})")
                    else:
                        output.append(f"          └── {topic} — {topic_articles} articles")
    
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Articles by hierarchy
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg = domain_stats.get('avg_citations', 0)
        else:
            domain_citations = 0
            domain_avg = 0
        
        output.append("")
        output.append("█" * 80)
        if include_metrics:
            output.append(f"DOMAIN: {domain} — {domain_articles} articles, {domain_citations} citations (avg: {domain_avg:.1f})")
        else:
            output.append(f"DOMAIN: {domain} — {domain_articles} articles")
        output.append("█" * 80)
        output.append("")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg = field_stats.get('avg_citations', 0)
            else:
                field_citations = 0
                field_avg = 0
            
            if include_metrics:
                output.append(f"▓▓▓ FIELD: {field} — {field_articles} articles, {field_citations} citations (avg: {field_avg:.1f}) ▓▓▓")
            else:
                output.append(f"▓▓▓ FIELD: {field} — {field_articles} articles ▓▓▓")
            output.append("")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg = subfield_stats.get('avg_citations', 0)
                else:
                    subfield_citations = 0
                    subfield_avg = 0
                
                if include_metrics:
                    output.append(f"▒▒▒ SUBFIELD: {subfield} — {subfield_articles} articles, {subfield_citations} citations (avg: {subfield_avg:.1f}) ▒▒▒")
                else:
                    output.append(f"▒▒▒ SUBFIELD: {subfield} — {subfield_articles} articles ▒▒▒")
                output.append("")
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg = topic_citations / topic_articles if topic_articles > 0 else 0
                    
                    if include_metrics:
                        output.append(f"  ● TOPIC: {topic} — {topic_articles} articles, {topic_citations} citations (avg: {topic_avg:.1f})")
                    else:
                        output.append(f"  ● TOPIC: {topic} — {topic_articles} articles")
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
                        
                        # Always show citation info for individual articles
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
    output.append(f"encompassing multiple fields and subfields.")
    
    if include_metrics:
        output.append(f"The overall average citation rate is {avg_overall:.2f} citations per article.")
        output.append(f"Among them, {highly_cited} articles are highly cited, making them particularly valuable for inclusion in your research.")
    else:
        output.append(f"Among them, {highly_cited} articles are highly cited, making them particularly valuable for inclusion in your research.")
    
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
# NEW: VISUALIZATION FUNCTIONS FOR UI DASHBOARDS
# ============================================================================

def create_topic_treemap(hierarchy: Dict, stats: Dict):
    """
    Creates interactive treemap of research topics
    """
    import plotly.graph_objects as go
    
    # Prepare data for treemap
    labels = []
    parents = []
    values = []
    colors = []
    text = []
    
    # Add root node
    labels.append('All Articles')
    parents.append('')
    values.append(sum(s['articles'] for s in stats.values()))
    colors.append('#667eea')
    text.append(f"Total: {values[0]} articles")
    
    # Add domains
    for domain, domain_stats in stats.items():
        domain_idx = len(labels)
        labels.append(domain)
        parents.append('All Articles')
        values.append(domain_stats['articles'])
        colors.append('#764ba2')
        text.append(f"{domain}<br>{domain_stats['articles']} articles")
        
        # Add fields
        for field, field_stats in domain_stats['fields'].items():
            labels.append(field)
            parents.append(domain)
            values.append(field_stats['articles'])
            colors.append('#f093fb')
            text.append(f"{field}<br>{field_stats['articles']} articles")
            
            # Add subfields
            for subfield, subfield_stats in field_stats['subfields'].items():
                labels.append(subfield)
                parents.append(field)
                values.append(subfield_stats['articles'])
                colors.append('#4facfe')
                text.append(f"{subfield}<br>{subfield_stats['articles']} articles")
    
    fig = go.Figure(go.Treemap(
        labels=labels,
        parents=parents,
        values=values,
        text=text,
        textinfo='text',
        hoverinfo='label+value',
        marker=dict(
            colors=colors,
            colorscale='Viridis',
            showscale=True,
            line=dict(width=2, color='white')
        ),
        hovertemplate='<b>%{label}</b><br>Articles: %{value}<extra></extra>'
    ))
    
    fig.update_layout(
        title='Research Topic Hierarchy - Treemap',
        width=800,
        height=600
    )
    
    return fig

def create_topic_bubble_chart(hierarchy: Dict, stats: Dict, hot_metrics: List[Dict]):
    """
    Creates interactive bubble chart of research topics
    """
    import plotly.express as px
    
    data = []
    
    for domain, domain_stats in stats.items():
        for field, field_stats in domain_stats['fields'].items():
            for subfield, subfield_stats in field_stats['subfields'].items():
                articles = subfield_stats['articles']
                citations = subfield_stats['citations'] if subfield_stats['citations'] is not None else 0
                avg_citations = subfield_stats['avg_citations'] if subfield_stats['avg_citations'] is not None else 0
                
                # Find hot metrics for this subfield
                hot_score = 0
                for metric in hot_metrics:
                    if metric['subfield'] == subfield:
                        hot_score = metric['ets']
                        break
                
                data.append({
                    'Domain': domain,
                    'Field': field,
                    'Subfield': subfield,
                    'Articles': articles,
                    'Citations': citations,
                    'Avg Citations': round(avg_citations, 1),
                    'Hot Score': hot_score,
                    'Color': hot_score
                })
    
    df = pd.DataFrame(data)
    
    fig = px.scatter(df,
                     x='Citations',
                     y='Avg Citations',
                     size='Articles',
                     color='Hot Score',
                     hover_name='Subfield',
                     hover_data=['Domain', 'Field', 'Articles'],
                     color_continuous_scale='Viridis',
                     title='Research Topics: Citations vs Avg Citations',
                     labels={
                         'Citations': 'Total Citations',
                         'Avg Citations': 'Avg Citations per Article'
                     })
    
    fig.update_traces(
        marker=dict(line=dict(width=1, color='white')),
        hovertemplate='<b>%{hovertext}</b><br>' +
                     'Domain: %{customdata[0]}<br>' +
                     'Field: %{customdata[1]}<br>' +
                     'Articles: %{customdata[2]}<br>' +
                     'Citations: %{x}<br>' +
                     'Avg Citations: %{y}<extra></extra>'
    )
    
    return fig

def create_citation_timeline(articles: List[dict]):
    """
    Creates citation timeline chart
    """
    import plotly.graph_objects as go
    
    years_data = defaultdict(lambda: {'articles': 0, 'citations': 0, 'highly_cited': 0})
    
    for article in articles:
        year = article.get('publication_year', 0)
        if year > 0:
            years_data[year]['articles'] += 1
            years_data[year]['citations'] += article.get('cited_by_count', 0)
            if article.get('is_highly_cited', False):
                years_data[year]['highly_cited'] += 1
    
    years = sorted(years_data.keys())
    article_counts = [years_data[y]['articles'] for y in years]
    citation_counts = [years_data[y]['citations'] for y in years]
    highly_cited_counts = [years_data[y]['highly_cited'] for y in years]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=years,
        y=article_counts,
        name='Articles',
        marker_color='#667eea',
        yaxis='y'
    ))
    
    fig.add_trace(go.Scatter(
        x=years,
        y=citation_counts,
        name='Citations',
        marker_color='#764ba2',
        yaxis='y2',
        line=dict(width=2)
    ))
    
    fig.add_trace(go.Bar(
        x=years,
        y=highly_cited_counts,
        name='Highly Cited',
        marker_color='#f093fb',
        yaxis='y'
    ))
    
    fig.update_layout(
        title='Publication and Citation Trends',
        xaxis_title='Year',
        yaxis=dict(
            title='Number of Articles',
            side='left'
        ),
        yaxis2=dict(
            title='Number of Citations',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5
        )
    )
    
    return fig

# ============================================================================
# APPLICATION INTERFACE
# ============================================================================

def main():
    """Main application function"""
    
    # Language switcher
    col_lang1, col_lang2 = st.columns([6, 1])
    with col_lang2:
        language = st.selectbox("🌐", ["English", "Русский"], key="language_selector")
    
    lang = 'en' if language == "English" else 'ru'
    t = LANGUAGES[lang]
    
    # Initialize session state
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
    if 'include_metrics' not in st.session_state:
        st.session_state.include_metrics = True
    if 'threshold_total' not in st.session_state:
        st.session_state.threshold_total = None
    if 'threshold_per_year' not in st.session_state:
        st.session_state.threshold_per_year = None
    
    # Header
    import os
    from PIL import Image
    
    logo_path = "logo.png"
    if os.path.exists(logo_path):
        col1, col2, col3 = st.columns([1, 2, 10])
        with col2:
            st.image(logo_path, use_container_width=True)
            st.markdown(f"<p style='font-size: 1rem; color: #666; text-align: center; margin-top: 0.5rem;'>{t['app_subtitle']}</p>", unsafe_allow_html=True)
    else:
        st.markdown(f"<h1 class='main-header'>{t['app_title']}</h1>", unsafe_allow_html=True)
        st.markdown(f"<p style='font-size: 1rem; color: #666; margin-bottom: 1.5rem;'>{t['app_subtitle']}</p>", unsafe_allow_html=True)
    
    # Clear old cache
    clear_old_cache()
    
    # Step 1: Enter ISSN and logo
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
                                # Save logo temporarily
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
    
    # Step 2: Select years
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
            if st.button(t['back_btn'], use_container_width=True):
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
                                        # Get thresholds from session state
                                        threshold_total = st.session_state.threshold_total
                                        threshold_per_year = st.session_state.threshold_per_year
                                        hierarchy_unsorted = group_articles_by_hierarchy(articles, threshold_total, threshold_per_year)
                                        # Apply sorting based on current include_metrics setting
                                        hierarchy = sort_hierarchy_by_rules(hierarchy_unsorted, st.session_state.include_metrics)
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
    
    # Step 3: Results
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
        articles = st.session_state.articles
        
        # Calculate statistics for display
        stats = calculate_hierarchy_statistics(hierarchy, st.session_state.include_metrics)
        total_articles = sum(s['articles'] for s in stats.values())
        total_domains = len(hierarchy)
        total_citations = sum(s['citations'] for s in stats.values()) if st.session_state.include_metrics else 0
        highly_cited = sum(1 for domain in hierarchy.values() 
                          for field in domain.values()
                          for subfield in field.values()
                          for topic in subfield.values()
                          for a in topic if a.get('is_highly_cited', False))
        
        if total_articles > 0:
            # Metrics in beautiful cards
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
            
            # Back button to Step 2
            if st.button(t['back_btn'], use_container_width=True):
                st.session_state.step = 2
                st.rerun()
            
            # Citation metrics toggle section
            st.markdown("---")
            st.markdown("### 📊 Citation Settings")
            
            col_metrics1, col_metrics2 = st.columns([2, 1])
            with col_metrics1:
                include_metrics = st.checkbox(
                    t['include_metrics'],
                    value=st.session_state.include_metrics,
                    key="include_metrics_checkbox"
                )
                
                if include_metrics != st.session_state.include_metrics:
                    st.session_state.include_metrics = include_metrics
                    # Recalculate hierarchy with new metrics setting
                    threshold_total = st.session_state.threshold_total
                    threshold_per_year = st.session_state.threshold_per_year
                    hierarchy_unsorted = group_articles_by_hierarchy(
                        st.session_state.articles, threshold_total, threshold_per_year
                    )
                    st.session_state.hierarchy = sort_hierarchy_by_rules(hierarchy_unsorted, include_metrics)
                    st.rerun()
            
            # Threshold inputs (only shown when metrics are included)
            if st.session_state.include_metrics:
                st.markdown("#### 🔥 Highly Cited Thresholds")
                st.markdown("*Leave fields empty to disable 'Highly Cited' marking*")
                
                col_thresh1, col_thresh2 = st.columns(2)
                with col_thresh1:
                    threshold_total_input = st.number_input(
                        t['highly_cited_threshold_total'],
                        min_value=0,
                        value=st.session_state.threshold_total if st.session_state.threshold_total is not None else 0,
                        step=1,
                        key="threshold_total_input"
                    )
                    threshold_total = threshold_total_input if threshold_total_input > 0 else None
                
                with col_thresh2:
                    threshold_per_year_input = st.number_input(
                        t['highly_cited_threshold_per_year'],
                        min_value=0,
                        value=st.session_state.threshold_per_year if st.session_state.threshold_per_year is not None else 0,
                        step=1,
                        key="threshold_per_year_input"
                    )
                    threshold_per_year = threshold_per_year_input if threshold_per_year_input > 0 else None
                
                # Check if thresholds changed
                if threshold_total != st.session_state.threshold_total or threshold_per_year != st.session_state.threshold_per_year:
                    st.session_state.threshold_total = threshold_total
                    st.session_state.threshold_per_year = threshold_per_year
                    # Recalculate hierarchy with new thresholds
                    hierarchy_unsorted = group_articles_by_hierarchy(
                        st.session_state.articles, threshold_total, threshold_per_year
                    )
                    st.session_state.hierarchy = sort_hierarchy_by_rules(hierarchy_unsorted, st.session_state.include_metrics)
                    st.rerun()
            
            # Custom message section
            st.markdown("---")
            st.markdown(f"### ✏️ {t['customize_message']}")
            
            with st.expander(f"📝 {t['customize_message']} ({language})"):
                if language == "English":
                    edited_message = st.text_area(
                        t['message_preview'],
                        value=st.session_state.custom_message_en,
                        height=300,
                        key="custom_message_editor_en"
                    )
                    if edited_message != st.session_state.custom_message_en:
                        st.session_state.custom_message_en = edited_message
                    
                    if st.button(t['use_default'], key="reset_en"):
                        st.session_state.custom_message_en = DEFAULT_MESSAGES['en']['body']
                        st.rerun()
                else:
                    edited_message = st.text_area(
                        t['message_preview'],
                        value=st.session_state.custom_message_ru,
                        height=300,
                        key="custom_message_editor_ru"
                    )
                    if edited_message != st.session_state.custom_message_ru:
                        st.session_state.custom_message_ru = edited_message
                    
                    if st.button(t['use_default'], key="reset_ru"):
                        st.session_state.custom_message_ru = DEFAULT_MESSAGES['ru']['body']
                        st.rerun()
            
            # ============================================================================
            # NEW: EDITORIAL DASHBOARD WITH TABS
            # ============================================================================
            
            st.markdown("---")
            st.markdown(f"### {t['editorial_dashboard']}")
            
            # Initialize all analyzers
            hot_analyzer = HotTopicsAnalyzer(articles)
            dynamics_analyzer = CitationDynamicsAnalyzer(articles)
            author_analyzer = AuthorAnalyzer(articles)
            predictor = CitationPredictor(articles)
            editors_choice = EditorsChoiceModule(articles, hierarchy)
            profile_generator = JournalProfileGenerator(
                st.session_state.journal_info, articles, hierarchy
            )
            profile = profile_generator.generate_profile()
            
            # Create tabs for different dashboards
            tabs = st.tabs([
                "📊 Overview",
                "🔥 Hot Topics",
                "📈 Dynamics",
                "👥 Authors",
                "🔮 Predictions",
                "⭐ Editor's Choice",
                "📋 Research Hierarchy"
            ])
            
            # Tab 1: Overview
            with tabs[0]:
                st.markdown("### 📊 Editorial Dashboard Overview")
                
                # Key metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("📄 Total Articles", len(articles))
                with col2:
                    avg_citations_total = sum(a.get('cited_by_count', 0) for a in articles) / len(articles) if articles else 0
                    st.metric("📊 Avg Citations", f"{avg_citations_total:.1f}")
                with col3:
                    all_metrics = hot_analyzer.calculate_metrics_for_all_topics(hierarchy)
                    hot_topics_count = len([m for m in all_metrics if m['ets'] > 70])
                    st.metric("🔥 Hot Topics", hot_topics_count)
                with col4:
                    author_analysis = author_analyzer.analyze()
                    active_authors = sum(1 for a in author_analysis.values() if a.get('active', False))
                    st.metric("👥 Active Authors", active_authors)
                
                # SWOT Analysis
                st.markdown("#### 📊 SWOT Analysis")
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Strengths**")
                    for s in profile['editorial_summary']['strengths'][:5]:
                        st.markdown(f"✅ {s}")
                    
                    st.markdown("**Opportunities**")
                    for o in profile['editorial_summary']['opportunities'][:5]:
                        st.markdown(f"🚀 {o}")
                
                with col2:
                    st.markdown("**Weaknesses**")
                    for w in profile['editorial_summary']['weaknesses'][:5]:
                        st.markdown(f"⚠️ {w}")
                    
                    st.markdown("**Threats**")
                    for t_list in profile['editorial_summary']['threats'][:5]:
                        st.markdown(f"🔴 {t_list}")
                
                # Strategic Recommendations
                st.markdown("#### 🎯 Strategic Recommendations")
                for rec in profile['editorial_summary']['strategic_recommendations']:
                    st.markdown(f"• {rec}")
                
                # Action Items
                st.markdown("#### ✅ Action Items")
                for action in profile['editorial_summary']['action_items']:
                    st.markdown(f"• {action}")
                
                # Publication trends
                st.markdown("#### 📈 Publication and Citation Trends")
                fig_timeline = create_citation_timeline(articles)
                st.plotly_chart(fig_timeline, use_container_width=True)
            
            # Tab 2: Hot Topics
            with tabs[1]:
                st.markdown(f"### {t['hot_topics_dashboard']}")
                
                all_metrics = hot_analyzer.calculate_metrics_for_all_topics(hierarchy)
                
                # Top hot topics
                st.markdown("#### 🏆 Top 10 Hottest Topics")
                for idx, metric in enumerate(all_metrics[:10], 1):
                    zone_colors = {
                        '🔥 EMERGING STAR': '#FF6B6B',
                        '📈 GROWING POWER': '#FFA94D',
                        '⚡ ESTABLISHED HOT': '#FFD93D',
                        '🌱 PROMISING': '#6BCB77',
                        '📉 DECLINING': '#A9A9A9',
                        '💤 DORMANT': '#D3D3D3'
                    }
                    color = zone_colors.get(metric['hot_zone'], '#667eea')
                    
                    st.markdown(f"""
                    <div style="background: {color}15; border-left: 4px solid {color}; 
                                padding: 12px; border-radius: 8px; margin: 8px 0;">
                        <b style="color: {color};">{idx}. {metric['topic'][:60]}</b><br>
                        <span style="font-size: 0.85rem;">
                            ETS: {metric['ets']:.1f} | 
                            CAGR: {metric['cagr']:.1f}% | 
                            RCR: {metric['rcr']:.2f} |
                            Articles: {metric['articles']}
                        </span><br>
                        <span style="font-size: 0.8rem; color: #666;">
                            {metric['hot_zone']} | 
                            Momentum: {metric['momentum']:.2f}
                        </span>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Bubble chart
                st.markdown("#### 📊 Topic Visualization")
                fig_bubble = create_topic_bubble_chart(hierarchy, stats, all_metrics)
                st.plotly_chart(fig_bubble, use_container_width=True)
            
            # Tab 3: Citation Dynamics
            with tabs[2]:
                st.markdown(f"### {t['citation_dynamics']}")
                
                dynamics_results = dynamics_analyzer.analyze_all_articles()
                categories = dynamics_analyzer.get_categories_summary(dynamics_results)
                
                # Category summary
                st.markdown("#### 📊 Category Distribution")
                category_data = []
                for cat, items in categories.items():
                    category_data.append({
                        'Category': cat,
                        'Count': len(items)
                    })
                
                df_cat = pd.DataFrame(category_data)
                fig_cat = px.bar(df_cat, x='Category', y='Count', 
                                title='Article Categories Distribution',
                                color='Count',
                                color_continuous_scale='Viridis')
                st.plotly_chart(fig_cat, use_container_width=True)
                
                # Dormant and Awakening articles
                st.markdown("#### 💤 Dormant / 🌅 Awakening Articles")
                
                dormant = [r for r in dynamics_results if 'DORMANT' in r['category']]
                awakening = [r for r in dynamics_results if 'AWAKENING' in r['category']]
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("💤 Dormant", len(dormant))
                with col2:
                    st.metric("🌅 Awakening", len(awakening))
                with col3:
                    avg_revival = sum(r.get('revival_chance', 0) for r in dynamics_results) / len(dynamics_results) if dynamics_results else 0
                    st.metric("📊 Avg Revival Chance", f"{avg_revival:.1%}")
                
                # Show top awakening candidates
                if awakening:
                    st.markdown("#### 🌅 Top Awakening Candidates")
                    for r in awakening[:5]:
                        st.markdown(f"""
                        <div style="padding: 8px; margin: 4px 0; background: #f8f9fa; border-radius: 4px;">
                            <b>{r['title'][:80]}</b><br>
                            <span style="font-size: 0.85rem; color: #666;">
                                Awakening Score: {r['awakening_score']:.0f}/100 | 
                                Revival Chance: {r['revival_chance']:.1%}
                            </span>
                        </div>
                        """, unsafe_allow_html=True)
            
            # Tab 4: Authors
            with tabs[3]:
                st.markdown(f"### {t['author_dashboard']}")
                
                # Top authors
                st.markdown("#### 🏆 Top Authors by Citations")
                top_authors = author_analyzer.get_top_authors(10)
                
                author_data = []
                for author in top_authors:
                    author_data.append({
                        'Author': author['name'][:30],
                        'Articles': author['articles'],
                        'Citations': author['total_citations'],
                        'H-index': author['h_index'],
                        'Career Stage': author['career_stage'],
                        'Active': '✅' if author['active'] else '❌'
                    })
                
                df_authors = pd.DataFrame(author_data)
                st.dataframe(df_authors, use_container_width=True)
                
                # Collaboration network
                st.markdown("#### 🤝 Collaboration Network")
                network = author_analyzer.get_collaboration_network(15)
                
                if network['edges']:
                    import networkx as nx
                    G = nx.Graph()
                    
                    for node in network['nodes']:
                        G.add_node(node['id'], articles=node['articles'])
                    
                    for edge in network['edges']:
                        G.add_edge(edge['source'], edge['target'], weight=edge['weight'])
                    
                    if G.nodes():
                        pos = nx.spring_layout(G, k=2, iterations=50)
                        
                        # Create edge trace
                        edge_trace = go.Scatter(
                            x=[],
                            y=[],
                            line=dict(width=1, color='#888'),
                            hoverinfo='none',
                            mode='lines'
                        )
                        
                        for edge in G.edges():
                            x0, y0 = pos[edge[0]]
                            x1, y1 = pos[edge[1]]
                            edge_trace['x'] += (x0, x1, None)
                            edge_trace['y'] += (y0, y1, None)
                        
                        # Create node trace
                        node_trace = go.Scatter(
                            x=[],
                            y=[],
                            text=[],
                            mode='markers+text',
                            hoverinfo='text',
                            marker=dict(
                                size=[],
                                color=[],
                                line=dict(width=2, color='white')
                            )
                        )
                        
                        for node in G.nodes():
                            x, y = pos[node]
                            node_trace['x'] += (x,)
                            node_trace['y'] += (y,)
                            node_trace['text'] += (node,)
                            node_trace['marker']['size'] += (10 + G.nodes[node]['articles'] * 2,)
                            node_trace['marker']['color'] += ('#667eea',)
                        
                        fig_network = go.Figure(data=[edge_trace, node_trace])
                        fig_network.update_layout(
                            title='Author Collaboration Network',
                            showlegend=False,
                            hovermode='closest',
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                        )
                        st.plotly_chart(fig_network, use_container_width=True)
                else:
                    st.info("Not enough collaboration data to build network.")
            
            # Tab 5: Predictions
            with tabs[4]:
                st.markdown(f"### {t['predictor_dashboard']}")
                
                predictions = predictor.predict_all_articles()
                
                # Filters
                col1, col2 = st.columns(2)
                with col1:
                    min_score = st.slider("Minimum Score", 0, 100, 50, key="pred_min_score")
                with col2:
                    categories_list = list(set(p['category'] for p in predictions))
                    selected_categories = st.multiselect("Categories", categories_list, 
                                                       default=categories_list[:2])
                
                filtered = [p for p in predictions 
                           if p['predicted_score'] >= min_score
                           and (not selected_categories or p['category'] in selected_categories)]
                
                st.markdown(f"#### 🔮 Citation Potential Predictions ({len(filtered)} articles)")
                
                for pred in filtered[:15]:
                    color = '#4CAF50' if pred['predicted_score'] > 70 else '#FFA726' if pred['predicted_score'] > 50 else '#EF5350'
                    
                    st.markdown(f"""
                    <div style="background: {color}10; border-left: 4px solid {color};
                                padding: 12px; border-radius: 8px; margin: 8px 0;">
                        <div style="display: flex; justify-content: space-between;">
                            <b>{pred['title'][:100]}</b>
                            <span style="font-weight: bold; color: {color};">Score: {pred['predicted_score']:.0f}</span>
                        </div>
                        <span style="font-size: 0.85rem; color: #666;">
                            {pred['category']} | 📅 {pred['year']} | 
                            📊 Actual: {pred['actual_citations']} | 
                            📈 Predicted: {pred['predicted_citations']}
                        </span>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if pred['recommendations']:
                        with st.expander("💡 Recommendations"):
                            for rec in pred['recommendations']:
                                st.markdown(f"• {rec}")
            
            # Tab 6: Editor's Choice
            with tabs[5]:
                st.markdown(f"### {t['editors_choice']}")
                
                editors_selection = editors_choice.select_candidates()
                
                # Top Picks
                st.markdown("#### 🏆 Top Picks")
                
                for idx, pick in enumerate(editors_selection['top_picks'], 1):
                    article = pick['article']
                    stars = '⭐' * min(5, int(pick['score'] / 20 + 1))
                    
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, #FFD70015 0%, #FFA50015 100%);
                                padding: 16px; border-radius: 12px; margin: 10px 0;
                                border: 1px solid #FFD700;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <span style="font-size: 1.2rem; font-weight: bold;">#{idx}</span>
                            <span style="font-size: 1.5rem;">{stars}</span>
                        </div>
                        <b style="font-size: 1.1rem;">{article.get('title', 'No title')}</b><br>
                        <span style="color: #555;">
                            👤 {article.get('authors', 'Unknown')} | 
                            📅 {article.get('publication_year', 'N/A')}
                        </span><br>
                        <span style="color: #666; font-size: 0.9rem;">
                            📊 Citations: {article.get('cited_by_count', 0)} | 
                            📈 Score: {pick['score']:.0f}
                        </span>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.expander("📋 Selection Criteria"):
                        for criterion in pick['criteria']:
                            st.markdown(f"✅ {criterion}")
                        st.markdown(f"💡 **Recommendation**: {pick['recommendation']}")
                
                # Statistics
                st.markdown("#### 📊 Selection Statistics")
                stats_ec = editors_selection['statistics']
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Candidates", stats_ec['total_candidates'])
                with col2:
                    st.metric("Average Score", f"{stats_ec['avg_score']:.1f}")
                with col3:
                    if stats_ec['top_categories']:
                        st.metric("Top Category", stats_ec['top_categories'][0][0])
            
            # Tab 7: Research Hierarchy (original view)
            with tabs[6]:
                st.markdown(f"### {t['research_hierarchy']}")
                
                for domain, fields in hierarchy.items():
                    domain_stats = stats.get(domain, {})
                    domain_articles = domain_stats.get('articles', 0)
                    domain_citations = domain_stats.get('citations', 0) if st.session_state.include_metrics else 0
                    
                    if st.session_state.include_metrics:
                        expander_title = f"{t['domain_icon']} {domain} — {domain_articles} {t['articles_count']}, {domain_citations} {t['citations']}"
                    else:
                        expander_title = f"{t['domain_icon']} {domain} — {domain_articles} {t['articles_count']}"
                    
                    with st.expander(expander_title):
                        for field, subfields in fields.items():
                            field_stats = domain_stats.get('fields', {}).get(field, {})
                            field_articles = field_stats.get('articles', 0)
                            field_citations = field_stats.get('citations', 0) if st.session_state.include_metrics else 0
                            
                            if st.session_state.include_metrics:
                                st.markdown(f"**{t['field_icon']} {field}** — {field_articles} {t['articles_count']}, {field_citations} {t['citations']}")
                            else:
                                st.markdown(f"**{t['field_icon']} {field}** — {field_articles} {t['articles_count']}")
                            
                            for subfield, topics in subfields.items():
                                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                                subfield_articles = subfield_stats.get('articles', 0)
                                subfield_citations = subfield_stats.get('citations', 0) if st.session_state.include_metrics else 0
                                
                                if st.session_state.include_metrics:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;**{t['subfield_icon']} {subfield}** — {subfield_articles} {t['articles_count']}, {subfield_citations} {t['citations']}")
                                else:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;**{t['subfield_icon']} {subfield}** — {subfield_articles} {t['articles_count']}")
                                
                                for topic, topic_articles in topics.items():
                                    topic_articles_count = len(topic_articles)
                                    topic_citations_sum = sum(a.get('cited_by_count', 0) for a in topic_articles)
                                    
                                    if st.session_state.include_metrics:
                                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**{t['topic_icon']} {topic}** — {topic_articles_count} {t['articles_count']}, {topic_citations_sum} {t['citations']}")
                                    else:
                                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**{t['topic_icon']} {topic}** — {topic_articles_count} {t['articles_count']}")
            
            # Export section
            st.markdown("---")
            st.markdown(f"### {t['export_btn']}")
            
            journal_abbr = generate_journal_abbreviation(journal_name)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**{t['pdf_reports']}**")
                
                # PDF English
                pdf_en_data = generate_pdf_en(
                    journal_name, 
                    journal_abbr, 
                    years, 
                    hierarchy, 
                    st.session_state.journal_logo, 
                    st.session_state.custom_message_en,
                    st.session_state.include_metrics
                )
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
                pdf_ru_data = generate_pdf_ru(
                    journal_name, 
                    journal_abbr, 
                    years, 
                    hierarchy, 
                    st.session_state.journal_logo,
                    st.session_state.custom_message_ru,
                    st.session_state.include_metrics
                )
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
                st.markdown(f"**{t['txt_reports']}**")
                
                # TXT English
                txt_en_data = generate_txt_en(
                    journal_name, 
                    years, 
                    hierarchy, 
                    st.session_state.custom_message_en,
                    st.session_state.include_metrics
                )
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
                txt_ru_data = generate_txt_ru(
                    journal_name, 
                    years, 
                    hierarchy, 
                    st.session_state.custom_message_ru,
                    st.session_state.include_metrics
                )
                filename_ru_txt = generate_filename(journal_abbr, years, 'ru', 'txt')
                st.download_button(
                    label="📝 TXT (Русский)",
                    data=txt_ru_data,
                    file_name=filename_ru_txt,
                    mime="text/plain",
                    use_container_width=True,
                    key="txt_ru"
                )
            
            # New analysis button
            st.markdown("---")
            if st.button(t['new_analysis_btn'], use_container_width=True):
                # Clear state
                keys_to_clear = ['step', 'journal_info', 'journal_logo', 'articles', 
                                'hierarchy', 'selected_years', 'years_input']
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                st.session_state.step = 1
                st.rerun()
        else:
            st.warning(t['no_articles'])
            if st.button(t['back_btn'], use_container_width=True):
                st.session_state.step = 2
                st.rerun()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p>© CTA, https://chimicatechnoacta.ru / developed by daM©</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
