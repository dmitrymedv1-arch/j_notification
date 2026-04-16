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
from scipy import stats as scipy_stats
from sklearn.linear_model import LinearRegression
import networkx as nx

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
        'fwci_label': 'FWCI',
        'percentile_label': 'Percentile',
        'top10_label': 'Top 10%',
        'trend_growing': '🚀 Growing',
        'trend_declining': '📉 Declining',
        'trend_stable': '⚖️ Stable',
        'gap_strong': '💪 Strong area',
        'gap_weak': '⚠️ White spot',
        'collab_international': '🌍 International',
        'lineage_index': '🔗 Lineage Index',
        'citation_velocity': '⚡ Velocity',
        'hot_topics': '🔥 Hot Topics',
        'world_comparison': '🌍 World Comparison',
        'trend_analysis': '📈 Trend Analysis',
        'collaboration_analysis': '👥 Collaboration Analysis',
        'research_lineage': '🔬 Research Lineage'
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
        'fwci_label': 'FWCI',
        'percentile_label': 'Перцентиль',
        'top10_label': 'Топ 10%',
        'trend_growing': '🚀 Растущая',
        'trend_declining': '📉 Угасающая',
        'trend_stable': '⚖️ Стабильная',
        'gap_strong': '💪 Сильная сторона',
        'gap_weak': '⚠️ Белое пятно',
        'collab_international': '🌍 Международная',
        'lineage_index': '🔗 Индекс преемственности',
        'citation_velocity': '⚡ Скорость',
        'hot_topics': '🔥 Горячие темы',
        'world_comparison': '🌍 Сравнение с миром',
        'trend_analysis': '📈 Анализ трендов',
        'collaboration_analysis': '👥 Анализ коллабораций',
        'research_lineage': '🔬 Преемственность исследований'
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
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subfield_baselines_cache (
            subfield_id TEXT,
            year INTEGER,
            avg_citations REAL,
            median_citations REAL,
            p90_citations REAL,
            p95_citations REAL,
            p99_citations REAL,
            total_works INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME,
            PRIMARY KEY (subfield_id, year)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS citation_networks_cache (
            source_id TEXT,
            year_filter TEXT,
            network_data TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME,
            PRIMARY KEY (source_id, year_filter)
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_works_expires ON works_cache(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sources_expires ON sources_cache(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_works_expires ON source_works_cache(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_subfield_baselines_expires ON subfield_baselines_cache(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_citation_networks_expires ON citation_networks_cache(expires_at)')
    
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

def cache_subfield_baseline(subfield_id: str, year: int, baseline_data: dict):
    conn = get_cache_connection()
    cursor = conn.cursor()
    expires_at = datetime.now() + timedelta(days=30)
    cursor.execute('''
        INSERT OR REPLACE INTO subfield_baselines_cache 
        (subfield_id, year, avg_citations, median_citations, p90_citations, p95_citations, p99_citations, total_works, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        subfield_id, year,
        baseline_data.get('avg_citations'),
        baseline_data.get('median_citations'),
        baseline_data.get('p90_citations'),
        baseline_data.get('p95_citations'),
        baseline_data.get('p99_citations'),
        baseline_data.get('total_works'),
        expires_at
    ))
    conn.commit()
    conn.close()

def get_cached_subfield_baseline(subfield_id: str, year: int) -> Optional[dict]:
    conn = get_cache_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT avg_citations, median_citations, p90_citations, p95_citations, p99_citations, total_works
        FROM subfield_baselines_cache 
        WHERE subfield_id = ? AND year = ? AND (expires_at IS NULL OR expires_at > ?)
    ''', (subfield_id, year, datetime.now()))
    result = cursor.fetchone()
    conn.close()
    if result:
        return {
            'avg_citations': result[0],
            'median_citations': result[1],
            'p90_citations': result[2],
            'p95_citations': result[3],
            'p99_citations': result[4],
            'total_works': result[5]
        }
    return None

def cache_citation_network(source_id: str, year_filter: str, network_data: dict):
    conn = get_cache_connection()
    cursor = conn.cursor()
    expires_at = datetime.now() + timedelta(days=30)
    cursor.execute('''
        INSERT OR REPLACE INTO citation_networks_cache (source_id, year_filter, network_data, expires_at)
        VALUES (?, ?, ?, ?)
    ''', (source_id, year_filter, json.dumps(network_data), expires_at))
    conn.commit()
    conn.close()

def get_cached_citation_network(source_id: str, year_filter: str) -> Optional[dict]:
    conn = get_cache_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT network_data FROM citation_networks_cache 
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
    cursor.execute('DELETE FROM subfield_baselines_cache WHERE expires_at <= ?', (now_str,))
    cursor.execute('DELETE FROM citation_networks_cache WHERE expires_at <= ?', (now_str,))
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

def calculate_citation_velocity(work: dict, current_year: int = None) -> float:
    """
    Calculate citation velocity - percentage of citations received in last 12 months.
    Returns value between 0 and 100.
    """
    if current_year is None:
        current_year = datetime.now().year
    
    # Get yearly citation counts if available
    counts_by_year = work.get('counts_by_year', [])
    
    if not counts_by_year:
        return 0.0
    
    total_citations = work.get('cited_by_count', 0)
    if total_citations == 0:
        return 0.0
    
    # Sum citations from last 12 months
    current_date = datetime.now()
    recent_citations = 0
    
    for year_data in counts_by_year:
        year = year_data.get('year', 0)
        if year == current_year or year == current_year - 1:
            recent_citations += year_data.get('cited_by_count', 0)
    
    velocity = (recent_citations / total_citations) * 100
    return min(100.0, velocity)

def get_citation_percentile(article_citations: int, baseline_data: dict) -> float:
    """
    Calculate percentile rank of article citations within its subfield and year.
    Returns percentile (0-100).
    """
    if not baseline_data:
        return 50.0  # Default if no baseline data
    
    p90 = baseline_data.get('p90_citations', 0)
    p95 = baseline_data.get('p95_citations', 0)
    p99 = baseline_data.get('p99_citations', 0)
    median = baseline_data.get('median_citations', 0)
    
    if article_citations >= p99:
        return 99.0
    elif article_citations >= p95:
        return 95.0
    elif article_citations >= p90:
        return 90.0
    elif article_citations >= median:
        return 75.0
    elif article_citations >= median * 0.5:
        return 50.0
    elif article_citations > 0:
        return 25.0
    else:
        return 10.0

def calculate_fwci(article_citations: int, baseline_data: dict) -> float:
    """
    Calculate Field-Weighted Citation Impact.
    FWCI = article_citations / world_average_citations_in_same_subfield_year
    """
    if not baseline_data:
        return 1.0  # Default if no baseline data
    
    world_avg = baseline_data.get('avg_citations', 1.0)
    if world_avg <= 0:
        return 1.0
    
    fwci = article_citations / world_avg
    return round(fwci, 2)

# ============================================================================
# WORLD BASELINE FETCHING
# ============================================================================

def fetch_world_baseline_for_subfield(subfield_id: str, year: int) -> Optional[dict]:
    """
    Fetch world baseline citation data for a specific subfield and year.
    Returns dict with avg, median, percentiles.
    """
    # Check cache first
    cached = get_cached_subfield_baseline(subfield_id, year)
    if cached:
        logger.info(f"Using cached baseline for {subfield_id}, {year}")
        return cached
    
    logger.info(f"Fetching world baseline for subfield {subfield_id}, year {year}")
    
    try:
        # Query OpenAlex for works in this subfield and year
        url = f"{OPENALEX_BASE_URL}/works"
        params = {
            "filter": f"concepts.id:{subfield_id},publication_year:{year}",
            "per-page": 200,
            "mailto": MAILTO
        }
        
        all_citations = []
        cursor = "*"
        
        while True:
            params["cursor"] = cursor
            response = requests.get(url, params=params, headers=POLITE_POOL_HEADER, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"Error fetching baseline: {response.status_code}")
                break
            
            data = response.json()
            works = data.get('results', [])
            
            if not works:
                break
            
            for work in works:
                citations = work.get('cited_by_count', 0)
                all_citations.append(citations)
            
            next_cursor = data.get('meta', {}).get('next_cursor')
            if not next_cursor:
                break
            cursor = next_cursor
            time.sleep(0.1)
        
        if not all_citations:
            return None
        
        all_citations.sort()
        total_works = len(all_citations)
        
        # Calculate statistics
        avg_citations = sum(all_citations) / total_works if total_works > 0 else 0
        
        median_idx = total_works // 2
        median_citations = all_citations[median_idx] if total_works > 0 else 0
        
        p90_idx = int(total_works * 0.9)
        p90_citations = all_citations[p90_idx] if p90_idx < total_works else all_citations[-1] if all_citations else 0
        
        p95_idx = int(total_works * 0.95)
        p95_citations = all_citations[p95_idx] if p95_idx < total_works else all_citations[-1] if all_citations else 0
        
        p99_idx = int(total_works * 0.99)
        p99_citations = all_citations[p99_idx] if p99_idx < total_works else all_citations[-1] if all_citations else 0
        
        baseline_data = {
            'avg_citations': avg_citations,
            'median_citations': median_citations,
            'p90_citations': p90_citations,
            'p95_citations': p95_citations,
            'p99_citations': p99_citations,
            'total_works': total_works
        }
        
        # Cache the result
        cache_subfield_baseline(subfield_id, year, baseline_data)
        
        return baseline_data
        
    except Exception as e:
        logger.error(f"Error fetching baseline for {subfield_id}, {year}: {str(e)}")
        return None

def get_subfield_id_from_article(article: dict) -> Optional[str]:
    """
    Extract subfield concept ID from article.
    """
    primary_topic = article.get('primary_topic', {})
    if primary_topic:
        subfield = primary_topic.get('subfield', {})
        subfield_id = subfield.get('id')
        if subfield_id:
            # Extract ID from URL like https://openalex.org/subfields/1234
            match = re.search(r'/subfields/(\d+)', subfield_id)
            if match:
                return f"https://openalex.org/subfields/{match.group(1)}"
            return subfield_id
    
    # Alternative: look through concepts
    concepts = article.get('concepts', [])
    for concept in concepts:
        if concept.get('level') == 2:  # Subfield level
            return concept.get('id')
    
    return None

# ============================================================================
# ARTICLE DATA ENRICHMENT (UPDATED WITH NEW METRICS)
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

def enrich_article_data(article: dict, threshold_total: int = None, threshold_per_year: int = None,
                        world_baselines: Dict = None) -> dict:
    """
    Enrich article data with complete information including FWCI and percentile.
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
    author_countries = []
    
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
                    
                    # Extract country if available
                    institutions = authorship.get('institutions', [])
                    for inst in institutions:
                        country_code = inst.get('country_code', '')
                        if country_code and country_code not in author_countries:
                            author_countries.append(country_code)
    
    authors_str = ', '.join(authors)
    if len(authorships) > 10:
        authors_str += f" et al. ({len(authorships)} authors total)"
    
    # Check for international collaboration
    is_international = len(set(author_countries)) >= 2 if author_countries else False
    
    # Get topic hierarchy
    domain, field, subfield, primary_topic = extract_topic_hierarchy(article)
    
    # Calculate citation metrics with thresholds
    citations_total, citations_per_year, is_highly_cited = calculate_citation_activity(
        article, None, threshold_total, threshold_per_year
    )
    
    # Calculate citation velocity
    citation_velocity = calculate_citation_velocity(article)
    
    # Get subfield ID and fetch/use world baseline
    subfield_id = get_subfield_id_from_article(article)
    publication_year = article.get('publication_year', 0)
    
    fwci = 1.0
    percentile = 50.0
    is_top10_percent = False
    
    if world_baselines and subfield_id and publication_year:
        key = f"{subfield_id}_{publication_year}"
        baseline = world_baselines.get(key)
        if baseline:
            fwci = calculate_fwci(citations_total, baseline)
            percentile = get_citation_percentile(citations_total, baseline)
            is_top10_percent = percentile >= 90.0
    
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
    
    # Get referenced works for lineage analysis
    referenced_works = article.get('referenced_works', [])
    
    enriched = {
        'doi': doi_clean,
        'doi_url': f"https://doi.org/{doi_clean}" if doi_clean else '',
        'title': article.get('title', ''),
        'publication_year': publication_year,
        'publication_date': article.get('publication_date', ''),
        'cited_by_count': citations_total,
        'citations_per_year': round(citations_per_year, 1),
        'citation_velocity': round(citation_velocity, 1),
        'is_highly_cited': is_highly_cited,
        'is_top10_percent': is_top10_percent,
        'fwci': fwci,
        'percentile': round(percentile, 1),
        'authors': authors_str,
        'authors_list': authors,
        'author_countries': author_countries,
        'is_international': is_international,
        'num_authors': len(authors),
        'journal_name': journal_name,
        'volume': volume,
        'issue': issue,
        'pages': pages_str,
        'domain': domain,
        'field': field,
        'subfield': subfield,
        'subfield_id': subfield_id,
        'primary_topic': primary_topic,
        'type': article.get('type', ''),
        'is_oa': article.get('open_access', {}).get('is_oa', False) if article.get('open_access') else False,
        'referenced_works': referenced_works,
        'raw_work': article  # Keep reference to raw data for network analysis
    }
    
    return enriched

# ============================================================================
# HIERARCHICAL ARTICLE GROUPING (UPDATED)
# ============================================================================

def group_articles_by_hierarchy(articles: List[dict], threshold_total: int = None, threshold_per_year: int = None,
                                world_baselines: Dict = None) -> Dict[str, Dict[str, Dict[str, Dict[str, List[dict]]]]]:
    """
    Group articles by hierarchy: Domain -> Field -> Subfield -> Topic
    Includes enriched data with FWCI and percentiles.
    """
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    
    for article in articles:
        enriched = enrich_article_data(article, threshold_total, threshold_per_year, world_baselines)
        
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
    Calculate statistics for each hierarchy level including FWCI.
    """
    stats = {}
    
    for domain, fields in hierarchy.items():
        domain_articles = 0
        domain_citations = 0
        domain_fwci_sum = 0
        field_stats = {}
        
        for field, subfields in fields.items():
            field_articles = 0
            field_citations = 0
            field_fwci_sum = 0
            subfield_stats = {}
            
            for subfield, topics in subfields.items():
                subfield_articles = 0
                subfield_citations = 0
                subfield_fwci_sum = 0
                topic_stats = {}
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_fwci_sum = sum(a.get('fwci', 1.0) for a in articles)
                    
                    topic_stats[topic] = {
                        'articles': topic_articles,
                        'citations': topic_citations if include_metrics else None,
                        'avg_citations': (topic_citations / topic_articles) if (include_metrics and topic_articles > 0) else None,
                        'avg_fwci': (topic_fwci_sum / topic_articles) if (include_metrics and topic_articles > 0) else 1.0,
                        'articles_list': articles
                    }
                    
                    subfield_articles += topic_articles
                    subfield_citations += topic_citations
                    subfield_fwci_sum += topic_fwci_sum
                
                subfield_stats[subfield] = {
                    'articles': subfield_articles,
                    'citations': subfield_citations if include_metrics else None,
                    'avg_citations': (subfield_citations / subfield_articles) if (include_metrics and subfield_articles > 0) else None,
                    'avg_fwci': (subfield_fwci_sum / subfield_articles) if (include_metrics and subfield_articles > 0) else 1.0,
                    'topics': topic_stats
                }
                
                field_articles += subfield_articles
                field_citations += subfield_citations
                field_fwci_sum += subfield_fwci_sum
            
            field_stats[field] = {
                'articles': field_articles,
                'citations': field_citations if include_metrics else None,
                'avg_citations': (field_citations / field_articles) if (include_metrics and field_articles > 0) else None,
                'avg_fwci': (field_fwci_sum / field_articles) if (include_metrics and field_articles > 0) else 1.0,
                'subfields': subfield_stats
            }
            
            domain_articles += field_articles
            domain_citations += field_citations
            domain_fwci_sum += field_fwci_sum
        
        stats[domain] = {
            'articles': domain_articles,
            'citations': domain_citations if include_metrics else None,
            'avg_citations': (domain_citations / domain_articles) if (include_metrics and domain_articles > 0) else None,
            'avg_fwci': (domain_fwci_sum / domain_articles) if (include_metrics and domain_articles > 0) else 1.0,
            'fields': field_stats
        }
    
    return stats

# ============================================================================
# HIERARCHY SORTING FUNCTIONS (UPDATED)
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
        # Sort by avg_fwci (descending) for better accuracy
        domains_sorted = sorted(
            hierarchy.keys(),
            key=lambda d: (
                -stats[d].get('avg_fwci', 0) if stats[d].get('avg_fwci') is not None else -float('inf'),
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
                    -domain_stats['fields'][f].get('avg_fwci', 0) if domain_stats['fields'][f].get('avg_fwci') is not None else -float('inf'),
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
                        -field_stats['subfields'][sf].get('avg_fwci', 0) if field_stats['subfields'][sf].get('avg_fwci') is not None else -float('inf'),
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
                            -subfield_stats['topics'][t].get('avg_fwci', 0) if subfield_stats['topics'][t].get('avg_fwci') is not None else -float('inf'),
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
# TOPIC TREND ANALYSIS (NEW)
# ============================================================================

def compute_topic_trends(articles: List[dict], hierarchy: Dict, min_articles_for_trend: int = 5) -> Dict:
    """
    Analyze temporal trends for topics and subfields.
    Returns dict with trend direction and growth rate.
    """
    trends = {}
    
    # Group articles by year
    articles_by_year = defaultdict(list)
    for article in articles:
        year = article.get('publication_year', 0)
        if year > 0:
            articles_by_year[year].append(article)
    
    if len(articles_by_year) < 3:
        return trends
    
    # Analyze trends for each subfield and topic
    for domain, fields in hierarchy.items():
        for field, subfields in fields.items():
            for subfield, topics in subfields.items():
                # Analyze subfield trend
                subfield_yearly_counts = defaultdict(int)
                subfield_yearly_citations = defaultdict(list)
                
                for topic, topic_articles in topics.items():
                    for article in topic_articles:
                        year = article.get('publication_year', 0)
                        if year > 0:
                            subfield_yearly_counts[year] += 1
                            subfield_yearly_citations[year].append(article.get('cited_by_count', 0))
                
                if len(subfield_yearly_counts) >= min_articles_for_trend:
                    # Calculate trend using linear regression
                    years = sorted(subfield_yearly_counts.keys())
                    counts = [subfield_yearly_counts[y] for y in years]
                    
                    if len(years) >= 3:
                        # Log transform for growth rate
                        log_counts = [np.log(c + 1) for c in counts]
                        slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(years, log_counts)
                        
                        growth_rate = np.exp(slope) - 1
                        trend_direction = 'growing' if slope > 0.05 and p_value < 0.05 else ('declining' if slope < -0.05 and p_value < 0.05 else 'stable')
                        
                        # Calculate average citations trend
                        avg_citations_by_year = {}
                        for year in years:
                            if subfield_yearly_citations[year]:
                                avg_citations_by_year[year] = np.mean(subfield_yearly_citations[year])
                        
                        if len(avg_citations_by_year) >= 3:
                            years_cit = sorted(avg_citations_by_year.keys())
                            cit_vals = [avg_citations_by_year[y] for y in years_cit]
                            cit_slope, _, _, _, _ = scipy_stats.linregress(years_cit, cit_vals)
                        else:
                            cit_slope = 0
                        
                        trends[f"{subfield}"] = {
                            'type': 'subfield',
                            'domain': domain,
                            'field': field,
                            'trend': trend_direction,
                            'growth_rate': round(growth_rate * 100, 1),
                            'citations_trend': round(cit_slope, 2),
                            'r_squared': round(r_value ** 2, 3),
                            'momentum_score': round(growth_rate * (1 + abs(cit_slope) / 10), 3)
                        }
                
                # Analyze each topic within subfield
                for topic, topic_articles in topics.items():
                    topic_yearly_counts = defaultdict(int)
                    topic_yearly_citations = defaultdict(list)
                    
                    for article in topic_articles:
                        year = article.get('publication_year', 0)
                        if year > 0:
                            topic_yearly_counts[year] += 1
                            topic_yearly_citations[year].append(article.get('cited_by_count', 0))
                    
                    if len(topic_yearly_counts) >= min_articles_for_trend:
                        years = sorted(topic_yearly_counts.keys())
                        counts = [topic_yearly_counts[y] for y in years]
                        
                        if len(years) >= 3:
                            log_counts = [np.log(c + 1) for c in counts]
                            slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(years, log_counts)
                            
                            growth_rate = np.exp(slope) - 1
                            trend_direction = 'growing' if slope > 0.05 and p_value < 0.05 else ('declining' if slope < -0.05 and p_value < 0.05 else 'stable')
                            
                            trends[f"{subfield}|{topic}"] = {
                                'type': 'topic',
                                'domain': domain,
                                'field': field,
                                'subfield': subfield,
                                'topic': topic,
                                'trend': trend_direction,
                                'growth_rate': round(growth_rate * 100, 1),
                                'articles_count': len(topic_articles),
                                'momentum_score': round(growth_rate * 100 * (1 + len(topic_articles) / 50), 2)
                            }
    
    return trends

# ============================================================================
# WORLD COMPARISON AND GAP ANALYSIS (NEW)
# ============================================================================

def compute_world_comparison(hierarchy: Dict, world_baselines: Dict, total_world_articles_by_subfield: Dict = None) -> Dict:
    """
    Compare journal distribution with world distribution to identify gaps and strengths.
    """
    if not world_baselines:
        return {}
    
    # Calculate journal distribution by subfield
    journal_distribution = defaultdict(int)
    journal_citations_by_subfield = defaultdict(list)
    
    for domain, fields in hierarchy.items():
        for field, subfields in fields.items():
            for subfield, topics in subfields.items():
                for topic, articles in topics.items():
                    for article in articles:
                        subfield_name = article.get('subfield', subfield)
                        journal_distribution[subfield_name] += 1
                        journal_citations_by_subfield[subfield_name].append(article.get('cited_by_count', 0))
    
    total_journal_articles = sum(journal_distribution.values())
    
    # Calculate world distribution (simplified - would need actual world data)
    # For now, use baseline data as proxy
    world_distribution = {}
    for key, baseline in world_baselines.items():
        subfield_id, year = key.split('_')
        # This is simplified - real implementation would need world article counts
        pass
    
    # Compute gap analysis
    gaps = {}
    for subfield, journal_count in journal_distribution.items():
        journal_share = journal_count / total_journal_articles if total_journal_articles > 0 else 0
        
        # Calculate average citations for this subfield in journal
        avg_journal_citations = np.mean(journal_citations_by_subfield[subfield]) if journal_citations_by_subfield[subfield] else 0
        
        # Estimate world average from baselines (take most recent year)
        world_avg_citations = 0
        for key, baseline in world_baselines.items():
            if subfield in key:
                world_avg_citations = baseline.get('avg_citations', 0)
                break
        
        # Calculate relative performance
        citation_ratio = avg_journal_citations / world_avg_citations if world_avg_citations > 0 else 1.0
        
        gaps[subfield] = {
            'journal_articles': journal_count,
            'journal_share': round(journal_share * 100, 1),
            'avg_journal_citations': round(avg_journal_citations, 1),
            'world_avg_citations': round(world_avg_citations, 1),
            'citation_ratio': round(citation_ratio, 2),
            'gap_type': 'strong' if citation_ratio > 1.5 else ('weak' if citation_ratio < 0.7 else 'neutral')
        }
    
    return gaps

# ============================================================================
# COLLABORATION ANALYSIS (NEW)
# ============================================================================

def analyze_collaboration_patterns(hierarchy: Dict) -> Dict:
    """
    Analyze collaboration patterns across topics.
    """
    collaboration_stats = {}
    
    for domain, fields in hierarchy.items():
        for field, subfields in fields.items():
            for subfield, topics in subfields.items():
                for topic, articles in topics.items():
                    num_authors_list = []
                    international_count = 0
                    citations_list = []
                    
                    for article in articles:
                        num_authors = article.get('num_authors', 0)
                        num_authors_list.append(num_authors)
                        citations_list.append(article.get('cited_by_count', 0))
                        if article.get('is_international', False):
                            international_count += 1
                    
                    if num_authors_list:
                        # Calculate correlation between authors and citations
                        if len(num_authors_list) >= 3 and len(citations_list) >= 3:
                            correlation, p_value = scipy_stats.spearmanr(num_authors_list, citations_list)
                            correlation = correlation if not np.isnan(correlation) else 0
                        else:
                            correlation = 0
                        
                        # Find optimal author count (based on top 25% cited articles)
                        sorted_by_citations = sorted(zip(citations_list, num_authors_list), reverse=True)
                        top_quarter_idx = max(1, len(sorted_by_citations) // 4)
                        top_articles = sorted_by_citations[:top_quarter_idx]
                        optimal_authors = int(np.median([a for _, a in top_articles])) if top_articles else 0
                        
                        collaboration_stats[f"{subfield}|{topic}"] = {
                            'topic': topic,
                            'subfield': subfield,
                            'field': field,
                            'domain': domain,
                            'avg_authors': round(np.mean(num_authors_list), 1),
                            'median_authors': int(np.median(num_authors_list)),
                            'optimal_authors': optimal_authors,
                            'international_share': round(international_count / len(articles) * 100, 1),
                            'author_citation_correlation': round(correlation, 2),
                            'sample_size': len(articles)
                        }
    
    return collaboration_stats

# ============================================================================
# RESEARCH LINEAGE INDEX (NEW)
# ============================================================================

def build_citation_network(articles: List[dict], source_id: str, year_filter: str) -> Dict:
    """
    Build citation network for journal articles to analyze research lineage.
    """
    # Check cache first
    cached = get_cached_citation_network(source_id, year_filter)
    if cached:
        logger.info(f"Using cached citation network for {source_id}")
        return cached
    
    # Create mapping from DOI to article index and work ID
    doi_to_article = {}
    article_dois = []
    
    for idx, article in enumerate(articles):
        doi = article.get('doi', '')
        if doi:
            clean_doi = doi.replace('https://doi.org/', '')
            doi_to_article[clean_doi] = idx
            article_dois.append(clean_doi)
    
    # Build citation network
    citation_graph = nx.DiGraph()
    
    # Add nodes
    for idx, article in enumerate(articles):
        title = article.get('title', f'Article_{idx}')
        citation_graph.add_node(idx, title=title, citations=article.get('cited_by_count', 0))
    
    # Add edges for internal citations
    internal_citations = 0
    total_citations = 0
    
    for idx, article in enumerate(articles):
        referenced_works = article.get('referenced_works', [])
        article_citations = article.get('cited_by_count', 0)
        total_citations += article_citations
        
        for ref in referenced_works:
            # Check if reference is within journal
            ref_doi = ref.replace('https://doi.org/', '')
            if ref_doi in doi_to_article:
                target_idx = doi_to_article[ref_doi]
                citation_graph.add_edge(idx, target_idx)
                internal_citations += 1
    
    # Calculate lineage index for each article
    lineage_indices = {}
    for idx in citation_graph.nodes:
        in_degree = citation_graph.in_degree(idx)
        total_refs = len(articles[idx].get('referenced_works', []))
        lineage_index = in_degree / max(1, total_refs)
        lineage_indices[idx] = round(lineage_index, 3)
    
    # Find clusters (strongly connected components)
    components = list(nx.strongly_connected_components(citation_graph))
    clusters = []
    for comp in components:
        if len(comp) >= 3:  # Only consider clusters of 3+ articles
            cluster_articles = [articles[i] for i in comp]
            cluster_citations = sum(a.get('cited_by_count', 0) for a in cluster_articles)
            clusters.append({
                'size': len(comp),
                'articles': cluster_articles,
                'total_citations': cluster_citations,
                'avg_citations': cluster_citations / len(comp) if comp else 0
            })
    
    # Calculate overall lineage index for journal
    internal_citation_rate = internal_citations / max(1, total_citations)
    
    network_data = {
        'graph': citation_graph,
        'lineage_indices': lineage_indices,
        'internal_citation_rate': round(internal_citation_rate, 3),
        'clusters': clusters,
        'total_nodes': citation_graph.number_of_nodes(),
        'total_edges': citation_graph.number_of_edges()
    }
    
    # Cache the network data (convert graph to serializable format)
    serializable_data = {
        'lineage_indices': lineage_indices,
        'internal_citation_rate': internal_citation_rate,
        'total_nodes': network_data['total_nodes'],
        'total_edges': network_data['total_edges'],
        'clusters': [
            {
                'size': c['size'],
                'total_citations': c['total_citations'],
                'avg_citations': c['avg_citations']
            } for c in clusters
        ]
    }
    cache_citation_network(source_id, year_filter, serializable_data)
    
    return network_data

def calculate_topic_lineage_index(hierarchy: Dict, network_data: Dict) -> Dict:
    """
    Calculate lineage index for each topic based on citation network.
    """
    if not network_data:
        return {}
    
    lineage_indices = network_data.get('lineage_indices', {})
    
    topic_lineage = defaultdict(list)
    
    # Map articles to topics
    for domain, fields in hierarchy.items():
        for field, subfields in fields.items():
            for subfield, topics in subfields.items():
                for topic, articles in topics.items():
                    for idx, article in enumerate(articles):
                        # Find article index in network (by DOI or position)
                        # Simplified: use article index from list
                        if idx in lineage_indices:
                            topic_lineage[f"{subfield}|{topic}"].append(lineage_indices[idx])
    
    # Calculate average lineage per topic
    result = {}
    for topic_key, indices in topic_lineage.items():
        result[topic_key] = {
            'avg_lineage_index': round(np.mean(indices), 3),
            'max_lineage_index': round(max(indices), 3),
            'sample_size': len(indices)
        }
    
    return result

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
# PDF REPORT GENERATION (RUSSIAN) WITH NEW ANALYTICS
# ============================================================================

def generate_pdf_ru(journal_name: str, journal_abbr: str, years: List[int], 
                    hierarchy: Dict, logo_path: str = None, custom_message: str = None,
                    include_metrics: bool = True, trends: Dict = None, 
                    collaboration_stats: Dict = None, lineage_data: Dict = None,
                    world_gaps: Dict = None) -> bytes:
    """Generate PDF report in Russian with hierarchical grouping and new analytics"""
    
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
        avg_fwci = np.mean([a.get('fwci', 1.0) for domain in hierarchy.values() 
                           for field in domain.values()
                           for subfield in field.values()
                           for topic in subfield.values()
                           for a in topic]) if total_articles > 0 else 1.0
        
        stats_data = [
            ["Показатель", "Значение"],
            ["Всего статей", str(total_articles)],
            ["Областей науки", str(total_domains)],
            ["Всего цитирований", str(total_citations)],
            ["Средняя цитируемость", f"{total_citations/total_articles:.2f}" if total_articles > 0 else "0"],
            ["Средний FWCI", f"{avg_fwci:.2f}"],
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
    
    # ========== HOT TOPICS SECTION (NEW) ==========
    if trends:
        story.append(PageBreak())
        story.append(Paragraph("🔥 Горячие темы и тренды", title_style))
        story.append(Spacer(1, 0.5*cm))
        
        # Sort topics by momentum score
        sorted_topics = sorted(
            [t for t in trends.values() if t.get('type') == 'topic' and t.get('trend') == 'growing'],
            key=lambda x: x.get('momentum_score', 0), reverse=True
        )[:10]
        
        if sorted_topics:
            hot_topics_data = [["Тема", "Тренд", "Рост/год", "Статей"]]
            for topic_data in sorted_topics:
                hot_topics_data.append([
                    clean_text(topic_data.get('topic', '')[:50]),
                    "🚀 Растущая",
                    f"+{topic_data.get('growth_rate', 0)}%",
                    str(topic_data.get('articles_count', 0))
                ])
            
            hot_table = Table(hot_topics_data, colWidths=[doc.width/2, doc.width/6, doc.width/6, doc.width/6])
            hot_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), russian_font_name),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(hot_table)
    
    # ========== WORLD COMPARISON SECTION (NEW) ==========
    if world_gaps:
        story.append(PageBreak())
        story.append(Paragraph("🌍 Сравнение с мировыми исследованиями", title_style))
        story.append(Spacer(1, 0.5*cm))
        
        strong_areas = [g for g in world_gaps.items() if g[1].get('gap_type') == 'strong'][:5]
        weak_areas = [g for g in world_gaps.items() if g[1].get('gap_type') == 'weak'][:5]
        
        if strong_areas:
            story.append(Paragraph("💪 Сильные стороны журнала", subtitle_style))
            strong_data = [["Субполе", "Статей в журнале", "Citation Ratio"]]
            for subfield, gap_data in strong_areas:
                strong_data.append([
                    clean_text(subfield[:40]),
                    str(gap_data.get('journal_articles', 0)),
                    f"{gap_data.get('citation_ratio', 1.0)}x"
                ])
            
            strong_table = Table(strong_data, colWidths=[doc.width/2, doc.width/4, doc.width/4])
            strong_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), russian_font_name),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(strong_table)
            story.append(Spacer(1, 0.5*cm))
        
        if weak_areas:
            story.append(Paragraph("⚠️ Потенциальные белые пятна", subtitle_style))
            weak_data = [["Субполе", "Статей в журнале", "Citation Ratio"]]
            for subfield, gap_data in weak_areas:
                weak_data.append([
                    clean_text(subfield[:40]),
                    str(gap_data.get('journal_articles', 0)),
                    f"{gap_data.get('citation_ratio', 1.0)}x"
                ])
            
            weak_table = Table(weak_data, colWidths=[doc.width/2, doc.width/4, doc.width/4])
            weak_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E67E22')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), russian_font_name),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(weak_table)
    
    # ========== COLLABORATION ANALYSIS SECTION (NEW) ==========
    if collaboration_stats:
        story.append(PageBreak())
        story.append(Paragraph("👥 Анализ коллабораций", title_style))
        story.append(Spacer(1, 0.5*cm))
        
        # Find topics with highest international collaboration
        high_collab = sorted(
            [c for c in collaboration_stats.values() if c.get('international_share', 0) > 30],
            key=lambda x: x.get('international_share', 0), reverse=True
        )[:5]
        
        if high_collab:
            collab_data = [["Тема", "Ср. авторов", "Междунар. доля", "Корреляция"]]
            for collab in high_collab:
                collab_data.append([
                    clean_text(collab.get('topic', '')[:40]),
                    str(collab.get('avg_authors', 0)),
                    f"{collab.get('international_share', 0)}%",
                    f"{collab.get('author_citation_correlation', 0):.2f}"
                ])
            
            collab_table = Table(collab_data, colWidths=[doc.width/2.2, doc.width/6, doc.width/6, doc.width/6])
            collab_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), russian_font_name),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(collab_table)
    
    story.append(PageBreak())
    
    # ========== TABLE OF CONTENTS ==========
    story.append(Paragraph("Содержание", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
            anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} статей, {domain_citations} цитирований, FWCI: {domain_avg_fwci:.2f}</a>', toc_domain_style))
        else:
            anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} статей</a>', toc_domain_style))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} статей, {field_citations} цитирований, FWCI: {field_avg_fwci:.2f}', toc_field_style))
            else:
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} статей', toc_field_style))
            
            for subfield in subfields.keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} статей, {subfield_citations} цитирований, FWCI: {subfield_avg_fwci:.2f}', toc_subfield_style))
                else:
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} статей', toc_subfield_style))
        
        story.append(Spacer(1, 0.3*cm))
    
    story.append(PageBreak())
    
    # ========== ARTICLES BY HIERARCHY WITH ANCHORS ==========
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
        else:
            domain_citations = 0
            domain_avg_fwci = 1.0
        
        anchor_id = f"domain_{hashlib.md5(domain.encode('utf-8')).hexdigest()[:8]}"
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
        story.append(anchor_para)
        
        if include_metrics:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} статей, {domain_citations} цитирований, FWCI: {domain_avg_fwci:.2f}", domain_style))
        else:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} статей", domain_style))
        story.append(Spacer(1, 0.3*cm))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
            else:
                field_citations = 0
                field_avg_fwci = 1.0
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode('utf-8')).hexdigest()[:8]}"
            field_anchor_para = Paragraph(f'<a name="{field_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
            story.append(field_anchor_para)
            
            if include_metrics:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} статей, {field_citations} цитирований, FWCI: {field_avg_fwci:.2f}", field_style))
            else:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} статей", field_style))
            story.append(Spacer(1, 0.2*cm))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                else:
                    subfield_citations = 0
                    subfield_avg_fwci = 1.0
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode('utf-8')).hexdigest()[:8]}"
                subfield_anchor_para = Paragraph(f'<a name="{subfield_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white, fontName=russian_font_name))
                story.append(subfield_anchor_para)
                
                if include_metrics:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} статей, {subfield_citations} цитирований, FWCI: {subfield_avg_fwci:.2f}", subfield_style))
                else:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} статей", subfield_style))
                story.append(Spacer(1, 0.2*cm))
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg_fwci = np.mean([a.get('fwci', 1.0) for a in articles]) if articles else 1.0
                    topic_avg = topic_citations / topic_articles if topic_articles > 0 else 0
                    
                    # Get trend info for this topic
                    trend_icon = ""
                    if trends:
                        topic_key = f"{subfield}|{topic}"
                        if topic_key in trends:
                            trend = trends[topic_key].get('trend', '')
                            if trend == 'growing':
                                trend_icon = " 🚀"
                            elif trend == 'declining':
                                trend_icon = " 📉"
                    
                    if include_metrics:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)}{trend_icon} — {topic_articles} статей, {topic_citations} цитирований, FWCI: {topic_avg_fwci:.2f}", topic_style))
                    else:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)}{trend_icon} — {topic_articles} статей", topic_style))
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
                        
                        # Citation info with FWCI and percentile
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        fwci = article.get('fwci', 1.0)
                        percentile = article.get('percentile', 50.0)
                        is_highly = article.get('is_highly_cited', False)
                        is_top10 = article.get('is_top10_percent', False)
                        velocity = article.get('citation_velocity', 0)
                        
                        citation_text = f"<b>Цитирований:</b> {citations} | <b>в год:</b> {citations_per_year} | <b>FWCI:</b> {fwci:.2f} | <b>Перцентиль:</b> {percentile:.0f}%"
                        if velocity > 30:
                            citation_text += f" | ⚡ Скорость: {velocity:.0f}%"
                        if is_highly:
                            citation_text += " 🔥 Активно цитируемая"
                        if is_top10:
                            citation_text += " 🏆 Топ 10%"
                        
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{citation_text}", citation_style))
                        
                        # Show international collaboration badge
                        if article.get('is_international', False):
                            story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;🌍 Международная коллаборация ({len(article.get('author_countries', []))} стран)", meta_style))
                        
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
    avg_fwci_overall = np.mean([a.get('fwci', 1.0) for domain in hierarchy.values() 
                               for field in domain.values()
                               for subfield in field.values()
                               for topic in subfield.values()
                               for a in topic]) if total_articles > 0 else 1.0
    
    conclusion_text = f"""
    Данный отчет содержит {total_articles} статей из журнала «{clean_text(journal_name)}», 
    сгруппированных по иерархической структуре: {total_domains} областей науки, 
    включающих множество полей и подполей."""
    
    if include_metrics:
        conclusion_text += f""" Общая средняя цитируемость составляет {avg_overall:.2f} цитирований на статью.
    Средний Field-Weighted Citation Impact (FWCI) журнала: {avg_fwci_overall:.2f}.
    Из них {highly_cited} статей являются активно цитируемыми, что делает их особенно ценными для включения в Ваши научные работы.<br/><br/>"""
    
    conclusion_text += """
    Рекомендуем обратить особое внимание на статьи с пометкой «Активно цитируемая» и «Топ 10%» — 
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
# PDF REPORT GENERATION (ENGLISH) WITH NEW ANALYTICS
# ============================================================================

def generate_pdf_en(journal_name: str, journal_abbr: str, years: List[int], 
                    hierarchy: Dict, logo_path: str = None, custom_message: str = None,
                    include_metrics: bool = True, trends: Dict = None,
                    collaboration_stats: Dict = None, lineage_data: Dict = None,
                    world_gaps: Dict = None) -> bytes:
    """Generate PDF report in English with hierarchical grouping and new analytics"""
    
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
    avg_fwci_overall = np.mean([a.get('fwci', 1.0) for domain in hierarchy.values() 
                               for field in domain.values()
                               for subfield in field.values()
                               for topic in subfield.values()
                               for a in topic]) if total_articles > 0 else 1.0
    
    if include_metrics:
        stats_data = [
            ["Metric", "Value"],
            ["Total Articles", str(total_articles)],
            ["Research Domains", str(total_domains)],
            ["Total Citations", str(total_citations)],
            ["Average Citations per Article", f"{avg_overall:.2f}"],
            ["Average FWCI", f"{avg_fwci_overall:.2f}"],
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
    
    # ========== HOT TOPICS SECTION (NEW) ==========
    if trends:
        story.append(PageBreak())
        story.append(Paragraph("🔥 Hot Topics & Trends", title_style))
        story.append(Spacer(1, 0.5*cm))
        
        # Sort topics by momentum score
        sorted_topics = sorted(
            [t for t in trends.values() if t.get('type') == 'topic' and t.get('trend') == 'growing'],
            key=lambda x: x.get('momentum_score', 0), reverse=True
        )[:10]
        
        if sorted_topics:
            hot_topics_data = [["Topic", "Trend", "Growth/Year", "Articles"]]
            for topic_data in sorted_topics:
                hot_topics_data.append([
                    clean_text(topic_data.get('topic', '')[:50]),
                    "🚀 Growing",
                    f"+{topic_data.get('growth_rate', 0)}%",
                    str(topic_data.get('articles_count', 0))
                ])
            
            hot_table = Table(hot_topics_data, colWidths=[doc.width/2, doc.width/6, doc.width/6, doc.width/6])
            hot_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(hot_table)
    
    # ========== WORLD COMPARISON SECTION (NEW) ==========
    if world_gaps:
        story.append(PageBreak())
        story.append(Paragraph("🌍 World Comparison", title_style))
        story.append(Spacer(1, 0.5*cm))
        
        strong_areas = [g for g in world_gaps.items() if g[1].get('gap_type') == 'strong'][:5]
        weak_areas = [g for g in world_gaps.items() if g[1].get('gap_type') == 'weak'][:5]
        
        if strong_areas:
            story.append(Paragraph("💪 Journal Strengths", subtitle_style))
            strong_data = [["Subfield", "Journal Articles", "Citation Ratio"]]
            for subfield, gap_data in strong_areas:
                strong_data.append([
                    clean_text(subfield[:40]),
                    str(gap_data.get('journal_articles', 0)),
                    f"{gap_data.get('citation_ratio', 1.0)}x"
                ])
            
            strong_table = Table(strong_data, colWidths=[doc.width/2, doc.width/4, doc.width/4])
            strong_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(strong_table)
            story.append(Spacer(1, 0.5*cm))
        
        if weak_areas:
            story.append(Paragraph("⚠️ Potential White Spots", subtitle_style))
            weak_data = [["Subfield", "Journal Articles", "Citation Ratio"]]
            for subfield, gap_data in weak_areas:
                weak_data.append([
                    clean_text(subfield[:40]),
                    str(gap_data.get('journal_articles', 0)),
                    f"{gap_data.get('citation_ratio', 1.0)}x"
                ])
            
            weak_table = Table(weak_data, colWidths=[doc.width/2, doc.width/4, doc.width/4])
            weak_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E67E22')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(weak_table)
    
    # ========== COLLABORATION ANALYSIS SECTION (NEW) ==========
    if collaboration_stats:
        story.append(PageBreak())
        story.append(Paragraph("👥 Collaboration Analysis", title_style))
        story.append(Spacer(1, 0.5*cm))
        
        high_collab = sorted(
            [c for c in collaboration_stats.values() if c.get('international_share', 0) > 30],
            key=lambda x: x.get('international_share', 0), reverse=True
        )[:5]
        
        if high_collab:
            collab_data = [["Topic", "Avg Authors", "International Share", "Correlation"]]
            for collab in high_collab:
                collab_data.append([
                    clean_text(collab.get('topic', '')[:40]),
                    str(collab.get('avg_authors', 0)),
                    f"{collab.get('international_share', 0)}%",
                    f"{collab.get('author_citation_correlation', 0):.2f}"
                ])
            
            collab_table = Table(collab_data, colWidths=[doc.width/2.2, doc.width/6, doc.width/6, doc.width/6])
            collab_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5DBDB')),
            ]))
            story.append(collab_table)
    
    story.append(PageBreak())
    
    # ========== TABLE OF CONTENTS ==========
    story.append(Paragraph("Table of Contents", title_style))
    story.append(Spacer(1, 0.5*cm))
    
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
            anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} articles, {domain_citations} citations, FWCI: {domain_avg_fwci:.2f}</a>', toc_domain_style))
        else:
            anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
            story.append(Paragraph(f'<a href="#{anchor_id}"><b>{clean_text(domain)}</b> — {domain_articles} articles</a>', toc_domain_style))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} articles, {field_citations} citations, FWCI: {field_avg_fwci:.2f}', toc_field_style))
            else:
                field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
                story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{field_anchor_id}">{clean_text(field)}</a> — {field_articles} articles', toc_field_style))
            
            for subfield in subfields.keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} articles, {subfield_citations} citations, FWCI: {subfield_avg_fwci:.2f}', toc_subfield_style))
                else:
                    subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                    story.append(Paragraph(f'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#{subfield_anchor_id}">{clean_text(subfield)}</a> — {subfield_articles} articles', toc_subfield_style))
        
        story.append(Spacer(1, 0.3*cm))
    
    story.append(PageBreak())
    
    # ========== ARTICLES BY HIERARCHY WITH ANCHORS ==========
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
        else:
            domain_citations = 0
            domain_avg_fwci = 1.0
        
        anchor_id = f"domain_{hashlib.md5(domain.encode()).hexdigest()[:8]}"
        anchor_para = Paragraph(f'<a name="{anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
        story.append(anchor_para)
        
        if include_metrics:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} articles, {domain_citations} citations, FWCI: {domain_avg_fwci:.2f}", domain_style))
        else:
            story.append(Paragraph(f"{clean_text(domain)} — {domain_articles} articles", domain_style))
        story.append(Spacer(1, 0.3*cm))
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
            else:
                field_citations = 0
                field_avg_fwci = 1.0
            
            field_anchor_id = f"field_{hashlib.md5(f"{domain}_{field}".encode()).hexdigest()[:8]}"
            field_anchor_para = Paragraph(f'<a name="{field_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
            story.append(field_anchor_para)
            
            if include_metrics:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} articles, {field_citations} citations, FWCI: {field_avg_fwci:.2f}", field_style))
            else:
                story.append(Paragraph(f"&nbsp;&nbsp;{clean_text(field)} — {field_articles} articles", field_style))
            story.append(Spacer(1, 0.2*cm))
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                else:
                    subfield_citations = 0
                    subfield_avg_fwci = 1.0
                
                subfield_anchor_id = f"subfield_{hashlib.md5(f"{domain}_{field}_{subfield}".encode()).hexdigest()[:8]}"
                subfield_anchor_para = Paragraph(f'<a name="{subfield_anchor_id}"/>', ParagraphStyle('AnchorStyle', parent=styles['Normal'], fontSize=1, textColor=colors.white))
                story.append(subfield_anchor_para)
                
                if include_metrics:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} articles, {subfield_citations} citations, FWCI: {subfield_avg_fwci:.2f}", subfield_style))
                else:
                    story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(subfield)} — {subfield_articles} articles", subfield_style))
                story.append(Spacer(1, 0.2*cm))
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg_fwci = np.mean([a.get('fwci', 1.0) for a in articles]) if articles else 1.0
                    topic_avg = topic_citations / topic_articles if topic_articles > 0 else 0
                    
                    # Get trend info
                    trend_icon = ""
                    if trends:
                        topic_key = f"{subfield}|{topic}"
                        if topic_key in trends:
                            trend = trends[topic_key].get('trend', '')
                            if trend == 'growing':
                                trend_icon = " 🚀"
                            elif trend == 'declining':
                                trend_icon = " 📉"
                    
                    if include_metrics:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)}{trend_icon} — {topic_articles} articles, {topic_citations} citations, FWCI: {topic_avg_fwci:.2f}", topic_style))
                    else:
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{clean_text(topic)}{trend_icon} — {topic_articles} articles", topic_style))
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
                        
                        # Citation info with FWCI and percentile
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        fwci = article.get('fwci', 1.0)
                        percentile = article.get('percentile', 50.0)
                        is_highly = article.get('is_highly_cited', False)
                        is_top10 = article.get('is_top10_percent', False)
                        velocity = article.get('citation_velocity', 0)
                        
                        citation_text = f"<b>Citations:</b> {citations} | <b>per year:</b> {citations_per_year} | <b>FWCI:</b> {fwci:.2f} | <b>Percentile:</b> {percentile:.0f}%"
                        if velocity > 30:
                            citation_text += f" | ⚡ Velocity: {velocity:.0f}%"
                        if is_highly:
                            citation_text += " 🔥 Highly Cited"
                        if is_top10:
                            citation_text += " 🏆 Top 10%"
                        
                        story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{citation_text}", citation_style))
                        
                        # International collaboration badge
                        if article.get('is_international', False):
                            story.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;🌍 International collaboration ({len(article.get('author_countries', []))} countries)", meta_style))
                        
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
    The journal's average Field-Weighted Citation Impact (FWCI) is {avg_fwci_overall:.2f}.
    Among them, {highly_cited} articles are highly cited, making them particularly valuable for inclusion in your research.<br/><br/>"""
    
    conclusion_text += """
    We recommend paying special attention to articles marked as "Highly Cited" and "Top 10%" — 
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
# TXT REPORT GENERATION (RUSSIAN) WITH NEW ANALYTICS
# ============================================================================

def generate_txt_ru(journal_name: str, years: List[int], hierarchy: Dict, custom_message: str = None,
                   include_metrics: bool = True, trends: Dict = None) -> str:
    """Generate TXT report in Russian with hierarchical grouping and new analytics"""
    
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
    avg_fwci_overall = np.mean([a.get('fwci', 1.0) for domain in hierarchy.values() 
                               for field in domain.values()
                               for subfield in field.values()
                               for topic in subfield.values()
                               for a in topic]) if total_articles > 0 else 1.0
    
    output.append("СТАТИСТИКА")
    output.append("-" * 40)
    output.append(f"Всего статей: {total_articles}")
    output.append(f"Областей науки: {total_domains}")
    if include_metrics:
        output.append(f"Всего цитирований: {total_citations}")
        output.append(f"Средняя цитируемость: {avg_overall:.2f}")
        output.append(f"Средний FWCI: {avg_fwci_overall:.2f}")
        output.append(f"Активно цитируемые статьи: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Hot topics section
    if trends:
        output.append("🔥 ГОРЯЧИЕ ТЕМЫ И ТРЕНДЫ")
        output.append("-" * 40)
        sorted_topics = sorted(
            [t for t in trends.values() if t.get('type') == 'topic' and t.get('trend') == 'growing'],
            key=lambda x: x.get('momentum_score', 0), reverse=True
        )[:10]
        
        if sorted_topics:
            for topic_data in sorted_topics:
                output.append(f"  • {topic_data.get('topic', '')} — рост: +{topic_data.get('growth_rate', 0)}%/год, статей: {topic_data.get('articles_count', 0)}")
        output.append("")
        output.append("=" * 80)
        output.append("")
    
    # Table of Contents
    output.append("СОДЕРЖАНИЕ")
    output.append("-" * 40)
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
            output.append(f"{domain} — {domain_articles} статей, {domain_citations} цитирований, FWCI: {domain_avg_fwci:.2f}")
        else:
            output.append(f"{domain} — {domain_articles} статей")
        
        for field in fields.keys():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
                output.append(f"  └── {field} — {field_articles} статей, {field_citations} цитирований, FWCI: {field_avg_fwci:.2f}")
            else:
                output.append(f"  └── {field} — {field_articles} статей")
            
            for subfield in fields[field].keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                    output.append(f"      └── {subfield} — {subfield_articles} статей, {subfield_citations} цитирований, FWCI: {subfield_avg_fwci:.2f}")
                else:
                    output.append(f"      └── {subfield} — {subfield_articles} статей")
    
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Articles by hierarchy
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
        else:
            domain_citations = 0
            domain_avg_fwci = 1.0
        
        output.append("")
        output.append("█" * 80)
        if include_metrics:
            output.append(f"ОБЛАСТЬ: {domain} — {domain_articles} статей, {domain_citations} цитирований, FWCI: {domain_avg_fwci:.2f}")
        else:
            output.append(f"ОБЛАСТЬ: {domain} — {domain_articles} статей")
        output.append("█" * 80)
        output.append("")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
            else:
                field_citations = 0
                field_avg_fwci = 1.0
            
            if include_metrics:
                output.append(f"▓▓▓ ПОЛЕ: {field} — {field_articles} статей, {field_citations} цитирований, FWCI: {field_avg_fwci:.2f} ▓▓▓")
            else:
                output.append(f"▓▓▓ ПОЛЕ: {field} — {field_articles} статей ▓▓▓")
            output.append("")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                else:
                    subfield_citations = 0
                    subfield_avg_fwci = 1.0
                
                if include_metrics:
                    output.append(f"▒▒▒ ПОДПОЛЕ: {subfield} — {subfield_articles} статей, {subfield_citations} цитирований, FWCI: {subfield_avg_fwci:.2f} ▒▒▒")
                else:
                    output.append(f"▒▒▒ ПОДПОЛЕ: {subfield} — {subfield_articles} статей ▒▒▒")
                output.append("")
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg_fwci = np.mean([a.get('fwci', 1.0) for a in articles]) if articles else 1.0
                    
                    # Get trend info
                    trend_icon = ""
                    if trends:
                        topic_key = f"{subfield}|{topic}"
                        if topic_key in trends:
                            trend = trends[topic_key].get('trend', '')
                            if trend == 'growing':
                                trend_icon = " 🚀"
                            elif trend == 'declining':
                                trend_icon = " 📉"
                    
                    if include_metrics:
                        output.append(f"  ● ТЕМА: {topic}{trend_icon} — {topic_articles} статей, {topic_citations} цитирований, FWCI: {topic_avg_fwci:.2f}")
                    else:
                        output.append(f"  ● ТЕМА: {topic}{trend_icon} — {topic_articles} статей")
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
                        
                        # Citation info with FWCI and percentile
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        fwci = article.get('fwci', 1.0)
                        percentile = article.get('percentile', 50.0)
                        highly = " 🔥 АКТИВНО ЦИТИРУЕМАЯ" if article.get('is_highly_cited') else ""
                        top10 = " 🏆 ТОП 10%" if article.get('is_top10_percent') else ""
                        velocity = article.get('citation_velocity', 0)
                        
                        output.append(f"       Цитирований: {citations} | в год: {citations_per_year} | FWCI: {fwci:.2f} | Перцентиль: {percentile:.0f}%{highly}{top10}")
                        if velocity > 30:
                            output.append(f"       ⚡ Скорость набора: {velocity:.0f}%")
                        
                        if article.get('is_international', False):
                            output.append(f"       🌍 Международная коллаборация ({len(article.get('author_countries', []))} стран)")
                        
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
        output.append(f"Средний Field-Weighted Citation Impact (FWCI) журнала: {avg_fwci_overall:.2f}.")
        output.append(f"Из них {highly_cited} статей являются активно цитируемыми, что делает их особенно ценными для включения")
    else:
        output.append(f"Из них {highly_cited} статей являются активно цитируемыми, что делает их особенно ценными для включения")
    
    output.append("в Ваши научные работы.")
    output.append("")
    output.append("Рекомендуем обратить особое внимание на статьи с пометкой «Активно цитируемая» и «Топ 10%» —")
    output.append("они демонстрируют высокий научный интерес и могут стать важной частью")
    output.append("Вашего исследования.")
    output.append("")
    output.append("=" * 80)
    output.append(f"Отчет сгенерирован: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    output.append(f"© {journal_name}")
    output.append("=" * 80)
    
    return "\n".join(output)

# ============================================================================
# TXT REPORT GENERATION (ENGLISH) WITH NEW ANALYTICS
# ============================================================================

def generate_txt_en(journal_name: str, years: List[int], hierarchy: Dict, custom_message: str = None,
                   include_metrics: bool = True, trends: Dict = None) -> str:
    """Generate TXT report in English with hierarchical grouping and new analytics"""
    
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
    avg_fwci_overall = np.mean([a.get('fwci', 1.0) for domain in hierarchy.values() 
                               for field in domain.values()
                               for subfield in field.values()
                               for topic in subfield.values()
                               for a in topic]) if total_articles > 0 else 1.0
    
    output.append("STATISTICS")
    output.append("-" * 40)
    output.append(f"Total Articles: {total_articles}")
    output.append(f"Research Domains: {total_domains}")
    if include_metrics:
        output.append(f"Total Citations: {total_citations}")
        output.append(f"Average Citations per Article: {avg_overall:.2f}")
        output.append(f"Average FWCI: {avg_fwci_overall:.2f}")
        output.append(f"Highly Cited Articles: {highly_cited}")
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Hot topics section
    if trends:
        output.append("🔥 HOT TOPICS & TRENDS")
        output.append("-" * 40)
        sorted_topics = sorted(
            [t for t in trends.values() if t.get('type') == 'topic' and t.get('trend') == 'growing'],
            key=lambda x: x.get('momentum_score', 0), reverse=True
        )[:10]
        
        if sorted_topics:
            for topic_data in sorted_topics:
                output.append(f"  • {topic_data.get('topic', '')} — growth: +{topic_data.get('growth_rate', 0)}%/year, articles: {topic_data.get('articles_count', 0)}")
        output.append("")
        output.append("=" * 80)
        output.append("")
    
    # Table of Contents
    output.append("TABLE OF CONTENTS")
    output.append("-" * 40)
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
            output.append(f"{domain} — {domain_articles} articles, {domain_citations} citations, FWCI: {domain_avg_fwci:.2f}")
        else:
            output.append(f"{domain} — {domain_articles} articles")
        
        for field in fields.keys():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
                output.append(f"  └── {field} — {field_articles} articles, {field_citations} citations, FWCI: {field_avg_fwci:.2f}")
            else:
                output.append(f"  └── {field} — {field_articles} articles")
            
            for subfield in fields[field].keys():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                    output.append(f"      └── {subfield} — {subfield_articles} articles, {subfield_citations} citations, FWCI: {subfield_avg_fwci:.2f}")
                else:
                    output.append(f"      └── {subfield} — {subfield_articles} articles")
    
    output.append("")
    output.append("=" * 80)
    output.append("")
    
    # Articles by hierarchy
    for domain, fields in hierarchy.items():
        domain_stats = stats.get(domain, {})
        domain_articles = domain_stats.get('articles', 0)
        
        if include_metrics:
            domain_citations = domain_stats.get('citations', 0)
            domain_avg_fwci = domain_stats.get('avg_fwci', 1.0)
        else:
            domain_citations = 0
            domain_avg_fwci = 1.0
        
        output.append("")
        output.append("█" * 80)
        if include_metrics:
            output.append(f"DOMAIN: {domain} — {domain_articles} articles, {domain_citations} citations, FWCI: {domain_avg_fwci:.2f}")
        else:
            output.append(f"DOMAIN: {domain} — {domain_articles} articles")
        output.append("█" * 80)
        output.append("")
        
        for field, subfields in fields.items():
            field_stats = domain_stats.get('fields', {}).get(field, {})
            field_articles = field_stats.get('articles', 0)
            
            if include_metrics:
                field_citations = field_stats.get('citations', 0)
                field_avg_fwci = field_stats.get('avg_fwci', 1.0)
            else:
                field_citations = 0
                field_avg_fwci = 1.0
            
            if include_metrics:
                output.append(f"▓▓▓ FIELD: {field} — {field_articles} articles, {field_citations} citations, FWCI: {field_avg_fwci:.2f} ▓▓▓")
            else:
                output.append(f"▓▓▓ FIELD: {field} — {field_articles} articles ▓▓▓")
            output.append("")
            
            for subfield, topics in subfields.items():
                subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                subfield_articles = subfield_stats.get('articles', 0)
                
                if include_metrics:
                    subfield_citations = subfield_stats.get('citations', 0)
                    subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0)
                else:
                    subfield_citations = 0
                    subfield_avg_fwci = 1.0
                
                if include_metrics:
                    output.append(f"▒▒▒ SUBFIELD: {subfield} — {subfield_articles} articles, {subfield_citations} citations, FWCI: {subfield_avg_fwci:.2f} ▒▒▒")
                else:
                    output.append(f"▒▒▒ SUBFIELD: {subfield} — {subfield_articles} articles ▒▒▒")
                output.append("")
                
                for topic, articles in topics.items():
                    topic_articles = len(articles)
                    topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                    topic_avg_fwci = np.mean([a.get('fwci', 1.0) for a in articles]) if articles else 1.0
                    
                    # Get trend info
                    trend_icon = ""
                    if trends:
                        topic_key = f"{subfield}|{topic}"
                        if topic_key in trends:
                            trend = trends[topic_key].get('trend', '')
                            if trend == 'growing':
                                trend_icon = " 🚀"
                            elif trend == 'declining':
                                trend_icon = " 📉"
                    
                    if include_metrics:
                        output.append(f"  ● TOPIC: {topic}{trend_icon} — {topic_articles} articles, {topic_citations} citations, FWCI: {topic_avg_fwci:.2f}")
                    else:
                        output.append(f"  ● TOPIC: {topic}{trend_icon} — {topic_articles} articles")
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
                        
                        # Citation info with FWCI and percentile
                        citations = article.get('cited_by_count', 0)
                        citations_per_year = article.get('citations_per_year', 0)
                        fwci = article.get('fwci', 1.0)
                        percentile = article.get('percentile', 50.0)
                        highly = " 🔥 HIGHLY CITED" if article.get('is_highly_cited') else ""
                        top10 = " 🏆 TOP 10%" if article.get('is_top10_percent') else ""
                        velocity = article.get('citation_velocity', 0)
                        
                        output.append(f"       Citations: {citations} | per year: {citations_per_year} | FWCI: {fwci:.2f} | Percentile: {percentile:.0f}%{highly}{top10}")
                        if velocity > 30:
                            output.append(f"       ⚡ Velocity: {velocity:.0f}%")
                        
                        if article.get('is_international', False):
                            output.append(f"       🌍 International collaboration ({len(article.get('author_countries', []))} countries)")
                        
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
        output.append(f"The journal's average Field-Weighted Citation Impact (FWCI) is {avg_fwci_overall:.2f}.")
        output.append(f"Among them, {highly_cited} articles are highly cited, making them particularly valuable for inclusion in your research.")
    else:
        output.append(f"Among them, {highly_cited} articles are highly cited, making them particularly valuable for inclusion in your research.")
    
    output.append("")
    output.append("We recommend paying special attention to articles marked as 'Highly Cited' and 'Top 10%' —")
    output.append("they demonstrate significant scientific interest and can become an important part")
    output.append("of your research.")
    output.append("")
    output.append("=" * 80)
    output.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append(f"© {journal_name}")
    output.append("=" * 80)
    
    return "\n".join(output)

# ============================================================================
# APPLICATION INTERFACE (UPDATED)
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
    if 'world_baselines' not in st.session_state:
        st.session_state.world_baselines = None
    if 'trends' not in st.session_state:
        st.session_state.trends = None
    if 'collaboration_stats' not in st.session_state:
        st.session_state.collaboration_stats = None
    if 'world_gaps' not in st.session_state:
        st.session_state.world_gaps = None
    
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
                                        # Fetch world baselines for normalization
                                        st.session_state.world_baselines = {}
                                        unique_subfields = set()
                                        
                                        # First, collect unique subfields from articles
                                        for article in articles:
                                            subfield_id = get_subfield_id_from_article(article)
                                            year = article.get('publication_year', 0)
                                            if subfield_id and year:
                                                unique_subfields.add((subfield_id, year))
                                        
                                        # Fetch baselines (with progress indicator)
                                        baseline_progress = st.progress(0)
                                        for idx, (subfield_id, year) in enumerate(unique_subfields):
                                            baseline = fetch_world_baseline_for_subfield(subfield_id, year)
                                            if baseline:
                                                key = f"{subfield_id}_{year}"
                                                st.session_state.world_baselines[key] = baseline
                                            baseline_progress.progress((idx + 1) / max(1, len(unique_subfields)))
                                        baseline_progress.empty()
                                        
                                        # Group articles with enriched data including FWCI
                                        threshold_total = st.session_state.threshold_total
                                        threshold_per_year = st.session_state.threshold_per_year
                                        hierarchy_unsorted = group_articles_by_hierarchy(
                                            articles, threshold_total, threshold_per_year, st.session_state.world_baselines
                                        )
                                        # Apply sorting based on current include_metrics setting
                                        hierarchy = sort_hierarchy_by_rules(hierarchy_unsorted, st.session_state.include_metrics)
                                        
                                        # Compute additional analytics
                                        st.session_state.trends = compute_topic_trends(articles, hierarchy)
                                        st.session_state.collaboration_stats = analyze_collaboration_patterns(hierarchy)
                                        st.session_state.world_gaps = compute_world_comparison(hierarchy, st.session_state.world_baselines)
                                        
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
                        st.session_state.articles, threshold_total, threshold_per_year, st.session_state.world_baselines
                    )
                    st.session_state.hierarchy = sort_hierarchy_by_rules(hierarchy_unsorted, include_metrics)
                    st.session_state.trends = compute_topic_trends(st.session_state.articles, st.session_state.hierarchy)
                    st.session_state.collaboration_stats = analyze_collaboration_patterns(st.session_state.hierarchy)
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
                        st.session_state.articles, threshold_total, threshold_per_year, st.session_state.world_baselines
                    )
                    st.session_state.hierarchy = sort_hierarchy_by_rules(hierarchy_unsorted, st.session_state.include_metrics)
                    st.session_state.trends = compute_topic_trends(st.session_state.articles, st.session_state.hierarchy)
                    st.session_state.collaboration_stats = analyze_collaboration_patterns(st.session_state.hierarchy)
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
            
            # Display hierarchy in UI
            st.markdown("---")
            st.markdown(f"### {t['research_hierarchy']}")
            
            for domain, fields in hierarchy.items():
                domain_stats = stats.get(domain, {})
                domain_articles = domain_stats.get('articles', 0)
                domain_citations = domain_stats.get('citations', 0) if st.session_state.include_metrics else 0
                domain_avg_fwci = domain_stats.get('avg_fwci', 1.0) if st.session_state.include_metrics else 1.0
                
                if st.session_state.include_metrics:
                    expander_title = f"{t['domain_icon']} {domain} — {domain_articles} {t['articles_count']}, {domain_citations} {t['citations']}, FWCI: {domain_avg_fwci:.2f}"
                else:
                    expander_title = f"{t['domain_icon']} {domain} — {domain_articles} {t['articles_count']}"
                
                with st.expander(expander_title):
                    for field, subfields in fields.items():
                        field_stats = domain_stats.get('fields', {}).get(field, {})
                        field_articles = field_stats.get('articles', 0)
                        field_citations = field_stats.get('citations', 0) if st.session_state.include_metrics else 0
                        field_avg_fwci = field_stats.get('avg_fwci', 1.0) if st.session_state.include_metrics else 1.0
                        
                        if st.session_state.include_metrics:
                            st.markdown(f"**{t['field_icon']} {field}** — {field_articles} {t['articles_count']}, {field_citations} {t['citations']}, FWCI: {field_avg_fwci:.2f}")
                        else:
                            st.markdown(f"**{t['field_icon']} {field}** — {field_articles} {t['articles_count']}")
                        
                        for subfield, topics in subfields.items():
                            subfield_stats = field_stats.get('subfields', {}).get(subfield, {})
                            subfield_articles = subfield_stats.get('articles', 0)
                            subfield_citations = subfield_stats.get('citations', 0) if st.session_state.include_metrics else 0
                            subfield_avg_fwci = subfield_stats.get('avg_fwci', 1.0) if st.session_state.include_metrics else 1.0
                            
                            if st.session_state.include_metrics:
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;**{t['subfield_icon']} {subfield}** — {subfield_articles} {t['articles_count']}, {subfield_citations} {t['citations']}, FWCI: {subfield_avg_fwci:.2f}")
                            else:
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;**{t['subfield_icon']} {subfield}** — {subfield_articles} {t['articles_count']}")
                            
                            for topic, articles in topics.items():
                                topic_articles = len(articles)
                                topic_citations = sum(a.get('cited_by_count', 0) for a in articles)
                                topic_avg_fwci = np.mean([a.get('fwci', 1.0) for a in articles]) if articles else 1.0
                                
                                # Get trend info
                                trend_icon = ""
                                if st.session_state.trends:
                                    topic_key = f"{subfield}|{topic}"
                                    if topic_key in st.session_state.trends:
                                        trend = st.session_state.trends[topic_key].get('trend', '')
                                        if trend == 'growing':
                                            trend_icon = " 🚀"
                                        elif trend == 'declining':
                                            trend_icon = " 📉"
                                
                                if st.session_state.include_metrics:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**{t['topic_icon']} {topic}{trend_icon}** — {topic_articles} {t['articles_count']}, {topic_citations} {t['citations']}, FWCI: {topic_avg_fwci:.2f}")
                                else:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**{t['topic_icon']} {topic}{trend_icon}** — {topic_articles} {t['articles_count']}")
                                
                                for idx, article in enumerate(articles[:5]):  # Show first 5 articles for compactness
                                    fwci = article.get('fwci', 1.0)
                                    percentile = article.get('percentile', 50.0)
                                    velocity = article.get('citation_velocity', 0)
                                    is_top10 = article.get('is_top10_percent', False)
                                    
                                    st.markdown(f"""
                                    <div style="padding: 8px; margin: 4px 0 4px 60px; background: #f8f9fa; border-radius: 8px; font-size: 0.85rem;">
                                        <b>{idx+1}. {article.get('title', 'No title')[:80]}{'...' if len(article.get('title', '')) > 80 else ''}</b><br>
                                        {t['authors_icon']} {article.get('authors', 'N/A')[:80]}<br>
                                        📊 {t['citations']}: {article.get('cited_by_count', 0)} ({t['citations_per_year']}: {article.get('citations_per_year', 0)}) | FWCI: {fwci:.2f} | Перцентиль: {percentile:.0f}%
                                        {f' ⚡ {velocity:.0f}%' if velocity > 30 else ''}
                                        {f' 🔥' if article.get('is_highly_cited') else ''}
                                        {f' 🏆' if is_top10 else ''}<br>
                                        {t['link_icon']} <a href="{article.get('doi_url', '#')}" target="_blank">{t['view_article']}</a>
                                    </div>
                                    """, unsafe_allow_html=True)
                                
                                if len(articles) > 5:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;*... and {len(articles) - 5} more articles*")
            
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
                    st.session_state.include_metrics,
                    st.session_state.trends,
                    st.session_state.collaboration_stats,
                    None,  # lineage_data (optional)
                    st.session_state.world_gaps
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
                    st.session_state.include_metrics,
                    st.session_state.trends,
                    st.session_state.collaboration_stats,
                    None,  # lineage_data (optional)
                    st.session_state.world_gaps
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
                    st.session_state.include_metrics,
                    st.session_state.trends
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
                    st.session_state.include_metrics,
                    st.session_state.trends
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
                                'hierarchy', 'selected_years', 'years_input', 'world_baselines',
                                'trends', 'collaboration_stats', 'world_gaps']
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
