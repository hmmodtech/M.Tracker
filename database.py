import sqlite3, os, logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
DB_PATH = os.environ.get('DB_PATH', 'ttracker.db')

DEFAULT_CHANNELS = [
    ("hamza20300",            "حمزة 20300"),
    ("QudsN",                 "قدس نيوز"),
    ("alraqib98",             "الرقيب"),
    ("alhodhud",              "الهدهد"),
    ("asmailpress",           "اسماعيل برس"),
    ("hanialshaer",           "هاني الشاعر"),
    ("Almustashaar",          "المستشار"),
    ("hpress",                "اچ برس"),
    ("EabriLive",             "عابر لايف"),
    ("mumenjmmeqdad",         "مؤمن المقداد"),
    ("alburaij",              "البريج نيوز"),
    ("IDFSpokespersonArabic", "الناطق الإسرائيلي"),
    ("mediagovps",            "الإعلام الحكومي"),
    ("TheTimesOfIsrael2022",  "تايمز أوف إسرائيل"),
    ("muthanapress84",        "مثنى برس"),
]

CRITICAL_KW = [
    "استهداف","قصف","شهيد","شهداء","جرحى",
    "إصابات","انتشال","تحت الأنقاض","أشلاء",
]

ALL_KEYWORDS = [
    "كواد كابتر","زنانة","مروحي","استهداف","قصف",
    "الزوارق الحربي","مدفعي",
    "طائرات الاستطلاع","المسيرات","إطلاق نار","غارة جوية","صاروخ تحذيري",
    "خط اصفر","صلاح الدين",
    "اشتباكات","مسلحين",
    "شهيد","شهداء","جرحى","إصابات","انتشال","تحت الأنقاض","أشلاء",
    "نزوح","خيام","نازحين","إيواء","مراكز الإيواء",
    "جرافات عسكرية","اعتقالات","وحدات خاصة",
    "الاعتقال","أسير","اسرى",
    "السابع من أكتوبر","صفقة تبادل","وقف إطلاق النار","مفاوضات","التهدئة",
    "معبر رفح","معبر كرم أبو سالم",
    "عاجل","إصابة مباشرة","منطقة حمراء",
]

def get_conn():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA foreign_keys=ON')
    return c

def init_db():
    """Create tables and seed defaults. Safe to call multiple times."""
    conn = get_conn()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS channels (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT NOT NULL UNIQUE,
            display     TEXT NOT NULL,
            description TEXT DEFAULT '',
            source_type TEXT DEFAULT 'telegram',
            source_url  TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now')),
            active      INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            channel         TEXT NOT NULL,
            text            TEXT NOT NULL,
            summary         TEXT DEFAULT '',
            msg_date        TEXT DEFAULT '',
            link            TEXT DEFAULT '',
            location_name   TEXT DEFAULT '',
            location_coords TEXT DEFAULT '',
            location_gmaps  TEXT DEFAULT '',
            has_critical    INTEGER DEFAULT 0,
            scraped_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(channel, link)
        );
        CREATE TABLE IF NOT EXISTS bookmarks (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL UNIQUE,
            saved_at   TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS keywords (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            word        TEXT NOT NULL UNIQUE,
            is_critical INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_msg_ch ON messages(channel);
        CREATE INDEX IF NOT EXISTS idx_msg_sc ON messages(scraped_at);
    ''')
    conn.commit()

    # Always ensure all default channels exist (INSERT OR IGNORE is safe)
    for username, display in DEFAULT_CHANNELS:
        conn.execute(
            'INSERT OR IGNORE INTO channels (username, display, source_type) VALUES (?, ?, ?)',
            (username, display, 'telegram')
        )
    conn.commit()

    # Always ensure all keywords exist
    for word in ALL_KEYWORDS:
        is_crit = 1 if word in CRITICAL_KW else 0
        conn.execute(
            'INSERT OR IGNORE INTO keywords (word, is_critical) VALUES (?, ?)',
            (word, is_crit)
        )
    conn.commit()
    conn.close()
    logger.info(f"DB initialized: {DB_PATH}")

# ── Channels ──────────────────────────────────────────────────
def get_channels():
    conn = get_conn()
    rows = conn.execute('''
        SELECT ch.id, ch.username, ch.display, ch.description,
               ch.source_type, ch.source_url, ch.active,
               COUNT(m.id) AS msg_count
        FROM channels ch
        LEFT JOIN messages m ON m.channel = ch.username
        GROUP BY ch.id
        ORDER BY ch.id ASC
    ''').fetchall()
    conn.close()
    return [dict(r) for r in rows]

def add_channel(username, display, desc='', source_type='telegram', source_url=''):
    conn = get_conn()
    try:
        cur = conn.execute(
            'INSERT OR IGNORE INTO channels (username, display, description, source_type, source_url) VALUES (?, ?, ?, ?, ?)',
            (username, display, desc, source_type, source_url)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()

def update_channel(cid, display, desc, source_url=''):
    conn = get_conn()
    try:
        conn.execute(
            'UPDATE channels SET display=?, description=?, source_url=? WHERE id=?',
            (display, desc, source_url, cid)
        )
        conn.commit()
    finally:
        conn.close()

def delete_channel(cid):
    conn = get_conn()
    try:
        row = conn.execute('SELECT username FROM channels WHERE id=?', (cid,)).fetchone()
        if row:
            conn.execute('DELETE FROM messages WHERE channel=?', (row['username'],))
        conn.execute('DELETE FROM channels WHERE id=?', (cid,))
        conn.commit()
    finally:
        conn.close()

# ── Messages ──────────────────────────────────────────────────
def add_message(channel, text, msg_date='', link='', summary='',
                location_name='', location_coords='', location_gmaps='', has_critical=0):
    conn = get_conn()
    try:
        conn.execute(
            '''INSERT INTO messages
               (channel, text, summary, msg_date, link,
                location_name, location_coords, location_gmaps, has_critical)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (channel, text, summary, msg_date, link,
             location_name, location_coords, location_gmaps, has_critical)
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False  # duplicate
    finally:
        conn.close()

def get_messages(channel=None, query=None, limit=300, critical_only=False):
    sql = '''
        SELECT m.id, m.channel, m.text, m.summary, m.msg_date, m.link,
               m.location_name, m.location_coords, m.location_gmaps,
               m.has_critical, m.scraped_at,
               CASE WHEN b.id IS NOT NULL THEN 1 ELSE 0 END AS bookmarked,
               ch.display AS channel_display
        FROM messages m
        LEFT JOIN bookmarks b ON b.message_id = m.id
        LEFT JOIN channels  ch ON ch.username  = m.channel
        WHERE 1=1
    '''
    params = []
    if channel and channel != 'ALL':
        sql += ' AND m.channel = ?'
        params.append(channel)
    if query:
        sql += ' AND m.text LIKE ?'
        params.append(f'%{query}%')
    if critical_only:
        sql += ' AND m.has_critical = 1'
    sql += ' ORDER BY m.id DESC LIMIT ?'
    params.append(limit)
    conn = get_conn()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()

def count_messages():
    conn = get_conn()
    try:
        return conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
    finally:
        conn.close()

# ── Bookmarks ─────────────────────────────────────────────────
def get_bookmarks():
    conn = get_conn()
    try:
        rows = conn.execute('''
            SELECT m.id, m.channel, m.text, m.summary, m.msg_date, m.link,
                   m.location_name, m.location_coords, m.location_gmaps,
                   m.has_critical, m.scraped_at, b.saved_at,
                   ch.display AS channel_display
            FROM bookmarks b
            JOIN messages m ON m.id = b.message_id
            LEFT JOIN channels ch ON ch.username = m.channel
            ORDER BY b.saved_at DESC
        ''').fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

def add_bookmark(mid):
    conn = get_conn()
    try:
        conn.execute('INSERT OR IGNORE INTO bookmarks (message_id) VALUES (?)', (mid,))
        conn.commit()
    finally:
        conn.close()

def remove_bookmark(mid):
    conn = get_conn()
    try:
        conn.execute('DELETE FROM bookmarks WHERE message_id = ?', (mid,))
        conn.commit()
    finally:
        conn.close()

# ── Keywords ──────────────────────────────────────────────────
def get_keywords():
    conn = get_conn()
    try:
        return [dict(r) for r in conn.execute(
            'SELECT id, word, is_critical FROM keywords ORDER BY is_critical DESC, id ASC'
        ).fetchall()]
    finally:
        conn.close()

def add_keyword(word, is_critical=0):
    conn = get_conn()
    try:
        cur = conn.execute(
            'INSERT OR IGNORE INTO keywords (word, is_critical) VALUES (?, ?)',
            (word, is_critical)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()

def remove_keyword(kid):
    conn = get_conn()
    try:
        conn.execute('DELETE FROM keywords WHERE id = ?', (kid,))
        conn.commit()
    finally:
        conn.close()

def get_all_keyword_words():
    conn = get_conn()
    try:
        return [r[0] for r in conn.execute('SELECT word FROM keywords').fetchall()]
    finally:
        conn.close()

# ── Stats ─────────────────────────────────────────────────────
def get_stats():
    since7  = (datetime.utcnow() - timedelta(days=7)).isoformat()
    since24 = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    conn = get_conn()
    try:
        top = conn.execute('''
            SELECT m.channel, ch.display, COUNT(*) cnt
            FROM messages m LEFT JOIN channels ch ON ch.username = m.channel
            WHERE m.scraped_at >= ? GROUP BY m.channel ORDER BY cnt DESC LIMIT 1
        ''', (since7,)).fetchone()
        vol = conn.execute('''
            SELECT m.channel, ch.display, COUNT(*) cnt
            FROM messages m LEFT JOIN channels ch ON ch.username = m.channel
            WHERE m.scraped_at >= ? GROUP BY m.channel ORDER BY cnt DESC
        ''', (since7,)).fetchall()
        return {
            'total_channels':   conn.execute('SELECT COUNT(*) FROM channels WHERE active=1').fetchone()[0],
            'total_messages':   conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0],
            'last_24h':         conn.execute('SELECT COUNT(*) FROM messages WHERE scraped_at>=?', (since24,)).fetchone()[0],
            'weekly_volume':    conn.execute('SELECT COUNT(*) FROM messages WHERE scraped_at>=?', (since7,)).fetchone()[0],
            'saved_intel':      conn.execute('SELECT COUNT(*) FROM bookmarks').fetchone()[0],
            'critical_count':   conn.execute('SELECT COUNT(*) FROM messages WHERE has_critical=1').fetchone()[0],
            'located_count':    conn.execute("SELECT COUNT(*) FROM messages WHERE location_coords!=''").fetchone()[0],
            'top_source':       dict(top) if top else {},
            'volume_by_source': [dict(r) for r in vol],
        }
    finally:
        conn.close()
