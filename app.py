from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import (LoginManager, UserMixin, login_user,
                         logout_user, login_required, current_user)
from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3
import os
import contextlib
from datetime import date
from calendar import monthrange

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'kakeibo-dev-secret-2026')

# ── Database ───────────────────────────────────────────────────────────────

_DATABASE_URL = os.environ.get('DATABASE_URL', '')
if _DATABASE_URL.startswith('postgres://'):
    _DATABASE_URL = _DATABASE_URL.replace('postgres://', 'postgresql://', 1)
IS_PG = bool(_DATABASE_URL)
SQLITE_DB = 'kakeibo.db'

if IS_PG:
    import psycopg2
    import psycopg2.extras


class _PGCursor:
    def __init__(self, cursor, has_returning=False):
        self._c = cursor
        self.lastrowid = None
        if has_returning:
            row = self._c.fetchone()
            self.lastrowid = row['id'] if row else None

    def fetchone(self):
        return self._c.fetchone()

    def fetchall(self):
        return self._c.fetchall()


class _PGConn:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        pg_sql = sql.replace('?', '%s')
        has_returning = 'RETURNING' in pg_sql.upper()
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(pg_sql, list(params) if params else [])
        return _PGCursor(cur, has_returning)


@contextlib.contextmanager
def get_db():
    if IS_PG:
        conn = psycopg2.connect(_DATABASE_URL)
        wrapped = _PGConn(conn)
        try:
            yield wrapped
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        conn = sqlite3.connect(SQLITE_DB)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def insert_get_id(conn, sql, params):
    if IS_PG:
        return conn.execute(sql + ' RETURNING id', params).lastrowid
    return conn.execute(sql, params).lastrowid


def _has_column(conn, table, col):
    """テーブルにカラムが存在するか確認（SQLite/PG両対応）"""
    if IS_PG:
        return bool(conn.execute(
            'SELECT 1 FROM information_schema.columns WHERE table_name=? AND column_name=?',
            (table, col)
        ).fetchone())
    else:
        rows = conn.execute(f'PRAGMA table_info({table})').fetchall()
        return any(row['name'] == col for row in rows)


def upsert_monthly_salary(conn, ym, amount, user_id):
    if conn.execute('SELECT 1 FROM monthly_salary WHERE ym=? AND user_id=?', (ym, user_id)).fetchone():
        conn.execute('UPDATE monthly_salary SET amount=? WHERE ym=? AND user_id=?', (amount, ym, user_id))
    else:
        conn.execute('INSERT INTO monthly_salary (ym, user_id, amount) VALUES (?,?,?)', (ym, user_id, amount))


def insert_ignore_monthly_salary(conn, ym, amount, user_id):
    if not conn.execute('SELECT 1 FROM monthly_salary WHERE ym=? AND user_id=?', (ym, user_id)).fetchone():
        conn.execute('INSERT INTO monthly_salary (ym, user_id, amount) VALUES (?,?,?)', (ym, user_id, amount))


# ── Flask-Login ────────────────────────────────────────────────────────────

login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message = 'ログインしてください'


class User(UserMixin):
    def __init__(self, id, username):
        self.id = id
        self.username = username


@login_manager.user_loader
def load_user(user_id):
    with get_db() as conn:
        row = conn.execute(
            'SELECT id, username FROM users WHERE id = ?', (user_id,)
        ).fetchone()
    if row:
        return User(row['id'], row['username'])
    return None


# ── DB Init ────────────────────────────────────────────────────────────────

def init_db():
    pk = 'SERIAL' if IS_PG else 'INTEGER'

    tables = [
        f'''CREATE TABLE IF NOT EXISTS users (
            id {pk} PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            credit_limit INTEGER DEFAULT 0
        )''',
        f'''CREATE TABLE IF NOT EXISTS salary (
            id {pk} PRIMARY KEY,
            amount INTEGER NOT NULL DEFAULT 0,
            credit_limit INTEGER DEFAULT 0
        )''',
        f'''CREATE TABLE IF NOT EXISTS fixed_expenses (
            id {pk} PRIMARY KEY,
            item_id INTEGER,
            name TEXT NOT NULL,
            amount INTEGER NOT NULL,
            type TEXT NOT NULL DEFAULT '固定',
            effective_ym TEXT NOT NULL DEFAULT '2000-01',
            active INTEGER DEFAULT 1,
            user_id INTEGER NOT NULL DEFAULT 0
        )''',
        f'''CREATE TABLE IF NOT EXISTS credit_cards (
            id {pk} PRIMARY KEY,
            name TEXT NOT NULL,
            closing_day INTEGER NOT NULL DEFAULT 25,
            fixed_months INTEGER DEFAULT 0,
            user_id INTEGER NOT NULL DEFAULT 0
        )''',
        f'''CREATE TABLE IF NOT EXISTS variable_expenses (
            id {pk} PRIMARY KEY,
            expense_date TEXT NOT NULL,
            amount INTEGER NOT NULL,
            category TEXT DEFAULT '',
            note TEXT DEFAULT '',
            payment_type TEXT NOT NULL DEFAULT 'cash',
            card_id INTEGER,
            billing_ym TEXT NOT NULL,
            user_id INTEGER NOT NULL DEFAULT 0
        )''',
        f'''CREATE TABLE IF NOT EXISTS extra_income (
            id {pk} PRIMARY KEY,
            income_date TEXT NOT NULL,
            amount INTEGER NOT NULL,
            note TEXT DEFAULT '',
            ym TEXT NOT NULL,
            user_id INTEGER NOT NULL DEFAULT 0
        )''',
        # monthly_salary は複合PK (ym, user_id) で新規作成
        '''CREATE TABLE IF NOT EXISTS monthly_salary (
            ym TEXT NOT NULL,
            user_id INTEGER NOT NULL DEFAULT 0,
            amount INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (ym, user_id)
        )''',
    ]

    with get_db() as conn:
        for stmt in tables:
            conn.execute(stmt)

        # ── カラム追加マイグレーション ──────────────────────────────
        def add_col(table, col, definition):
            if IS_PG:
                conn.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {definition}')
            else:
                try:
                    conn.execute(f'ALTER TABLE {table} ADD COLUMN {col} {definition}')
                except Exception:
                    pass

        # 既存テーブルに user_id を追加
        for table in ['fixed_expenses', 'credit_cards', 'variable_expenses', 'extra_income']:
            add_col(table, 'user_id', 'INTEGER NOT NULL DEFAULT 0')

        # users テーブルに credit_limit を追加
        add_col('users', 'credit_limit', 'INTEGER DEFAULT 0')

        # 固定費カラム追加
        for col, definition in [
            ('item_id',      'INTEGER'),
            ('type',         "TEXT NOT NULL DEFAULT '固定'"),
            ('effective_ym', "TEXT NOT NULL DEFAULT '2000-01'"),
        ]:
            add_col('fixed_expenses', col, definition)

        add_col('salary', 'credit_limit', 'INTEGER DEFAULT 0')
        add_col('credit_cards', 'fixed_months', 'INTEGER DEFAULT 0')

        # monthly_salary: 古い (ym TEXT PRIMARY KEY) スキーマを複合PKに移行
        if not _has_column(conn, 'monthly_salary', 'user_id'):
            conn.execute('''CREATE TABLE monthly_salary_new (
                ym TEXT NOT NULL,
                user_id INTEGER NOT NULL DEFAULT 0,
                amount INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (ym, user_id)
            )''')
            conn.execute('INSERT INTO monthly_salary_new (ym, user_id, amount) SELECT ym, 0, amount FROM monthly_salary')
            conn.execute('DROP TABLE monthly_salary')
            conn.execute('ALTER TABLE monthly_salary_new RENAME TO monthly_salary')

        # データ整合性
        conn.execute("UPDATE fixed_expenses SET item_id = id WHERE item_id IS NULL")
        conn.execute("UPDATE fixed_expenses SET effective_ym = '2000-01' WHERE effective_ym = '' OR effective_ym IS NULL")

        if not conn.execute('SELECT 1 FROM salary').fetchone():
            conn.execute('INSERT INTO salary (amount) VALUES (0)')

        # 旧 salary.amount を monthly_salary へ移行
        old = conn.execute('SELECT amount FROM salary LIMIT 1').fetchone()
        if old and old['amount'] > 0:
            today_ym = date.today().strftime('%Y-%m')
            insert_ignore_monthly_salary(conn, today_ym, old['amount'], 0)
            conn.execute('UPDATE salary SET amount = 0')


# ── Utility ────────────────────────────────────────────────────────────────

def add_months(d, n):
    month = d.month - 1 + n
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)


def calc_billing_ym(expense_date_str, payment_type, card_id=None):
    d = date.fromisoformat(expense_date_str)
    if payment_type == 'cash':
        return d.strftime('%Y-%m')
    with get_db() as conn:
        card = conn.execute('SELECT * FROM credit_cards WHERE id=?', (card_id,)).fetchone()
    if not card:
        return d.strftime('%Y-%m')
    if card['fixed_months']:
        return add_months(d, card['fixed_months']).strftime('%Y-%m')
    closing_day = card['closing_day']
    billing = add_months(d, 1) if d.day <= closing_day else add_months(d, 2)
    return billing.strftime('%Y-%m')


def fmt_ym(ym):
    year, month = ym.split('-')
    return f'{year}年{int(month)}月'


def fmt_money(n):
    return f'¥{n:,}'


app.jinja_env.filters['fmt_money'] = fmt_money
app.jinja_env.filters['fmt_ym'] = fmt_ym


def get_fixed_for_ym(conn, ym, user_id):
    return conn.execute('''
        SELECT f1.* FROM fixed_expenses f1
        WHERE f1.active = 1
          AND f1.amount > 0
          AND f1.user_id = ?
          AND f1.effective_ym <= ?
          AND f1.effective_ym = (
              SELECT MAX(f2.effective_ym)
              FROM fixed_expenses f2
              WHERE f2.item_id = f1.item_id
                AND f2.user_id = ?
                AND f2.active = 1
                AND f2.effective_ym <= ?
          )
        ORDER BY f1.item_id
    ''', (user_id, ym, user_id, ym)).fetchall()


def calc_balance_for_ym(conn, ym, user_id):
    ms = conn.execute('SELECT amount FROM monthly_salary WHERE ym=? AND user_id=?', (ym, user_id)).fetchone()
    salary = ms['amount'] if ms else 0
    fixed = get_fixed_for_ym(conn, ym, user_id)
    fixed_total = sum(f['amount'] for f in fixed)
    expenses = conn.execute(
        'SELECT amount FROM variable_expenses WHERE billing_ym=? AND user_id=?', (ym, user_id)
    ).fetchall()
    variable_total = sum(e['amount'] for e in expenses)
    extra = conn.execute('SELECT amount FROM extra_income WHERE ym=? AND user_id=?', (ym, user_id)).fetchall()
    extra_total = sum(i['amount'] for i in extra)
    return salary + extra_total - fixed_total - variable_total


def upsert_next_version(conn, item_id, next_ym, name, amount, type_, user_id):
    existing = conn.execute(
        'SELECT id FROM fixed_expenses WHERE item_id=? AND effective_ym=? AND active=1 AND user_id=?',
        (item_id, next_ym, user_id)
    ).fetchone()
    if existing:
        conn.execute(
            'UPDATE fixed_expenses SET name=?, amount=?, type=? WHERE id=?',
            (name, amount, type_, existing['id'])
        )
    else:
        conn.execute(
            'INSERT INTO fixed_expenses (item_id, name, amount, type, effective_ym, active, user_id) VALUES (?,?,?,?,?,1,?)',
            (item_id, name, amount, type_, next_ym, user_id)
        )


# ── Auth Routes ────────────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    with get_db() as conn:
        has_users = conn.execute('SELECT 1 FROM users LIMIT 1').fetchone()

    if not has_users:
        return redirect(url_for('register'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        with get_db() as conn:
            row = conn.execute(
                'SELECT id, username, password_hash FROM users WHERE username = ?', (username,)
            ).fetchone()
        if row and check_password_hash(row['password_hash'], password):
            login_user(User(row['id'], row['username']), remember=True)
            return redirect(request.args.get('next') or url_for('index'))
        flash('ユーザー名またはパスワードが間違っています')

    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    with get_db() as conn:
        has_users = conn.execute('SELECT 1 FROM users LIMIT 1').fetchone()

    if has_users and not current_user.is_authenticated:
        return redirect(url_for('login'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        confirm  = request.form.get('confirm', '')

        if not username or not password:
            flash('ユーザー名とパスワードを入力してください')
        elif password != confirm:
            flash('パスワードが一致しません')
        elif len(password) < 6:
            flash('パスワードは6文字以上にしてください')
        else:
            with get_db() as conn:
                if conn.execute('SELECT 1 FROM users WHERE username = ?', (username,)).fetchone():
                    flash('そのユーザー名はすでに使われています')
                else:
                    conn.execute(
                        'INSERT INTO users (username, password_hash) VALUES (?,?)',
                        (username, generate_password_hash(password))
                    )
                    flash(f'ユーザー「{username}」を登録しました。ログインしてください。')
                    return redirect(url_for('login'))

    return render_template('register.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


# ── App Routes ─────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    uid = current_user.id
    ym = request.args.get('ym', date.today().strftime('%Y-%m'))
    year, month = map(int, ym.split('-'))

    with get_db() as conn:
        user_row = conn.execute('SELECT credit_limit FROM users WHERE id=?', (uid,)).fetchone()
        credit_limit = user_row['credit_limit'] if user_row else 0

        ms = conn.execute('SELECT amount FROM monthly_salary WHERE ym=? AND user_id=?', (ym, uid)).fetchone()
        salary = ms['amount'] if ms else 0

        fixed = get_fixed_for_ym(conn, ym, uid)
        fixed_total = sum(f['amount'] for f in fixed)

        expenses = conn.execute(
            '''SELECT e.*, c.name as card_name, c.fixed_months as card_fixed_months
               FROM variable_expenses e
               LEFT JOIN credit_cards c ON e.card_id = c.id
               WHERE e.billing_ym = ? AND e.user_id = ?
               ORDER BY e.expense_date DESC''',
            (ym, uid)
        ).fetchall()
        variable_total = sum(e['amount'] for e in expenses)
        cash_total = sum(e['amount'] for e in expenses if e['payment_type'] == 'cash')
        credit_total = sum(e['amount'] for e in expenses
                           if e['payment_type'] == 'card' and not e['card_fixed_months'])
        etc_total = sum(e['amount'] for e in expenses
                        if e['payment_type'] == 'card' and e['card_fixed_months'])

        used_rows = conn.execute(
            '''SELECT e.amount, c.fixed_months as card_fixed_months
               FROM variable_expenses e
               LEFT JOIN credit_cards c ON e.card_id = c.id
               WHERE e.payment_type = 'card'
                 AND e.user_id = ?
                 AND substr(e.expense_date, 1, 7) = ?''',
            (uid, ym)
        ).fetchall()
        card_used_this_month = sum(r['amount'] for r in used_rows)

        extra_incomes = conn.execute(
            'SELECT * FROM extra_income WHERE ym=? AND user_id=? ORDER BY income_date DESC', (ym, uid)
        ).fetchall()
        extra_total = sum(i['amount'] for i in extra_incomes)

    current = date(year, month, 1)
    prev_ym = add_months(current, -1).strftime('%Y-%m')
    next_ym = add_months(current, 1).strftime('%Y-%m')

    prev_month_ended = date.today() >= current
    if prev_month_ended:
        with get_db() as conn:
            carryover = calc_balance_for_ym(conn, prev_ym, uid)
    else:
        carryover = 0

    balance = salary + extra_total + carryover - fixed_total - variable_total

    return render_template('index.html',
        ym=ym, salary=salary,
        fixed=fixed, fixed_total=fixed_total,
        expenses=expenses, variable_total=variable_total,
        cash_total=cash_total, credit_total=credit_total, etc_total=etc_total,
        credit_limit=credit_limit,
        card_used_this_month=card_used_this_month,
        extra_incomes=extra_incomes, extra_total=extra_total,
        carryover=carryover, prev_month_ended=prev_month_ended,
        balance=balance, prev_ym=prev_ym, next_ym=next_ym,
    )


@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    uid = current_user.id
    today_ym = date.today().strftime('%Y-%m')
    next_ym  = add_months(date.today().replace(day=1), 1).strftime('%Y-%m')

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'salary':
            amount = int(request.form.get('amount', 0) or 0)
            ym = request.form.get('ym', today_ym)
            with get_db() as conn:
                upsert_monthly_salary(conn, ym, amount, uid)
            flash(f'{fmt_ym(ym)}の給料を更新しました')

        elif action == 'credit_limit':
            limit = int(request.form.get('credit_limit', 0) or 0)
            with get_db() as conn:
                conn.execute('UPDATE users SET credit_limit=? WHERE id=?', (limit, uid))
            flash('クレカ上限額を更新しました')

        elif action == 'add_fixed':
            name     = request.form.get('name', '').strip()
            amount   = int(request.form.get('amount', 0) or 0)
            type_    = request.form.get('type', '固定')
            start_ym = request.form.get('start_ym', today_ym)
            one_time = request.form.get('one_time') == '1'
            if name and amount > 0:
                with get_db() as conn:
                    new_id = insert_get_id(
                        conn,
                        'INSERT INTO fixed_expenses (name, amount, type, effective_ym, user_id) VALUES (?,?,?,?,?)',
                        (name, amount, type_, start_ym, uid)
                    )
                    conn.execute('UPDATE fixed_expenses SET item_id=? WHERE id=?', (new_id, new_id))
                    if one_time:
                        end_ym = add_months(date.fromisoformat(start_ym + '-01'), 1).strftime('%Y-%m')
                        conn.execute(
                            'INSERT INTO fixed_expenses (item_id, name, amount, type, effective_ym, active, user_id) VALUES (?,?,0,?,?,1,?)',
                            (new_id, name, type_, end_ym, uid)
                        )
                label = f'{fmt_ym(start_ym)}のみ' if one_time else f'{fmt_ym(start_ym)}から有効'
                flash(f'固定費「{name}」を追加しました（{label}）')

        elif action == 'set_type':
            item_id = int(request.form.get('item_id'))
            new_type = request.form.get('type')
            with get_db() as conn:
                cur_row = conn.execute(
                    'SELECT * FROM fixed_expenses WHERE item_id=? AND user_id=? AND active=1 AND effective_ym<=? '
                    'ORDER BY effective_ym DESC LIMIT 1', (item_id, uid, today_ym)
                ).fetchone()
                if cur_row:
                    upsert_next_version(conn, item_id, next_ym,
                                        cur_row['name'], cur_row['amount'], new_type, uid)
            flash(f'翌月({fmt_ym(next_ym)})から「{new_type}」に変更します')

        elif action == 'edit_fixed':
            item_id   = int(request.form.get('item_id'))
            name      = request.form.get('name', '').strip()
            amount    = int(request.form.get('amount', 0) or 0)
            target_ym = request.form.get('target_ym', next_ym)
            if name:
                with get_db() as conn:
                    cur_row = conn.execute(
                        'SELECT * FROM fixed_expenses WHERE item_id=? AND user_id=? AND active=1 AND effective_ym<=? '
                        'ORDER BY effective_ym DESC LIMIT 1', (item_id, uid, today_ym)
                    ).fetchone()
                    if cur_row:
                        upsert_next_version(conn, item_id, target_ym,
                                            name, amount, cur_row['type'], uid)
                if amount == 0:
                    flash(f'{fmt_ym(target_ym)}から「{name}」を0円（無効）に変更します')
                else:
                    flash(f'{fmt_ym(target_ym)}から「{name} {fmt_money(amount)}」に変更します')

        elif action == 'delete_fixed':
            item_id = request.form.get('item_id')
            with get_db() as conn:
                conn.execute('UPDATE fixed_expenses SET active=0 WHERE item_id=? AND user_id=?', (item_id, uid))
            flash('固定費を削除しました')

        elif action == 'add_card':
            name         = request.form.get('name', '').strip()
            billing_type = request.form.get('billing_type', 'closing')
            closing_day  = int(request.form.get('closing_day', 25) or 25)
            fixed_months = int(request.form.get('fixed_months', 0) or 0)
            if billing_type == 'closing':
                fixed_months = 0
            else:
                closing_day = 25
            if name:
                with get_db() as conn:
                    conn.execute(
                        'INSERT INTO credit_cards (name, closing_day, fixed_months, user_id) VALUES (?,?,?,?)',
                        (name, closing_day, fixed_months, uid)
                    )
                if fixed_months:
                    flash(f'カード「{name}」を追加しました（使用日の{fixed_months}か月後引き落とし）')
                else:
                    flash(f'カード「{name}」を追加しました（締め日: {closing_day}日）')

        elif action == 'delete_card':
            cid = request.form.get('id')
            with get_db() as conn:
                conn.execute('DELETE FROM credit_cards WHERE id=? AND user_id=?', (cid, uid))
            flash('カードを削除しました')

        return redirect(url_for('settings'))

    with get_db() as conn:
        user_row = conn.execute('SELECT credit_limit FROM users WHERE id=?', (uid,)).fetchone()
        credit_limit = user_row['credit_limit'] if user_row else 0

        monthly_salaries = conn.execute(
            'SELECT ym, amount FROM monthly_salary WHERE user_id=? ORDER BY ym DESC LIMIT 12', (uid,)
        ).fetchall()

        fixed = get_fixed_for_ym(conn, today_ym, uid)

        pending_rows = conn.execute(
            'SELECT * FROM fixed_expenses WHERE effective_ym=? AND active=1 AND user_id=?', (next_ym, uid)
        ).fetchall()
        pending = {row['item_id']: row for row in pending_rows}

        cards = conn.execute('SELECT * FROM credit_cards WHERE user_id=? ORDER BY id', (uid,)).fetchall()

    base = date.today().replace(day=1)
    next_ym_list = [add_months(base, i).strftime('%Y-%m') for i in range(1, 13)]

    return render_template('settings.html',
        credit_limit=credit_limit,
        monthly_salaries=monthly_salaries,
        today_ym=today_ym,
        fixed=fixed, pending=pending,
        cards=cards, next_ym=next_ym,
        next_ym_list=next_ym_list)


@app.route('/expense', methods=['GET', 'POST'])
@login_required
def expense():
    uid = current_user.id

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'add':
            expense_date = request.form.get('expense_date') or date.today().isoformat()
            amount = int(request.form.get('amount', 0) or 0)
            category = request.form.get('category', '')
            note = request.form.get('note', '').strip()
            payment_type = request.form.get('payment_type', 'cash')
            card_id = request.form.get('card_id') or None

            if amount > 0:
                billing_ym = calc_billing_ym(expense_date, payment_type, card_id)
                with get_db() as conn:
                    conn.execute(
                        '''INSERT INTO variable_expenses
                           (expense_date, amount, category, note, payment_type, card_id, billing_ym, user_id)
                           VALUES (?,?,?,?,?,?,?,?)''',
                        (expense_date, amount, category, note, payment_type, card_id, billing_ym, uid)
                    )
                flash(f'追加しました（引き落とし: {fmt_ym(billing_ym)}）')
            else:
                flash('金額を入力してください')

        elif action == 'delete':
            eid = request.form.get('id')
            with get_db() as conn:
                conn.execute('DELETE FROM variable_expenses WHERE id=? AND user_id=?', (eid, uid))
            flash('削除しました')

        return redirect(url_for('expense'))

    with get_db() as conn:
        cards = conn.execute('SELECT * FROM credit_cards WHERE user_id=? ORDER BY id', (uid,)).fetchall()
        expenses = conn.execute(
            '''SELECT e.*, c.name as card_name
               FROM variable_expenses e
               LEFT JOIN credit_cards c ON e.card_id = c.id
               WHERE e.user_id = ?
               ORDER BY e.expense_date DESC
               LIMIT 60''',
            (uid,)
        ).fetchall()

    return render_template('expense.html',
        cards=cards, expenses=expenses,
        today=date.today().isoformat(),
        categories=['食費', '外食', '日用品', '交通費', '光熱費', '通信費', '医療費', '衣服', '娯楽', 'その他'],
    )


@app.route('/expense/<int:eid>/edit', methods=['GET', 'POST'])
@login_required
def expense_edit(eid):
    uid = current_user.id

    if request.method == 'POST':
        expense_date = request.form.get('expense_date') or date.today().isoformat()
        amount = int(request.form.get('amount', 0) or 0)
        category = request.form.get('category', '')
        note = request.form.get('note', '').strip()
        payment_type = request.form.get('payment_type', 'cash')
        card_id = request.form.get('card_id') or None
        if amount > 0:
            billing_ym = calc_billing_ym(expense_date, payment_type, card_id)
            with get_db() as conn:
                conn.execute(
                    '''UPDATE variable_expenses
                       SET expense_date=?, amount=?, category=?, note=?,
                           payment_type=?, card_id=?, billing_ym=?
                       WHERE id=? AND user_id=?''',
                    (expense_date, amount, category, note,
                     payment_type, card_id, billing_ym, eid, uid)
                )
            flash(f'更新しました（引き落とし: {fmt_ym(billing_ym)}）')
            return redirect(url_for('expense'))
        else:
            flash('金額を入力してください')

    with get_db() as conn:
        e = conn.execute('SELECT * FROM variable_expenses WHERE id=? AND user_id=?', (eid, uid)).fetchone()
        cards = conn.execute('SELECT * FROM credit_cards WHERE user_id=? ORDER BY id', (uid,)).fetchall()
    if not e:
        return redirect(url_for('expense'))

    return render_template('expense_edit.html',
        e=e, cards=cards,
        categories=['食費', '外食', '日用品', '交通費', '光熱費', '通信費', '医療費', '衣服', '娯楽', 'その他'],
    )


@app.route('/income', methods=['GET', 'POST'])
@login_required
def income():
    uid = current_user.id

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add':
            income_date = request.form.get('income_date') or date.today().isoformat()
            amount = int(request.form.get('amount', 0) or 0)
            note = request.form.get('note', '').strip()
            ym = income_date[:7]
            if amount > 0:
                with get_db() as conn:
                    conn.execute(
                        'INSERT INTO extra_income (income_date, amount, note, ym, user_id) VALUES (?,?,?,?,?)',
                        (income_date, amount, note, ym, uid)
                    )
                flash(f'臨時収入を追加しました（{fmt_ym(ym)}）')
            else:
                flash('金額を入力してください')
        elif action == 'delete':
            iid = request.form.get('id')
            with get_db() as conn:
                conn.execute('DELETE FROM extra_income WHERE id=? AND user_id=?', (iid, uid))
            flash('削除しました')
        return redirect(url_for('income'))

    with get_db() as conn:
        incomes = conn.execute(
            'SELECT * FROM extra_income WHERE user_id=? ORDER BY income_date DESC LIMIT 60', (uid,)
        ).fetchall()

    return render_template('income.html',
        incomes=incomes,
        today=date.today().isoformat(),
    )


if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5001)
