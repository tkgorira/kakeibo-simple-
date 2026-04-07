from flask import Flask, render_template, request, redirect, url_for, flash
import sqlite3
from datetime import date
from calendar import monthrange

app = Flask(__name__)
app.secret_key = 'kakeibo-2026'
DB = 'kakeibo.db'


# ── DB helpers ─────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS salary (
                id INTEGER PRIMARY KEY,
                amount INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS fixed_expenses (
                id INTEGER PRIMARY KEY,
                item_id INTEGER,
                name TEXT NOT NULL,
                amount INTEGER NOT NULL,
                type TEXT NOT NULL DEFAULT '固定',
                effective_ym TEXT NOT NULL DEFAULT '2000-01',
                active INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS credit_cards (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                closing_day INTEGER NOT NULL DEFAULT 25,
                fixed_months INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS variable_expenses (
                id INTEGER PRIMARY KEY,
                expense_date TEXT NOT NULL,
                amount INTEGER NOT NULL,
                category TEXT DEFAULT '',
                note TEXT DEFAULT '',
                payment_type TEXT NOT NULL DEFAULT 'cash',
                card_id INTEGER,
                billing_ym TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS extra_income (
                id INTEGER PRIMARY KEY,
                income_date TEXT NOT NULL,
                amount INTEGER NOT NULL,
                note TEXT DEFAULT '',
                ym TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS monthly_salary (
                ym TEXT PRIMARY KEY,
                amount INTEGER NOT NULL DEFAULT 0
            );
        ''')
        # マイグレーション: 既存DBに新カラムを追加
        for col, definition in [
            ('item_id',      'INTEGER'),
            ('type',         "TEXT NOT NULL DEFAULT '固定'"),
            ('effective_ym', "TEXT NOT NULL DEFAULT '2000-01'"),
        ]:
            try:
                conn.execute(f'ALTER TABLE fixed_expenses ADD COLUMN {col} {definition}')
            except Exception:
                pass
        # 既存行: item_id未設定のものに id を割り当て
        conn.execute("UPDATE fixed_expenses SET item_id = id WHERE item_id IS NULL")
        conn.execute("UPDATE fixed_expenses SET effective_ym = '2000-01' WHERE effective_ym = '' OR effective_ym IS NULL")
        try:
            conn.execute('ALTER TABLE credit_cards ADD COLUMN fixed_months INTEGER DEFAULT 0')
        except Exception:
            pass
        try:
            conn.execute('ALTER TABLE salary ADD COLUMN credit_limit INTEGER DEFAULT 0')
        except Exception:
            pass
        if not conn.execute('SELECT 1 FROM salary').fetchone():
            conn.execute('INSERT INTO salary (amount) VALUES (0)')
        # 旧 salary テーブルの金額を monthly_salary に移行（初回のみ）
        old = conn.execute('SELECT amount FROM salary').fetchone()
        if old and old['amount'] > 0:
            today_ym = date.today().strftime('%Y-%m')
            conn.execute(
                'INSERT OR IGNORE INTO monthly_salary (ym, amount) VALUES (?,?)',
                (today_ym, old['amount'])
            )
            conn.execute('UPDATE salary SET amount=0')


# ── Utility ────────────────────────────────────────────────────────────────

def add_months(d, n):
    """dateにn月加算（dateutil不要）"""
    month = d.month - 1 + n
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)


def calc_billing_ym(expense_date_str, payment_type, card_id=None):
    """引き落とし年月を返す (YYYY-MM)"""
    d = date.fromisoformat(expense_date_str)
    if payment_type == 'cash':
        return d.strftime('%Y-%m')

    with get_db() as conn:
        card = conn.execute('SELECT * FROM credit_cards WHERE id=?', (card_id,)).fetchone()
    if not card:
        return d.strftime('%Y-%m')

    # 固定月数が設定されている場合（例: ETCカード = 3か月後）
    if card['fixed_months']:
        return add_months(d, card['fixed_months']).strftime('%Y-%m')

    closing_day = card['closing_day']
    if d.day <= closing_day:
        billing = add_months(d, 1)   # 締め日以内 → 翌月
    else:
        billing = add_months(d, 2)   # 締め日超過 → 翌々月
    return billing.strftime('%Y-%m')


def fmt_ym(ym):
    year, month = ym.split('-')
    return f'{year}年{int(month)}月'


def fmt_money(n):
    return f'¥{n:,}'


app.jinja_env.filters['fmt_money'] = fmt_money
app.jinja_env.filters['fmt_ym'] = fmt_ym


def calc_balance_for_ym(conn, ym):
    """指定月の残高を計算して返す"""
    ms = conn.execute('SELECT amount FROM monthly_salary WHERE ym=?', (ym,)).fetchone()
    salary = ms['amount'] if ms else 0
    fixed = get_fixed_for_ym(conn, ym)
    fixed_total = sum(f['amount'] for f in fixed)
    expenses = conn.execute(
        'SELECT amount FROM variable_expenses WHERE billing_ym=?', (ym,)
    ).fetchall()
    variable_total = sum(e['amount'] for e in expenses)
    extra = conn.execute('SELECT amount FROM extra_income WHERE ym=?', (ym,)).fetchall()
    extra_total = sum(i['amount'] for i in extra)
    return salary + extra_total - fixed_total - variable_total


def get_fixed_for_ym(conn, ym):
    """指定月に有効な固定費（各item_idの最新バージョン、amount>0のみ）を返す"""
    return conn.execute('''
        SELECT f1.* FROM fixed_expenses f1
        WHERE f1.active = 1
          AND f1.amount > 0
          AND f1.effective_ym <= ?
          AND f1.effective_ym = (
              SELECT MAX(f2.effective_ym)
              FROM fixed_expenses f2
              WHERE f2.item_id = f1.item_id
                AND f2.active = 1
                AND f2.effective_ym <= ?
          )
        ORDER BY f1.item_id
    ''', (ym, ym)).fetchall()


def upsert_next_version(conn, item_id, next_ym, name, amount, type_):
    """翌月バージョンを作成または更新する"""
    existing = conn.execute(
        'SELECT id FROM fixed_expenses WHERE item_id=? AND effective_ym=? AND active=1',
        (item_id, next_ym)
    ).fetchone()
    if existing:
        conn.execute(
            'UPDATE fixed_expenses SET name=?, amount=?, type=? WHERE id=?',
            (name, amount, type_, existing['id'])
        )
    else:
        conn.execute(
            'INSERT INTO fixed_expenses (item_id, name, amount, type, effective_ym, active) VALUES (?,?,?,?,?,1)',
            (item_id, name, amount, type_, next_ym)
        )


# ── Routes ─────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    ym = request.args.get('ym', date.today().strftime('%Y-%m'))
    year, month = map(int, ym.split('-'))

    with get_db() as conn:
        salary_row = conn.execute('SELECT credit_limit FROM salary').fetchone()
        credit_limit = salary_row['credit_limit'] if salary_row else 0

        ms = conn.execute('SELECT amount FROM monthly_salary WHERE ym=?', (ym,)).fetchone()
        salary = ms['amount'] if ms else 0

        fixed = get_fixed_for_ym(conn, ym)
        fixed_total = sum(f['amount'] for f in fixed)

        expenses = conn.execute(
            '''SELECT e.*, c.name as card_name, c.fixed_months as card_fixed_months
               FROM variable_expenses e
               LEFT JOIN credit_cards c ON e.card_id = c.id
               WHERE e.billing_ym = ?
               ORDER BY e.expense_date DESC''',
            (ym,)
        ).fetchall()
        variable_total = sum(e['amount'] for e in expenses)
        cash_total = sum(e['amount'] for e in expenses if e['payment_type'] == 'cash')
        credit_total = sum(e['amount'] for e in expenses
                           if e['payment_type'] == 'card' and not e['card_fixed_months'])
        etc_total = sum(e['amount'] for e in expenses
                        if e['payment_type'] == 'card' and e['card_fixed_months'])

        # アラート用: 当月に使用したカード支出（expense_date基準）
        used_rows = conn.execute(
            '''SELECT e.amount, c.fixed_months as card_fixed_months
               FROM variable_expenses e
               LEFT JOIN credit_cards c ON e.card_id = c.id
               WHERE e.payment_type = 'card'
                 AND strftime('%Y-%m', e.expense_date) = ?''',
            (ym,)
        ).fetchall()
        card_used_this_month = sum(r['amount'] for r in used_rows)

        extra_incomes = conn.execute(
            'SELECT * FROM extra_income WHERE ym=? ORDER BY income_date DESC', (ym,)
        ).fetchall()
        extra_total = sum(i['amount'] for i in extra_incomes)

    current = date(year, month, 1)
    prev_ym = add_months(current, -1).strftime('%Y-%m')
    next_ym = add_months(current, 1).strftime('%Y-%m')

    # 前月が終了済みなら繰越計算（当月1日以降 = 前月は確定）
    prev_month_ended = date.today() >= current
    if prev_month_ended:
        with get_db() as conn:
            carryover = calc_balance_for_ym(conn, prev_ym)
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
def settings():
    today_ym = date.today().strftime('%Y-%m')
    next_ym  = add_months(date.today().replace(day=1), 1).strftime('%Y-%m')

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'salary':
            amount = int(request.form.get('amount', 0) or 0)
            ym = request.form.get('ym', date.today().strftime('%Y-%m'))
            with get_db() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO monthly_salary (ym, amount) VALUES (?,?)',
                    (ym, amount)
                )
            flash(f'{fmt_ym(ym)}の給料を更新しました')

        elif action == 'credit_limit':
            limit = int(request.form.get('credit_limit', 0) or 0)
            with get_db() as conn:
                conn.execute('UPDATE salary SET credit_limit=?', (limit,))
            flash('クレカ上限額を更新しました')

        elif action == 'add_fixed':
            name   = request.form.get('name', '').strip()
            amount = int(request.form.get('amount', 0) or 0)
            type_  = request.form.get('type', '固定')
            if name and amount > 0:
                with get_db() as conn:
                    cur = conn.execute(
                        'INSERT INTO fixed_expenses (name, amount, type, effective_ym) VALUES (?,?,?,?)',
                        (name, amount, type_, today_ym)
                    )
                    conn.execute('UPDATE fixed_expenses SET item_id=? WHERE id=?',
                                 (cur.lastrowid, cur.lastrowid))
                flash(f'固定費「{name}」を追加しました（今月から有効）')

        elif action == 'set_type':
            # タイプ変更 → 翌月バージョン作成
            item_id = int(request.form.get('item_id'))
            new_type = request.form.get('type')
            with get_db() as conn:
                current = conn.execute(
                    'SELECT * FROM fixed_expenses WHERE item_id=? AND active=1 AND effective_ym<=? '
                    'ORDER BY effective_ym DESC LIMIT 1', (item_id, today_ym)
                ).fetchone()
                if current:
                    upsert_next_version(conn, item_id, next_ym,
                                        current['name'], current['amount'], new_type)
            flash(f'翌月({fmt_ym(next_ym)})から「{new_type}」に変更します')

        elif action == 'edit_fixed':
            # 名前・金額変更 → 指定月バージョン作成
            item_id    = int(request.form.get('item_id'))
            name       = request.form.get('name', '').strip()
            amount     = int(request.form.get('amount', 0) or 0)
            target_ym  = request.form.get('target_ym', next_ym)
            if name:
                with get_db() as conn:
                    current = conn.execute(
                        'SELECT * FROM fixed_expenses WHERE item_id=? AND active=1 AND effective_ym<=? '
                        'ORDER BY effective_ym DESC LIMIT 1', (item_id, today_ym)
                    ).fetchone()
                    if current:
                        upsert_next_version(conn, item_id, target_ym,
                                            name, amount, current['type'])
                if amount == 0:
                    flash(f'{fmt_ym(target_ym)}から「{name}」を0円（無効）に変更します')
                else:
                    flash(f'{fmt_ym(target_ym)}から「{name} {fmt_money(amount)}」に変更します')

        elif action == 'delete_fixed':
            item_id = request.form.get('item_id')
            with get_db() as conn:
                conn.execute('UPDATE fixed_expenses SET active=0 WHERE item_id=?', (item_id,))
            flash('固定費を削除しました')

        elif action == 'add_card':
            name         = request.form.get('name', '').strip()
            billing_type = request.form.get('billing_type', 'closing')
            closing_day  = int(request.form.get('closing_day', 25) or 25)
            fixed_months = int(request.form.get('fixed_months', 0) or 0)
            if billing_type == 'closing':
                fixed_months = 0
            else:
                closing_day = 25  # 使わないがデフォルト値
            if name:
                with get_db() as conn:
                    conn.execute(
                        'INSERT INTO credit_cards (name, closing_day, fixed_months) VALUES (?,?,?)',
                        (name, closing_day, fixed_months)
                    )
                if fixed_months:
                    flash(f'カード「{name}」を追加しました（使用日の{fixed_months}か月後引き落とし）')
                else:
                    flash(f'カード「{name}」を追加しました（締め日: {closing_day}日）')

        elif action == 'delete_card':
            cid = request.form.get('id')
            with get_db() as conn:
                conn.execute('DELETE FROM credit_cards WHERE id=?', (cid,))
            flash('カードを削除しました')

        return redirect(url_for('settings'))

    with get_db() as conn:
        salary_row = conn.execute('SELECT credit_limit FROM salary').fetchone()
        credit_limit = salary_row['credit_limit'] if salary_row else 0

        monthly_salaries = conn.execute(
            'SELECT ym, amount FROM monthly_salary ORDER BY ym DESC LIMIT 12'
        ).fetchall()

        # 今月有効な固定費
        fixed = get_fixed_for_ym(conn, today_ym)

        # 翌月に変更予定のバージョン {item_id: row}
        pending_rows = conn.execute(
            'SELECT * FROM fixed_expenses WHERE effective_ym=? AND active=1', (next_ym,)
        ).fetchall()
        pending = {row['item_id']: row for row in pending_rows}

        cards = conn.execute('SELECT * FROM credit_cards ORDER BY id').fetchall()

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
def expense():
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
                           (expense_date, amount, category, note, payment_type, card_id, billing_ym)
                           VALUES (?,?,?,?,?,?,?)''',
                        (expense_date, amount, category, note, payment_type, card_id, billing_ym)
                    )
                flash(f'追加しました（引き落とし: {fmt_ym(billing_ym)}）')
            else:
                flash('金額を入力してください')

        elif action == 'delete':
            eid = request.form.get('id')
            with get_db() as conn:
                conn.execute('DELETE FROM variable_expenses WHERE id=?', (eid,))
            flash('削除しました')

        return redirect(url_for('expense'))

    with get_db() as conn:
        cards = conn.execute('SELECT * FROM credit_cards ORDER BY id').fetchall()
        expenses = conn.execute(
            '''SELECT e.*, c.name as card_name
               FROM variable_expenses e
               LEFT JOIN credit_cards c ON e.card_id = c.id
               ORDER BY e.expense_date DESC
               LIMIT 60''',
        ).fetchall()

    return render_template('expense.html',
        cards=cards, expenses=expenses,
        today=date.today().isoformat(),
        categories=['食費', '外食', '日用品', '交通費', '光熱費', '通信費', '医療費', '衣服', '娯楽', 'その他'],
    )


@app.route('/expense/<int:eid>/edit', methods=['GET', 'POST'])
def expense_edit(eid):
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
                       WHERE id=?''',
                    (expense_date, amount, category, note,
                     payment_type, card_id, billing_ym, eid)
                )
            flash(f'更新しました（引き落とし: {fmt_ym(billing_ym)}）')
            return redirect(url_for('expense'))
        else:
            flash('金額を入力してください')

    with get_db() as conn:
        e = conn.execute('SELECT * FROM variable_expenses WHERE id=?', (eid,)).fetchone()
        cards = conn.execute('SELECT * FROM credit_cards ORDER BY id').fetchall()
    if not e:
        return redirect(url_for('expense'))

    return render_template('expense_edit.html',
        e=e, cards=cards,
        categories=['食費', '外食', '日用品', '交通費', '光熱費', '通信費', '医療費', '衣服', '娯楽', 'その他'],
    )


@app.route('/income', methods=['GET', 'POST'])
def income():
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
                        'INSERT INTO extra_income (income_date, amount, note, ym) VALUES (?,?,?,?)',
                        (income_date, amount, note, ym)
                    )
                flash(f'臨時収入を追加しました（{fmt_ym(ym)}）')
            else:
                flash('金額を入力してください')
        elif action == 'delete':
            iid = request.form.get('id')
            with get_db() as conn:
                conn.execute('DELETE FROM extra_income WHERE id=?', (iid,))
            flash('削除しました')
        return redirect(url_for('income'))

    with get_db() as conn:
        incomes = conn.execute(
            'SELECT * FROM extra_income ORDER BY income_date DESC LIMIT 60'
        ).fetchall()

    return render_template('income.html',
        incomes=incomes,
        today=date.today().isoformat(),
    )


if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5001)
