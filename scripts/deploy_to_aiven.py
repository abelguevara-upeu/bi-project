#!/usr/bin/env python3
"""
deploy_to_aiven.py

Conecta a una base PostgreSQL (Aiven) usando psycopg2, crea un esquema por cada archivo
datamart (ej: datamart_ventas.db.sql -> esquema datamart_ventas) y ejecuta
los scripts SQL `bi/dbs/sql/datamart_*.db.sql` dentro de su esquema correspondiente.

Uso (local):
    export DATABASE_URL='postgres://...'?sslmode=require
    python3 scripts/deploy_to_aiven.py

Nota: para seguridad no incluyas credenciales en el repo. Usa la variable de entorno DATABASE_URL
"""
import os
import glob
import sys
import psycopg2
import re

SQL_GLOB = os.path.join(os.path.dirname(__file__), '..', 'dbs', 'sql', 'datamart_*.db.sql')


def load_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def split_sql_tuple_values(s):
    """Split a SQL tuple like: 1,'a, b',NULL,0 into a list of values, handling single quotes.
    This is a simple parser: it supports single-quoted strings with doubled-quote escaping.
    """
    vals = []
    cur = []
    i = 0
    length = len(s)
    while i < length:
        ch = s[i]
        if ch == "'":
            cur.append(ch)
            i += 1
            # inside quoted string: copy until closing quote (handle doubled '')
            while i < length:
                cur.append(s[i])
                if s[i] == "'":
                    if i + 1 < length and s[i + 1] == "'":
                        # escaped quote, consume both
                        i += 2
                        cur.append("'")
                        continue
                    else:
                        i += 1
                        break
                i += 1
            continue
        if ch == ',':
            vals.append(''.join(cur).strip())
            cur = []
            i += 1
            continue
        else:
            cur.append(ch)
            i += 1
    if cur:
        vals.append(''.join(cur).strip())
    return vals


def detect_boolean_columns(sql_text):
    """Detect boolean column positions for each table in the provided SQL text.
    Returns a dict: {table_name: [idx_bool1, idx_bool2, ...]}
    Simple heuristic: find CREATE TABLE "NAME" (...) and detect lines with BOOLEAN.
    """
    bool_map = {}
    create_re = re.compile(r'CREATE TABLE IF NOT EXISTS\s+"(?P<table>[^"]+)"\s*\((?P<body>.*?)\);', re.S | re.I)
    col_re = re.compile(r'\s*"(?P<col>[^"]+)"\s+(?P<type>\w+)', re.I)
    for m in create_re.finditer(sql_text):
        table = m.group('table')
        body = m.group('body')
        cols = []
        for line in body.splitlines():
            cm = col_re.match(line)
            if cm:
                col = cm.group('col')
                ctype = cm.group('type').upper()
                cols.append((col, ctype))
        bool_positions = [i for i, (_, t) in enumerate(cols) if t == 'BOOLEAN']
        if bool_positions:
            bool_map[table] = bool_positions
    return bool_map


def transform_inserts_for_booleans(sql_text, bool_map):
    """Transform INSERT INTO "TABLE" VALUES (...) replacing 0/1 with FALSE/TRUE for boolean columns.
    Only supports single-tuple INSERTs like: INSERT INTO "T" VALUES (...);
    """
    insert_re = re.compile(r'INSERT INTO\s+"(?P<table>[^"]+)"\s+VALUES\s*\((?P<vals>.*?)\);', re.S | re.I)

    def repl(match):
        table = match.group('table')
        vals_text = match.group('vals')
        if table not in bool_map:
            return match.group(0)
        vals = split_sql_tuple_values(vals_text)
        positions = bool_map[table]
        for pos in positions:
            if pos < len(vals):
                v = vals[pos].strip()
                # if it's a quoted '0' or '1'
                if v.startswith("'") and v.endswith("'") and len(v) >= 2:
                    inner = v[1:-1]
                    if inner in ('0', '1'):
                        vals[pos] = 'TRUE' if inner == '1' else 'FALSE'
                else:
                    if v == '1':
                        vals[pos] = 'TRUE'
                    elif v == '0':
                        vals[pos] = 'FALSE'
        new_vals = ','.join(vals)
        return f'INSERT INTO "{table}" VALUES ({new_vals});'

    new_sql = insert_re.sub(repl, sql_text)
    return new_sql


def schema_name_from_path(p):
    """Derive a safe schema name from a file path, e.g. datamart_ventas.db.sql -> datamart_ventas"""
    n = os.path.basename(p)
    if n.lower().endswith('.db.sql'):
        base = n[:-7]
    else:
        base = os.path.splitext(n)[0]
    s = re.sub(r'[^0-9a-zA-Z]+', '_', base).lower()
    # Postgres schema names must not start with a digit; prefix if needed
    if re.match(r'^[0-9]', s):
        s = 's_' + s
    return s


def main():
    # Connection URL can come from env DATABASE_URL or hardcoded for convenience (not recommended)
    dsn = os.environ.get('DATABASE_URL')
    if not dsn:
        print('Error: set DATABASE_URL environment variable', file=sys.stderr)
        sys.exit(1)

    # Create list of files to execute
    sql_files = sorted(glob.glob(SQL_GLOB))
    if not sql_files:
        print('No se encontraron archivos SQL en:', SQL_GLOB, file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        cur = conn.cursor()

        for path in sql_files:
            schema = schema_name_from_path(path)
            print(f'Creando esquema {schema} si no existe...')
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

            print('Aplicando', path)
            sql = load_file(path)

            # Remove BEGIN TRANSACTION; to let psycopg2 manage the transaction
            sql_clean = sql.replace('BEGIN TRANSACTION;', '')

            # Detect boolean columns and transform INSERTs that use 0/1 into TRUE/FALSE where needed
            bool_map = detect_boolean_columns(sql_clean)
            if bool_map:
                sql_clean = transform_inserts_for_booleans(sql_clean, bool_map)

            # Execute within the specific schema (set search_path)
            wrapped = f'SET search_path = "{schema}", public;\n' + sql_clean

            try:
                cur.execute(wrapped)
                conn.commit()
                print('OK:', path)
            except Exception as e:
                conn.rollback()
                print('Error al aplicar', path, '-', str(e), file=sys.stderr)
                print('Abortando.', file=sys.stderr)
                cur.close()
                conn.close()
                sys.exit(2)

        cur.close()
        conn.close()
        print('Todos los scripts aplicados correctamente.')

    except Exception as e:
        print('Fallo de conexión o ejecución:', str(e), file=sys.stderr)
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        sys.exit(3)


if __name__ == '__main__':
    main()
