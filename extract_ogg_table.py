# -*- coding: utf-8 -*-

import os
import re
import ast
import time
import gzip
import sqlite3
from argparse import ArgumentParser
import codecs


_INSERT_INTO_TOKEN = 'INSERT INTO `image`'

_FIELD_NAMES = ('img_name, img_size, img_width, img_height, img_metadata,'
                ' img_bits, img_media_type, img_major_mime, img_minor_mime,'
                ' img_description, img_user, img_user_text, img_timestamp,'
                ' img_sha1').split(', ')
IMAGE_TABLE_SCHEMA = ('CREATE TABLE image (%s);' % ', '.join(_FIELD_NAMES))
READ_SIZE = 2 ** 15  # 32kb
_LITERAL = r"('([^'\\]*(?:\\.[^'\\]*)*)'|\d+)"
_TUPLE_RE = re.compile(r"\(%s(,%s)*\)" % (_LITERAL, _LITERAL))


def fix_mysqldump_single_quote_escape(statement):
    # TODO: .replace('\\"', '"') ?
    return statement.replace("\\'", "''")


def split_oversized_insert(statement, chunk_size=300):
    if not statement.startswith(_INSERT_INTO_TOKEN):
        raise ValueError('statement should start with INSERT INTO')
    value_start_idx = statement.index('VALUES (') + len('VALUES (') - 1
    preface = statement[:value_start_idx]
    ret = []
    subparts = statement.split('),')
    for chunk in chunked_iter(subparts, chunk_size):
        chunk_statement = u'),'.join(chunk)
        if not chunk_statement.endswith(';'):
            chunk_statement = chunk_statement + ');'
        if not chunk_statement.startswith(preface):
            chunk_statement = preface + chunk_statement
        #print chunk_statement[:48].encode('utf-8'), chunk_statement[-48:].encode('utf-8')
        #print
        ret.append(chunk_statement)
    return ret


def create_table(location=':memory:'):
    conn = sqlite3.connect(location)
    conn.execute(IMAGE_TABLE_SCHEMA)
    conn.commit()
    return conn


class DatabaseLoader(object):
    def __init__(self, source_file, target_db_file):
        self.source_file = source_file
        self.start_time = time.time()
        self.total_size = bytes2human(os.path.getsize(source_file), 2)
        self.cur_stmt_count = 0
        self.skipped_stmt_count = 0
        self.decoder = codecs.getreader('utf-8')

        self.buff = u''

        self.temp_table = create_table(':memory:')
        self.perm_table = create_table(target_db_file)

        with gzip.open(self.source_file) as gf_encoded:
            self._load(gf_encoded)

    def _load(self, file_handle, verbose=True):
        file_handle_encoded = file_handle
        file_handle = self.decoder(file_handle, errors='replace')
        internet_of_things = []
        stmt_count = 0
        for line in file_handle:
            if not line.startswith('INSERT'):
                continue
            for m in _TUPLE_RE.finditer(line):
                internet_of_things.append(ast.literal_eval(m.group()))

            stmt_count += 1
            cur_count = len(internet_of_things)
            cur_bytes_read = bytes2human(file_handle_encoded.fileobj.tell(), 2)
            cur_duration = round(time.time() - self.start_time, 2)

            if verbose:
                print cur_count, 'records.', cur_bytes_read, 'out of', self.total_size, 'read. (',
                print stmt_count, 'statements,', self.skipped_stmt_count, 'skipped)',
                print cur_duration, 'seconds.'

            if len(internet_of_things) > 100000:
                import pdb;pdb.set_trace()


    def _old_load(self, file_handle, verbose=True):
        file_handle_encoded = file_handle
        file_handle = self.decoder(file_handle, errors='replace')
        data = file_handle.read(4096)
        self.buff = data[data.index(_INSERT_INTO_TOKEN):]

        tt_cur = self.temp_table.cursor()
        while data:
            data = file_handle.read(READ_SIZE)
            self.buff += data

            ii_end = self.buff.find(_INSERT_INTO_TOKEN, 11)
            if ii_end < 0:
                continue
            full_statement, self.buff = self.buff[:ii_end].strip(), self.buff[ii_end:]

            full_statement = fix_mysqldump_single_quote_escape(full_statement)
            for _retry_i in range(9):
                try:
                    chunk_size = 500 - (_retry_i * 17)
                    chunked_statements = split_oversized_insert(full_statement, chunk_size=chunk_size)
                    for stmt in chunked_statements:
                        tt_cur.execute(stmt)
                except sqlite3.OperationalError as oe:
                    #print oe  # some exceptions can be very very long
                    if 'syntax error' in oe.message:
                        full_statement = fix_mysqldump_single_quote_escape(full_statement)
                    continue
                else:
                    self.cur_stmt_count += 1
                    break
            else:
                #raise RuntimeError("couldn't decipher an INSERT INTO breakup scheme")
                self.skipped_stmt_count += 1
                continue

            self.temp_table.commit()
            cur_count = tt_cur.execute('SELECT COUNT(*) FROM image').fetchone()[0]
            cur_bytes_read = bytes2human(file_handle_encoded.fileobj.tell(), 2)
            cur_duration = round(time.time() - self.start_time, 2)
            if cur_count > 100000:
                self._flush_temp()
                tt_cur = self.temp_table.cursor()
            if verbose:
                print cur_count, 'records.', cur_bytes_read, 'out of', self.total_size, 'read. (',
                print self.cur_stmt_count, 'statements,', self.skipped_stmt_count, 'skipped)',
                print cur_duration, 'seconds.'


        import pdb;pdb.set_trace()

    def _flush_temp(self):
        audios = self.temp_table.execute('SELECT * FROM image WHERE img_media_type="AUDIO"').fetchall()
        pt_cur = self.perm_table.cursor()
        _query = 'INSERT INTO image VALUES (%s)' % ', '.join('?' * len(_FIELD_NAMES))
        for a in audios:
            pt_cur.execute(_query, a)
        self.perm_table.commit()
        self.temp_table = create_table(':memory:')



def db_main(filename):
    db_loader = DatabaseLoader(filename, 'ogg_table.db')

def chunked_iter(src, size, **kw):
    """
    Generates 'size'-sized chunks from 'src' iterable. Unless
    the optional 'fill' keyword argument is provided, iterables
    not even divisible by 'size' will have a final chunk that is
    smaller than 'size'.

    Note that fill=None will in fact use None as the fill value.

    >>> list(chunked_iter(range(10), 3))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    >>> list(chunked_iter(range(10), 3, fill=None))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, None, None]]
    """
    if not is_iterable(src):
        raise TypeError('expected an iterable')
    size = int(size)
    if size <= 0:
        raise ValueError('expected a positive integer chunk size')
    do_fill = True
    try:
        fill_val = kw.pop('fill')
    except KeyError:
        do_fill = False
        fill_val = None
    if kw:
        raise ValueError('got unexpected keyword arguments: %r' % kw.keys())
    if not src:
        return
    postprocess = lambda chk: chk
    if isinstance(src, basestring):
        postprocess = lambda chk, _sep=type(src)(): _sep.join(chk)
    cur_chunk = []
    i = 0
    for item in src:
        cur_chunk.append(item)
        i += 1
        if i % size == 0:
            yield postprocess(cur_chunk)
            cur_chunk = []
    if cur_chunk:
        if do_fill:
            lc = len(cur_chunk)
            cur_chunk[lc:] = [fill_val] * (size - lc)
        yield postprocess(cur_chunk)
    return


def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True


def is_scalar(obj):
    return not is_iterable(obj) or isinstance(obj, basestring)


_SIZE_SYMBOLS = ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
_SIZE_BOUNDS = [(1024 ** i, sym) for i, sym in enumerate(_SIZE_SYMBOLS)]
_SIZE_RANGES = zip(_SIZE_BOUNDS, _SIZE_BOUNDS[1:])


def bytes2human(nbytes, ndigits=0):
    """
    >>> bytes2human(128991)
    '126K'
    >>> bytes2human(100001221)
    '95M'
    >>> bytes2human(0, 2)
    '0.00B'
    """
    abs_bytes = abs(nbytes)
    for (size, symbol), (next_size, next_symbol) in _SIZE_RANGES:
        if abs_bytes <= next_size:
            break
    hnbytes = float(nbytes) / size
    return '{hnbytes:.{ndigits}f}{symbol}'.format(hnbytes=hnbytes,
                                                  ndigits=ndigits,
                                                  symbol=symbol)


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    args = prs.parse_args()
    db_main(args.filename)
