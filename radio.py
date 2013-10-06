# -*- coding: utf-8 -*-

import os
import sqlite3
from gzip import GzipFile
from argparse import ArgumentParser
import codecs


IMAGE_TABLE_SCHEMA = ('CREATE TABLE image (img_name, img_size, img_width,'
                      ' img_height, img_metadata, img_bits, img_media_type,'
                      ' img_major_mime, img_minor_mime, img_description,'
                      ' img_user, img_user_text, img_timestamp, img_sha1);')
READ_SIZE = 2 ** 15  # 32kb


def fix_mysqldump_single_quote_escape(statement):
    # TODO: .replace('\\"', '"') ?
    return statement.replace("\\'", "''")


def split_oversized_insert(statement, chunk_size=300):
    if not statement.startswith('INSERT INTO'):
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


def create_table():
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(IMAGE_TABLE_SCHEMA)
    conn.commit()
    return conn


def db_main(filename):
    conn = create_table()

    total_size = bytes2human(os.path.getsize(filename), 2)
    cur_stmt_count = 0
    skipped_stmt_count = 0

    reader = codecs.getreader('utf-8')
    with GzipFile(filename, 'r') as gf_encoded:
        gf = reader(gf_encoded, errors='replace')
        data = gf.read(4096)
        buff = data[data.index('INSERT INTO'):]

        while data:
            data = gf.read(READ_SIZE)
            buff += data

            ii_end = buff.find('INSERT INTO', 11)
            if ii_end < 0:
                continue
            full_statement, buff = buff[:ii_end].strip(), buff[ii_end:]
            
            full_statement = fix_mysqldump_single_quote_escape(full_statement)
            for _retry_i in range(9):
                try:
                    chunk_size = 500 - (_retry_i * 17)
                    chunked_statements = split_oversized_insert(full_statement, chunk_size=chunk_size)
                    for stmt in chunked_statements:
                        cur.execute(stmt)
                except sqlite3.OperationalError as oe:
                    #print oe  # some exceptions can be very very long
                    if 'syntax error' in oe.message:
                        full_statement = fix_mysqldump_single_quote_escape(full_statement)
                    continue
                else:
                    cur_stmt_count += 1
                    break
            else:
                skipped_stmt_count += 1
                print 'skipping an insert statement'
                continue
                #raise RuntimeError("couldn't decipher an INSERT INTO breakup scheme")
            conn.commit()
            cur_count = cur.execute('SELECT COUNT(*) FROM image').fetchone()[0]
            cur_bytes_read = bytes2human(gf_encoded.fileobj.tell(), 2)
            print cur_count, 'records.', cur_bytes_read, 'out of', total_size, 'read.',
            print '(', cur_stmt_count, 'statements,', skipped_stmt_count, 'skipped)'

    import pdb;pdb.set_trace()


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
