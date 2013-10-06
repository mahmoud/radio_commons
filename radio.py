# -*- coding: utf-8 -*-

from gzip import GzipFile
from argparse import ArgumentParser
import codecs

def main(filename):
    with GzipFile(filename) as gf:
        data = gf.read(1024 * 1024 * 64)
    import pdb;pdb.set_trace()


def db_main(filename):
    import sqlite3
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute('CREATE TABLE image (img_name, img_size, img_width, img_height, img_metadata, img_bits, img_media_type, img_major_mime, img_minor_mime, img_description, img_user, img_user_text, img_timestamp, img_sha1);')
    
    buff = u''
    reader = codecs.getreader('utf-8')
    ii_start, ii_end = 0, None
    with GzipFile(filename, 'r') as gf_encoded:
        gf = reader(gf_encoded, errors='replace')
        data = gf.read(4096)
        buff = data[data.index('INSERT INTO'):]
        data = gf.read(1024 * 1024 * 2)
        buff += data

        ii_end = buff.find('INSERT INTO', 11)
        if ii_end > 0:
            full_statement, buff = buff[ii_start:ii_end], buff[ii_end:]
            full_statement = full_statement

        replaced = full_statement.replace("\\'", "''")
        splitted = replaced.split('),')
        joined = u'),'.join(splitted[:200]) + ')'
    full_statement = joined
    cur.execute(full_statement)
    conn.commit()
    cur.execute('SELECT * FROM image')
    res = cur.fetchall()
    import pdb;pdb.set_trace()


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    args = prs.parse_args()
    db_main(args.filename)
