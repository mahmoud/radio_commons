# -*- coding: utf-8 -*-

import os
import time
import gzip
import sqlite3
from argparse import ArgumentParser
import codecs

import phpserialize

_FIELD_NAMES = ('img_name, img_size, img_width, img_height, img_metadata,'
                ' img_bits, img_media_type, img_major_mime, img_minor_mime,'
                ' img_description, img_user, img_user_text, img_timestamp,'
                ' img_sha1').split(', ')
IMAGE_TABLE_SCHEMA = ('CREATE TABLE image (%s);' % ', '.join(_FIELD_NAMES))
METADATA_ONLY_FIELDS = ['length', 'channels', 'nom_bitrate', 'vendor', 'stream_count']
METADATA_TABLE_SCHEMA = 'CREATE TABLE audio_metadata (%s);' % ', '.join(_FIELD_NAMES + METADATA_ONLY_FIELDS)


def main(filename):
    ogg_db = sqlite3.connect(filename)
    ogg_cursor = ogg_db.execute('SELECT * FROM image where img_minor_mime="ogg"')
    metadata_db = sqlite3.connect(filename.partition('.')[0] + '_metadata.db')
    metadata_db.execute(METADATA_TABLE_SCHEMA)

    _broken_metadata = {}
    _multistream_map = {}
    _insert_query = 'INSERT INTO audio_metadata VALUES (%s)' % ', '.join('?' * len(_FIELD_NAMES + METADATA_ONLY_FIELDS))
    i = 0
    start_time = time.time()
    for record in ogg_cursor:
        name = record[0]
        metadata_string = record[4]
        try:
            metadata = phpserialize.loads(metadata_string.replace(u'\\"', u'\"'))
        except:
            try:
                metadata = phpserialize.loads(metadata_string.encode('utf-8').decode('unicode-escape'))
            except:
                _broken_metadata[name] = metadata_string
                continue
        length = metadata.get('length')
        streams = metadata.get('streams', {})
        stream_count = len(streams)
        if not stream_count:
            continue
        elif stream_count > 1:
            _multistream_map[name] = metadata

        stream = streams.values()[0]
        header = stream.get('header', {})
        channels = header.get('audio_channels', 0)
        nominal_bitrate = header.get('bitrate_nominal')
        vendor = stream.get('vendor')

        metadata_db.execute(_insert_query, record + (length, channels, nominal_bitrate, vendor, stream_count))
        i += 1
        if i % 100 == 0:
            print i, 'records complete in', time.time() - start_time, 'seconds.'
    import pdb;pdb.set_trace()
    return




if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    args = prs.parse_args()
    main(args.filename)
