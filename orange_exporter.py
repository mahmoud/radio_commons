
import sqlite3
from collections import namedtuple
from argparse import ArgumentParser

'''
http://orange.biolab.si/doc/reference/Orange.data.formats/
'''
OrangeType = namedtuple('OrangeType', 'f_type, flag')

CONTINUOUS = OrangeType('c', '')
DISCRETE = OrangeType('d', '')
IGNORE = OrangeType('s', 'ignore')
CLASS = OrangeType('d', 'class')
META_STR = OrangeType('s', 'meta')
META_DISCRETE = OrangeType('d', 'meta')


_FIELD_NAMES = ('img_name, img_size, img_width, img_height, img_metadata,'
                ' img_bits, img_media_type, img_major_mime, img_minor_mime,'
                ' img_description, img_user, img_user_text, img_timestamp,'
                ' img_sha1').split(', ')
METADATA_ONLY_FIELDS = ['length', 'channels', 'nom_bitrate',
                        'vendor', 'stream_count']

_FTP = [('img_name', META_STR),
        ('img_size', CONTINUOUS),
        ('img_width', CONTINUOUS),
        ('img_height', CONTINUOUS),
        ('img_metadata', IGNORE),
        ('img_bits', DISCRETE),
        ('img_media_type', DISCRETE),
        ('img_major_mime', DISCRETE),
        ('img_minor_mime', DISCRETE),
        ('img_description', IGNORE),
        ('img_user', DISCRETE),
        ('img_user_text', DISCRETE),
        ('img_timestamp', CONTINUOUS),
        ('img_sha1', IGNORE),
        ('length', CONTINUOUS),
        ('channels', DISCRETE),
        ('nom_bitrate', CONTINUOUS),
        ('vendor', DISCRETE),
        ('stream_count', DISCRETE)]
_FIELD_TYPE_PAIRS = _FTP
_TARGET_FIELDS = [ftp for ftp in _FTP if ftp[1] is not IGNORE]


def load_rows(filename):
    conn = sqlite3.connect(filename)
    field_names = [name for name, _ in _TARGET_FIELDS]
    query = 'SELECT %s FROM audio_metadata' % ','.join(field_names)
    return conn.execute(query).fetchall()


def main(filename):
    output_name = filename.partition('.')[0] + '.tab'
    rows = load_rows(filename)

    tab_results = []
    tab_results.append([name for name, otype in _TARGET_FIELDS])
    tab_results.append([otype.f_type for name, otype in _TARGET_FIELDS])
    tab_results.append([otype.flag for name, otype in _TARGET_FIELDS])
    tab_results.extend(rows)

    with open(output_name, 'w') as output:
        for row in tab_results:
            out_vals = []
            for v in row:
                if v is None:
                    v = ''
                if type(v) is unicode:
                    out_vals.append(v.encode('unicode-escape'))
                else:
                    out_vals.append(str(v))
            output.write('\t'.join(out_vals))
            output.write('\n')
    return len(tab_results)


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    args = prs.parse_args()
    main(args.filename)
