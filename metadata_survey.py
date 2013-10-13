# -*- coding: utf-8 -*-

import json
import time
import sqlite3
from argparse import ArgumentParser

import phpserialize

# pop metadata['streams'] and have a separate survey of those values


def collect_keys(d):
    if not isinstance(d, dict):
        return []
    level = d.keys()
    for k, v in d.items():
        level.extend('%s.%s' % (k, c) for c in collect_keys(v))
    return level


def analyze(paths):
    survey = {}
    for p in paths:
        survey[p] = survey.setdefault(p, 0) + 1

    return survey


def survey_dicts(lod):
    return analyze([path
                    for d in lod
                    for path in collect_keys(d)])


def survey_metadata(filename):
    ogg_db = sqlite3.connect(filename)
    ogg_cursor = ogg_db.execute('SELECT * FROM image where img_minor_mime="ogg"')
    _broken_metadata = {}
    start_time = time.time()
    metadata_dicts = []
    streams_dicts = []
    i = 0
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
        streams = metadata.pop('streams', {})
        streams_dicts.extend(streams.values())
        metadata_dicts.append(metadata)
        i += 1
        if i % 100 == 0:
            cur_dur = time.time() - start_time
            print i, 'records complete in', round(cur_dur, 2), 'seconds.',
            print len(_broken_metadata), 'records skipped.'

    return {'general': survey_dicts(metadata_dicts),
            'streams': survey_dicts(streams_dicts)}


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    args = prs.parse_args()

    res = survey_metadata(args.filename)
    print json.dumps(res, indent=2, sort_keys=True)
