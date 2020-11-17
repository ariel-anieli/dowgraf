import argparse
import datetime
import functools
import itertools
import json
import logging
import operator
import os
import random
import re
import requests
import string
import sys

logging.basicConfig(stream=sys.stdout, format='%(message)s', level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--user-credentials', required=True)
parser.add_argument('-H', '--host',             required=False)
parser.add_argument('-of',  '--output-folder', default='/tmp/dowgraf')
parser.add_argument('-op',  '--output-prefix', default='DUT')
parser.add_argument('-t', '--time-interval')
parser.add_argument('-tr',  '--time-range')
parser.add_argument('-vr',  '--variables')
group  = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-sd',  '--search-dashboard')
group.add_argument('-sp',  '--search-panels')
group.add_argument('-U',  '--url')

args = parser.parse_args()
base = 'http://{}@{}'.format(args.user_credentials, args.host)
head = {'Content-Type' : 'application/json'}

def fold_if_true_and_apply(args, *funcs, _if=lambda pred: pred):
    return [functools.reduce(
        lambda _in, func: func(_in),
        funcs,
        arg) for arg in args if _if(arg)]

def get_image(db_uid, panel, start, stop, args):

    param = {
        'panelId' : panel['id'],
        'from'    : start if start<stop else stop,
        'to'      : stop  if start<stop else start,
        'width'   : 1000,
        'height'  : 500,
        'tz'      : 'Europe/Paris'
    }

    qry = '/'.join([args['base'], 'render/d-solo', db_uid])
    prm = [
        ('var-' + key,value)
        for key,values in args['vars'].items()
        for value in values
    ]

    [prm.append((key, value)) for key,value in param.items()]

    get_data_in_time_range = lambda start, stop: requests.get(
        qry,
        headers = head,
        params = prm)

    fold_if_true_and_apply(
        [(start, stop)],
        lambda _time : {'rsp' : get_data_in_time_range(_time[0], _time[1])},
        lambda _: _.update(cnt = _['rsp'].content) or _,
        lambda _: _.update(tpe = _['rsp'].headers['Content-Type'].split('/')[-1]) or _,
        lambda _: _.update(nme = re.sub('.DUT', args['prfx'], panel['title'])) or _,
        lambda _: _.update(fle = open(args['fold'] + '/' + _['nme'] + '.' + _['tpe'], 'wb')) or _,
        lambda _: _['fle'].write(_['cnt']) and _['fle'].close(),
    )

if __name__ =="__main__":

    if args.search_dashboard:

        srch_db = lambda qry: requests.get(
            base + '/api/search',
            headers = head,
            params = {'query': qry})

        fold_if_true_and_apply(
            [args.search_dashboard],
            srch_db,
            lambda rsp: json.loads(rsp.text),
            json.dumps,
            logging.info)
        
    elif args.url:
        ARGS = {
            'fold' : args.output_folder,
            'prfx' : args.output_prefix
        }

        url_with_creds_and_db_uid = functools.reduce(
            lambda _str, ptr: re.sub(ptr[0], ptr[1], _str),
            [
                ('(?<=//)', args.user_credentials + '@'),
                ('\?.*$', ''),
                ('/[^/]+$', ''),
                ('(?<=/)d(?=/)', 'api/dashboards/uid'),
            ],
            args.url
        )

        base_name_with_creds = re.sub('/api.*', '', url_with_creds_and_db_uid)

        vars = [':'.join(elem)
                for elem in [var.split('=')
                for var in re.findall('(?<=&|\?)[^&]+(?=&|$)',
                                      re.sub('var-', '', args.url))
                ]]

        ARGS.update(vars=[val for val in vars
                          if val.split(':')[0] not in ['from',
                                                       'to',
                                                       'orgId']])
        ARGS.update(base=base_name_with_creds)
        
        db_uid = re.search('(?<=/d/)[^/]+(?=/)', args.url) \
            and re.search('(?<=/d/)[^/]+(?=/)', args.url).group(0)

        VARS   = {
            elem[0] : elem[1]
            for elem in [item.split(':')
            for item in vars]
        }

        ARGS.update(
            vars={
                k : [elem.split(':')[1] for elem in list(v)]
                for k,v in itertools.groupby(
                        ARGS['vars'],
                        key=lambda elem: elem.split(':')[0]
                )
            }
        )

        fold_if_true_and_apply(
            [url_with_creds_and_db_uid],
            lambda url:  requests.get(url, headers=head),
            lambda rsp:  json.loads(rsp.text),
            lambda obj:  obj['dashboard']['panels'],
            lambda seq:  [{'id':pnl['id'], 'title':pnl['title']}
                          for pnl in seq],
            lambda pnls: os.mkdir(ARGS['fold']) or pnls,
            lambda pnls: [get_image(db_uid, panel, VARS['from'], VARS['to'], ARGS)
                          for panel in pnls]
        )

    elif args.search_panels and (args.time_interval or args.time_range):

        TIME = args.time_interval if args.time_interval \
            else args.time_range if args.time_range \
            else None

        TYPE = 'itvl' if args.time_interval \
            else 'range' if args.time_range \
            else None

        ARGS = {
            'type' : TYPE,
            'fold' : args.output_folder,
            'vars' : args.variables.split(','),
            'prfx' : args.output_prefix,
            'base' : base
        }

        def get_time_timespan_and_tell_if_add_or_sub(_time):
            if re.match('^\d.*P', _time):
                _date, span    = _time.split('/')
            elif not re.search('P', _time):
                _date = _time
                span = 'P1H'
            elif re.search('^P', _time):
                _date = ''
                span = _time

            return {'date' : _date,
                    'span' : span,
                    'op'   : operator.add if _date else operator.sub}

        def bld_time_itvl(_time):

            def scan_time(init, pattern):
                match = re.search(pattern[0], init['sch']).group(0) \
                    if re.search(pattern[0], init['sch']) else 0
                init['sch'] = re.sub(pattern[1], '', init['sch'])
                init['fnd'].append(int(match))

                return init

            return {
                key : value
                for key,value in zip(
                        ['year',
                         'month',
                         'day',
                         'hour',
                         'minute',
                         'second']
                        , functools.reduce(
                            scan_time,
                            [('(?<=P)\d+(?=Y)',  '(?<=P)\d+Y'),
                             ('(?<=P)\d+(?=M)',  '(?<=P)\d+M'),
                             ('(?<=P)\d+(?=D)',  '(?<=P)\d+D'),
                             ('(?<=PT)\d+(?=H)', '(?<=PT)\d+H'),
                             ('(?<=PT)\d+(?=M)', '(?<=PT)\d+M'),
                             ('(?<=PT)\d+(?=S)', '(?<=PT)\d+S')],
                            {'fnd' : [],
                             'sch' : _time}
                        )['fnd']
                )}

        shift_time = lambda _date, shift, add_or_sub: _date.replace(
            year   = add_or_sub(_date.year,   shift['year']),
            month  = add_or_sub(_date.month,  shift['month']),
            day    = add_or_sub(_date.day,    shift['day']),
            hour   = add_or_sub(_date.hour,   shift['hour']),
            minute = add_or_sub(_date.minute, shift['minute']),
            second = add_or_sub(_date.second, shift['second'])
        ).timestamp()

        get_time = lambda _date: datetime.datetime.utcnow() \
            if not _date else datetime.datetime.fromisoformat(_date)

        srch_pnl = lambda db_uid: requests.get(
            '/'.join([base, 'api/dashboards/uid', db_uid]),
            headers = head)

        def get_each_time_range(db_uid, panels, _time, args):

            if args['type']=='itvl':
                return fold_if_true_and_apply(
                    [_time],
                    get_time_timespan_and_tell_if_add_or_sub,
                    lambda _: _.update(span  = bld_time_itvl(_['span']))                     or _,
                    lambda _: _.update(date  = get_time(_['date']))                          or _,
                    lambda _: _.update(shift = shift_time(_['date'], _['span'], _['op']))    or _,
                    lambda _: _.update(start = _['date'].timestamp())                        or _,
                    lambda _: _.update(start = _['start'], shift = _['shift']) or _,
                    lambda _: [get_image(db_uid, panel, _['start'], _['shift'], args) for panel in panels]
                )

            elif args['type']=='range':
                _range = _time.split(':')
                return [get_image(db_uid, panel, _range[0], _range[1], args) for panel in panels]

        fold_if_true_and_apply(
            [args.search_panels],
            srch_pnl,
            lambda rsp: json.loads(rsp.text),
            lambda obj: obj['dashboard']['panels'],
            lambda seq: [{'id':pnl['id'], 'title':pnl['title']} for pnl in seq],
            lambda pnl: os.mkdir(ARGS['fold']) or pnl,
            lambda pnl: [get_each_time_range(args.search_panels, pnl, _time, ARGS) for _time in TIME.split(',')]
        )