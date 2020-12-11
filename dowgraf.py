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

def mapping(tr):
    return lambda red: lambda acc,res: red(acc,tr(res))

def filtering(prd):
    return lambda red: lambda acc,res: red(acc,res) if prd(res) else acc

def comp(*funcs):
    head, *tail = reversed(funcs)
    return lambda *args,**kwargs: functools.reduce(lambda res, fn: fn(res),
                                                   tail,
                                                   head(*args,**kwargs))

def append_to_acc(acc,res):
    acc.append(res)
    return acc

def get_image(panel, arg):

    base_parameters = {
        'panelId' : panel['id'],
        'width'   : 1000,
        'height'  : 500,
        'tz'      : 'Europe/Paris'
    }

    parameters = [parameter for parameter in itertools.chain(
        arg['parameters'],
        base_parameters.items())
    ]

    qry = '/'.join([arg['base'], 'render/d-solo', arg['uid']])

    get_data_in_time_range = lambda url_params: requests.get(
        qry,
        headers = head,
        params  = url_params
    )

    fold_if_true_and_apply(
        [parameters],
        lambda params : {'rsp' : get_data_in_time_range(params)},
        lambda _: _.update(cnt = _['rsp'].content) or _,
        lambda _: _.update(tpe = _['rsp'].headers['Content-Type'].split('/')[-1]) or _,
        lambda _: _.update(nme = re.sub('.DUT', arg['prfx'], panel['title'])) or _,
        lambda _: _.update(fle = open(arg['fold'] + '/' + _['nme'] + '.' + _['tpe'], 'wb')) or _,
        lambda _: _['fle'].write(_['cnt']) and _['fle'].close(),
    )

def find_ids_and_titles(found, panel):
    if panel['type']=='row' and panel['panels']:
        [found.append({'id'    : entry['id'],
                       'title' : entry['title'],
        }) for entry in panel['panels']]
    elif panel['type']!='row':
        found.append({'id'    : panel['id'],
                      'title' : panel['title'],
        })

    return found

if __name__ =="__main__":

    if args.search_dashboard:

        @mapping
        def search_into_db_with_keyword(qry):
            return {
                'qry' : qry,
                'rsp' : requests.get(
                    base + '/api/search',
                    headers = head,
                    params = {'query': qry})
            }

        @mapping
        def extract_db_from_rsp(rsp):
            return {
                'qry' : rsp['qry'],
                'db'  : json.loads(rsp['rsp'].text)
            }

        dashboards = functools.reduce(
            comp(
                search_into_db_with_keyword,
                filtering(lambda rsp: rsp['rsp'].ok),
                extract_db_from_rsp
            )(append_to_acc),
            args.search_dashboard.split(','),
            []
        )

        logging.info(json.dumps(dashboards))

    elif args.url:
        arguments = {
            'fold' : args.output_folder,
            'prfx' : args.output_prefix
        }

        url_with_creds_and_db_uid = functools.reduce(
            lambda string, pattern: re.sub(pattern[0], pattern[1], string),
            [('(?<=//)'      , args.user_credentials + '@'),
             ('\?.*$'        , ''),
             ('/[^/]+$'      , ''),
             ('(?<=/)d(?=/)' , 'api/dashboards/uid')],
            args.url
        )

        arguments.update(
            base       = re.sub('/api.*', '', url_with_creds_and_db_uid),
            parameters = re.findall('(?<=&|\?)([^=]+)=([^&]+)(?=&|$)', args.url),
            uid        = re.findall('(?<=/d/)[^/]+(?=/)', args.url).pop()
        )

        fold_if_true_and_apply(
            [url_with_creds_and_db_uid],
            lambda url    : requests.get(url, headers=head),
            lambda rsp    : json.loads(rsp.text),
            lambda obj    : obj['dashboard']['panels'],
            lambda panels : functools.reduce(find_ids_and_titles, panels,[]),
            lambda panels : os.mkdir(arguments['fold']) or panels,
            lambda panels : [get_image(panel, arguments) for panel in panels]
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
