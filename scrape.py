#!/usr/bin/env python
import requests
import pandas as pd
from datetime import date, datetime
from warnings import warn
import re

base_data_url = 'http://static.politico.com/mapdata/2016/'

raw_races = (
    requests.get(
        'http://static.politico.com/mapdata/2016/config_primaries.json'
      )
    .json()['races']
)


df = pd.DataFrame(raw_races).T
df.date = pd.to_datetime(df.date, format='%Y%m%d')

races = (
    df.loc[(df.date <= date.today()) & (df.officeID == 'P'),
           ['stateAbb', 'date']]
    .values
)


def format_state_date(state, date):
    '''Take a state abbreviation and date and return them in string formatted \
    for get_results().
    state (string): a two-letter state abbreviation.
    date (datetime.date-like): a datetime.date.strftime()-mungeable date.'''

    return '%s_%s' % (state.upper(), date.strftime('%Y%m%d'))


def get_results(state_date, base_data_url=base_data_url):
    '''Fetch raw results data for date and state.
    state_date (string): state & date separated by underscore \
    (e.g., "IA_20160201")
    base_data_url (string): "endpoint" to find raw results xml at. \
    defaults to "http://static.politico.com/mapdata/2016/"'''

    url = requests.compat.urljoin(base_data_url, '%s.xml' % state_date)
    response = requests.get(url)

    try:
        return response.text
    except Exception as e:
        warn('Error encountered: %s' % e.message)
        return None


def parse_line(line, candidate_names=None):
    '''Parse line in raw results.
    line (string): the line to parse.'''

    meta, data = line.split('||')

    if candidate_names:
        pattern = re.compile('|'.join(candidate_names.keys()))
        data = pattern.sub(lambda x: candidate_names[x.group()], data)

    df = pd.DataFrame(
        [pd.Series(meta.split(';') + d.split(';')) for d in data.split('|')]
    )
    return df


def parse_results(results):
    '''Parse raw results.
    results (string): raw results.'''

    meta, data = results.split('\n\n')
    lines = [l for l in data.split('\n') if l != '']

    candidates = {
        c.split(';')[0]: ', '.join(re.sub(r';;|;$', '', c).split(';')[1:])
        for c in meta.split('\n')[1].split('|')
    }

    results = pd.concat([parse_line(l, candidates) for l in lines],
                        ignore_index=True)
    return results


def process_race(state, date):
    '''Take a state abbreviation and a date, fetch and process raw results.
    state (string): a two-letter state abbreviation.
    date (datetime.date-like): a datetime.date.strftime()-mungeable date.'''

    state_date = format_state_date(state, date)
    raw_results = get_results(state_date)
    parsed_data = parse_results(raw_results)

    return parsed_data


results = pd.concat([process_race(s, d) for s, d in races],
                    ignore_index=True)

results.columns = [
    'state', 'race_type', 'elec_type', 'fips', 'place_name', '_',
    'percent_reporting', '_', '_', 'race_id', '_', '_',
    'candidate', 'party', 'votes', 'vote_share', 'winner', '_',
    '_', 'delegates_state', '_'
]

results.drop('_', axis=1, inplace=True)
results.to_csv('results_%s.csv' % datetime.now().isoformat(),
               index=False, encoding='utf8')
