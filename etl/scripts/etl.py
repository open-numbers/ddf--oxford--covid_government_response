import pandas as pd
import os.path as osp
import re
from inflection import humanize, underscore
from update_source import source_path, sha_path
from ddf_utils.str import to_concept_id
from ddf_utils.io import cleanup, dump_json
from ddf_utils.package import get_datapackage

renames = {
    'CountryCode': 'country',
    'CountryName': 'name',
    'Date': 'day'
}
entity_concepts = ['country']
time_concepts = ['day']
id_concepts = ['concept'] + entity_concepts
numeric_dtypes = ['float64', 'float32', 'int32', 'int64']

script_dir = osp.abspath(osp.dirname(__file__))
output_dir = osp.join(script_dir, '..', '..')
cleanup(output_dir)

def ddf_table(df, key, split_datapoints=True, renames={}, id_concepts=['concept'], out_dir=output_dir):
    # renaming
    df = df.rename(columns=renames)
    df = df.rename(columns=concept_id)
    df = df.apply(lambda col: col.apply(concept_id) if col.name in id_concepts else col)

    # dedepulicating
    df = remove_duplicates(df, key)

    # sorting
    indicators = get_indicators(df, key)
    df = df.sort_values(key)
    df = df[sorted(key) + sorted(indicators)]

    # export
    if collection_type(key) == 'datapoints' and split_datapoints:
        for ind in indicators:
            split_df = df[key + [ind]]
            to_csv(split_df, key, out_dir=out_dir)
    else:
        to_csv(df, key, out_dir=out_dir)

    return df

def concept_id(str):
    return to_concept_id(underscore(str))

def remove_duplicates(df, key):
    dups = df.duplicated(subset=key)
    deduped = df[~dups]
    diff = len(df.index) - len(deduped.index)
    if diff > 0:
        print(f'Dropped {diff} duplicate keys: {df[dups]}')
    return deduped

def to_csv(df, key, out_dir=output_dir):
    file_path = osp.join(out_dir, get_file_name(df, key))
    df.to_csv(file_path, index=False)

def collection_type(key):
    if len(key) > 1:
        return 'datapoints'
    elif key[0] == 'concept':
        return 'concepts'
    else:
        return 'entities'

def get_indicators(df, key):
    return list(filter(lambda col: col not in key, df.columns))

def get_file_name(df, key):
    col_type = collection_type(key)
    name = 'ddf--' + col_type
    if col_type == 'datapoints':
        indicators = get_indicators(df, key)
        name += '--' + '--'.join(indicators) + '--by--' + '--'.join(key)
    elif col_type == 'entities':
        name += '--' + key[0]
    name += '.csv'
    return name


def get_concepts(dfs):
    concepts = set()
    concept_types = {}
    names = {}
    for df in dfs:
        for col in df.columns:
            if col not in concepts:
                concepts.add(col)
                concept_types[col] = get_concept_type(df[col])
                names[col] = humanize(col)

    return pd.DataFrame({
        'concept': list(concepts),
        'concept_type': [concept_types[c] for c in concepts],
        'name': [names[c] for c in concepts]
    })

def get_concept_type(series):
    if series.name in entity_concepts:
        return 'entity_domain'
    if series.name in time_concepts:
        return 'time'
    if series.dtype in numeric_dtypes:
        return 'measure'
    if series.dtype == 'bool':
        return 'boolean'
    return 'string'


if __name__ == '__main__':
    source = osp.join('..', 'source', source_path)

    df = pd.read_csv(source)

    # country entity
    country = df[['CountryCode', 'CountryName']]
    country = ddf_table(country, key=['country'], renames=renames, id_concepts=id_concepts)

    # datapoints
    indicator_cols = filter(lambda col: col not in ['CountryName'], df.columns)
    data = df[indicator_cols]
    data = ddf_table(data, key=['country','day'], renames=renames, id_concepts=id_concepts)

    # concepts
    concepts = get_concepts([country, data])
    ddf_table(concepts, key=['concept'])

    # datapackage
    dp = get_datapackage(output_dir, update=True)
    dp['source'] = {}
    with open(sha_path, 'r') as f:
        dp['source']['sha'] = f.readline()
    dp_path = osp.join(output_dir, 'datapackage.json')
    dump_json(dp_path, dp)
