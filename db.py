import argparse
import json
import os
from tinydb import TinyDB, Query


def get_entries(db, entry_id):
    q = Query()
    return db.search(q.id == entry_id)


def entry_exists(db, entry_id):
    q = Query()
    return len(db.search(q.id == entry_id)) > 0


def truncate(db):
    db.truncate()
    print('DB truncated')


def remove_entry(db, entry_id):

    if not entry_exists(db, entry_id):
        print(f'no data found for ID {entry_id}')
        return

    q = Query()
    db.remove(q.id == entry_id)


def get_entry_ids(db):

    res = []
    for e in iter(db):
        res.append(e['id'])
    res.sort()
    return res


def list_ids(db):

    ids = get_entry_ids(db)
    if not ids:
        print(f'the DB is empty')
        return

    print(f'{len(ids)} IDs found:')
    for i in ids:
        print(f'  {i}')


def load_samples(db, samples_dir, truncate_db, keep_existing):

    print(f'loading samples from {samples_dir}, truncate DB: {truncate_db}, keep existing: {keep_existing}')
    if truncate_db:
        truncate(db)

    files = os.listdir(samples_dir)
    for f in files:
        if '.json' in f:
            load_sample(db, f'{samples_dir}\\{f}', f.replace('.json', ''), keep_existing)
        else:
            print(f'warning: {f} has no json extension, skipping')


def load_sample(db, json_file, entry_id, keep_existing):

    with open(json_file, 'r') as f:
        data = json.load(f)
        data['id'] = entry_id

        if keep_existing:
            if entry_exists(db, entry_id):
                print(f'warning: {entry_id} already exists, will not add')
            else:
                db.insert(data)
                print(f'{entry_id} inserted')
        else:
            if entry_exists(db, entry_id):
                print(f'warning: overwriting {entry_id}')
                remove_entry(db, entry_id)
            db.insert(data)
            print(f'{entry_id} inserted')


def save_sample(db, samples_dir, i, json_indent):

    data = get_entries(db, i)

    if not data:
        print(f'no data found for ID {i}')
        return

    if len(data) > 1:
        print(f'error: invalid number of entries for ID {i}')

    with open(f'{samples_dir}\\{i}.json', 'w') as f:
        if json_indent > 0:
            json.dump(data[0], f, indent=json_indent)
        else:
            json.dump(data[0], f)


def save_samples(db, samples_dir, json_indent):

    print(f'saving samples to {samples_dir}')

    if os.path.exists(samples_dir):
        print(f'warning: directory {samples_dir} already exists')
    else:
        os.makedirs(samples_dir)

    ids = get_entry_ids(db)
    for i in ids:
        save_sample(db, samples_dir, i, json_indent)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--DB', required=True, help='db file')
    parser.add_argument('--samplesDir', help='samples directory')
    parser.add_argument('--loadData', default=False, action='store_true',
                        help='load samples from samplesDir to DB')
    parser.add_argument('--extractData', default=False, action='store_true',
                        help='extract samples from DB to samplesDir')
    parser.add_argument('--jsonIndent', type=int, default=0, help='json indentation')
    parser.add_argument('--truncateDB', default=False, action='store_true', help='truncate DB before appending')
    parser.add_argument('--keepExisting', default=False, action='store_true',
                        help='keep entries with existing IDs')
    parser.add_argument('--listIDs', action='store_true', help='list IDs')
    parser.add_argument('--deleteData', default=False, action='store_true',
                        help='drop samples from DB')
    parser.add_argument('--byID', help='get and save entry by ID')

    args = parser.parse_args()

    db_file = args.DB
    db = TinyDB(db_file)

    samples_dir = args.samplesDir

    if args.loadData:
        load_samples(db, samples_dir, args.truncateDB, args.keepExisting)

    if args.extractData:
        if args.byID:
            save_sample(db, samples_dir, args.byID, args.jsonIndent)
        else:
            save_samples(db, samples_dir, args.jsonIndent)

    if args.deleteData:
        if args.byID:
            remove_entry(db, args.byID)
        else:
            truncate(db)

    if args.listIDs:
        list_ids(db)


# usage examples:

# load samples to DB
# python3.9 db.py --loadData --DB ./db.json --samplesDir ./samples

# load samples to DB, keep (do not overwrite) DB data if the corresponding ID already exists
# python3.9 db.py --loadData --DB ./db.json --samplesDir ./samples --keepExisting

# load samples to DB, truncate DB before adding new data
#  python3.9 db.py --loadData --DB ./db.json --samplesDir ./samples --truncateDB

# extract samples from DB
#  python3.9 db.py --extractData --DB ./db.json --samplesDir ./samples2

# list existing IDs
# python3.9 db.py --listIDs --DB ./db.json

# extract data from DB by ID
# python3.9 db.py --extractData --DB ./db.json --byID sample02 --samplesDir ./samples2
# or (use indent when dumping json):
# python3.9 db.py --extractData --DB ./db.json --byID sample02 --samplesDir ./samples2 --jsonIndent 4

# remove data from DB by ID
# python3.9 db.py --deleteData --DB ./db.json --byID sample02

# remove all data from DB
# python3.9 db.py --deleteData --DB ./db.json


if __name__ == '__main__':
    main()
