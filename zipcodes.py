import pandas as pd
import json
from tqdm import tqdm

output_file = 'zip_code_data.csv'
output_df   = pd.DataFrame(columns=['zip code', 'county name', 'state', 'branch'])

# load data
zip_data = pd.read_csv('zip_code_database.csv')
county_data = json.load(open('county_data.json'))
branch_data = json.load(open('branches.json'))
state_fips_data = json.load(open('fips.json'))

def get_county_name(county_obj):
    county_id   = county_obj['geoid']
    state_fips  = county_id[:2]
    county_fips = county_id[2:]
    state_file  = 'states/' + state_fips + '.geojson'

    state_feats = json.load(open(state_file))['features']
    county_name = None
    for feature in state_feats:
        if feature['properties']['COUNTYFP'] == county_fips:
            county_name = feature['properties']['Name'] + ' County'
            break

    state_abbr = None
    state_name = None
    for state in state_fips_data:
        if state['fips'] == state_fips:
            state_abbr = state['abbr']
            state_name = state['name']
            break
        
    return [county_name, state_abbr, state_name]

for county in tqdm(county_data, desc='Processing Counties', unit='county'):
    new_zip_obj = {
        'zip code': None,
        'county name': None,
        'state': None,
        'branch': None
    }

    county_data = get_county_name(county)
    new_zip_obj['county name'] = county_data[0]
    new_zip_obj['state'] = county_data[2]
    state_abbr = county_data[1]

    branch_id = county['branch']
    for branch in branch_data:
        if int(branch['value']) == int(branch_id):
            new_zip_obj['branch'] = branch['label']
            break
    
    county_zips = zip_data[(zip_data['county'] == new_zip_obj['county name']) & (zip_data['state'] == state_abbr)]
    for index, row in county_zips.iterrows():
        new_zip_obj['zip code'] = row['zip']
        output_df = output_df._append(new_zip_obj, ignore_index=True)
    
output_df.to_csv(output_file, index=False)