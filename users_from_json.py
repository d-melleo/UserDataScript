import json
import re
import pandas as pd

PATH_TO_JSON = "C:/Users/dmelleo/Desktop/users.json"
PATH_TO_EXPORT = "C:/Users/dmelleo/Desktop/users.csv"

# FIND_USERS_WITH_SERVICE = "Call Center - Premium"
FIND_USERS_WITH_SERVICE = None


# Sort out inactive users
GET_ACTIVE_USERS_ONLY = True
# Toggle columns
COLUMN_ID = True
COLUMN_UUID = False
COLUMN_NAME = True
COLUMN_STATUS = False
COLUMN_USERNAME = False
COLUMN_EMAIL = False
COLUMN_EXTENSION = False
COLUMN_NUMBER = False
COLUMN_ROLE = False
COLUMN_TEAMS = False
COLUMN_CP_SERVICES = False
COLUMN_PLATFORM_SERVICES = False


# Legal colun names
OUTPUT_COLUMN_NAMES = {
    'id': 'ID',
    'uuid': 'UUID',
    'name': 'Name',
    'status': 'Status',
    'username': 'Username',
    'email': 'Email',
    'extension': 'Extension',
    'number': 'Number',
    'role': 'Role',
    'teams': 'Teams',
    'services': 'Services',
    'features': 'Features'
}


# Read info from json file
def load_json(path: str) -> dict:
    try:
        with open(path, "r") as file:
            data: dict = json.load(file)
            return data
    except FileNotFoundError as e:
        print(f"\nPATH TO A FILE NOT FOUND: {path}\n")
        raise e


def remove_inactive_users(df: pd.DataFrame) -> pd.DataFrame:
    return df[df['active']]

def get_id(df: pd.DataFrame) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['id']] = df['profiles'].apply(lambda x: ''.join([str(y['id']) for y in x]))
    return df

def get_uuid(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={'nextiva_uuid': OUTPUT_COLUMN_NAMES['uuid']})

def get_name(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df[OUTPUT_COLUMN_NAMES['name']] = pd.concat([df['first_name'], df['last_name']], axis=1).apply(lambda x: ' '.join(x), axis=1)
    except KeyError:
        df[OUTPUT_COLUMN_NAMES['name']] = pd.concat([df['firstName'], df['lastName']], axis=1).apply(lambda x: ' '.join(x), axis=1)
    return df

def get_status(df: pd.DataFrame) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['status']] = df['status'].apply(lambda x: 'Inactive' if x == '' else x)
    return df

def get_username(df: pd.DataFrame) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['username']] = df['profiles'].apply(lambda x: ''.join([y['nextOS']['primary_identifier'] for y in x]))
    return df

def get_email(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={'email': OUTPUT_COLUMN_NAMES['email']})

def get_extension(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df[OUTPUT_COLUMN_NAMES['extension']] = df['profiles'].apply(lambda x: ''.join([y['extension'] for y in x]))
    except KeyError:
        df[OUTPUT_COLUMN_NAMES['extension']] = df['profiles'].apply(lambda x: ''.join([y['voiceExtension'] for y in x]))
    return df

def get_number(df: pd.DataFrame) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['number']] = df['profiles'].apply(lambda x: ''.join([y['phone_number'] for y in x]))
    df[OUTPUT_COLUMN_NAMES['number']] = df[OUTPUT_COLUMN_NAMES['number']].apply(lambda x: re.sub(r"\+1-", '', x))
    return df

def get_role(df: pd.DataFrame) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['role']] = df['roles'].apply(lambda x: ''.join([y['name'] for y in x]))
    return df

def get_teams(df: pd.DataFrame) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['teams']] = df['team'].apply(lambda x: [y['name'] for y in x])
    df[OUTPUT_COLUMN_NAMES['teams']] = df[OUTPUT_COLUMN_NAMES['teams']].apply(lambda x: ', '.join(x))
    return df

def get_services(df: pd.DataFrame, service_name: str=None) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['services']] = df['profiles'].apply(lambda x: [z['features'] for y in x for z in y['productFeatures'] if z['product'] == 'Voice'])
    
    if service_name:
        df[OUTPUT_COLUMN_NAMES['services']] = df[OUTPUT_COLUMN_NAMES['services']].apply(lambda x: ', '.join([', '.join(y) for y in x if service_name in y]))
        df = df[df[OUTPUT_COLUMN_NAMES['services']].apply(lambda x: len(x) > 0)]
    else:
        df[OUTPUT_COLUMN_NAMES['services']] = df[OUTPUT_COLUMN_NAMES['services']].apply(lambda x: ', '.join([', '.join(y) for y in x]))
    return df

def get_features(df: pd.DataFrame) -> pd.DataFrame:
    df[OUTPUT_COLUMN_NAMES['features']] = df['profiles'].apply(lambda x: [z['features'] for y in x for z in y['productFeatures'] if z['product'] != 'Voice'])
    df[OUTPUT_COLUMN_NAMES['features']] = df[OUTPUT_COLUMN_NAMES['features']].apply(lambda x: ', '.join([', '.join(y) for y in x]))
    return df

def remove_redundant_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = [x for x in OUTPUT_COLUMN_NAMES.values() if x in df.columns]
    return df.reindex(columns=columns)

def export_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)


# Main logic
if __name__ == "__main__":
    # Load json from file in dictionary
    data: dict = load_json(PATH_TO_JSON)
    # Read data frame
    df = pd.DataFrame(data['result'])
    
    
    # Sout out inactive users
    if GET_ACTIVE_USERS_ONLY: df = remove_inactive_users(df)
    # Get requested columns
    if COLUMN_ID: df = get_id(df)
    if COLUMN_UUID: df = get_uuid(df)
    if COLUMN_NAME: df = get_name(df)
    if COLUMN_STATUS: df = get_status(df)
    if COLUMN_USERNAME: df = get_username(df)
    if COLUMN_EMAIL: df = get_email(df)
    if COLUMN_EXTENSION: df = get_extension(df)
    if COLUMN_NUMBER: df = get_number(df)
    if COLUMN_ROLE: df = get_role(df)
    if COLUMN_TEAMS: df = get_teams(df)
    if COLUMN_CP_SERVICES: df = get_services(df, FIND_USERS_WITH_SERVICE)
    if COLUMN_PLATFORM_SERVICES: df = get_features(df)
    
    
    # Remove redundant columns
    df = remove_redundant_columns(df)
    # Save as .csv file
    export_csv(df, PATH_TO_EXPORT)
