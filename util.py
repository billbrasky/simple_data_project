import yaml


# Reads a YAML file
def getyaml( s ):
    with open( s + '.yml', 'r' ) as f:
        data = yaml.load( f )
    return data

def getLoginData():
    logindata = getyaml( 'local-password' )

    return logindata


# Writes the SQL code for an individual create table statement
def writeTable( tn, td, schema = None ):

    schema = getLoginData()['schema']

    res = 'CREATE TABLE {0}.{1} (\n{2}\n);'.format( schema, tn, ',\n'.join( td ))

    return res
