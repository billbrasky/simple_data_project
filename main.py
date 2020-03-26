import yaml, csv
from pg_extension import *
import util
import cleandata

# Attempts to load WIP color library
try:
    from color.color import *
except ImportError as e:
    print( ' '.join( e.args ))
    print( 'No Colors!' )

# Hacky way of logging... use logging?
def log( listoftuples ):
    for tple in listoftuples:
        text, affects = tple
        if 'color' not in sys.modules:
            print( text )

        else:
            text = colorstr( text )
            for affect in affects:
                text = getattr( text, affect )
            print( text )



# returns a cursor object
def getcursor( database = None, user = None, setisolation = False ):

    o = util.getLoginData()

    if setisolation:
        o['database'] = 'postgres'

    conn = pg.connect( "dbname = '{database}' user = '{user}' password = '{password}'".format( **o ))
    
    if setisolation:
        conn.set_isolation_level( ISOLATION_LEVEL_AUTOCOMMIT )

    cur = conn.cursor( cursor_factory = db )

    return cur



# Processes data plan to create the desired database architecure.
def processDataPlan( dataplan, database = None, schema = None ):

    login_data = util.getLoginData()
    database = login_data['database']
    schema = login_data['schema']

#    definitions = dataplan['definitions']
    tables = dataplan['tables']

    query = """DROP SCHEMA IF EXISTS {0};
CREATE SCHEMA {0};

""".format( schema )


    foreignkeys = ''
    alter = """
ALTER TABLE {schema}.{table} 
    ADD FOREIGN KEY ("{key}")
    REFERENCES {schema}.{ftable} ("{fkey}");
"""
    insertion = {'insert': '', 'values': ''}
    datatypes = {}
    for tablename, table in tables.items(): # < 3.6 order is not preserved
        insert = f'INSERT INTO {schema}.{tablename}\n\t(\n\t\t{{domain}}\n\t)\n\tVALUES'
        values = '(\n\t\t{{values}}\n\t);\n\n'

        td = []
        temp = { 'domain': [], 'values': [] }
        for columnname, column in table.items():

            columntype = column['type']
            columnlength = column.get( 'length' )
            isnotnull = column.get( 'notNull', False )

            sqltype = columntype.upper()

            if columnlength is not None:
                sqltype += '({})'.format( columnlength )

            if isnotnull:
                sqltype += ' NOT NULL'

            text = '    {0} {1}'.format( columnname, sqltype )

            isprimary = column.get( 'pk', False )
            isforeign = column.get( 'fk', False )

            if isforeign:
                o = {
                    'schema': schema,
                    'table': tablename,
                    'ftable': '_'.join( columnname.split( '_' )[:-1] ),
                    'key': columnname,
                    'fkey': columnname.split( '_' )[-1]
                }

                foreignkeys += alter.format( **o )
    
            elif isprimary:
                text += ' PRIMARY KEY'

            else:
                temp['domain'].append( columnname )
                temp['values'].append( '{{{}}}'.format( columnname )) #column['origin']
                datatypes[columnname] = column['type'] #column['origin']

            td.append( text )

        query += util.writeTable( tablename, td ) + '\n\n'

        temp = {x: ',\n\t\t'.join( y ) for x, y in temp.items() }
        insertion['insert'] = insert.format( domain = temp['domain'] )
        insertion['values'] = values.format( values = temp['values'] )
    query += foreignkeys

    return query, insertion, datatypes

def setup( 
        query = None,
        database = None, 
        schema = None, 
        user = None ):
    
    logindata = util.getLoginData()
    
    database = logindata['db']
    user = logindata['u']

    cur = getcursor( database, user, True )

    try:
        cur.execute( 'DROP DATABASE IF EXISTS {0};'.format( database ))
        cur.execute( 'CREATE DATABASE {0};'.format( database ))

    except Exception as e:
        e = ' '.join( e.args )
        s = 'Couldn\'t drop and create database!'
        logtext = [(s, ['bold', 'yellow']), (e, ['bold', 'red'])]
        log( logtext )
        cur.closeall()
        return


dataplan = util.getyaml( 'dataplan' )
data = cleandata.cleanData()

query, insert_data, data_types = processDataPlan( dataplan )

cur = getcursor()
print( insert_data )
#cur.insertData( data_types, insert_data, data, True, False )