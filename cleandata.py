
import csv


def isListOfEmpties( l: list ) -> bool:


    if set( l ) == set( [''] ):
        return True

    return False

def getData() -> list:

    with open( 'raw.csv', newline = '' ) as f:
        raw = list( csv.reader( f, delimiter = ',', quotechar = '"' ))

    return raw

def organizeLabels( s: str ) -> list:
    s = s.replace( '#', '' )

    if s == '':
        return []

    l = s.split( '::' )

    return l


def cleanData():
    data = getData()

    res = {}
    company = None
    keys = [
        'job', 
        'post_date', 
        'location', 
        'salary', 
        'labels', 
        'company_url', 
        'link'
    ]

    for row in data:
        row = row[1:]

        if isListOfEmpties( row ):
            continue

        if row[0] != '' and isListOfEmpties( row[1:] ):
            company = row[0]

        elif company is not None:
            
            if res.get( company ) is None:
                res[company] = []


            label_index = keys.index( 'labels' )
            row[label_index] = organizeLabels( row[label_index] )

            new_object = {k:row[i] for k, i in zip( keys, range( len( keys )))}
            res[company].append( new_object )

    return res



