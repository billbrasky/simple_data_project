import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# class extension of cursor object
class db( pg.extensions.cursor ):
    def closeall( self ):
        conn = self.connection
        self.close()
        conn.close()

    def insertData( self, data_types, insertion, data, write_to_file = False, 
        execute = True ):

        query_log = ''

        for key, items in data.items():
            for item in items:
                #
                ##unique to the jobs data
                item['company'] = key
                ##
                #
                query = insertion.format( **item )

                if write_to_file:
                    query_log += query

                if execute:
                    try:
                        self.execute( query )

                    except Exception as e:
                        e = colorstr( ' '.join( e.args ))
                        s = colorstr( 'The insertion broke' )
                        logtext = [(s, ['bold', 'yellow']), (e, ['bold', 'red'])]
                        log( logtext )
        #                    output = re.sub( sqlcolors, sqlrepl, query )
        #                    output = re.sub( nonsqlcolors, sqlrepl1, output )
        #                    print( output )

                        self.closeall()
                        break
            
                    self.connection.commit()

        if write_to_file:
            if not os.path.exists( 'log' ):
                os.mkdir( 'log' )

            with open( 'log/insert.sql', 'w' ) as f:
                f.write( query_log )



    # inserts data
    def insertdata( self, datatypes, insertion, datapath, writetofile = False ):

        querylog = ''

        with open( datapath, newline = '' ) as f:
            raw = csv.reader( f, quotechar = '"', delimiter = ',' )
            headers = next( raw )

            for row in raw:
                datapoint = {}
                for h in headers[1:]:
                    value = row[headers.index(h)]
                    value = processor( value, datatypes[h], h )
                    datapoint[h] = value


                query = insertion.format( **datapoint )
                if writetofile:
                    querylog += query

#                print( query )

                try:
                    self.execute( query )

                except Exception as e:
                    e = colorstr( ' '.join( e.args ))
                    s = colorstr( 'The insertion broke' )
                    logtext = [(s, ['bold', 'yellow']), (e, ['bold', 'red'])]
                    log( logtext )
#                    output = re.sub( sqlcolors, sqlrepl, query )
#                    output = re.sub( nonsqlcolors, sqlrepl1, output )
#                    print( output )

                    self.closeall()
                    break

        if writetofile:
            with open( 'log/insert.sql', 'w' ) as f:
                f.write( querylog )

        # self.connection.commit()

    # Excute a select query that is pretty printed to terminal
    def query( self, query, title = '' ):
        print( title )

        self.execute( query )

        widths = None
        result = list( self.fetchall())

        for row in result:
            if widths is None:
                widths = [len( str( x )) for x in row ]

            else:
                for i in range( len( row )):
                    word = str( row[i] )
                    width = widths[i]

                    if word is None:
                        continue

                    if len( word ) > width:
                        widths[i] = len( word )

        with open( 'log/output.txt', 'w' ) as f:
            for row in result:
                words = []

                for i in range( len( row )):
                    word = str( row[i] )
                    width = widths[i]

                    while len( word ) < width:
                        word += ' '
                    words.append( word )

                output = ' | '.join( words )
                f.write( output + '\n' )

                print( output )

    # apply updates to database
    def update( self, s ):

        queries = getyaml( s )['queries']

        with open( s + '.sql', 'r' ) as f:
            updatetemplate = f.read()

        for query in queries:

            sqlupdate = updatetemplate.format( select = query )
            print( sqlupdate)
            print( '-----------')
            self.execute( sqlupdate )
            self.connection.commit()
