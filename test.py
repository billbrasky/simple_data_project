import cleandata

data = cleandata.cleanData()

class jobs():
    def __init__( self, company, title = None, data_posted = None, location = None, 
        salary = None, labels = None, company_url = None, link = None ):

        self.company = company
        self.title = title
        self.date_posted = date_posted
        self.location = location
        self.salary = salary
        self.labels = labels
        self.company_url = company_url
        self.link = link


class Node:
    def __init__( self, info, count = 0 ):
        self.info = info
        self.count = count
        self.children = {}

    def __str__( self ):
        return str( self.info )

class BinarySearchTree:
    def __init__( self ):
        self.root = Node( 0 )

    def create( self, data ):
        current = self.root

        for datum in data:
            if current.children.get( datum ) is None:
                current.children[datum] = Node( datum, 0 )

            current = current.children[datum]
            current.count += 1

    def find( self, partial ):
        if self.root is None:
            return 0

        current = self.root

        for p in partial:
            if current.children.get( p ) is None:
                return 0


            current = current.children[p]


        return current.count

companies = list( data.keys())

company_tree = BinarySearchTree()

for company in companies:
    company = company.lower()
    company_tree.create( company )

    



def search( words, tree ):
    res = {}

    for word in words:

        res[word] = tree.find( word )

    return res

words = ['cox', 'ama', 'amazon']

res = search( words, company_tree )

for x, y in res.items():
    print( x, y )