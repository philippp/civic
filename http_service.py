import flask
import optparse
import db.interface
import logging
import pprint

from digestion.locations import address_lookup
address_resolver = address_lookup.AddressLookup()

def parse_options():
    parser = optparse.OptionParser()
    parser.add_option("-d", "--database", dest="database", default="DEV",
                      help="Database as defined in secrets.py.",
                      metavar="DATABASE")
    opts, args = parser.parse_args()
    return opts

app = flask.Flask(__name__)

@app.route('/address')
@app.route('/address/<lookup_address>')
def address(lookup_address=None):
    if lookup_address:
        lookup_address = address_resolver.lookup(lookup_address)
    return flask.render_template('address.html',
                                 lookup_address=lookup_address)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    opts = parse_options()
    db_interface = db.interface.DBInterface()
    db_interface.initialize(opts.database)
    address_resolver.initialize(db_interface)    
    app.run()
