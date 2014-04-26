import secrets
import MySQLdb
import pprint

class DBInterface(object):
    def __init__(self):
        self.db = None

    def initialize(self):
        self.db = MySQLdb.connect(**secrets.DB_CONFIG)
        self.cursor = self.db.cursor()

    """
    Writes a row to the datebase. Takes the column names of the table,
    with indices corresponding to the values in row. Table is the table to write
    into, and if update_cols is populated, we update the specified cells. """
    def write_row(self, colnames, row, table, update_cols=None):
        joined_colnames = ",".join(colnames)
        value_template = " ,".join(["%s" for c in colnames])
        sqlstr = "INSERT INTO %s (%s) VALUES (%s)" % (
                    table, joined_colnames, value_template)
        if update_cols:
            sqlstr += " ON DUPLICATE KEY UPDATE "
            sqlstr += ",".join(
                ["%s = VALUES(%s)" % (c, c) for c in update_cols])
            sqlstr += ";"
        try:
            self.cursor.execute(sqlstr, row)
        except Exception, e:
            logging.error(str(e))
            logging.error("Column names: ", str(colnames))
            logging.error("Data row: ", str(row))
            raise e
        self.db.commit()
        return self.cursor.lastrowid

    def read_rows(self, colnames, table, limit=0, **kwargs):
        joined_colnames = ",".join(colnames)
        sqlstr = "SELECT %s FROM %s" % (joined_colnames, table)
        if kwargs.keys():
            sqlstr += " WHERE "
            conditionals = list()
            for key, val in kwargs.iteritems():
                if type(val) in (str, unicode):
                    val = "\"%s\"" % val
                conditionals.append("%s = %s" % (key, val))
            sqlstr += " AND ".join(conditionals)
        if limit > 0:
            sqlstr += " LIMIT %d" % limit
        self.cursor.execute(sqlstr)
        return self.cursor.fetchall()

