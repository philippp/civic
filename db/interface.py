import secrets
import MySQLdb
import pprint
import logging

class DBInterface(object):
    def __init__(self):
        self.db = None

    def initialize(self, database):
        self.db = MySQLdb.connect(**secrets.DB_CONFIGS[database])
        self.cursor = self.db.cursor()

    """
    Commits all outstanding writes to DB. Only meaningful when write_row
    has been called with autocommit = False.
    """
    def commit_writes(self):
        self.db.commit()

    def get_rows_from_last_committed_write(self):
        return self.cursor.rowcount
    """
    Writes one or more rows to the datebase. Takes the column names of the table,
    with indices corresponding to the values in row. Table is the table to write
    into, and if update_cols is populated, we update the specified cells. """
    def write_row(self, colnames, row, table, update_cols=None, autocommit=True):
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
            if len(row) and hasattr(row[0], '__iter__'):
                self.cursor.executemany(sqlstr, row)
            else:
                self.cursor.execute(sqlstr, row)
        except Exception, e:
            logging.error(str(e))
            logging.error("Column names: ", str(colnames))
            logging.error("Data row: ", str(row))
            raise e
        if autocommit:
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
                if hasattr(val, "__iter__"):
                    values = list()
                    for individual_value in val:
                        if type(individual_value) in (str, unicode):
                            individual_value = "\"%s\"" % individual_value
                        values.append(individual_value)
                    conditionals.append("%s IN (%s)" % (key, ",".join(values)))
                else:
                    conditionals.append("%s = %s" % (key, val))
            sqlstr += " AND ".join(conditionals)
        if limit > 0:
            sqlstr += " LIMIT %d" % limit
        self.cursor.execute(sqlstr)
        return self.cursor.fetchall()

