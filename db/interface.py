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
    def write_rows(self, colnames, row, table, update_cols=None, autocommit=True):
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
            self.cursor.executemany(sqlstr, row)
        except Exception, e:
            logging.error(str(e))
            logging.error("Column names: ", str(colnames))
            logging.error("Data row: ", str(row))
            raise e
        if autocommit:
            self.db.commit()
        return self.cursor.lastrowid

    def make_equals_or_in(self, key, value_or_list):
        if type(value_or_list) in (str, unicode):
            value_or_list = "\"%s\"" % value_or_list
        if hasattr(value_or_list, "__iter__"):
            values = list()
            for individual_value in value_or_list:
                if type(individual_value) in (str, unicode):
                    individual_value = "\"%s\"" % individual_value
                values.append(individual_value)
            return "%s IN (%s)" % (key, ",".join(values))
        else:
            return "%s = %s" % (key, value_or_list)


    def read_rows(self, colnames, table, limit=-1, limit2=0, ordergroup='', **kwargs):
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
        if ordergroup:
            sqlstr += (" " + ordergroup)
        if limit >= 0:
            sqlstr += " LIMIT %d" % limit
            if limit2 > 0:
                sqlstr += ",%d" % limit2
        self.cursor.execute(sqlstr)
        return self.cursor.fetchall()


    """
    Writes one or more rows to the datebase. Takes the column names of the table,
    with indices corresponding to the values in row. Table is the table to write
    into, and if update_cols is populated, we update the specified cells. """
    def update_rows(self, table, update_dict=None, where_dict=None, autocommit=True):

        update_list = list()
        for k, v in update_dict.iteritems():
            if type(v) in (str, unicode):
                v = "\"%s\"" % v
            update_list.append("%s=%s" % (k, v))
        update_str = ", ".join(update_list)
        

        where_list = list()
        for key, val in where_dict.iteritems():
            where_list.append(self.make_equals_or_in(key, val))
        where_str = " AND ".join(where_list)
        

        sqlstr = "UPDATE %s SET %s WHERE %s" % (
            table, update_str, where_str)
        print sqlstr
        self.cursor.execute(sqlstr)
        if autocommit:
            self.db.commit()
        return self.cursor.lastrowid
