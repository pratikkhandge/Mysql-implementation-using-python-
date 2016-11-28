"""
Description: Module used to handle mysql basic apis for mysql database operation
"""
__author__ = "pratik khandge"
__copyright__ = ""
__credits__ = ["pratik khandge"]
__license__ = ""
__version__ = "0.1"
__maintainer__ = "pratik"
__email__ = "pratik.khandge@gmail.com"
__status__ = "Developement"

# python imports
import MySQLdb


class MySqlConn(object):
    """
    Description: Class handle mysql operations
    """
    def __init__(self, connection_obj, database=None):
        """
        Constructor for mysql.
        """
        self.conn = MySQLdb.connect(host=connection_obj['HOST'], port=connection_obj['PORT'],
                                    user=connection_obj['USER'], passwd=connection_obj['PASSWORD'],
                                    db=database if database else connection_obj['DATABASE'])
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()

    def fetch_one_record(self, query):
        """
        Get one record from database.
        """
        try:
            doc = {}
            self.cursor.execute(query)
            row = self.cursor.fetchone()
            if row is None:
                return doc
            desc = self.cursor.description
            for (name, value) in zip(desc, row):
                doc[name[0]] = value
        except Exception, e:
            doc = {}
            raise Exception("Could not fetch data Exception {}:".format(e))
        finally:
            return doc

    def fetch_all_assoc(self, sql):
        """
        Get all records.
        """
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            if rows is None:
                return None
            desc = self.cursor.description
            result = []
            for row in rows:
                doc = {}
                for (name, value) in zip(desc, row):
                    doc[name[0]] = value
                result.append(doc)
        except Exception, e:
            result = None
            raise Exception("Could not fetch data Exception {}:".format(e))
        finally:
            return result

    def execute(self, sql):
        """
        Execute sql query.
        """
        try:
            result = self.cursor.execute(sql)
            self.conn.commit()
        except Exception, e:
            result = None
            self.conn.rollback()
            raise Exception("Could not execute sql statement Exception {}:".format(e))
        finally:
            return result

    def call_procedure(self, name, args=None):
        """
        Call sql procedure.
        """
        try:
            if args:
                self.cursor.callproc(name, args)
            else:
                self.cursor.callproc(name)
            rows = self.cursor.fetchall()
            if rows is None:
                return None
            desc = self.cursor.description
            result = []
            for row in rows:
                doc = {}
                for (name, value) in zip(desc, row):
                    doc[name[0]] = value
                result.append(doc)
            # end for
            self.cursor.nextset()
            return result
        except Exception, e:
            raise Exception("Could not execute procedure Exception {}:".format(e))
