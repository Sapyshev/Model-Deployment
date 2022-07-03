import sqlite3


class DBHelper:

    def __init__(self, dbname="todo.sqlite.2"):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname)

    def setup(self):
#         tblstmt0 = "CREATE TABLE IF NOT EXISTS users (owner text)"
        tblstmt = "CREATE TABLE IF NOT EXISTS registry (offered text, owner text, ofd text)"
        tblstmt2 = "CREATE TABLE IF NOT EXISTS labeled (user text, offered text, ofd text, label text)"
        tblstmt3 = "CREATE TABLE IF NOT EXISTS validated (user text, ofd text, label text)"
        tblstmt4 = "CREATE TABLE IF NOT EXISTS unvalidated (user text, ofd text, label text)"
        tblstmt5 = "CREATE TABLE IF NOT EXISTS banned (user text)"
        itemidx = "CREATE INDEX IF NOT EXISTS offeredIndex ON registry (offered ASC)" 
        ownidx = "CREATE INDEX IF NOT EXISTS ownIndex ON registry (owner ASC)"
        self.conn.execute(tblstmt)
        self.conn.execute(tblstmt2)
        self.conn.execute(tblstmt3)
        self.conn.execute(tblstmt4)
        self.conn.execute(tblstmt5)
        self.conn.execute(itemidx)
        self.conn.execute(ownidx)
        self.conn.commit()

    def add_user_registry(self, owner, offered, ofd):
        stmt = "INSERT INTO registry (offered, owner, ofd) VALUES (?, ?, ?)"
        args = (offered, owner, ofd)
        self.conn.execute(stmt, args)
        self.conn.commit()

    def delete_user(self, owner):
        stmt = "DELETE FROM registry WHERE owner = (?)"
        args = (owner, )
        self.conn.execute(stmt, args)
        self.conn.commit()
    
    def get_users_with_registry(self):
        stmt = "SELECT distinct(owner) FROM registry"
        return [x[0] for x in self.conn.execute(stmt)]

    def get_offered(self, owner):
        stmt = "SELECT offered FROM registry WHERE owner = (?)"
        args = (owner, )
        return [x[0] for x in self.conn.execute(stmt, args)][-1]
    
    def get_item(self, owner):
        stmt = "SELECT ofd FROM registry WHERE owner = (?)"
        args = (owner, )
        return [x[0] for x in self.conn.execute(stmt, args)][-1]
    
    def add_label(self, user, offered, ofd, label):
        stmt = "INSERT INTO labeled (user, offered, ofd, label) VALUES (?, ?, ?, ?)"
        args = (user, offered, ofd, label)
        self.conn.execute(stmt, args)
        self.conn.commit()
    def add_validated(self, user, ofd, label):
        stmt = "INSERT INTO validated (user, ofd, label) VALUES (?, ?, ?)"
        args = (user, ofd, label)
        self.conn.execute(stmt, args)
        self.conn.commit()
    def add_banned(self, user):
        stmt = "INSERT INTO banned (user) VALUES (?)"
        args = (user, )
        self.conn.execute(stmt, args)
        self.conn.commit()
    def remove_banned(self, user):
        stmt = "DELETE FROM banned WHERE user = '{}'".format(user)
        self.conn.execute(stmt)
        self.conn.commit()
    def add_unvalidated(self, user, ofd, label):
        stmt = "INSERT INTO unvalidated (user, ofd, label) VALUES (?, ?, ?)"
        args = (user, ofd, label)
        self.conn.execute(stmt, args)
        self.conn.commit()
    def get_banned(self):
        stmt = "SELECT * FROM banned"
        return [x[0] for x in self.conn.execute(stmt)]
    def get_all_unvalidated(self):
        stmt = "SELECT * FROM unvalidated"
        return [x for x in self.conn.execute(stmt)]
    
    def get_all_labels(self):
        stmt = "SELECT * FROM labeled"
        return [x for x in self.conn.execute(stmt)]