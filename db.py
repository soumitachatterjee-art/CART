import mysql.connector
from mysql.connector import Error
from typing import Optional, Any, Tuple, List, TYPE_CHECKING

if TYPE_CHECKING:
    from connection_pool import ConnectionPoolManager

class MySQLDB:
    def __init__(
        self, 
        db_config: dict, 
        *, 
        dictionary: bool = False, 
        buffered: bool = False, 
        autocommit: bool = False,
        connection_pool: Optional['ConnectionPoolManager'] = None
    ):
        """
        db_config: dict containing connection params like host, user, password, database, port, etc.
        dictionary: if True, cursors return dicts instead of tuples.
        buffered: if True, use buffered cursor (can fetch rowcount, etc.)
        autocommit: if True, connection autocommit is enabled.
        connection_pool: optional ConnectionPoolManager to use for connection pooling.
        """
        self.db_config = db_config
        self.dictionary = dictionary
        self.buffered = buffered
        self.autocommit = autocommit
        self.connection_pool = connection_pool

        self._conn: Optional[mysql.connector.MySQLConnection] = None
        self._cursor: Optional[mysql.connector.cursor.MySQLCursor] = None
        self._committed = False  # whether commit was done in __exit__
        self._using_pool = False  # track if connection is from pool

    def __enter__(self) -> "MySQLDB":
        # Get connection from pool if available, otherwise create new connection
        if self.connection_pool:
            self._conn = self.connection_pool.get_connection()
            self._using_pool = True
        else:
            self._conn = mysql.connector.connect(**self.db_config)
            self._using_pool = False
        
        # set autocommit mode if requested
        self._conn.autocommit = self.autocommit
        self._cursor = self._conn.cursor(dictionary=self.dictionary, buffered=self.buffered)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        """
        On exiting the context:
         * If an exception occurred: rollback (unless autocommit)
         * Else: commit (if not autocommit)
         * Close cursor
         * Return connection to pool or close it
        """
        try:
            if exc_type:
                # an exception happened
                if self._conn and not self.autocommit:
                    self._conn.rollback()
            else:
                # normal exit
                if self._conn and not self.autocommit:
                    self._conn.commit()
                    self._committed = True
        finally:
            if self._cursor:
                try:
                    self._cursor.close()
                except Exception:
                    pass
            
            if self._conn:
                try:
                    if self._using_pool and self.connection_pool:
                        # Return connection to pool instead of closing
                        self.connection_pool.return_connection(self._conn)
                    else:
                        # Close connection if not using pool
                        self._conn.close()
                except Exception:
                    pass
        # Do not suppress exceptions — return False
        return False

    def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        if not self._cursor:
            raise RuntimeError("Cursor is not initialized. Use within `with` block.")
        self._cursor.execute(query, params or ())

    def executemany(self, query: str, data: List[Tuple[Any, ...]]) -> None:
        if not self._cursor:
            raise RuntimeError("Cursor is not initialized. Use within `with` block.")
        self._cursor.executemany(query, data)


    def fetchone(self) -> Any:
        if not self._cursor:
            raise RuntimeError("Cursor is not initialized.")
        return self._cursor.fetchone()

    def fetchall(self) -> Any:
        if not self._cursor:
            raise RuntimeError("Cursor is not initialized.")
        return self._cursor.fetchall()

    def rowcount(self) -> int:
        if not self._cursor:
            raise RuntimeError("Cursor is not initialized.")
        return self._cursor.rowcount

    def lastrowid(self) -> int:
        if not self._cursor:
            raise RuntimeError("Cursor is not initialized.")
        return self._cursor.lastrowid

# # Example usage
# DB_CONFIG = {
#     "host": "localhost",
#     "user": "your_user",
#     "password": "your_pw",
#     "database": "your_db",
#     "port": 3306,
#     # other options if needed
# }

# def get_active_users():
#     with MySQLDB(DB_CONFIG, dictionary=True) as db:
#         db.execute("SELECT id, name, email FROM users WHERE active=%s", (1,))
#         return db.fetchall()

# def add_new_user(name: str, email: str):
#     with MySQLDB(DB_CONFIG) as db:
#         db.execute("INSERT INTO users (name, email) VALUES (%s, %s)", (name, email))
#         return db.lastrowid()