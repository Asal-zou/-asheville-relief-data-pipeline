import psycopg2

def get_postgres_connection(dbname, user, password, host, port=5432):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
