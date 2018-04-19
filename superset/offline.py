# A lot of this is taken from 
#
#     https://gist.github.com/pajachiet/62eb85805cee55053d208521e0bdaf13
#
# to work around problems mentioned at
#
#     https://github.com/apache/incubator-superset/issues/3085

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import superset
from superset import security, sm, utils
from superset.connectors.sqla.models import SqlaTable
from superset.models.core import Database

db_uri = superset.app.config.get('SQLALCHEMY_DATABASE_URI')
db = create_engine(db_uri)
Session = sessionmaker(bind=db)

def run_superset_backend_query(sql):
    return db.engine.execute(sql)

def create_missing_database_access_permission_view():
    """Adds database_access permission on all databases.
    """
    query = """
        INSERT INTO ab_permission_view (permission_id, view_menu_id)
        SELECT  ab_permission.id, ab_view_menu.id
        FROM    ab_permission,
                dbs
        LEFT JOIN ab_view_menu
        ON      ab_view_menu.name = dbs.perm
        WHERE   ab_permission.name = 'database_access'
        AND     NOT ab_permission.id || ' ' || ab_view_menu.id IN (
            SELECT  permission_id || ' ' || view_menu_id
            FROM    ab_permission_view
        )
    """
    run_superset_backend_query(query)

def find_views_in_database(database_id):
    session = Session()
    already_exists = {
        t.table_name for t in session.query(SqlaTable).filter_by(database_id=database_id)
    }

    db = session.query(Database).filter_by(id=database_id).one()
    eng = db.get_sqla_engine()
    sql = "SELECT matviewname FROM pg_matviews WHERE schemaname = 'public'"
    return {row[0] for row in eng.execute(sql)} - already_exists

def create_all_tables_for_database(database_id):
    session = Session()

    db = session.query(Database).filter_by(id=database_id).one()
    for table_name in find_views_in_database(database_id):
        t = SqlaTable(database_id=database_id, schema='public', table_name=table_name)
        t.database = db     # If we don't do this then we get rows in ab_view_menu that start with `[None]` instead of `[$database_name]`.
        # This will create the table's entry in ab_view_menu:
        session.add(t)
        # This will create the entries in ab_permission_view:
        superset.security.merge_perm(superset.sm, 'datasource_access', t.get_perm())
        superset.security.merge_perm(superset.sm, 'schema_access', t.schema_perm)

    session.commit()

def get_databases():
    session = Session()
    return [(d.id, d.name) for d in session.query(Database).all()]

def create_database_roles():
    """Create roles for databases access

    - for each database defined in superset
    - for all datasources defined in superset, associated to a database
    """

    create_database_role_template = """
        INSERT INTO ab_role
        (name)
        VALUES
        ('{database}')
        ON CONFLICT DO NOTHING
    """

    create_database_permission_role_template = """
        INSERT INTO ab_permission_view_role (role_id, permission_view_id)
        SELECT  ab_role.id, ab_permission_view.id
        FROM    ab_role,
                ab_permission_view
        LEFT JOIN ab_permission ON ab_permission.id = ab_permission_view.permission_id
        LEFT JOIN ab_view_menu ON ab_view_menu.id = ab_permission_view.view_menu_id
        WHERE   ab_role.name = '{database}'
        AND     ab_permission.name = 'datasource_access'
        AND     ab_view_menu.name LIKE '[{database}]%%'
        AND     NOT EXISTS (SELECT  1
                            FROM    ab_permission_view_role pvr2
                            WHERE   (pvr2.role_id, pvr2.permission_view_id) = (ab_role.id, ab_permission_view.id))
        UNION
        SELECT  ab_role.id, ab_permission_view.id
        FROM    ab_role,
                ab_permission_view
        LEFT JOIN ab_permission ON ab_permission.id = ab_permission_view.permission_id
        LEFT JOIN ab_view_menu ON ab_view_menu.id = ab_permission_view.view_menu_id
        WHERE   ab_role.name = '{database}'
        AND     ab_permission.name = 'database_access'
        AND     ab_view_menu.name LIKE '[{database}]%%'
        AND     NOT EXISTS (SELECT  1
                            FROM    ab_permission_view_role pvr2
                            WHERE   (pvr2.role_id, pvr2.permission_view_id) = (ab_role.id, ab_permission_view.id))
    """
    for (_database_id, database) in get_databases():
        query = create_database_role_template.format(database=database)
        run_superset_backend_query(query)
        query = create_database_permission_role_template.format(database=database)
        run_superset_backend_query(query)

def update_permissions_for_all_databases():
    """Updates all reporting databases so their roles include permissions for every table in their reporting schema.
    We should call this out of cron every hour or so (or even more frequently).
    """
    for (database_id, database_name) in get_databases():
        if database_name == 'main':
            pass
        else:
            create_all_tables_for_database(database_id)
    create_missing_database_access_permission_view()
    create_database_roles()

def create_database(name, sql_username, password):
    session = Session()

    db = Database(
        database_name=name,
        sqlalchemy_uri="postgresql+psycopg2://%s:%s@10.0.21.10/reporting_%s" % (sql_username, password, sql_username),
        extra='{"metadata_params": {}, "engine_params": {}}',
        expose_in_sqllab=True,
        allow_run_sync=True,
        allow_run_async=False,
        allow_ctas=False,
        allow_dml=False)

    db.set_sqlalchemy_uri(db.sqlalchemy_uri)
    security.merge_perm(sm, 'database_access', db.perm)
    for schema in db.all_schema_names():
        security.merge_perm(
            sm, 'schema_access', utils.get_schema_perm(db, schema))

    session.add(db)
    session.commit()
    return db.id

def add_reporting_database(name, sql_username, password):
    """Adds a new reporting database to Superset
    and sets up a Role with permissions to use it.
    Once you do this, cron will keep the Datasources and Role up-to-date
    even if you add slugs to more forms in Vertical Change.

    It would be really nice to also do an `ALTER USER foo WITH PASSWORD bar` right here,
    but Superset only has a readonly connection to the main VC database,
    and it's probably best to keep it that way.
    So we'll keep that manual for now....
    """
    database_id = create_database(name, sql_username, password)
    create_all_tables_for_database(database_id)
    create_missing_database_access_permission_view()
    create_database_roles()
