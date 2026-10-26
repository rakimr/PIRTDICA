import os
from sqlalchemy import event, Table, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.db_connection import create_runtime_engine


DATABASE_URL = os.environ.get("SUPABASE_DATABASE_URL") or os.environ.get("DATABASE_URL")
engine = create_runtime_engine(DATABASE_URL, pool_size=3, max_overflow=2)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


@event.listens_for(Table, "after_create")
def _enable_rls_on_new_table(target, connection, **kw):
    """Enable row level security on every newly created Postgres table so
    tables added via Base.metadata.create_all() never ship exposed through
    Supabase's PostgREST API (rls_disabled_in_public lint). The backend's
    own connection is the table owner, so RLS does not affect the app."""
    if connection.dialect.name != "postgresql":
        return
    try:
        connection.execute(text(
            f'ALTER TABLE "{target.schema or "public"}"."{target.name}" ENABLE ROW LEVEL SECURITY'
        ))
    except Exception as e:
        print(f"[database] WARNING: could not enable RLS on {target.name}: {e}")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
