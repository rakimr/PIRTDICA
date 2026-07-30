import os
from sqlalchemy import create_engine, event, Table, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


def _ensure_pooler_url(url):
    if not url or "supabase" not in url:
        return url
    if ":5432" in url:
        url = url.replace(":5432", ":6543")
    return url


DATABASE_URL = os.environ.get("SUPABASE_DATABASE_URL") or os.environ.get("DATABASE_URL")
DATABASE_URL = _ensure_pooler_url(DATABASE_URL)

engine = create_engine(DATABASE_URL)
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
