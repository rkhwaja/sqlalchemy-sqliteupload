from contextlib import contextmanager, suppress
from logging import info
from os import getcwd

from sqlalchemy import Column, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqliteupload import __version__
from sqliteupload.dialect import RegisterDialect

def test_version():
	assert __version__ == '0.1.0'

Base = declarative_base()

class _ExampleTable(Base):
	__tablename__ = "example"
	name = Column(String, primary_key=True)

@contextmanager
def _Session(engine):
	sessionMaker = sessionmaker(bind=engine)
	session = sessionMaker()
	try:
		yield session
	except:
		session.rollback()
		raise
	finally:
		session.close()

def test_create():
	databaseUrl = "sqlite:///:memory:"
	databaseUrl = "sqlite:///test.db"
	engine = create_engine(databaseUrl, connect_args={})
	Base.metadata.create_all(engine)

	with _Session(engine) as session:
		session.add(_ExampleTable(name="name1"))
		session.commit()

	with _Session(engine) as session:
		entries = session.query(_ExampleTable).all()
		assert len(entries) == 1, "Should be 1 entry"

def test_create_dialect():
	from fs import open_fs
	from fs.errors import ResourceNotFound

	RegisterDialect()
	scheme = "osfs"
	remoteDirectory = getcwd()
	remoteFilename = "remote.db"
	databaseUrl = f"sqliteupload:///{remoteDirectory}/{remoteFilename}?fs={scheme}"
	with suppress(ResourceNotFound):
		open_fs(f"{scheme}://{remoteDirectory}").remove(remoteFilename)

	info("Deleted existing remote DB, about to make a new DB")
	engine = create_engine(databaseUrl)
	Base.metadata.create_all(engine)

	info("About to write to new remote DB")
	with _Session(engine) as session:
		session.add(_ExampleTable(name="name1"))
		info("about to commit")
		session.commit()
		info("Commit done")

	info("New file should be uploaded by now")
	with _Session(engine) as session:
		entries = session.query(_ExampleTable).all()
		assert len(entries) == 1, "Should be 1 entry"

	info("Should not be re-uploaded because it's just a read")
