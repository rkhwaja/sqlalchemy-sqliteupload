from contextlib import suppress
from hashlib import md5
from io import BytesIO
from logging import debug, info
from os import remove
from os.path import abspath

from fs import open_fs
from fs.errors import ResourceNotFound
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite

def _hash_of_bytes(bytes_):
	hash_ = md5()
	hash_.update(bytes_)
	return hash_.hexdigest()

class SQLiteUploadDialect(SQLiteDialect_pysqlite):

	def __init__(self, *args, **kw):
		super().__init__(*args, **kw)
		self._localPath = abspath("local.db")
		self._localHash = None
		debug(f"localPath: {self._localPath}")

	def close(self, *args, **kwargs):
		with open(self._localPath, "rb") as f:
			bytes_ = f.read()

		if _hash_of_bytes(bytes_) == self._localHash:
			debug(f"Hash unchanged: {self._localHash}, not uploading local file")
		else:
			self._fs.writebytes(self._remoteFilename, bytes_)
			debug(f"Uploaded to {self._remoteFilename} on {self._fs}")

	def connect(self, *args, **kw):
		uploadUrl = f"{kw['fs']}://{args[0]}"
		del kw["fs"]
		self._load_remote_db(uploadUrl)
		return super().connect(self._localPath, **kw)

	def do_close(self, *args, **kw):
		out = super().do_close(*args, **kw)
		self.close()
		return out

	def _load_remote_db(self, remotePath):
		lastSeparator = remotePath.rfind("/")
		fsurl = remotePath[:lastSeparator]
		self._remoteFilename = remotePath[lastSeparator + 1:]
		self._fs = open_fs(fsurl)
		
		try:
			remoteBytes = self._fs.readbytes(self._remoteFilename)
			with open(self._localPath, "wb") as localFile: # truncate any existing files
				localFile.write(remoteBytes)
			self._localHash = _hash_of_bytes(remoteBytes)
			debug(f"Loaded remote DB from {self._fs}:{self._remoteFilename}")
		except ResourceNotFound:
			debug(f"No file at {self._fs}:{self._remoteFilename}, deleting {self._localPath}")
			with suppress(FileNotFoundError):
				remove(self._localPath)
			self._localHash = None

urlScheme = "sqliteupload"

def RegisterDialect():
	registry.register(urlScheme, "sqliteupload.dialect", SQLiteUploadDialect.__name__)
