import os
import re
import tempfile
import threading
from contextlib import contextmanager

import requests
from dagster import ConfigurableResource
from girder_client import GirderClient
from pydantic import PrivateAttr


class GirderClientWithSession(GirderClient):
    def __init__(self, host=None, port=None, apiRoot=None, scheme=None, apiUrl=None,
                 apiKey=None, token=None, session=None, cacheSettings=None,
                 progressReporterCls=None):
        super().__init__(
            host=host, port=port, apiRoot=apiRoot, scheme=scheme, apiUrl=apiUrl,
            cacheSettings=cacheSettings, progressReporterCls=progressReporterCls,
        )
        if token:
            self.setToken(token)
        if apiKey:
            self.authenticate(apiKey=apiKey)
        self._session = session


_girder_client_cache: dict[tuple, GirderClientWithSession] = {}
_girder_client_cache_lock = threading.Lock()


class GirderCredentials(ConfigurableResource):
    api_url: str
    api_key: str


class GirderConnection(ConfigurableResource):
    credentials: GirderCredentials
    _client: GirderClientWithSession = PrivateAttr()

    def _make_client(self):
        session = requests.Session()
        return GirderClientWithSession(
            apiUrl=self.credentials.api_url,
            apiKey=self.credentials.api_key,
            session=session,
        )

    @property
    def client(self):
        if not self._client:
            raise Exception("Girder client is not initialized. Use yield_for_execution.")
        return self._client

    @contextmanager
    def yield_for_execution(self, context):
        key = (self.credentials.api_url, self.credentials.api_key)
        with _girder_client_cache_lock:
            client = _girder_client_cache.get(key)
            if client is None or client.get("user/me") is None:
                _girder_client_cache[key] = self._make_client()
        self._client = _girder_client_cache[key]
        yield self

    def list_folder_items(self, folder_id: str, name_regex: str = None) -> list:
        items = self._client.listItem(folder_id)
        if name_regex:
            pattern = re.compile(name_regex)
            return [item for item in items if pattern.match(item["name"])]
        return list(items)

    def get_file_from_item(self, item_id: str) -> dict:
        files = self._client.get(
            f"item/{item_id}/files",
            parameters={"limit": 1, "offset": 0, "sort": "created", "sortdir": -1},
        )
        return files[0]

    def download_item_to_tempfile(self, item_id: str, suffix: str = "") -> str:
        fobj = self.get_file_from_item(item_id)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        self._client.downloadFile(fobj["_id"], tmp)
        tmp.flush()
        tmp.close()
        return tmp.name

    def existing_file(self, folder_id: str, filename: str):
        for item in self._client.listItem(folder_id, name=filename):
            return next(self._client.listFile(item["_id"]), None)

    def replace_existing_file(self, fobj: dict, file_path: str) -> dict:
        size = os.path.getsize(file_path)
        with open(file_path, "rb") as stream:
            self._client.uploadFileContents(fobj["_id"], stream, size)
        return fobj

    def upload_file_to_folder(self, folder_id: str, file_path: str,
                              mime_type: str = None, filename: str = None) -> dict:
        filename = os.path.basename(file_path) if filename is None else filename
        if existing := self.existing_file(folder_id, filename):
            return self.replace_existing_file(existing, file_path)
        return self._client.uploadFileToFolder(
            folder_id, file_path, mimeType=mime_type, filename=filename
        )
