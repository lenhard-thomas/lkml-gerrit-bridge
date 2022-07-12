# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import subprocess
import zlib

from functools import lru_cache
from typing import Dict, List, Optional

from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
from message import Message

load_dotenv()
EPOCH_HASH = 'ae9e7be4a03765456fe38287533e6446e8bbc93c'

class MessageDao(object):
    def __init__(self) -> None:
        self._initialize_connection()
        self._initialize_tables()

    def _initialize_connection(self) -> None:
        connector = Connector()
        self.connection = connector.connect(
            os.environ.get("HOST"),
            "pymysql",
            user = os.environ.get("USER"),
            password = os.environ.get("PASSWORD")
        )
    
    def _initialize_tables(self) -> None:
        db_name = os.environ.get("DB")
        if not db_name:
            raise Exception("Missing environment variable for name of database.")
        with self.connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS " + db_name)
            self.connection.select_db(db_name)
            # Mapping from message id to message
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS Messages"
                "(message_id VARCHAR(255) NOT NULL,"
                "subject VARCHAR(255) NOT NULL,"
                "from_ VARCHAR(255) NOT NULL,"
                "in_reply_to VARCHAR(255),"
                "content BLOB NOT NULL,"
                "archive_hash VARCHAR(255),"
                "change_id VARCHAR(255),"
                "PRIMARY KEY (message_id))"
            )
            # Mapping from name of state attribute to 
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS States"
                "(state_name VARCHAR(255) NOT NULL,"
                "value VARCHAR(255) NOT NULL,"
                "PRIMARY KEY (state_name))"
            )
        self.connection.commit()


    def store(self, message: Message) -> None:
        # Convert children into list of ids and store message
        json_content = json.dumps(message.content)
        comp_content = zlib.compress(json_content.encode())
        sql = "REPLACE INTO Messages VALUES (%s, %s, %s, %s, %s, %s, %s)"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (message.id, message.subject, message.from_, 
            message.in_reply_to, comp_content, message.archive_hash, message.change_id))
        self.connection.commit()

    def _get_children(self, message_id: str) -> List[Optional[Message]]:
        sql = "SELECT * FROM Messages WHERE in_reply_to=%s"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (message_id,))
            res = cursor.fetchall()
        return [self.get(tup[0]) for tup in res]

    @lru_cache
    def get(self, message_id: str) -> Optional[Message]:
        sql = "SELECT * FROM Messages WHERE message_id=%s"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (message_id,))
            res = cursor.fetchone()
        if res is None:
            return None
        msg = Message(id = res[0], 
                      subject = res[1], 
                      from_ = res[2],
                      in_reply_to = res[3],
                      content = json.loads(zlib.decompress(res[4]).decode()),
                      archive_hash = res[5])
        msg.change_id = res[6]
        msg.children = self._get_children(message_id)
        return msg

    def size(self) -> int:
        sql = "SELECT COUNT(*) FROM Messages"
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchone()
        return res[0]

    def store_last_hash(self, last_hash: str) -> None:
        sql = "REPLACE INTO States VALUES (%s, %s)"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, ("last_hash", last_hash))
        self.connection.commit()

    def get_last_hash(self) -> str:
        sql = "SELECT value FROM States WHERE state_name=%s"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, ("last_hash"))
            res = cursor.fetchone()
        return EPOCH_HASH if res is None else res[0]


class FakeMessageDao(MessageDao):
    def __init__(self) -> None:
        # Maps message.id to message
        self._messages_seen = {}
        self.last_hash = EPOCH_HASH

    def store(self, message: Message) -> None:
        self._messages_seen[message.id] = message

    def get(self, message_id: str) -> Optional[Message]:
        return self._messages_seen.get(message_id)

    def size(self) -> int:
        return len(self._messages_seen)

    def store_last_hash(self, last_hash: str) -> None:
        self.last_hash = last_hash

    def get_last_hash(self) -> int:
        return self.last_hash
