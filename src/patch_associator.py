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
import abc
import re
import subprocess

from typing import Dict, List, Optional
from message import Message
from patch_parser import Patch
from message_dao import MessageDao

class PatchAssociator(object, metaclass = abc.ABCMeta):
    def __init__(self, message_dao : MessageDao) -> None:
        self._message_dao = message_dao

    @abc.abstractmethod
    def get_previous_version(self, message : Message) -> Optional[Message]:
        pass

class SimplePatchAssociator(PatchAssociator):

    def __init__(self, message_dao: MessageDao, git_path: str) -> None:
        super().__init__(message_dao)
        self.git_path = git_path

    def get_time(self, message: Message):
        return int(subprocess.check_output(
            ['git', '-C', self.git_path , 'show', '-s', '--format=%ct', message.archive_hash]))

    def newest_first(self, candidates: List[Message]):
        candidates_with_time = [(message, self.get_time(message)) for message in candidates]
        # sort by newest commit to latest commit
        candidates_with_time.sort(key=lambda x: -x[1])
        return [message for (message, time) in candidates_with_time]

    def get_previous_version(self, message: Message) -> Optional[Message]:
        candidates = self._message_dao.previous_version_candidates(message)
        candidates = self.newest_first(candidates)
        previous_version_number = message.version() - 1
        compiled = re.compile(fr'\[.+ v{previous_version_number}.*\]')
        for candidate in candidates:
            if compiled.match(candidate.subject):
                return candidate
            if previous_version_number == 1:
                # Consider subjects that don't have a version number
                compiled_no_version = re.compile(fr"\[PATCH\]")
                if compiled_no_version.match(candidate.subject):
                    return candidate
        return None