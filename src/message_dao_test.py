import unittest
import subprocess
import zlib

from unittest import mock
from google.cloud.sql.connector import Connector

import message
import message_dao
import archive_converter
from test_helpers import test_data_path

class MessageDaoTest(unittest.TestCase):

    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        self.mock_connect = mock.patch.object(Connector, 'connect').start()
        mock_connection = self.mock_connect.return_value
        self.mock_commit = mock_connection.commit
        self.mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        self.mock_execute = self.mock_cursor.execute
        # Check initialization of DAO
        self.dao = message_dao.MessageDao('FAKE_GIT_PATH')
        self.mock_connect.assert_called_once()
        self.mock_commit.assert_called_once()
        self.assertEqual(3, self.mock_execute.call_count)
        self.reset_mocks(return_value=True, side_effect=True)
        self.mock_connect.side_effect = RuntimeError("Shouldn't be called after init")

    def reset_mocks(self, return_value=False, side_effect=False):
        self.mock_connect.reset_mock(return_value=return_value, side_effect=side_effect)
        self.mock_execute.reset_mock(return_value=return_value, side_effect=side_effect)
        self.mock_commit.reset_mock(return_value=return_value, side_effect=side_effect)

    def test_store(self):
        # Testing storing Message that's not already in DAO 
        email = archive_converter.generate_email_from_file(test_data_path('patch6.txt'))
        sql_text = "REPLACE INTO Messages VALUES (%s, %s, %s, %s, %s, %s, %s)"
        self.dao.store(email)
        self.mock_execute.assert_called_once_with(sql_text, mock.ANY)
        self.mock_commit.assert_called_once()
        self.reset_mocks()

        # Testing storing Message that's already in DAO 
        email = archive_converter.generate_email_from_file(test_data_path('patch6.txt'))
        self.dao.store(email)
        self.mock_execute.assert_called_once()
        self.mock_commit.assert_called_once()

    @mock.patch.object(subprocess, 'check_output')
    @mock.patch.object(message_dao, 'parse_message_from_str')
    def test_get(self, mock_parse_msg, mock_check_output):
        # Testing searching for a message id that's not in DAO
        self.mock_commit.side_effect = RuntimeError("Shouldn't be called during get")
        email = archive_converter.generate_email_from_file(test_data_path('patch6.txt'))
        mock_parse_msg.return_value = email
        self.mock_cursor.fetchone.return_value = (email.archive_hash, email.change_id)
        self.assertEqual(email, self.dao.get('fake_message_id'))
        self.assertEqual(2, self.mock_execute.call_count) 
        self.reset_mocks()

        # Testing searching for a message id that's not in DAO
        self.mock_cursor.fetchone.return_value = None
        self.assertIsNone(self.dao.get('not_in_dao'))
        self.mock_execute.assert_called_once()

    def test_size(self):
        # Testing size of empty dao
        self.mock_commit.side_effect = RuntimeError("Shouldn't be called during get")
        self.mock_cursor.fetchone.return_value = (0,)
        self.assertEqual(0, self.dao.size())
        self.mock_execute.assert_called_once()
        self.reset_mocks()

        # Testing size of one dao
        self.mock_cursor.fetchone.return_value = (1,)
        self.assertEqual(1, self.dao.size())
        self.mock_execute.assert_called_once()
        self.reset_mocks()

        # Testing size of many dao
        self.mock_cursor.fetchone.return_value = (10,)
        self.assertEqual(10, self.dao.size())
        self.mock_execute.assert_called_once()

    def test_store_hash(self):
        # Testing storing hash when it's not already in State table
        self.dao.store_last_hash("some_hash")
        sql_text = "REPLACE INTO States VALUES (%s, %s)"
        self.mock_execute.assert_called_once_with(sql_text, mock.ANY)
        self.mock_commit.assert_called_once()
        self.reset_mocks()

        # Testing storing hash when it's already in State table
        self.dao.store_last_hash("some_hash")
        self.mock_execute.assert_called_once()
        self.mock_commit.assert_called_once()

    def test_get_hash(self):
        # Testing searching for hash when it's not already in State table
        self.mock_commit.side_effect = RuntimeError("Shouldn't be called during get_hash")
        self.mock_cursor.fetchone.return_value = None
        self.assertEqual(message_dao.EPOCH_HASH, self.dao.get_last_hash())
        self.mock_execute.assert_called_once()
        self.reset_mocks()

        # Testing searching for hash when it's already in State table
        self.mock_cursor.fetchone.return_value = ("fake_hash",)
        self.assertEqual("fake_hash", self.dao.get_last_hash())
        self.mock_execute.assert_called_once()

if __name__ == '__main__':
    # TODO(@lenhard): Issue with Google's Connector that causes segmentation fault
    unittest.main()
    pass