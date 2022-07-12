import json
import unittest
import zlib

from google.cloud.sql.connector import Connector

import message_dao
import archive_converter
from test_helpers import test_data_path

class MessageDaoTest(unittest.TestCase):

    def setUp(self):
        patcher = unittest.mock.patch.object(Connector, 'connect')
        self.addCleanup(patcher.stop)
        self.mock_connect = patcher.start()
        self.mock_con = self.mock_connect.return_value  
        self.mock_cur = self.mock_con.cursor.return_value.__enter__.return_value
        self.dao = message_dao.MessageDao()

    def assert_call_counts(self, connect, execute, commit):
        self.assertEqual(connect, self.mock_connect.call_count)
        self.assertEqual(execute, self.mock_cur.execute.call_count)
        self.assertEqual(commit, self.mock_con.commit.call_count)

    def test_init(self):
        # Testing initialization of DAO
        self.assert_call_counts(1, 3, 1)

    def test_store(self):
        # Testing storing Message that's not already in DAO 
        email = archive_converter.generate_email_from_file(test_data_path('patch6.txt'))
        self.dao.store(email)
        self.assert_call_counts(1, 4, 2)

        # Testing storing Message that's already in DAO 
        email = archive_converter.generate_email_from_file(test_data_path('patch6.txt'))
        self.dao.store(email)
        self.assert_call_counts(1, 5, 3)

    def test_get(self):
        # Testing searching for a message id that's not in DAO
        email = archive_converter.generate_email_from_file(test_data_path('patch6.txt'))
        json_content = json.dumps(email.content)
        content = zlib.compress(json_content.encode())
        tup = (email.id, email.subject, email.from_, email.in_reply_to, content,
               email.archive_hash, email.change_id)
        self.mock_cur.fetchone.return_value = tup
        self.assertEqual(email.content, self.dao.get('foo').content)
        self.assert_call_counts(1, 5, 1)

        # Testing searching for a message id that's not in DAO
        self.mock_cur.fetchone.return_value = None
        self.assertIsNone(self.dao.get('not_in_dao'))
        self.assert_call_counts(1, 6, 1)

    def test_size(self):
        # Testing size of empty dao
        self.mock_cur.fetchone.return_value = (0,)
        self.assertEqual(0, self.dao.size())
        self.assert_call_counts(1, 4, 1)

        # Testing size of one dao
        self.mock_cur.fetchone.return_value = (1,)
        self.assertEqual(1, self.dao.size())
        self.assert_call_counts(1, 5, 1)

        # Testing size of many dao
        self.mock_cur.fetchone.return_value = (10,)
        self.assertEqual(10, self.dao.size())
        self.assert_call_counts(1, 6, 1)

    def test_store_hash(self):
        # Testing storing hash when it's not already in State table
        self.dao.store_last_hash("some_hash")
        self.assert_call_counts(1, 4, 2)

        # Testing storing hash when it's already in State table
        self.dao.store_last_hash("some_hash")
        self.assert_call_counts(1, 5, 3)

    def test_get_hash(self):
        # Testing searching for hash when it's not already in State table
        self.mock_cur.fetchone.return_value = None
        self.assertEqual(message_dao.EPOCH_HASH, self.dao.get_last_hash())
        self.assert_call_counts(1, 4, 1)

        # Testing searching for hash when it's already in State table
        self.mock_cur.fetchone.return_value = ("fake_hash",)
        self.assertEqual("fake_hash", self.dao.get_last_hash())
        self.assert_call_counts(1, 5, 1)

if __name__ == '__main__':
    # TODO(@lenhard): Issue with Google's Connector that causes segmentation fault
    # unittest.main()
    pass