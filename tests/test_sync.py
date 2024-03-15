import os
import time
import unittest
from os import makedirs
from os.path import basename, join, exists, isdir
from shutil import rmtree
from tempfile import mkdtemp, mktemp
from typing import List, Tuple

from mysutils.file import write_file, save_json, read_file, load_json
from mysutils.tmp import removable_tmp
from mysutils.unittest import FileTestCase
from logging import getLogger
from mysutils.logging import config_log
from botocore.exceptions import ClientError
from functools import partial
from mysutils.yaml import save_yaml, load_yaml

from filecloudsync import s3
from filecloudsync.s3 import Operation, Location

logger = getLogger(__name__)
config_log('info')

TEST_FOLDER = mkdtemp()
TEST_BUCKET = basename(TEST_FOLDER).replace('_', '')


def create_files() -> None:
    makedirs(TEST_FOLDER, exist_ok=True)
    write_file(join(TEST_FOLDER, 'test.txt'), 'Hello world!')
    save_yaml({'model': 'BERT' }, join(TEST_FOLDER, 'config.yml'))
    save_yaml({'tokenizer': 'RoBERTa'}, join(TEST_FOLDER, 'config2.yml'))
    makedirs(join(TEST_FOLDER, 'data'), exist_ok=True)
    save_json({'a': 1, 'b': 2, 'c': 3 }, join(TEST_FOLDER, 'data', 'data.json'))


def clean_test_files(bucket, *files):
    client = s3.connect()
    for file in files:
        s3.remove_sync_status(client.meta.endpoint_url, bucket, file)
        if file and exists(file):
            if isdir(file):
                rmtree(file)
            else:
                os.remove(file)
    if s3.exists(client, bucket):
        keys = s3.get_bucket_keys(client, bucket)
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=TEST_BUCKET)
    else:
        logger.warning("The bucket does not exist, nothing to remove")


def save_log_handle(filename, key: str, operation: Operation, location: Location) -> None:
    write_file(filename, f'The key or file {key} has been {operation.value} in {location.name}')


def on_finish_handle(filename: str, changes: List[Tuple[str, Operation, Location]]) -> None:
    write_file(filename, f'The last changes: {str(changes)}')


class MyTestCase(FileTestCase):
    def test_sync_local_empty(self):
        """ Test if the bucket synchronizes with an empty local folder """
        tmp_dir = None
        try:
            logger.info(f'Synchronizing bucket {TEST_BUCKET} to folder...')
            create_files()
            client = s3.connect()
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            s3.upload_file(join(TEST_FOLDER, 'test.txt'), client, TEST_BUCKET, 'test.txt')
            s3.upload_file(join(TEST_FOLDER, 'config.yml'), client, TEST_BUCKET, 'config.yml')
            s3.upload_file(join(TEST_FOLDER, 'config2.yml'), client, TEST_BUCKET, 'config2.yml')
            s3.upload_file(join(TEST_FOLDER, 'data', 'data.json'), client, TEST_BUCKET, 'data/data.json')
            files = {f for f in s3.get_bucket_keys(client, TEST_BUCKET)}
            self.assertSetEqual(files, {'test.txt', 'config.yml', 'config2.yml', 'data/data.json'})
            with removable_tmp(True) as tmp_dir:
                s3.sync(client, TEST_BUCKET, tmp_dir)
                files = [join(tmp_dir, f) for f in ['test.txt', 'config.yml', 'config2.yml', join('data', 'data.json')]]
                self.assertExists(*files)
                content = read_file(join(tmp_dir, 'test.txt'))
                self.assertEqual(len(content), 1)
                self.assertEqual(content[0], 'Hello world!')
                self.assertDictEqual(load_yaml(join(tmp_dir, 'config.yml')), {'model': 'BERT' })
                self.assertDictEqual(load_yaml(join(tmp_dir, 'config2.yml')), {'tokenizer': 'RoBERTa'})
                self.assertDictEqual(load_yaml(join(tmp_dir, 'data', 'data.json')), {'a': 1, 'b': 2, 'c': 3 })
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER, tmp_dir)

    def test_sync_empty_bucket(self):
        """ Test if the folder synchronizes with an empty bucket """
        try:
            logger.info(f'Synchronizing folder to the bucket {TEST_BUCKET}...')
            create_files()
            client = s3.connect()
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            s3.sync(client, TEST_BUCKET, TEST_FOLDER)
            files = {f for f in s3.get_bucket_keys(client, TEST_BUCKET)}
            self.assertSetEqual(files, {'test.txt', 'config.yml', 'config2.yml', 'data/data.json'})
            content = read_file(join(TEST_FOLDER, 'test.txt'))
            self.assertEqual(len(content), 1)
            self.assertEqual(content[0], 'Hello world!')
            self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'model': 'BERT' })
            self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'tokenizer': 'RoBERTa'})
            self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'data', 'data.json')), {'a': 1, 'b': 2, 'c': 3 })
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER)

    def test_local_and_bucket_synchronization(self):
        """ Test if the folder and bucket are synchronized even with changes """
        tmp_dir = None
        try:
            logger.info(f'Synchronizing folder to the bucket {TEST_BUCKET} with changes...')
            # Create an initial synchronization
            create_files()
            client = s3.connect()
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            s3.sync(client, TEST_BUCKET, TEST_FOLDER)
            files = {f for f in s3.get_bucket_keys(client, TEST_BUCKET)}
            self.assertSetEqual(files, {'test.txt', 'config.yml', 'config2.yml', 'data/data.json'})
            # Create a second synchronization to make the modification in the bucket
            with removable_tmp(True) as tmp_dir:
                s3.sync(client, TEST_BUCKET, tmp_dir)
                files = [join(tmp_dir, f) for f in ['test.txt', 'config.yml', 'config2.yml', join('data', 'data.json')]]
                self.assertExists(*files)
                # Create a modification in local and synchronize
                save_yaml({'new_model': 'RoBERTa'}, join(TEST_FOLDER, 'config.yml'))
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'new_model': 'RoBERTa'})
                # Create a modification in the bucket and synchronize
                s3.write_yaml({'new_model': 'LLaMa'}, client, TEST_BUCKET, 'config.yml')
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'new_model': 'LLaMa'})
                # Add a file in local and synchronize
                save_json({'tokens': [1, 2, 3]}, join(TEST_FOLDER, 'data', 'tokenizer.json'))
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                self.assertDictEqual(s3.read_json(client, TEST_BUCKET, 'data/tokenizer.json'), {'tokens': [1, 2, 3]})
                # Add a file in the bucket and synchronize
                s3.write_json({'vectors': [(1, 2, 3), (4, 5, 6)]}, client, TEST_BUCKET, 'data/model.json.gz')
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                self.assertDictEqual(
                    load_json(join(TEST_FOLDER, 'data', 'model.json.gz')), {'vectors': [[1, 2, 3], [4, 5, 6]]}
                )
                # Delete a file in local and synchronize
                os.remove(join(TEST_FOLDER, 'data', 'tokenizer.json'))
                self.assertNotExists(join(TEST_FOLDER, 'data', 'tokenizer.json'))
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='data/tokenizer.json')
                # Delete a file in the bucket and synchronize
                client.delete_object(Bucket=TEST_BUCKET, Key='data/model.json.gz')
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                self.assertNotExists(join(TEST_FOLDER, 'data', 'model.json.gz'))
                # Delete in local and modify in the bucket
                os.remove(join(TEST_FOLDER, 'config.yml'))
                s3.write_yaml({'Operation': 'deleted_in_local'}, client, TEST_BUCKET, 'config.yml')
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'Operation': 'deleted_in_local'})
                # Delete in the bucket and modify in local
                client.delete_object(Bucket=TEST_BUCKET, Key='config2.yml')
                save_yaml({'Operation': 'deleted_in_remote'}, join(TEST_FOLDER, 'config2.yml'))
                s3.sync(client, TEST_BUCKET, TEST_FOLDER)
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'Operation': 'deleted_in_remote'})
                # Modify in local and delete in the bucket

                # Modify in the bucket and delete in local
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER, tmp_dir)

    def test_s3_monitor_with_files(self):
        try:
            logger.info(f'Creating a bucket {TEST_BUCKET} monitor with a list of files...')
            # Create an initial synchronization
            create_files()
            client = s3.connect()
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            monitor = s3.Monitor(TEST_BUCKET, TEST_FOLDER, 2, {'config.yml'})
            monitor.start()
            try:
                time.sleep(1)
                # Modifying the local file
                save_yaml({'config': 'modify'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(2)
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify'})
                # Deleting the local file
                os.remove(join(TEST_FOLDER, 'config.yml'))
                time.sleep(2)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config.yml')
                # Creating a local file
                save_yaml({'config': 'modify 2'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(2)
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify 2'})
                # Modifying bucket file
                s3.write_json({'config': 'modify 3'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(2)
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 3'})
                # Deleting bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config.yml')
                time.sleep(2)
                self.assertNotExists(join(TEST_FOLDER, 'config.yml'))
                # Creating bucket file
                s3.write_yaml({'config': 'modify 4'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(2)
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 4'})
                # Creating a not monitored local file
                save_yaml({'config': 'modify 5'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(2)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Modifying a not monitored local file
                save_yaml({'config': 'modify 6'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(2)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Deleting a not monitored local file
                os.remove(join(TEST_FOLDER, 'config2.yml'))
                time.sleep(2)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Creating a not monitored bucket file
                s3.write_yaml({'config': 'modify 7'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(2)
                self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
                # Modifying a not monitored bucket file
                s3.write_yaml({'config': 'modify 8'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(2)
                self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
                # Deleting a not monitored bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config2.yml')
                time.sleep(2)
                self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
                # Finishing
                print('Waiting some seconds...')
                time.sleep(5)
            finally:
                monitor.stop()
                monitor.join()
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER)
        print('Finished')

    def test_s3_monitor(self):
        tmp_file = mktemp()
        try:
            logger.info(f'Creating a bucket {TEST_BUCKET} monitor without a list of files...')
            # Create an initial synchronization
            create_files()
            client = s3.connect()
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            with s3.Monitor(TEST_BUCKET, TEST_FOLDER, 2) as monitor:
                monitor.add_on_change_handle(partial(save_log_handle, tmp_file))
                monitor.add_on_finish_handle(partial(on_finish_handle, f'{tmp_file}.finish'))
                time.sleep(1)
                # Modifying the local file
                save_yaml({'config': 'modify'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been modified in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify'})
                # Deleting the local file
                os.remove(join(TEST_FOLDER, 'config.yml'))
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been deleted in LOCAL'])
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config.yml')
                # Creating a local file
                save_yaml({'config': 'modify 2'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been added in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify 2'})
                # Modifying bucket file
                s3.write_yaml({'config': 'modify 3'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been modified in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 3'})
                # Deleting bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config.yml')
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been deleted in BUCKET'])
                self.assertNotExists(join(TEST_FOLDER, 'config.yml'))
                # Creating bucket file
                s3.write_yaml({'config': 'modify 4'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been added in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 4'})
                # Creating a previously not monitored local file
                save_yaml({'config': 'modify 5'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config2.yml'), {'config': 'modify 5'})
                # Modifying a previously not monitored local file
                save_yaml({'config': 'modify 6'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config2.yml'), {'config': 'modify 6'})
                # Deleting a not monitored local file
                os.remove(join(TEST_FOLDER, 'config2.yml'))
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been deleted in LOCAL'])
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Creating a not monitored bucket file
                s3.write_yaml({'config': 'modify 7'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been added in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'config': 'modify 7'})
                # Modifying a not monitored bucket file
                s3.write_yaml({'config': 'modify 8'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'config': 'modify 8'})
                # Deleting a not monitored bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config2.yml')
                time.sleep(3)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been deleted in BUCKET'])
                self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
                # Finishing
                print('Waiting some seconds...')
                time.sleep(5)
        finally:
            if exists(f'{tmp_file}.finish'):
                self.assertListEqual(read_file(f'{tmp_file}.finish'), ['The last changes: []'])
                clean_test_files(TEST_BUCKET, TEST_FOLDER, f'{tmp_file}.finish', tmp_file)
            else:
                clean_test_files(TEST_BUCKET, TEST_FOLDER, tmp_file)
        print('Finished')

    def test_readonly(self):
        try:
            logger.info(f'Creating a readonly bucket {TEST_BUCKET} and syncrhonize it...')
            # Create an initial synchronization to start to work with
            client = s3.connect()
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            s3.write_yaml({'model': 'BERT', 'threshold': 0.8}, client, TEST_BUCKET, 'config.yml')
            s3.write_json({'data': [1, 2, 3, 4, 5]}, client, TEST_BUCKET, 'data.json')
            s3.sync(client, TEST_BUCKET, TEST_FOLDER, readonly=True)
            self.assertDictEqual({'model': 'BERT', 'threshold': 0.8}, load_yaml(join(TEST_FOLDER, 'config.yml')))
            self.assertDictEqual({'data': [1, 2, 3, 4, 5]}, load_json(join(TEST_FOLDER, 'data.json')))
            # Removing a local file
            os.remove(join(TEST_FOLDER, 'config.yml'))
            s3.sync(client, TEST_BUCKET, TEST_FOLDER, readonly=True)
            self.assertTrue(s3.exists(client, TEST_BUCKET, 'config.yml'))
            self.assertNotExists(join(TEST_FOLDER, 'config.yml'))
            # Creating a local file
            save_yaml({'model': 'ROBERTA'}, join(TEST_FOLDER, 'config2.yml'))
            s3.sync(client, TEST_BUCKET, TEST_FOLDER, readonly=True)
            self.assertFalse(s3.exists(client, TEST_BUCKET, 'config2.yml'))
            # Modifying a local file
            save_yaml({'model': 'LLaMA'}, join(TEST_FOLDER, 'config.yml'))
            s3.sync(client, TEST_BUCKET, TEST_FOLDER, readonly=True)
            self.assertDictEqual({'model': 'BERT', 'threshold': 0.8},
                                 s3.read_yaml(client, TEST_BUCKET, 'config.yml'))

            # # Create an initial synchronization to start to work with
            # create_files()
            # client = s3.connect()
            # client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            # s3.sync(client, TEST_BUCKET, TEST_FOLDER)
            # # Check if the files exist after the synchronization
            # self.assertTrue(s3.exists(client, TEST_BUCKET, 'test.txt', 'config.yml', 'config2.yml', 'data/data.json'))
            # # Check if exists() function works properly
            # self.assertFalse(s3.exists(client, TEST_BUCKET, 'test.txt', 'config.yml', 'config2.yml', 'data.json'))
            # # Make modifications in the s3 bucket
            # s3.write_yaml({'test': 'Hello World'}, client, TEST_BUCKET, 'test.yml')  # Create file
            # s3.write_yaml({'model': 'MyNewModel'}, client, TEST_BUCKET, 'config.yml')  # Modify file
            # client.delete_object(Bucket=TEST_BUCKET, Key='test.txt')
            # # Checking the readonly synchronization when the bucket is modified
            # s3.sync(client, TEST_BUCKET, TEST_FOLDER, readonly=True)
            # # Check the created file
            # self.assertDictEqual({'test': 'Hello World'}, load_yaml(join(TEST_FOLDER, 'test.yml')))
            # # Check the modified file
            # self.assertDictEqual({'model': 'MyNewModel'}, load_yaml(join(TEST_FOLDER, 'config.yml')))
            # self.assertNotExists(join(TEST_FOLDER, 'test.txt'))  # Check deleted file
            # TODO: finish the unit tests with readonly buckets
            # TODO: if a local file is modified, it must be restored from the bucket copy in readonly mode
            # TODO: if a local file is deleted, it must be restored from the bucket copy in readonly mode
            # TODO: check the conditions when a less updated local file is synchronized in readonly mode
            # Check with a monitor
            # with s3.Monitor(TEST_BUCKET, TEST_FOLDER, 2, readonly=True) as monitor:
            #     monitor.add_on_change_handle(partial(save_log_handle, tmp_file))
            #     monitor.add_on_finish_handle(partial(on_finish_handle, f'{tmp_file}.finish'))
            #     time.sleep(1)
            #     # Modifying the local file
            #     save_yaml({'config': 'modify'}, join(TEST_FOLDER, 'config.yml'))
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been modified in LOCAL'])
            #     self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify'})
            #     # Deleting the local file
            #     os.remove(join(TEST_FOLDER, 'config.yml'))
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been deleted in LOCAL'])
            #     with self.assertRaises(ClientError):
            #         client.head_object(Bucket=TEST_BUCKET, Key='config.yml')
            #     # Creating a local file
            #     save_yaml({'config': 'modify 2'}, join(TEST_FOLDER, 'config.yml'))
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been added in LOCAL'])
            #     self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify 2'})
            #     # Modifying bucket file
            #     s3.write_yaml({'config': 'modify 3'}, client, TEST_BUCKET, 'config.yml')
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been modified in BUCKET'])
            #     self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 3'})
            #     # Deleting bucket file
            #     client.delete_object(Bucket=TEST_BUCKET, Key='config.yml')
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been deleted in BUCKET'])
            #     self.assertNotExists(join(TEST_FOLDER, 'config.yml'))
            #     # Creating bucket file
            #     s3.write_yaml({'config': 'modify 4'}, client, TEST_BUCKET, 'config.yml')
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been added in BUCKET'])
            #     self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 4'})
            #     # Creating a previously not monitored local file
            #     save_yaml({'config': 'modify 5'}, join(TEST_FOLDER, 'config2.yml'))
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in LOCAL'])
            #     self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config2.yml'), {'config': 'modify 5'})
            #     # Modifying a previously not monitored local file
            #     save_yaml({'config': 'modify 6'}, join(TEST_FOLDER, 'config2.yml'))
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in LOCAL'])
            #     self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config2.yml'), {'config': 'modify 6'})
            #     # Deleting a not monitored local file
            #     os.remove(join(TEST_FOLDER, 'config2.yml'))
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been deleted in LOCAL'])
            #     with self.assertRaises(ClientError):
            #         client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
            #     # Creating a not monitored bucket file
            #     s3.write_yaml({'config': 'modify 7'}, client, TEST_BUCKET, 'config2.yml')
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been added in BUCKET'])
            #     self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'config': 'modify 7'})
            #     # Modifying a not monitored bucket file
            #     s3.write_yaml({'config': 'modify 8'}, client, TEST_BUCKET, 'config2.yml')
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in BUCKET'])
            #     self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'config': 'modify 8'})
            #     # Deleting a not monitored bucket file
            #     client.delete_object(Bucket=TEST_BUCKET, Key='config2.yml')
            #     time.sleep(3)
            #     self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been deleted in BUCKET'])
            #     self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
            #     # Finishing
            #     print('Waiting some seconds...')
            #     time.sleep(5)
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER)

    def test_sync_partial_files(self):
        """ Test if the bucket synchronizes with an empty local folder """
        try:
            files_to_sync = {'config.yml'}
            logger.info(f'Synchronizing just one file in bucket {TEST_BUCKET}...')
            create_files()
            client = s3.connect()
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            s3.sync(client, TEST_BUCKET, TEST_FOLDER, files_to_sync)
            files = {f for f in s3.get_bucket_keys(client, TEST_BUCKET)}
            self.assertEqual(len(files), 1)
            self.assertSetEqual(files, {'config.yml'})
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER)

    def test_exists(self) -> None:
        """ Test if the bucket synchronizes with an empty local folder """
        try:
            logger.info(f'Testing the exist() function with the bucket {TEST_BUCKET}...')
            create_files()
            client = s3.connect()
            self.assertFalse(s3.exists(client, TEST_BUCKET))
            self.assertFalse(s3.exists(client, TEST_BUCKET, 'test.txt'))
            self.assertFalse(s3.exists(client, TEST_BUCKET, 'config.yml'))
            self.assertFalse(s3.exists(client, TEST_BUCKET, 'config2.yml'))
            self.assertFalse(s3.exists(client,  TEST_BUCKET, 'data/data.json'))
            client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
            s3.sync(client, TEST_BUCKET, TEST_FOLDER)
            self.assertTrue(s3.exists(client, TEST_BUCKET))
            self.assertTrue(s3.exists(client, TEST_BUCKET, 'test.txt'))
            self.assertTrue(s3.exists(client, TEST_BUCKET, 'config.yml'))
            self.assertTrue(s3.exists(client, TEST_BUCKET, 'config2.yml'))
            self.assertTrue(s3.exists(client, TEST_BUCKET, 'data/data.json'))
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER)

    def test_split(self) -> None:
        logger.info(f'Testing the split() function with the bucket {TEST_BUCKET}...')
        self.assertTupleEqual(s3.split('s3://my_bucket/file.txt'), ('s3://', 'my_bucket', 'file.txt'))
        self.assertTupleEqual(s3.split('my_bucket/file.txt'), ('', 'my_bucket', 'file.txt'))
        self.assertTupleEqual(s3.split('my_bucket/'), ('', 'my_bucket', ''))
        self.assertTupleEqual(s3.split('my_bucket'), ('', 'my_bucket', ''))
        self.assertTupleEqual(s3.split('s3://my_bucket'), ('s3://', 'my_bucket', ''))
        self.assertTupleEqual(s3.split('s3://my_bucket/'), ('s3://', 'my_bucket', ''))

    def test_remove(self) -> None:
        logger.info(f'Testing the synchronization with temporal folder...')
        create_files()
        client = s3.connect()
        client.create_bucket(ACL='private', Bucket=TEST_BUCKET)
        monitor = s3.Monitor(TEST_BUCKET, TEST_FOLDER, 2, remove=True)
        monitor.start()
        try:
            time.sleep(1)
            # Modifying the local file
            save_yaml({'config': 'modify'}, join(TEST_FOLDER, 'config.yml'))
            time.sleep(3)
            self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify'})
            # Deleting the local file
            os.remove(join(TEST_FOLDER, 'config.yml'))
            time.sleep(3)
            with self.assertRaises(ClientError):
                client.head_object(Bucket=TEST_BUCKET, Key='config.yml')
            # Creating a local file
            save_yaml({'config': 'modify 2'}, join(TEST_FOLDER, 'config.yml'))
            time.sleep(3)
            self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify 2'})
            # Modifying bucket file
            s3.write_json({'config': 'modify 3'}, client, TEST_BUCKET, 'config.yml')
            time.sleep(3)
            self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 3'})
            # Deleting bucket file
            client.delete_object(Bucket=TEST_BUCKET, Key='config.yml')
            time.sleep(3)
            self.assertNotExists(join(TEST_FOLDER, 'config.yml'))
            # Creating bucket file
            s3.write_yaml({'config': 'modify 4'}, client, TEST_BUCKET, 'config.yml')
            time.sleep(3)
            self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 4'})
        finally:
            try:
                self.assertExists(s3.status_file(client.meta.endpoint_url, TEST_BUCKET, TEST_FOLDER, Location.BUCKET))
                self.assertExists(s3.status_file(client.meta.endpoint_url, TEST_BUCKET, TEST_FOLDER, Location.LOCAL))
                self.assertExists(TEST_FOLDER)
                monitor.stop()
                monitor.join()
                self.assertNotExists(s3.status_file(client.meta.endpoint_url, TEST_BUCKET, TEST_FOLDER, Location.BUCKET))
                self.assertNotExists(s3.status_file(client.meta.endpoint_url, TEST_BUCKET, TEST_FOLDER, Location.LOCAL))
                self.assertNotExists(TEST_FOLDER)
            finally:
                clean_test_files(TEST_BUCKET, TEST_FOLDER)

    # TODO: Check synchronizing buckets because it does not work very well
    # TODO: Check synchronizing big files with several parts


if __name__ == '__main__':
    unittest.main()
