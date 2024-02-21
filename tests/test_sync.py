import os
import time
import unittest
from os import makedirs
from os.path import basename, join, exists
from shutil import rmtree
from tempfile import mkdtemp, mktemp
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


def clean_test_files(bucket, *folders):
    client = s3.connect()
    for folder in folders:
        s3.remove_sync_status(client.meta.endpoint_url, bucket, folder)
        if folder and exists(folder):
            rmtree(folder)
    try:
        keys = s3.get_bucket_keys(client, bucket)
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=TEST_BUCKET)
    except client.exceptions.NoSuchBucket:
        logger.warning("The bucket does not exist, nothing to remove")


def save_log_handle(filename, key: str, operation: Operation, location: Location) -> None:
    write_file(filename, f'The key or file {key} has been {operation.value} in {location.name}')


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
            monitor = s3.Monitor(TEST_BUCKET, TEST_FOLDER, 5, {'config.yml'})
            monitor.start()
            try:
                time.sleep(1)
                # Modifying the local file
                save_yaml({'config': 'modify'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(6)
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify'})
                # Deleting the local file
                os.remove(join(TEST_FOLDER, 'config.yml'))
                time.sleep(6)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config.yml')
                # Creating a local file
                save_yaml({'config': 'modify 2'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(6)
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify 2'})
                # Modifying bucket file
                s3.write_json({'config': 'modify 3'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(6)
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 3'})
                # Deleting bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config.yml')
                time.sleep(6)
                self.assertNotExists(join(TEST_FOLDER, 'config.yml'))
                # Creating bucket file
                s3.write_yaml({'config': 'modify 4'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(6)
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 4'})
                # Creating a not monitored local file
                save_yaml({'config': 'modify 5'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(6)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Modifying a not monitored local file
                save_yaml({'config': 'modify 6'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(6)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Deleting a not monitored local file
                os.remove(join(TEST_FOLDER, 'config2.yml'))
                time.sleep(6)
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Creating a not monitored bucket file
                s3.write_yaml({'config': 'modify 7'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(6)
                self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
                # Modifying a not monitored bucket file
                s3.write_yaml({'config': 'modify 8'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(6)
                self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
                # Deleting a not monitored bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config2.yml')
                time.sleep(6)
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
            with s3.Monitor(TEST_BUCKET, TEST_FOLDER, 5) as monitor:
                monitor.add(partial(save_log_handle, tmp_file))
                time.sleep(1)
                # Modifying the local file
                save_yaml({'config': 'modify'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been modified in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify'})
                # Deleting the local file
                os.remove(join(TEST_FOLDER, 'config.yml'))
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been deleted in LOCAL'])
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config.yml')
                # Creating a local file
                save_yaml({'config': 'modify 2'}, join(TEST_FOLDER, 'config.yml'))
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been added in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config.yml'), {'config': 'modify 2'})
                # Modifying bucket file
                s3.write_yaml({'config': 'modify 3'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been modified in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 3'})
                # Deleting bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config.yml')
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been deleted in BUCKET'])
                self.assertNotExists(join(TEST_FOLDER, 'config.yml'))
                # Creating bucket file
                s3.write_yaml({'config': 'modify 4'}, client, TEST_BUCKET, 'config.yml')
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config.yml has been added in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config.yml')), {'config': 'modify 4'})
                # Creating a previously not monitored local file
                save_yaml({'config': 'modify 5'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config2.yml'), {'config': 'modify 5'})
                # Modifying a previously not monitored local file
                save_yaml({'config': 'modify 6'}, join(TEST_FOLDER, 'config2.yml'))
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in LOCAL'])
                self.assertDictEqual(s3.read_yaml(client, TEST_BUCKET, 'config2.yml'), {'config': 'modify 6'})
                # Deleting a not monitored local file
                os.remove(join(TEST_FOLDER, 'config2.yml'))
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been deleted in LOCAL'])
                with self.assertRaises(ClientError):
                    client.head_object(Bucket=TEST_BUCKET, Key='config2.yml')
                # Creating a not monitored bucket file
                s3.write_yaml({'config': 'modify 7'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been added in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'config': 'modify 7'})
                # Modifying a not monitored bucket file
                s3.write_yaml({'config': 'modify 8'}, client, TEST_BUCKET, 'config2.yml')
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been modified in BUCKET'])
                self.assertDictEqual(load_yaml(join(TEST_FOLDER, 'config2.yml')), {'config': 'modify 8'})
                # Deleting a not monitored bucket file
                client.delete_object(Bucket=TEST_BUCKET, Key='config2.yml')
                time.sleep(6)
                self.assertListEqual(read_file(tmp_file), ['The key or file config2.yml has been deleted in BUCKET'])
                self.assertNotExists(join(TEST_FOLDER, 'config2.yml'))
                # Finishing
                print('Waiting some seconds...')
                time.sleep(5)
        finally:
            clean_test_files(TEST_BUCKET, TEST_FOLDER)
        print('Finished')

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


if __name__ == '__main__':
    unittest.main()
