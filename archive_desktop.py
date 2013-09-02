import argparse
import datetime
import logging
import os.path
import subprocess
import tarfile
import tempfile
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key


logger = logging.getLogger(__name__)


class DesktopArchiveManager(object):
    def __init__(
        self, bucket, aws_access_key, aws_secret_key, local_path, remote_path
    ):
        self.local_path = local_path
        self.remote_path = remote_path

        self.boto = S3Connection(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,

        )
        self.bucket = self.boto.get_bucket(bucket)

    def archive_files(self):
        archive_path = self.create_archive(self.local_path)
        index_path = self.create_index_from_archive(archive_path)

        self.create_directory(self.get_prefix())
        archive = self.store_file_at_path(
            archive_path,
            self.get_prefix('archive.tar.gz'),
        )
        logger.info(
            'Archive stored: %s',
            archive.generate_url(24*60*60)
        )
        index = self.store_file_at_path(
            index_path,
            self.get_prefix('index.tsv.txt'),
        )
        logger.info(
            'Index stored: %s',
            index.generate_url(24*60*60)
        )
        logger.info('Files archived')

    def store_file_at_path(self, local_path, remote_path):
        logger.debug('Uploading file %s to %s.' % (local_path, remote_path))
        key = Key(self.bucket)
        key.key = remote_path
        key.set_contents_from_filename(
            local_path,
            {
                'Content-Type': 'application/octet-stream',
                'x-amz-meta-mtime': str(int(time.time())),
            },
            policy='private',
        )
        return key

    def get_prefix(self, *components):
        return os.path.join(
            self.remote_path,
            datetime.datetime.now().strftime('%Y-%m-%d'),
            *components
        )

    def create_directory(self, prefix):
        key = Key(self.bucket)
        key.key = prefix
        key.set_contents_from_string(
            '',
            {
                'Content-Type': 'application/x-directory',
                'x-amz-meta-gid': '20',
                'x-amz-meta-uid': '501',
                'x-amz-meta-mode': '16877',
                'x-amz-meta-mtime': str(int(time.time()))
            },
            policy='private',
        )
        logger.debug('Created directory %s.' % prefix)

    def create_archive(self, path):
        logger.debug('Creating archive.')
        _, tempfilename = tempfile.mkstemp()

        proc = subprocess.Popen(
            'tar -zcvf %s *' % tempfilename,
            cwd=os.path.realpath(path),
            shell=True,
        )
        logger.debug('Waiting for archive to be completed.')
        proc.wait()
        logger.debug('Archive written to %s.', tempfilename)

        return tempfilename

    def create_index_from_archive(self, archive_path):
        logger.debug('Generating archive index.')
        _, tempfilename = tempfile.mkstemp()

        archive = tarfile.open(archive_path)
        with open(tempfilename, 'w') as out:
            for member in archive.getmembers():
                out.write(
                    u'%s\t%s\n' % (
                        member.name,
                        member.size,
                    )
                )

        logger.debug('Index written to %s.', tempfilename)
        return tempfilename


def get_access_key_and_secret_from_s3fs():
    with open(os.path.expanduser('~/.passwd-s3fs'), 'r') as in_:
        contents = in_.read()
    return contents.strip().split(':')


if __name__ == '__main__':
    try:
        access_default, secret_default = get_access_key_and_secret_from_s3fs()
    except IOError:
        access_default = None
        key_default = None

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'bucket',
        type=str,
        nargs=1,
    )
    parser.add_argument(
        '--path',
        dest='path',
        type=str,
        default=os.path.expanduser('~/Desktop/')
    )
    parser.add_argument(
        '--aws-access-key',
        dest='aws_access_key',
        type=str,
        default=access_default,
    )
    parser.add_argument(
        '--aws-secret-key',
        dest='aws_secret_key',
        type=str,
        default=secret_default,
    )
    parser.add_argument(
        '--bucket-prefix',
        dest='bucket_prefix',
        type=str,
        default='desktop',
    )
    parser.add_argument(
        '--loglevel',
        dest='loglevel',
        type=str,
        default='INFO',
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.getLevelName(args.loglevel)
    )

    manager = DesktopArchiveManager(
        args.bucket[0],
        aws_access_key=args.aws_access_key,
        aws_secret_key=args.aws_secret_key,
        local_path=args.path,
        remote_path=args.bucket_prefix,
    )
    manager.archive_files()
