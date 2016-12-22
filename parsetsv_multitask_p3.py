#!/usr/bin/env python3
# coding:utf-8

import signal
import sys
import os
import glob
import logging
import logging.handlers
import datetime
import click
import pickle
import struct
import tempfile
import shutil
import math
import concurrent.futures
import errno


class SignalException(Exception):
    def __init__(self, message):
        super(SignalException, self).__init__(message)


def do_exit(sig, stack):
    raise SignalException("Exiting")


# ファイルを分割し、index、offset、offset + lengthを返す。
def tsv_separate_generator(inputf):
    CHUNK_SIZE = 1024 * 1024 * 100
    with open(inputf, 'rb') as f:
        f_size = os.stat(f.fileno()).st_size
        split_count = math.ceil(f_size / CHUNK_SIZE)
        start_offset = len(f.readline())
        for split_idx in range(split_count):
            offset = CHUNK_SIZE * (split_idx + 1) - 1
            f.seek(offset)
            last_line_len = len(f.readline())
            if offset < f_size:
                end_offset = offset + last_line_len
            else:
                end_offset = f_size
            yield (
                split_idx,
                start_offset,
                end_offset,
            )
            if end_offset >= f_size or last_line_len == 0:
                break
            start_offset = end_offset


def sum_file(self, files):
    with tempfile.NamedTemporaryFile(delete=False, dir='/var/tmp/',) as f:
        s = 0
        for file in self.files:
            with open(file) as f1:
                os.sendfile(f.fileno(), f1.fileno(), s)
            s += os.stat(file).st_size
        return f.name


class ReadTsvGenerator(object):

    def __init__(self, inputf, iterable):
        self.inputf = inputf
        self.iterable = iterable

    def read_tsv(self):
        with open(self.inputf, "rb") as f:
            start_offset = self.iterable[1],
            end_offset = self.iterable[2],
            f.seek(start_offset[0])
            start = start_offset[0]
            while start < end_offset[0]:
                row = f.readline()
                start += len(row)
                row = [
                    i.decode(
                        'utf-8'
                    ) for i in row.strip(b'\n').split(b'\t')
                    ]
                row = (
                    int(row[0]),
                    int(row[1]),
                    int(row[2]),
                    float(row[3]),
                    int(row[4]),
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                )
                yield row


class ParseTsvGenerator(object):
    def __init__(self, iterable):
        self.iterable = iterable

    def pickle_tsv(self):
        lines = self.iterable
        next(lines)
        for record in lines:
            yield pickle.dumps(record)

    def struct_tsv(self):
        lines = self.iterable
        next(lines)
        for record in lines:
            s = struct.Struct(
                'i h l d ? %ds %ds %ds %ds' % (
                    len(record[5]), len(record[6]),
                    len(record[7]), len(record[8]),
                    )
                )
            yield s.pack(*record)


class ParseRowsTsv(object):

    def __init__(self, file, inputf, outputf):
        self.file = file
        self.inputf = os.path.abspath(os.path.expanduser(inputf))
        self.outputf = os.path.abspath(os.path.expanduser(outputf))

    # 単一タスク
    def dotask(self, rule):
        parsetsv = ParseTsvGenerator(
            ReadTsvGenerator(self.inputf, rule).read_tsv())
        if self.file == 'pickle':
            w = parsetsv.pickle_tsv()
        elif self.file == 'struct':
            w = parsetsv.struct_tsv()
        with tempfile.NamedTemporaryFile(
            delete=False, dir='/var/tmp', suffix='_dotask', prefix='tmp_',
                ) as f:
            for row in w:
                f.write(row)
            return f.name

    # マルチプロセス
    def multi_do_task(self):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            future_to_tsv = {
                executor.submit(
                    self.dotask, rule
                ): rule for rule in tsv_separate_generator(self.inputf)}
            with tempfile.TemporaryDirectory(
                    suffix='_tsv', prefix='tmp_', dir='/var/tmp') as temp_dir:
                with tempfile.NamedTemporaryFile(
                        suffix='_tsv', prefix='tmp_',
                        delete=False, dir=temp_dir,) as f:
                    s = 0
                    for future in concurrent.futures.as_completed(
                            future_to_tsv):
                        chunk = future_to_tsv[future][2] - \
                            future_to_tsv[future][1]
                        with open(future.result()) as separatefile:
                            os.sendfile(
                                f.fileno(), separatefile.fileno(), s, chunk)
                            s += os.stat(separatefile.fileno()).st_size
                        try:
                            os.remove(separatefile.name)
                        except OSError as exc:
                            if exc.errno != errno.ENOENT:
                                raise
                    shutil.move(f.name, self.outputf)


@click.command()
@click.option(
    '--file', type=click.Choice(['pickle', 'struct']),
    default='pickle')
@click.option('-i', '--inputf', default='~/kadai_1.tsv')
@click.option('-o', '--outputf', default='~/zone/kadai_2v3.p')
def cmd(file, inputf, outputf):
    s = datetime.datetime.now()
    print(s + datetime.timedelta(0, 0, 0, 0, 0, 9))
    # シグナル
    signal.signal(signal.SIGINT, do_exit)
    signal.signal(signal.SIGHUP, do_exit)
    signal.signal(signal.SIGTERM, do_exit)
    # ログハンドラーを設定する
    LOG_MANYROWSTSV = 'logging_warning.out'
    my_logger = logging.getLogger('MyLogger')
    my_logger.setLevel(logging.WARNING)
    handler = logging.handlers.RotatingFileHandler(
        LOG_MANYROWSTSV, maxBytes=2000, backupCount=5,)
    my_logger.addHandler(handler)

    parser = ParseRowsTsv(file, inputf, outputf)

    try:
        parser.multi_do_task()

    except SignalException as e1:
        my_logger.warning('%s: %s' % (e1, datetime.datetime.now()))
        logfiles = glob.glob('%s*' % LOG_MANYROWSTSV)
        print(logfiles)
        sys.exit(1)
    finally:
        e = datetime.datetime.now()
        print(str(e-s))


def main():
    cmd()


if __name__ == '__main__':
    main()
