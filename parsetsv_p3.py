#!/usr/bin/env python3
# coding:utf-8

import signal
import sys
import os
import glob
import logging
import logging.handlers
import csv
import datetime
import click
import pickle
import struct
import tempfile
import shutil


def write_file(iterable, output_filename):
    with tempfile.TemporaryDirectory(
            suffix='_tsv', prefix='tmp_', dir='/var/tmp') as temp_dir:
        fname = write_str_into_file(iterable, temp_dir)
        shutil.move(fname, output_filename)


def write_str_into_file(iterable, dir_name):
    with tempfile.NamedTemporaryFile(delete=False, dir=dir_name,) as f:
        for row in iterable:
            f.write(row)
        return f.name


class SignalException(Exception):
    def __init__(self, message):
        super(SignalException, self).__init__(message)


def do_exit(sig, stack):
    raise SignalException("Exiting")


class ParseRowsTsv(object):

    def __init__(
        self, file, inputf, outputf
            ):
        self.inputf = os.path.abspath(os.path.expanduser(inputf))
        self.outputf = os.path.abspath(os.path.expanduser(outputf))
        self.file = file

    def write_into_file(self):
        if self.file == 'pickle':
            write_file(self.pickle_tsv(), self.outputf)
        elif self.file == 'struct':
            write_file(self.struct_tsv(), self.outputf)

    def read_tsv(self):
        with open(self.inputf, "r") as f:
            reader = csv.reader(f, delimiter="\t", lineterminator='\n')
            yield next(reader)
            for row in reader:
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

    def pickle_tsv(self):
        for record in self.read_tsv():
            yield pickle.dumps(record)

    def struct_tsv(self):
        lines = self.read_tsv()
        line = lines.next()
        inits = struct.Struct(
            's '.join(
                [str(len(line[i])) for i in range(9)]) + 's')
        yield inits.pack(*line)
        for record in lines:
            s = struct.Struct(
                'i h l d ? %ds %ds %ds %ds' % (
                    len(record[5]), len(record[6]),
                    len(record[7]), len(record[8]),
                    )
                )
            yield s.pack(*record)


@click.command()
@click.option(
    '--file', type=click.Choice(['pickle', 'struct']),
    default='pickle')
@click.option('-i', '--inputf', default='~/kadai_1.tsv')
@click.option('-o', '--outputf', default='~/zone/kadai_2v2.p')
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
        parser.write_into_file()

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
