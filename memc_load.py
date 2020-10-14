#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import glob
import gzip
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from optparse import OptionParser
from queue import Queue

# pip install python-memcached
import memcache

# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_apps_installed(memc_addr, queue, errors, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    memc_client = memcache.Client([memc_addr])
    while True:
        apps_installed = queue.get()
        if not apps_installed:
            return
        ua.lat = apps_installed.lat
        ua.lon = apps_installed.lon
        key = "%s:%s" % (apps_installed.dev_type, apps_installed.dev_id)
        ua.apps.extend(apps_installed.apps)
        packed = ua.SerializeToString()
        try:
            if dry_run:
                logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
            else:
                memc_client.set(key, packed)
        except Exception as e:
            logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
            errors.put(1)


def parse_apps_installed(line):
    line_parts = line.decode().strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def process_line(line, device_memc, errors):
    apps_installed = parse_apps_installed(line)
    if not apps_installed:
        logging.info('apps_installed is None')
        errors.put(1)
        return
    queue = device_memc.get(apps_installed.dev_type)
    if not queue:
        logging.error("Unknown device type: %s" % apps_installed.dev_type)
        errors.put(1)
        return
    queue.put(apps_installed)


def main(options):
    devices = ['idfa', 'gaid', 'adid', 'dvid']
    for fn in glob.iglob(options.pattern):
        total = 0
        errors_queue = Queue()
        pool = ThreadPoolExecutor(len(devices))
        installers = []
        device_memc = dict()
        for device in devices:
            queue = Queue()
            device_memc[device] = queue
            installer = pool.submit(insert_apps_installed, getattr(options, device), queue, errors_queue, options.dry)
            installers.append(installer)

        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)
        parsers = []
        for line in fd:
            line = line.strip()
            if not line:
                continue
            total += 1
            with ThreadPoolExecutor(max_workers=int(options.workers)) as executor:
                parsers.append(executor.submit(process_line, line, device_memc, errors_queue))

        logging.info('Wait while all lines parsed')
        wait(parsers)
        for queue in device_memc.values():
            queue.put(None)
        logging.info('Wait while all apps inserted')
        wait(installers)

        errors = 0
        while not errors_queue.empty():
            errors += errors_queue.get()

        logging.info(f'Parsed {total} lines')
        logging.info(f'Found {errors} errors')
        err_rate = float(errors) / total
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successful load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)


def proto_test():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-w", "--workers", action="store", default=10)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        proto_test()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
