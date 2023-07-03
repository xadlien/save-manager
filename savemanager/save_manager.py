import time
import argparse
import os
import datetime
import shutil
from savemanager.save import Save, LocalSave, AWSSave

#error codes:
#   1 -d directory does not exist


def main():

    # init parser
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-key", required=False, action="store", default=None, help="Access Key ID for S3 sync")
    parser.add_argument("--access-secret", required=False, action="store", default=None, help="Access Key Secret for S3 sync")
    parser.add_argument("-d", "--directory", required=True, action="store", help="directory to backup from")
    parser.add_argument("-b", "--backup-uri", required=True, action="store", default=None, help="uri to backup to, e.g. s3://..., or file://...")
    
    # parse arguments
    args = parser.parse_args()

    # create save based on uri given
    protocol = args.backup_uri.split(':')[0]

    if protocol == "file":
        path = args.backup_uri[7:]
        save = LocalSave(path, args.directory)
    elif protocol == "s3":
        if args.access_key is None or args.access_secret is None:
            print("--access-key and --access-secret required for s3 sync")
            exit(1)
        save = AWSSave(args.access_key, args.access_secret, args.backup_uri, args.directory)
    else:
        print("Only the following protocols are supported:")
        print("  * file")
        print("  * s3")
        print(f"you specified {protocol}")
        exit(1)

    # run sync
    save.sync()

main()