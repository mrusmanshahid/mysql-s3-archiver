from archiver import Archiver
import logging
import argparse

if __name__ == "__main__":

    logging.basicConfig(
        level = logging.INFO, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser("")
    parser.add_argument("config_file", help="Please provide an absolute path of config file to pick", type=str)
    args = parser.parse_args()
    archiver =  Archiver()
    archiver.read_config(args.config_file)
    archiver.run_archiver()
