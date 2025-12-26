# START OF FILE cli.py

from argparse import ArgumentParser

parser = ArgumentParser(prog="era", usage="Provide a path to the excel file to convert and the username.")
parser.add_argument("path")
# Add a required argument for the username
parser.add_argument("--user", required=True, help="The username from the 'users' collection to associate with the import.")
args = parser.parse_args()