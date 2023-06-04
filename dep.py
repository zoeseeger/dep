#!/usr/bin/env python

import sys
import argparse

from src.options import Options
from src.ddl import DdlInput
from src.dag import DagInput
from src.doc import DocInput


class Main():

    def __init__(self):

        self.readInputParams()
        self.determineRunOptions()

    def readInputParams(self):
        """Read input command line parameters."""

        parser = argparse.ArgumentParser(description='Data Engineering Processor!', prog='dep')
        parser.add_argument('--dryrun', action='store_true', default=False, help='generate only vars.json')
        parser.add_argument('-j', '--json', type=str, default="input.json", help='input json')
        parser.add_argument('-d', '--doc', type=str, default="*.doc", help='input word doc from confluence')
        parser.add_argument('-o', '--outdir', type=str, default="dep-output", help='output directory')
        parser.add_argument('--nojson', action='store_true', help='don\'t read in json input')
        parser.add_argument('--nodoc', action='store_true', help='don\'t read in doc input')
        parser.add_argument('--debug', action='store_true', help='don\'t make html if exists')
        self.args = parser.parse_args()

    def determineRunOptions(self):
        """Determine the options for the files."""

        # get default options
        self.options = Options(self.args.outdir, debug=self.args.debug)

        # read confluence
        if not self.args.nodoc:
            self.options.convertDocToHtmlSoup(self.args.doc)
            self.options.parseHtml()

        # determine dependent variable values
        self.options.defineVarsFromInput()

        # read input file
        if not self.args.nojson:
            self.options.readInputJson(self.args.json)
            self.options.updateOptionsFromJson()

        # check some input given
        self.options.checkInput()

    def writeOptions(self):
        """Write options to vars.json."""

        self.options.writeVarsToFile()

        if self.args.dryrun:
            sys.exit('Dry run complete.')

    def createFiles(self):
        """Make and write output files."""

        # create staging and tmp table_structures
        self.options.stagingTableStructure()
        self.options.tmpTableStructure()

        # make files
        self.ddl_input = DdlInput(self.options)
        self.dag_input = DagInput(self.options)
        self.doc_input = DocInput(self.options)

    def run(self):
        """Put vars into files."""

        self.writeOptions()
        self.createFiles()


if __name__ == "__main__":
    main = Main()
    main.run()