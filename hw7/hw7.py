import argparse
import re
import time
import os 
import random

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromTextWithFilename

from apache_beam.io import fileio

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import google.cloud.pubsub as pubsub
import google.cloud.storage as storage
import google.cloud.logging as logging
import google.cloud.pubsub_v1 as pubsub_v1

from flask import Flask, request

import numpy as np
import pandas as pd 


def list_of_blobs():
    """Lists all the blobs in the bucket."""
    paths = []

    bucket_name="ds561-tigeryi-hw7"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix="files/")
    
    for blob in blobs:
        paths.append(f"{blob.name}")
    return paths


class ReadFileContent(beam.DoFn):
    def setup(self):
        self.storage_client = storage.Client()
    def process(self, filename):
        bucket_name="ds561-tigeryi-hw7"
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.get_blob(filename)
        content = blob.download_as_string().decode("utf-8")
        links = re.findall(r'<a HREF="(\d+).html">', content)
        links_int = [int(x) for x in links]
        filename_int = int(blob.name.split(".")[0].split("/")[1])
        yield (filename_int, links_int)


def main(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://ds561-tigeryi-hw7/files/*.html',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      # required=True,
      default='gs://ds561-tigeryi-hw7/output/result.txt',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as pipeline:
    
    files_path = known_args.input
    list_files = list_of_blobs()

    # Read the text file[pattern] into a PCollection.
    # lines = p | ReadFromTextWithFilename(known_args.input)
    
    ind_lst = (
        pipeline | 'Create' >> beam.Create(list_files)
        | 'Read each file content' >> beam.ParDo(ReadFileContent())
    )

    # create a global dictionary to print
    big_dict = {}

    # Format the counts into a PCollection of strings.
    def format_result(ind_lst):
      (ind, lst) = ind_lst
      diction = {}
      diction[ind] = lst 
      big_dict[ind] = lst
      return diction

    output = ind_lst | 'Format' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | WriteToText(known_args.output)
    

    # Run the pipeline
    result = pipeline.run()

    # lets wait until result a available
    result.wait_until_finish()

    # print the output
    print(big_dict)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()