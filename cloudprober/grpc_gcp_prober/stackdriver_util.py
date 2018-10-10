"""Utilities for collecting metrics and errors to Stackdriver.
"""

import sys
import versions
from google.cloud import error_reporting


class StackdriverUtil(object):
  """Utility class for collection Stackdriver metrics and errors.

  Use this class to format output so that cloudprober can scrape and create
  metrics based on these output.

  Attributes:
    api_name: The cloud api name of the metrics being generated.
  """

  def __init__(self, api_name):
    self.metrics = {}
    self.success = False
    self.err_client = error_reporting.Client()
    self.api_name = api_name

  def add_metric(self, name, value):
    self.metrics[name] = value

  def add_metrics_dict(self, metrics_dict):
    self.metrics.update(metrics_dict)

  def set_success(self, success):
    self.success = success

  def output_metrics(self):
    """Format output before they can be made to Stackdriver metrics.

    Formatted output like 'metric<space>value' will be scraped by Stackdriver as
    custom metrics.
    """

    # output probe result.
    if self.success:
      sys.stdout.write('{}_success 1\n'.format(self.api_name))
    else:
      sys.stdout.write('{}_success 0\n'.format(self.api_name))

    # output other metrics.
    for key, value in self.metrics.iteritems():
      sys.stdout.write('{} {}\n'.format(key, int(value)))

  def report_error(self, err):
    """Format error message to output to error reporting."""
    # Write err log to Stackdriver logging
    sys.stderr.write(str(err) + '\n')
    # Report err to Stackdriver Error Reporting
    self.err_client.report(
        'PythonProbeFailure: gRPC(v={}) fails on {} API. Details: {}\n'.format(
            versions.GRPC_VERSION, self.api_name, str(err)))
