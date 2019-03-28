import os

import opencensus.trace.tracer
from opencensus.ext.stackdriver import trace_exporter as stackdriver_exporter
from opencensus.common.transports.async_ import AsyncTransport


def initialize_tracer(project_id=None):
  """ Initialize tracer

  Args:
    project_id: the project needed to be traced, default is fetched from environment variable $GOOGLE_CLOUD_PROJECT

  Raises:
    ValueError: An error occurred when project_id is neither passed in nor set as an environment variable
  """

  if project_id is None:
    if os.environ.get('GOOGLE_CLOUD_PROJECT') is not None:
      project_id = os.environ['GOOGLE_CLOUD_PROJECT']
    else:
      raise ValueError(
        'Can not find a valid project_id to initialize the tracer, check if $GOOGLE_CLOUD_PROJECT is set')

  exporter = stackdriver_exporter.StackdriverExporter(
    project_id=project_id,
    transport=AsyncTransport  # Use AsyncTransport to exclude exporting time
  )
  tracer = opencensus.trace.tracer.Tracer(
    exporter=exporter
  )
  return tracer
