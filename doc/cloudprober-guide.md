# How To Add Python Probers for Cloud APIs

The gRPC Cloudprober supports Python probers. Following steps
shows how to add probes for a new Cloud API in Python. For this
instruction, we take Firestore API as an example and walk through the process of
adding Python probes for Firestore.

## Enable Cloud API in GCP Project

If you are not a member of the GCP project `grpc-prober-testing`, send an email
to grpc-cloud-api-support@gmail.com and request permissions for the project.

Then use `$ gcloud init` to set `grpc-prober-testing` as the default project for
Cloud SDK.

Check [Enable and disable
APIs](https://support.google.com/cloud/answer/6158841?hl=en) for details to
enable the cloud API you want to probe. And enable all APIs that are needed when
making client calls.

## Add Cloud API Probes

The source code of the probes lives in [grpc_gcp_prober](../cloudprober/grpc_gcp_prober),
you will need to modify this folder to add probes for new APIs.

### Implement new probes

Create a new module named `firestore_probes.py` inside the source folder, and
implement Python probes for the new cloud API. You can use `spanner_probes.py`
as its prototype. For example, if you want to test the `ListDocuments` call
from firestore stub:

```py
def _documents(stub, util):
  """Probes to test ListDocuments grpc call from Firestore stub.

  Args:
    stub: An object of FirestoreStub.
    util: An object of StackdriverUtil.
  """
  start = time.time()
  list_document_request = firestore_pb2.ListDocumentsRequest(
      parent=_PARENT_RESOURCE
  )
  stub.ListDocuments(list_document_request)
  latency = (time.time() - start) * 1000  # calculate call latency in ms

  # use StackdriverUtil to collect custom metrics.
  util.add_metric('list_documents_latency_ms', latency)
```

Use a dict to map the probe name and the probe method.

```py
PROBE_FUNCTIONS = {
    'documents': _documents,
}
```

Notice that `stub` and `util` objects are initialized in `prober.py`. We will
discuss them in later sections. For complete code, check [firestore_probes.py](../cloudprober/grpc_gcp_prober/firestore_probes.py).

### Register new API stub

Register the new cloud API in `prober.py`. `prober.py` is an entrypoint for all
the probes of different cloud APIs. It creates the stub for the api and executes
the probe functions defined for the specific cloud api.

```py
from google.auth.transport.requests import Request
from google.firestore.v1beta1 import firestore_pb2_grpc
import firestore_probes

_FIRESTORE_TARGET = 'firestore.googleapis.com'

def _execute_probe(api, probe):
  # some other code ...

  if api == 'firestore':
    channel = secure_authorized_channel(cred, Request(), _FIRESTORE_TARGET)
    stub = firestore_pb2_grpc.FirestoreStub(channel)
    probe_functions = firestore_probes.PROBE_FUNCTIONS

  ...
```

### Register probe in cloudprober

Add the new probe you just implemented to `cloudprober.cfg`, so that when
cloudprober is running, it will executes the probe and forward all metrics to
Stackdriver. Use the template just like the other probes.

```
probe {
  type: EXTERNAL
  name: "firestore_documents"
  interval_msec: 300000
  timeout_msec: 30000
  targets { dummy_targets {} }  # No targets for external probe
  external_probe {
    mode: ONCE
    command: "python -m grpc_gcp_prober.prober --api=firestore"
  }
}
```

## Stackdriver Mornitoring

Use the [StackdriverUtil](../cloudprober/grpc_gcp_prober/stackdriver_util.py)
to add custom metrics.

```py
util = StackdriverUtil(api_name, probe_name)
util.add_metric('metric_name', metric_value)
```

The StackdriverUtil will format the output (e.g. "read_latency_ms 100") so they
can be scraped by cloudprober and then metrics will be automatically created and
forwarded to Stackdriver as [Custom Metrics](https://cloud.google.com/monitoring/custom-metrics/). Later on, the metrics can be retrieved via [Metric Explore](https://app.google.stackdriver.com/metrics-explorer).
The full name of the metric will be in the following format:

```
custom.googleapis.com/cloudprober/external/<probe_name>/<metirc>
```

## Stackdriver Error Reporting
[StackdriverUtil](../cloudprober/grpc_gcp_prober/stackdriver_util.py) also helps setting up
[Error Reporting](https://cloud.google.com/error-reporting/docs/setup/python)
to report any Error occurred during probes. In this way, if anything unusual
occurs, it can be reported immediately.

By default, all exceptions thrown by any probe will be reported to Error
Reporting by StackdriverUtil.

## Alerting Notification

There are two ways you can be notified for alerts:

1. Add [Alerting Policy](https://cloud.google.com/monitoring/alerts/) in
Stackdriver Monitoring. And set up notification when certain metircs are absent
or beyond/below a certain threshold.

2. Set up [Email Notification](https://cloud.google.com/error-reporting/docs/notifications)
in Error Reporting. The alert will be triggered whenever an Error/Exception is
reported by google-cloud-error-reporting client. Note that this option does not
support email alias, you need to use the email that is associated with the
Google Account and with necessary IAM roles.
