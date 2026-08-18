import io, logging, sys

# Piggyback on unittest's -v and -q flags to show/hide logging.
logging.basicConfig()
if '-v' in sys.argv:
  logging.getLogger().setLevel(logging.DEBUG)
  logging.getLogger('google.cloud.datastore_v1').setLevel(logging.INFO)
else:
  # used to be:
  #   elif 'discover' in sys.argv or '-q' in sys.argv or '--quiet' in sys.argv:
  # dropped that to suppress logging when running full single test files

  # don't emit logs. do this instead of setLevel() or disable() so that the log
  # messages still get evaluated and raise the same exceptions that they would if
  # they got emitted.
  handler = logging.getLogger().handlers[0]
  if hasattr(handler, 'setStream'):
    handler.setStream(io.StringIO())
