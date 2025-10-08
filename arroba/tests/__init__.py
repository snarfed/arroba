import io, logging, sys

# Piggyback on unittest's -v and -q flags to show/hide logging.
logging.basicConfig()
if '-v' in sys.argv:
  logging.getLogger().setLevel(logging.DEBUG)
elif 'discover' in sys.argv or '-q' in sys.argv or '--quiet' in sys.argv:
  logging.disable(logging.CRITICAL + 1)
  # don't emit logs. do this instead of setLevel() or disable() so that the log
  # messages still get evaluated and raise the same exceptions that they would if
  # they got emitted.
  logging.getLogger().handlers[0].setStream(io.StringIO())
