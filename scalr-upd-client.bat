@setlocal enabledelayedexpansion && "%~dp0\Python27\python" -E -x "%~f0" %* & exit /b !ERRORLEVEL!
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
try:
	from scalarizr.updclient.app import main
except ImportError, e:
	print "error: %s\n\nPlease make sure that scalarizr is properly installed" % (e)
	sys.exit(1)
main()
