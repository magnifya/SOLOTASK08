"""Allow ``python3 -m obsd`` and ``python3 -m obsd.cli``."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
