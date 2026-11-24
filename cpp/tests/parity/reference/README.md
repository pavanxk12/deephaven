# Python reference for the parity tests

The exact Python modules (from `algo/`, branch `algo-refactor`) that `candles_parity.py` and `master_parity.py` run
as the reference implementation: the C++ must produce the same bars and the same contract master. The package
`__init__.py` files are empty on purpose, so only these modules load. Do not edit them to make a parity test pass:
a difference is a C++ bug (or a deliberate, documented change). The rest of the Python stack is on `algo-refactor`.
