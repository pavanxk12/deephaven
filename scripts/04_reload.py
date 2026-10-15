# After editing files in the algo package: stop everything, forget the modules, load them again.
import algo
algo.reload()

import algo                                                         # noqa: E402  (fresh copy)
app = algo.connect(globals(), market="NSE")
# ...then run 01_day.py / 02_strategy.py / 03_console.py again
