#!/bin/sh
# check_plugin.sh lib.so ...: a strategy plugin must export exactly the three factory symbols and hold no writable
# global data (all state lives in the instance, so several instances can run at once).
fail=0
for so in "$@"; do
  n=$(nm -g "$so" | grep -cE ' T _?(algoGetInfo|algoCreate|algoDestroy)$')
  if [ "$n" != 3 ]; then echo "$so: expected the 3 factory exports, found $n"; fail=1; continue; fi
  if [ "$(uname)" = Darwin ]; then
    w=$(nm -m "$so" | grep -E '\(__DATA,__(data|bss|common)\)' | grep -vE '__dso_handle|GCC_except|_ZTV|_ZTI|_ZTS' || true)
  else
    w=$(nm "$so" | grep -E ' [dDbB] ' | grep -vE '__dso_handle|__TMC_END__|__bss_start|_edata|_end|completed\.|__frame_dummy|__do_global|GCC_except|_ZTV|_ZTI|_ZTS|DW\.ref' || true)
  fi
  if [ -n "$w" ]; then echo "$so: writable global data (strategy state must live in the instance):"; echo "$w"; fail=1; continue; fi
  echo "$so: exports ok, no writable globals"
done
exit $fail
