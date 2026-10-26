#!/bin/sh
# Install the files unpacked from a Kova release archive.
# PREFIX defaults to $HOME/.local. Override it to install elsewhere.
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
prefix=${PREFIX:-"$HOME/.local"}

mkdir -p "$prefix/bin" "$prefix/lib" "$prefix/share/kova"
install -m 755 "$root/kova" "$prefix/bin/kova"
install -m 755 "$root/kovadump" "$prefix/bin/kovadump"
install -m 755 "$root/libkova.so" "$prefix/lib/libkova.so"
install -m 644 "$root/kova.toml" "$prefix/share/kova/kova.toml"
install -m 644 "$root/seed-oracle.js" "$prefix/share/kova/seed-oracle.js"

odbcinst="$HOME/.odbcinst.ini"
if [ ! -f "$odbcinst" ] || ! grep -q '^\[Kova\]' "$odbcinst"; then
  printf '\n[Kova]\nDriver=%s/lib/libkova.so\n' "$prefix" >> "$odbcinst"
fi

printf '%s\n' "Installed into $prefix"
printf '%s\n' "  $prefix/bin/kova"
printf '%s\n' "  $prefix/bin/kovadump"
printf '%s\n' "  $prefix/lib/libkova.so"
printf '%s\n' "  $prefix/share/kova/kova.toml"

case ":$PATH:" in
  *":$prefix/bin:"*) ;;
  *)
    printf '%s\n' "Add $prefix/bin to PATH:"
    printf '%s\n' "  export PATH=\"$prefix/bin:\$PATH\""
    ;;
esac

printf '%s\n' "Start from a directory you want to keep. Pages and the log go to data/ there:"
printf '%s\n' "  mkdir -p \"\$HOME/kova-data\" && cd \"\$HOME/kova-data\""
printf '%s\n' "  KOVA_CONFIG=$prefix/share/kova/kova.toml kova"
