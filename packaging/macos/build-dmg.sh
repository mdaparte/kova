#!/bin/sh
# Build Kova.app and a disk image.
#
#   BINS=target/release packaging/macos/build-dmg.sh
#   OUT=dist/Kova-aarch64.dmg
#
# BINS holds kova, kova-setup, kovadump, kova-shell, kova-client, and iggy-server. Without BINS the
# image still has the icon and the launcher. On macOS this uses hdiutil.
# On Linux it writes an HFS+ hybrid and wraps that as UDZO.
# The image is not signed and not notarized.
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
here="$root/packaging/macos"
out=${OUT:-"$root/dist/Kova.dmg"}
stage=$(mktemp -d)
work=$(mktemp -d)
trap 'rm -rf "$stage" "$work"' EXIT

app="$stage/Kova.app"
mkdir -p "$app/Contents/MacOS/packaging" "$app/Contents/Resources"
cp "$here/Info.plist" "$app/Contents/Info.plist"
cp "$here/kova-launch" "$app/Contents/MacOS/kova-launch"
cp "$here/kova.icns" "$app/Contents/Resources/kova.icns"
cp "$root/packaging/iggy-server-config.py" "$app/Contents/MacOS/packaging/iggy-server-config.py"
cp "$root/LICENSE" "$root/NOTICE" "$app/Contents/Resources/"
printf 'APPL????' > "$app/Contents/PkgInfo"
chmod 755 "$app/Contents/MacOS/kova-launch" "$app/Contents/MacOS/packaging/iggy-server-config.py"

if [ -n "${BINS:-}" ]; then
  for name in kova kova-setup kovadump kova-shell kova-client iggy-server; do
    if [ ! -f "$BINS/$name" ]; then
      printf '%s\n' "missing $BINS/$name" >&2
      exit 1
    fi
    cp "$BINS/$name" "$app/Contents/MacOS/$name"
    chmod 755 "$app/Contents/MacOS/$name"
  done
  for extra in LICENSE-iggy NOTICE-iggy DEPENDENCIES-iggy; do
    if [ -f "$BINS/$extra" ]; then
      cp "$BINS/$extra" "$app/Contents/Resources/$extra"
    fi
  done
else
  printf '%s\n' "BINS is unset. The disk image has the icon and the launcher, and no server binaries." >&2
fi

ln -s /Applications "$stage/Applications"
mkdir -p "$stage/.background"
cp "$here/background.png" "$stage/.background/background.png"

mkdir -p "$(dirname "$out")"
rm -f "$out"

if [ "$(uname -s)" = "Darwin" ]; then
  rw="$work/kova-rw.dmg"
  hdiutil create -volname Kova -srcfolder "$stage" -ov -format UDRW -fs HFS+ "$rw"
  attached=$(hdiutil attach -readwrite -noverify -noautoopen "$rw")
  dev=$(printf '%s\n' "$attached" | awk '/\/dev\// { print $1; exit }')
  vol=$(printf '%s\n' "$attached" | awk '/\/Volumes\// { print $NF; exit }')
  if osascript <<'EOF'
tell application "Finder"
  tell disk "Kova"
    open
    set current view of container window to icon view
    set toolbar visible of container window to false
    set statusbar visible of container window to false
    set the bounds of container window to {120, 80, 840, 500}
    set theViewOptions to the icon view options of container window
    set arrangement of theViewOptions to not arranged
    set icon size of theViewOptions to 128
    set background picture of theViewOptions to file ".background:background.png"
    set position of item "Kova.app" of container window to {180, 190}
    set position of item "Applications" of container window to {540, 190}
    close
    open
    update without registering applications
    delay 2
  end tell
end tell
EOF
  then
    chflags hidden "$vol/.background" 2>/dev/null || true
  else
    printf '%s\n' "Finder layout was skipped. The disk image still opens." >&2
  fi
  hdiutil detach "$dev" || hdiutil detach -force "$dev"
  rm -f "$out" "$out.dmg"
  hdiutil convert "$rw" -format UDZO -imagekey zlib-level=9 -o "$out"
  if [ -f "$out.dmg" ]; then
    mv "$out.dmg" "$out"
  fi
else
  iso="$work/kova.iso"
  if ! (cd "$stage" && xorriso -as mkisofs -hfsplus -V Kova -r -o "$iso" .) >"$work/xorriso.log" 2>&1; then
    cat "$work/xorriso.log" >&2
    exit 1
  fi
  python3 "$here/udif.py" "$iso" "$out"
fi

printf '%s\n' "$out"
