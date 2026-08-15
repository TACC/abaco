#!/bin/sh
# Opens up the runtime paths abaco shares with actor containers.
#
# This runs as root via sudo (see the rule installed in the Dockerfile) and the
# caller is the unprivileged tapis user, so $1 is untrusted input. Without the
# checks below the sudo rule is a root `chmod -R 777` on any path the caller
# names -- `sudo folder_permissions.sh /etc` would be enough to take the
# container. Matching the path string is not enough either: /home/tapis is
# owned by tapis, which can `mv runtime_files x; ln -s / runtime_files`. So the
# path must be allow-listed, must not be a symlink, and must resolve to itself.
set -eu

target="${1:-}"

refuse() {
    echo "folder_permissions: refusing to operate on '$target': $1" >&2
    exit 1
}

case "$target" in
    /home/tapis/runtime_files|/var/run/docker.sock) ;;
    *) refuse "not an allow-listed path" ;;
esac
if [ -L "$target" ]; then
    refuse "is a symlink"
fi
resolved=$(realpath -e -- "$target") || refuse "does not exist"
case "$target:$resolved" in
    # /var/run is a symlink to /run on Debian; both are root-owned.
    /home/tapis/runtime_files:/home/tapis/runtime_files) ;;
    /var/run/docker.sock:/var/run/docker.sock|/var/run/docker.sock:/run/docker.sock) ;;
    *) refuse "resolves to '$resolved'" ;;
esac

if [ "$target" = /var/run/docker.sock ]; then
    chmod 777 -- "$resolved"
else
    # The checks above race with tapis swapping in a symlink before chmod runs, and
    # GNU chmod -R follows a symlink named on its command line (coreutils 9.1 has no
    # -P to turn that off). So pin the directory: enter it, verify where we really
    # are, and chmod "." -- a later swap can only rename the directory already
    # verified. Below the top, chmod -R never follows symlinks.
    cd -P -- "$resolved" || refuse "cannot enter"
    [ "$(pwd -P)" = /home/tapis/runtime_files ] || refuse "moved while being checked"
    chmod -R 777 .
fi
echo "$target"
