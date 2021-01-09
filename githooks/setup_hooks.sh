SCRIPT_DIR=$(dirname "$0")
SCRIPT_DIR="${PWD}/$SCRIPT_DIR"

ln -rfs "$SCRIPT_DIR/pre-push.sh" "$SCRIPT_DIR/../.git/hooks/pre-push"
