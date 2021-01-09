script_dir=$(dirname "$0")
script_dir="${PWD}/$script_dir"

ln -rfs "$script_dir/pre-push.sh" "$script_dir/../.git/hooks/pre-push"
