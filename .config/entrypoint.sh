#!/bin/sh

# # Create symlink for the plugin executable
# PLUGIN_DIR="/var/lib/grafana/plugins/nominaltest-nominalds-datasource"
# if [ -d "$PLUGIN_DIR" ]; then
#     cd "$PLUGIN_DIR"
    
#     # Determine the correct executable based on architecture
#     if [ -f "gpx_nominal_ds_linux_amd64" ]; then
#         ln -sf gpx_nominal_ds_linux_amd64 gpx_nominal_ds
#     elif [ -f "gpx_nominal_ds_linux_arm64" ]; then
#         ln -sf gpx_nominal_ds_linux_arm64 gpx_nominal_ds
#     elif [ -f "gpx_nominal_ds_linux_arm" ]; then
#         ln -sf gpx_nominal_ds_linux_arm gpx_nominal_ds
#     elif [ -f "gpx_nominal_ds_darwin_amd64" ]; then
#         ln -sf gpx_nominal_ds_darwin_amd64 gpx_nominal_ds
#     elif [ -f "gpx_nominal_ds_darwin_arm64" ]; then
#         ln -sf gpx_nominal_ds_darwin_arm64 gpx_nominal_ds
#     elif [ -f "gpx_nominal_ds_windows_amd64.exe" ]; then
#         ln -sf gpx_nominal_ds_windows_amd64.exe gpx_nominal_ds
#     fi
    
#     # Make sure the symlink is executable
#     chmod +x gpx_nominal_ds 2>/dev/null || true
# fi

if [ "${DEV}" = "false" ]; then
    echo "Starting test mode"
    exec /run.sh
fi

echo "Starting development mode"

if grep -i -q alpine /etc/issue; then
    exec /usr/bin/supervisord -c /etc/supervisord.conf
elif grep -i -q ubuntu /etc/issue; then
    exec /usr/bin/supervisord -c /etc/supervisor/supervisord.conf
else
    echo 'ERROR: Unsupported base image'
    exit 1
fi

