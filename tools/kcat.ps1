# Runs kcat in a container on the same network as the broker.
# $input passes piped text through to it. Without that, nothing gets sent.
#
# Send:  "a:1","b:2" | .\tools\kcat.ps1 -P -t demo '-K:'
# Read:  .\tools\kcat.ps1 -C -t demo -e -f 'p=%p key=%k value=%s\n'

$input | docker run --rm -i --network fleet-tracker_default edenhill/kcat:1.7.1 -b redpanda:9092 @args
