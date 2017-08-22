#!/bin/sh

IMAGE="erctcalvin/calvin:develop"
EXTERNAL_IP=""
PORT="5003" 
CONTROLPORT="5001"
USE_DOCKERS=yes
LOGLEVEL=INFO

HAS_DOCKER=$(which docker)
HAS_CALVIN=$(which csruntime)

usage() {
    echo "Usage: $(basename $0) -i <image> -e <ip> [proxy/dht]\n\
    -i <image>[:<tag>]   : Calvin image (and tag) to use [$IMAGE]\n\
    -e <external-ip>     : External IP to use"
    exit 1
}

wait_for_runtime() {
    retries=10
    rt=$1
    while test $retries -gt 0; do
	if test -n $USE_DOCKERS; then
            res=$(docker exec runtime-0 cscontrol http://$EXTERNAL_IP:$CONTROLPORT storage get_index '["node_name", {"name": "runtime-'$rt'"}]');
	else
	    res=$(cscontrol http://$EXTERNAL_IP:$CONTROLPORT storage get_index '["node_name", {"name": "runtime-'$rt'"}]');
	fi
        result=${res#*result}
        # Successful result is 53 characters, error 13 - but exact comparison is a bit fragile
        if test ${#result} -gt 25; then
            echo "runtime-$rt attached to registry"
            break
        fi
        retries=$((retries-1))
        sleep 1
    done
    if test $retries -eq 0; then
        echo Too many retries for runtime-$rt, giving up
        echo $result, ${#result}
        exit 1
    fi
}

while getopts "i:e:hn" opt; do
	case $opt in
        i) 
            IMAGE="$OPTARG"
            ;; 
        e)
            EXTERNAL_IP="$OPTARG"
            ;;
	esac
done

shift $(($OPTIND-1))

CMD=$1

if test -z $CMD; then
    usage
fi



case $CMD in
    dht)
        ./dcsruntime.sh -i $IMAGE -e $EXTERNAL_IP -c $CONTROLPORT -n runtime-0 -a '{"indexed_public": {"node_name": {"group": "cloud"}}}' --loglevel=$LOGLEVEL
        wait_for_runtime 0
        ./dcsruntime.sh -i $IMAGE -e $EXTERNAL_IP -n runtime-1 -a '{"indexed_public": {"node_name": {"group": "fog"}}}' --loglevel=$LOGLEVEL
        wait_for_runtime 1
        ./dcsruntime.sh -i $IMAGE -e $EXTERNAL_IP -n runtime-2 -a '{"indexed_public": {"node_name": {"group": "endpoint"}}}' --loglevel=$LOGLEVEL
        wait_for_runtime 2
        ;;
    proxy)
        echo "PROXY"
        ./dcsruntime.sh -i $IMAGE -e $EXTERNAL_IP -l -p $PORT -c $CONTROLPORT -n runtime-0 -a '{"indexed_public": {"node_name": {"group": "cloud"}}}' --loglevel=$LOGLEVEL
        wait_for_runtime 0
        ./dcsruntime.sh -i $IMAGE -e $EXTERNAL_IP -r $EXTERNAL_IP:$PORT -n runtime-1 -a '{"indexed_public": {"node_name": {"group": "fog"}}}' --loglevel=$LOGLEVEL
        wait_for_runtime 1
        ./dcsruntime.sh -i $IMAGE -e $EXTERNAL_IP -r $EXTERNAL_IP:$PORT -n runtime-2 -a '{"indexed_public": {"node_name": {"group": "endpoint"}}}' --loglevel=$LOGLEVEL
        wait_for_runtime 2
        ;;
    esac
