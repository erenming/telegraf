#!/bin/bash -e

if [[ $DICE_CLUSTER_TYPE == 'kubernetes' ]]; then
    export CONFIG_DIR=k8s
else
    export CONFIG_DIR=$DICE_CLUSTER_TYPE
fi
if [ ! $CONFIG_DIR ]; then
    export CONFIG_DIR=dcos
fi
if [[ $DICE_CLUSTER_TYPE == 'edas' ]]; then
    export DICE_CLUSTER_TYPE="kubernetes"
fi

if [[ $DICE_CLUSTER_TYPE == 'kubernetes' ]]; then
    export CLUSTER_TYPE="k8s"
else
    export CLUSTER_TYPE=$DICE_CLUSTER_TYPE
fi
if [ ! $CLUSTER_TYPE ]; then
    CLUSTER_TYPE=dcos
fi

if [[ $CLUSTER_TYPE == 'k8s' ]]; then
    export IS_K8S="true"
else
    export IS_K8S="false"
fi

# 调度器那边需要兼容k8s的处理, addon拉起的 telegraf 需要走调度器
if [[ $IS_K8S == 'true' ]]; then
    HOST_NUM=${HOSTNAME##*-}
    for line in $(env)
    do
        if [[ "$line" =~ ^N${HOST_NUM}\_.+=.+ ]]; then
            key=${line%%=*}
            value=${line#*=}
            newkey=${key#*_}
            export ${newkey}=${value}
            echo "$key -> $newkey = $value"
        fi
    done
fi

# default env
if [ ! $DICE_COMPONENT ]; then
    export DICE_COMPONENT="spot/telegraf"
fi

if [ -d "/rootfs/etc" ]; then
    if [ ! $HOST_ETC ]; then
        export HOST_ETC="/rootfs/etc"
    fi
fi

if [ -f "/rootfs/etc/cluster-node" ]; then
    if [ ! $CLUSTER_PATH ]; then
        export CLUSTER_PATH="/rootfs/etc/cluster-node"
    fi
fi

if [ -d "/rootfs/sys" ]; then
    if [ ! $HOST_SYS ]; then
        export HOST_SYS="/rootfs/sys"
    fi
fi

if [ -d "/rootfs/proc" ]; then
    if [ ! $HOST_PROC ]; then
        export HOST_PROC="/rootfs/proc"
    fi
fi

if [ -d "/rootfs" ]; then
    if [ ! $HOST_MOUNT_PREFIX ]; then
        export HOST_MOUNT_PREFIX="/rootfs"
    fi
fi

if [ ! $MAIN_PID ]; then
    export MAIN_PID="1"
fi

if [ ! $TELEGRAF_CONFIG ]; then
    export TELEGRAF_CONFIG=telegraf
fi

TELEGRAF_OPTS=
if [[ $DICE_IS_EDGE == 'true' ]]; then
    if [ -f "conf/${CONFIG_DIR}/${TELEGRAF_CONFIG}.conf" ]; then
        TELEGRAF_OPTS="--config-directory conf/${CONFIG_DIR}/${TELEGRAF_CONFIG}.conf"
    fi
else
    if [ -f "conf/${CONFIG_DIR}/${TELEGRAF_CONFIG}-mc.conf" ]; then
        TELEGRAF_OPTS="--config-directory conf/${CONFIG_DIR}/${TELEGRAF_CONFIG}-mc.conf"
    elif [ -f "conf/${CONFIG_DIR}/${TELEGRAF_CONFIG}.conf" ]; then
        TELEGRAF_OPTS="--config-directory conf/${CONFIG_DIR}/${TELEGRAF_CONFIG}.conf"
    fi
fi

if [ $TELEGRAF_PPROF_PORT ]; then
    TELEGRAF_PPROF_ADDR="--pprof-addr 0.0.0.0:${TELEGRAF_PPROF_PORT}"
fi

# run
if [[ $TELEGRAF_STATIC_CONF != '' ]]; then
    ./telegraf --config $TELEGRAF_STATIC_CONF $@
elif [[ $TELEGRAF_CONFIG_PATH != '' ]]; then
    ./telegraf $TELEGRAF_PPROF_ADDR --config $TELEGRAF_CONFIG_PATH $@
elif [[ $TELEGRAF_CONFIG != '' ]]; then
    echo "./telegraf --config conf/${TELEGRAF_CONFIG}.conf ${TELEGRAF_OPTS} $@"
    ./telegraf $TELEGRAF_PPROF_ADDR --config conf/${TELEGRAF_CONFIG}.conf ${TELEGRAF_OPTS} $@
elif [[ $ADDON_TYPE != '' ]]; then
    ./telegraf $TELEGRAF_PPROF_ADDR --config conf/addon/${ADDON_TYPE}.conf $@
else
    echo "./telegraf $TELEGRAF_PPROF_ADDR --config conf/telegraf.conf ${TELEGRAF_OPTS} $@"
    ./telegraf $TELEGRAF_PPROF_ADDR --config conf/telegraf.conf ${TELEGRAF_OPTS} $@
fi
