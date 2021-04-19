#!/bin/bash

IS_GFS_SERVER="false"
IS_RUNNING="false"
now=`date +%s`

if [ -e "/rootfs/usr/lib/systemd/system/glusterd.service" ]; then
  IS_GFS_SERVER="true"
else
  exit 0
fi

MY_UUID=$(cat < /rootfs/var/lib/glusterd/glusterd.info | grep UUID | awk -F "=" '{print $2}')

if [ -e "/rootfs/var/run/glusterd.pid" ]; then
	IS_RUNNING="true"
else
	echo "glusterfs,node_uuid=$MY_UUID status=\"Disconnected $now""000000000"
	exit 0
fi

# get peer status
for peer in $(nsenter --mount=/rootfs/proc/1/ns/mnt sh -c "gluster pool list" | tail -n +2 |awk '{print $1}'); do
  state=
  state=$(nsenter --mount=/rootfs/proc/1/ns/mnt sh -c "gluster pool list" | grep $peer | awk '{print $3}')
  description="glusterfs,node_uuid=$peer status=\"$state\" $now""000000000"
  echo "$description"
done

#echo "glusterfs,node_uuid=f8b1fc9c-1692-49eb-81d7-288a49280007 status=\"Disconnected\" 1589951120000000000"
#echo "glusterfs,node_uuid=eff08a48-0e52-4304-bd81-ee283c728ef5 status=\"Connected\" 1589951120000000000"
#echo "glusterfs,node_uuid=f36abea1-c8ea-4cc0-aad4-430d75adf1d0 status=\"Connected\" 1589951120000000000"