#!/bin/bash
# set -x
LOG_DIR=/var/log/hitachi/ansible-storage
LOG_BUNDLES_DIR=/var/log/hitachi/ansible-storage/log_bundles
if [ -d "$LOG_DIR" ]; then
  if [ ! -d "$LOG_BUNDLES_DIR" ]; then
    mkdir -p $LOG_BUNDLES_DIR
  fi
  cd $LOG_BUNDLES_DIR
  # Remove old files except two recent files
  ls -t | tail -n +3 | xargs rm -rf
  current_time=`date +%Y%m%d_%H_%M_%S`
  cd $LOG_DIR
  zip -q $LOG_BUNDLES_DIR/Ansible_Log_Bundle_$current_time.zip *.*
  echo "The Ansible_Log_Bundle_$current_time.zip file was generated in directory $LOG_BUNDLES_DIR."
else
  echo "$LOG_DIR directory does not exist."
fi
