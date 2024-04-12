from __future__ import absolute_import, print_function
from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.hitachi_block_client import (
    init_logger,
    Executors,
    HitachiBlockException
)
from ansible.module_utils.hitachi_block_constant import (
    Api,
    ModuleArgs
)


DOCUMENTATION = '''
---
module: hitachi_block_createVol
short_description: Creates a volume on a Hitachi block storage system.
description:
  - This module creates a volume on a Hitachi block storage system.
options:
  management_address:
    description:
      - The hostname or IP address of the storage system.
    required: true
  management_port:
    description:
      - The TCP/UDP port number of the storage system.
    required: false
    default: 443
  user:
    description:
      - The username used for authentication.
    required: true
  password:
    description:
      - The password used for authentication.
    required: true
    no_log: true
  ldev_id:
    description:
      - The volume of the storage device.
    required: true
  port_id:
    description:
      - The port number of the storage system.
    required: true
  capacity_mb:
    description:
      - The capacity of each volume in megabytes.
    required: true
'''

EXAMPLES = '''
- name: Create a volume
  hitachi_block_createVol:
    management_address: "example.com"
    user: "admin"
    password: "secret"
    ldev_id: 10005
    port_id: 2
    capacity_mb: 1000
'''



def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        pool_id=dict(type='int', required=True),
        capacity_mb=dict(type='int', required=True),
        ldev_id=dict(type='int', required=True),
        data_reduction_mode=dict(type='str', required=False, default=Api.DATA_REDUCTION_MODE_DISABLE),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info("Intialized attch_volume task")
    logger.debug('management_address: %s', module.params['management_address'])
    logger.debug('management_port: %d', module.params['management_port'])
    logger.debug('pool_id: %d', module.params['pool_id'])
    logger.debug('capacity_mb: %d', module.params['capacity_mb'])
    logger.debug('ldev_id: %d', module.params['ldev_id'])
    logger.debug('data_reduction_mode: %s', module.params['data_reduction_mode'])
    logger.debug('user: %s', module.params['user'])
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.create_ldev()

    except HitachiBlockException as err:
        import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_block_main()
