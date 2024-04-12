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
module: hitachi_block_addHost
short_description: Adds the iSCSI name of the host on the initiator side for the iSCSI target of the specified port.
description:
  - This module registers the iSCSI name of the host on the initiator side for the iSCSI target of the specified port.
options:
  management_address:
    description:
      - The hostname or IP address of the storage system.
    required: true
  management_port:
    description:
      - The port number of the storage system.
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
  port_id:
    description:
      - The number of the port.
    required: true
  host_group_number:
    description:
      - The target ID of the iSCSI target.
    required: true    
  iscsi_name:
    description:
      - The IQN of the initiator.
    required: true
'''

EXAMPLES = '''
  - name: Add iSCSI name to a iSCSI target
    add_host:
      management_address: "example.com"
      user: "admin"
      password:  "secret"
      port_id: "CL1-D"
      host_group_number: "1D-G00"
      iscsi_name: "iqn.1991-05.com.microsoft:win-g7mqgirmfls12016"
'''

def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        port_id=dict(type='str', required=True),
        host_group_number=dict(type='int', required=True),
        iscsi_name=dict(type='str', required=False),
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info(f"Initializing the add_host task")
    logger.debug('management_address: %s', module.params['management_address'])
    logger.debug('management_port: %d', module.params['management_port'])
    logger.debug('user: %s', module.params['user'])
    logger.debug('port_id: %s', module.params['port_id'])
    logger.debug('host_group_number: %d', module.params['host_group_number'])
    logger.debug('iscsi_name: %s', module.params['iscsi_name'])

    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.add_host()

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
