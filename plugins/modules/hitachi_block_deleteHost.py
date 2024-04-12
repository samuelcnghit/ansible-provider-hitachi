from __future__ import absolute_import, print_function
from ansible.module_utils.basic import AnsibleModule
import json

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
module: hitachi_block_deleteHost
short_description: Deletes a iSCSI name from the iSCSI target on a Hitachi block storage system.
description:
  - This module deletes a iSCSI name from the iSCSI target on a Hitachi block storage system.
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
  port_id:
    description:
      - The port number of the storage system.
    required: true
  host_group_number:
    description:
      - The host group number of the port.
    required: true
  iscsi_name:
    description:
      - The iSCSI name of the port.
      - Specify this item in the iqn or eui format.
      - 'iqn format:'
      - '  Specify a value in the range from 5 to 223. You can use the following characters:'
      - '    alphanumeric characters (lowercase), periods (.), hyphens (-), and colons (:)'
      - 'eui format:'
      - '  After "eui.", specify a hexadecimal number. Specify a value consisting of 20 characters.'
    required: true
'''

EXAMPLES = '''
- name: Delete the iSCSI name from the iSCSI target.
  createhg:
    management_address: "example.com"
    user: "admin"
    password: "secret"
    port_id: CL1-C
    host_group_number: 123
    iscsi_name: 'iqn.rest.example.of.iqn.form'
'''

def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        port_id=dict(type='str', required=True),
        host_group_number=dict(type='int', required=True),
        iscsi_name=dict(type='str', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info("Intialized deleteHost task")
    logger.debug('management_address: %s', module.params['management_address'])
    logger.debug('management_port: %d', module.params['management_port'])
    logger.debug('port_id: %s', module.params['port_id'])
    logger.debug('host_group_number: %d', module.params['host_group_number'])
    logger.debug('iscsi_name: %s', module.params['iscsi_name'])
    logger.debug('user: %s', module.params['user'])
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.delete_host()

    except HitachiBlockException as err:
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    logger.debug('response: %s', json.dumps(response))
    logger.info("Completed deleteHost task")
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_block_main()
