from __future__ import absolute_import, print_function
from http import server
from tokenize import Number
from ansible.module_utils.basic import AnsibleModule
import json

from ansible.module_utils.hitachi_vssb_client import (
    init_logger,
    Executors,
    HitachiBlockException
)
from ansible.module_utils.hitachi_vssb_constant import (
    VSSB_Api,
    ModuleArgs
)

DOCUMENTATION = '''
---
module: delete_computenode
short_description: Deletes Compute Node from a Hitachi Virtual Storage Platform One SDS Block storage system.
description:
  - This module deletes a Compute Node from a Hitachi Virtual Storage Platform One SDS Block storage system.
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
  server_nickname:
    description:
      - The name of the compute node.
    required: true
'''

EXAMPLES = '''
- name: Delete Compute Node
  delete_computenode:
    management_address: "example.com"
    user: "admin"
    password: "secret"
    server_nickname: "example_name"
'''

def hitachi_vssb_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=VSSB_Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        server_nickname=dict(type='str', required=True),
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info("Intialized delete_computenode task")
    logger.debug('management_address: %s', module.params['management_address'])
    logger.debug('management_port: %d', module.params['management_port'])
    logger.debug('user: %s', module.params['user'])
    logger.debug('server_nickname %s', module.params['server_nickname'])
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.delete_computenode()

    except HitachiBlockException as err:
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    logger.debug('response: %s', json.dumps(response))
    logger.info("Completed delete_computenode task")
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_vssb_main()
