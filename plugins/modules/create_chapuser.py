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
module: create_chapuser
short_description: Creates a CHAP user.
description:
  - This module creates a CHAP user on a Hitachi Virtual Storage Platform One SDS Block storage system.
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
  target_chap_user_name:
    description:
      - The name of the target CHAP user.
    required: true
  target_chap_secret:
    description:
      - The password of the target CHAP user.
    required: true
  initiator_chap_user_name:
    description:
      - The name of the initiator CHAP user.
    required: true
  initiator_chap_secret:
    description:
      - The password of the initiator CHAP user.
    required: true
'''

EXAMPLES = '''
  - name: Create a CHAP user
    create_chapuser:
      management_address: "example.com"
      user: "admin"
      password:  "secret"
      target_chap_user_name: "user123"
      target_chap_secret: "Thisisasecret"
      initiator_chap_user_name: "user234"
      initiator_chap_secret: "Thisisasecret2"
'''

def hitachi_vssb_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=VSSB_Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        target_chap_user_name=dict(type='str', required=True),
        target_chap_secret=dict(type='str', required=True, no_log=True),
        initiator_chap_user_name=dict(type='str', required=False),
        initiator_chap_secret=dict(type='str', required=False, no_log=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )

    logger = init_logger(module)
    logger.info("Intialized create_chapuser task")
    logger.debug('management_address: %s', module.params['management_address'])
    logger.debug('management_port: %d', module.params['management_port'])
    logger.debug('user: %s', module.params['user'])
    logger.debug('target_chap_user_name: %s', module.params['target_chap_user_name'])
    logger.debug('initiator_chap_user_name: %s', module.params['initiator_chap_user_name'])

    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.create_chapuser()

    except HitachiBlockException as err:
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    logger.info("Completed create_chapuser task")
    logger.debug('response: %s', json.dumps(response))
    module.exit_json(**response)

if __name__ == '__main__':  # pragma: no cover
    hitachi_vssb_main()
