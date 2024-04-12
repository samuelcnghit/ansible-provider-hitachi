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

DOCUMENTATION = """
---
module: hitachi_block_addChap
short_description: Adds a CHAP user authentication to an iSCSI target.
description:
  - This module adds a CHAP user authentication to an iSCSI target.
options:
  management_address:
    description:
      - The management address of the Hitachi storage system.
    required: true
  management_port:
    description:
      - The management port of the Hitachi storage system.
    required: false
    default: Api.SERVER_PORT_DEFAULT
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
      - The ID of the port.
    required: true
  host_group_number:
    description:
      - The number of the host group.
    required: true
  chap_user_name:
    description:
      - The CHAP user name.
    required: true
  way_of_chap_user:
    description:
      - The way of CHAP user.
    required: true
  chap_password:
    description:
      - The CHAP password.
    required: true
    no_log: true
"""

EXAMPLES = """
- name: Add CHAP authentication to an iSCSI target
  hitachi_block_addChap:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    port_id: "port123"
    host_group_number: 123
    chap_user_name: "chap_user"
    way_of_chap_user: "chap_user_way"
    chap_password: "chap_password"
"""


def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        port_id=dict(type='str', required=True),
        host_group_number=dict(type='int', required=True),
        chap_user_name=dict(type='str', required=True),
        way_of_chap_user=dict(type='str', required=True),
        chap_password=dict(type='str', required=True, no_log=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info(f"Initializing the add_chap_user task")
    logger.debug('management_address: %s', module.params['management_address'])
    logger.debug('management_port: %d', module.params['management_port'])
    logger.debug('user: %s', module.params['user'])
    logger.debug('port_id: %s', module.params['port_id'])
    logger.debug('host_group_number: %d', module.params['host_group_number'])
    logger.debug('chap_user_name: %s', module.params['chap_user_name'])
    logger.debug('way_of_chap_user: %s', module.params['way_of_chap_user'])

    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.add_chap_user()

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
