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


DOCUMENTATION = """
---
module: hitachi_block_createSI
short_description: Creates a ShadowImage pair.
description:
  - This module creates a ShadowImage Pair.
options:
  management_address:
    description:
      - The management address of the storage system.
    required: true
  management_port:
    description:
      - The management port of the storage system.
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
  copy_group_name:
    description:
      - The name of the copy group. Value should not exceed 29 characters.
    required: true
  pvol_ldev_id:
    description:
      - The LDEV number of the P-VOL with a decimal (base 10) number.
    required: true
  svol_ldev_id:
    description:
      - The LDEV number of the S-VOL with a decimal (base 10) number.
    required: true
  copy_pace:
    description:
      - Specify a value in the range from 1 to 15 to be the copy speed. The larger the value, the higher the speed.
    required: false
    default: 3
  consistency_group_id:
    description:
      - Specify the consistency group ID (0 to 127).
    required: true
"""

EXAMPLES = """
- name: Create ShadowImage pair
  hitachi_block_createSI:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    pvol_ldev_id: 10005
    svol_ldev_id: 10006
    copy_group_name: ABC
    consistency_group_id: 0
"""


def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        copy_group_name=dict(type='str', required=True),
        pvol_ldev_id=dict(type='int', required=True),
        svol_ldev_id=dict(type='int', required=True),
        copy_pace=dict(type='int', required=False, default=3),
        consistency_group_id=dict(type='int', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info(f"Initializing the create SI task")
    logger.debug('copy_group_name: %s', module.params['copy_group_name'])
    logger.debug('pvol_ldev_id: %d', module.params['pvol_ldev_id'])
    logger.debug('svol_ldev_id: %d', module.params['svol_ldev_id'])
    logger.debug('copy_pace %d', module.params['copy_pace'])
    logger.debug('consistency_group_id: %d', module.params['consistency_group_id'])
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.create_si()

    except HitachiBlockException as err:
        #import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    
    logger.info(f"Finished the create SI task")
    logger.debug('response: %s', json.dumps(response))
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_block_main()
