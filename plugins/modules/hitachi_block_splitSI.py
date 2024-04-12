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
module: hitachi_block_splitSI
short_description: Splits ShadowImages.
description:
  - This module splits ShadowImages.
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
  copy_group_name:
    description:
      - The name of the copy group.
    required: true
  copy_pair_name:
    description:
      - The name of the copy pair.
    required: true
  copy_pace:
    description:
      - The copy pace.
    required: false
    default: 3
"""

EXAMPLES = """
- name: Split shadow image
  hitachi_block_splitSI:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    copy_group_name: "CG001"
    copy_pair_name: "CP001"
    copy_pace: 5
"""




def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        copy_group_name=dict(type='str', required=True),
        copy_pair_name=dict(type='str', required=True),
        copy_pace=dict(type='int', required=False, default=3)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info(f"Initializing the split SI task")
    logger.debug('copy_group_name: %s', module.params['copy_group_name'])
    logger.debug('copy_pair_name: %s', module.params['copy_pair_name'])
    logger.debug('copy_pace: %d', module.params['copy_pace'])
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.split_si()

    except HitachiBlockException as err:
        #import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    
    logger.info(f"Finished the split SI task")
    logger.debug('response: %s', json.dumps(response))
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_block_main()
