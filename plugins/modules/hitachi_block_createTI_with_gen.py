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
module: hitachi_block_createTI_with_gen
short_description: Creates a Thin Image pair with autosplit option.
description:
  - This module allows you to create Thin Image pair with autosplit option.
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
  snapshot_group_name:
    description:
      - The name of the snapshot group. Value should not exceed 32 characters.
    required: true
  snapshot_pool_id:
    description:
      - Specify the snapshot pool ID. Value should be equal to or greater than 0.
    required: true  
  pvol_ldev_id:
    description:
      - The LDEV number of the P-VOL with a decimal (base 10) number.
    required: true
  is_consistency_group:
    description:
      - Set it to true to create the snapshot group in the consistency group mode.
    required: false
    default: false
  generations:
    description:
      - Specify true to split after it is created. 
    required: true
"""

EXAMPLES = """
- name: Create ThinImage pair with autosplit option
  hitachi_block_createTI_with_gen:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    pvol_ldev_id: 10005
    snapshot_pool_id: 1
    snapshot_group_name: ABC
    generations: 1 
"""



def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        snapshot_group_name=dict(type='str', required=True),
        snapshot_pool_id=dict(type='int', required=True),
        pvol_ldev_id=dict(type='int', required=True),
        is_consistency_group=dict(type='bool', required=False),
        generations=dict(type='int', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.create_ti_with_generations()

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
