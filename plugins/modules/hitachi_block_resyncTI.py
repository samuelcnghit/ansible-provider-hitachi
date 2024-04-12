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
module: hitachi_block_resyncTI
short_description: Resyncs a Thin Image pair.
description:
  - This module allows you to resync a Thin Image pair.
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
  pvol_ldev_id:
    description:
      - The LDEV number of the P-VOL with a decimal (base 10) number.
    required: true
  mu_number:
    description:
      - Specify the mirror unit number in the range from 0 to 1023.
    required: true
"""

EXAMPLES = """
- name: Resync ThinImage pair
  hitachi_block_resyncTI:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    pvol_ldev_id: 10005
    snapshot_group_name: xyz
    mu_number: 1
"""



def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        snapshot_group_name=dict(type='str', required=True),
        pvol_ldev_id=dict(type='int', required=True),
        mu_number=dict(type='int', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.resync_ti()

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
