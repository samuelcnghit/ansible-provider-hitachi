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
module: hitachi_block_deleteVol
short_description: Deletes a logical device (LDEV) .
description:
  - This module deletes a logical device (LDEV) from the Hitachi storage system.
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
  ldev_id:
    description:
      - The ID of the logical device (LDEV) to delete.
    required: true
  shredding_pattern:
    description:
      - The shredding pattern to securely erase the data on the LDEV (optional).
    required: false
  delete_ldev:
    description:
      - Whether to delete the LDEV or not.
    required: false
    default: false
  user:
    description:
      - The username used for authentication.
    required: true
  password:
    description:
      - The password used for authentication.
    required: true
    no_log: true
"""

EXAMPLES = """
- name: Delete a logical device (LDEV)
  hitachi_block_deleteVol:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    ldev_id: 123
    delete_ldev: true
"""



def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        ldev_id=dict(type='int', required=True),
        shredding_pattern=dict(type='str', required=False),
        delete_ldev=dict(type='bool', required=False),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.delete_volume()

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
