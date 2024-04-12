from __future__ import absolute_import, print_function
from http import server
from tokenize import Number
from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.hitachi_vssb_client import (
    init_logger,
    Executors,
    HitachiBlockException
)
from ansible.module_utils.hitachi_vssb_constant import (
    VSSB_Api,
    ModuleArgs
)

DOCUMENTATION = """
---
module: delete_volume
short_description: Deletes a volume from a Hitachi Virtual Storage Platform One SDS Block storage system.
description:
  - This module deletes a volume from a Hitachi Virtual Storage Platform One SDS Block storage system.
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
  volume_name:
    description:
      - The name of the volume to delete.
    required: true
"""

EXAMPLES = """
- name: Delete a volume
  delete_volume:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    volume_name: "example_volume"
"""


def hitachi_vssb_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=VSSB_Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        volume_name=dict(type='str', required=True),
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info("Initializing the delete volume task")
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
    logger.info("Completed the delete volume task")
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_vssb_main()
