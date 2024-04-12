from __future__ import absolute_import, print_function
from http import server
from unicodedata import name
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

DOCUMENTATION = '''
---
module: expand_volume
short_description: Expands a volume.
description:
  - This module expands a volume on a Hitachi Virtual Storage Platform One SDS Block storage system.
options:
  management_address:
    description:
      - The hostname or IP address of the storage system.
    required: true
  management_port:
    description:
      - The port number of the management_address.
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
      - The name of the volume to expand.
    required: true
  capacity_mb:
    description:
      - The new capacity of the volume in Mega bytes.
    required: true
'''

EXAMPLES = '''
- name: Expand a volume
  expand_volume:
    management_address: "example.com"
    management_port : 443
    user: "admin"
    password: "secret"
    volume_name: "example_volume"
    capacity: 102400
'''


def hitachi_vssb_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=VSSB_Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        volume_name=dict(type='str', required=True),
        capacity_mb=dict(type='int', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=False,
    )
    logger = init_logger(module)
    logger.info(f"Initializing the expand volume task")
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.expand_volume()

    except HitachiBlockException as err:
        import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
        
    logger.info(f"Finished the expand volume task")
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_vssb_main()
