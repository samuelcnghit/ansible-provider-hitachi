from __future__ import absolute_import, print_function
from http import server
from tokenize import Number
from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.hitachi_vssb_client import (
    init_logger,
    Executors,
    HitachiBlockException,
)
from ansible.module_utils.hitachi_vssb_constant import (
    VSSB_Api,
    ModuleArgs
)

DOCUMENTATION = """
---
module: create_volume
short_description: Manage pools and volumes.
description:
  - This module allows you to manage pools and volumes on a Hitachi Virtual Storage Platform One SDS Block storage system.
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
  pool_name:
    description:
      - The name of the pool.
    required: true
  capacity_mb:
    description:
      - The capacity of each volume in megabytes.
    required: true
  number:
    description:
      - The number of volumes to create.
    required: true
  base_name:
    description:
      - The base name of the volumes to create.
    required: true

"""

EXAMPLES = """
- name: Create volumes
  create_volume:
    management_address: "storage.example.com"
    user: "admin"
    password: "secret"
    pool_name: "example_pool"
    capacity: 102400
    number: 5
    base_name: "volume"
"""


def hitachi_vssb_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=VSSB_Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        pool_name=dict(type='str', required=True),
        capacity_mb=dict(type='int', required=True),
        number=dict(type='int', required=True),
        base_name=dict(type='str', required=True),
        start_number=dict(type='int', required=False, default=VSSB_Api.VOLUME_BASENAME_START_NUMBER_DEFAULT),
        number_of_digit=dict(type='int', required=False, default=VSSB_Api.VOLUME_BASENAME_NUMBER_OF_DIGIT_DEFAULT)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info("Initializing the create volume task")
    
    module.log("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        
        response = executors.create_volume()

    except HitachiBlockException as err:
        import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    logger.info("Completed the create volume task")
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_vssb_main()
