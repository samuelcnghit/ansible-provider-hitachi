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
module: hitachi_block_addlun
short_description: Adds a LUN to an iSCSI target.
description:
  - This module adds a LUN to the iSCSI target of the specified port.
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
    port_id:
        description:
            - The ID of the port.
        required: true
    host_group_number:
        description:
            - The number of the host group.
        required: true
    ldev_id:
        description:
            - The ID of the Logical Device (LDEV).
        required: true
    iscsi_name:
        description:
            - The name of the iSCSI target.
        required: false
    user:
        description:
            - The username used for authentication.
        required: true
    password:
        description:
            - The password used for authentication.
        required: true
        nolog: true
"""

EXAMPLES = """
- name: Add LUN to host group or iSCSI target
  hitachi_block_addlun:
    management_address: "storage.example.com"
    management_port: 8080
    port_id: "port123"
    host_group_number: 123
    ldev_id: 456
    iscsi_name: "iscsi_target"
    user: "admin"
    password: "password"
"""

def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        port_id=dict(type='str', required=True),
        host_group_number=dict(type='int', required=True),
        ldev_id=dict(type='int', required=True),
        iscsi_name=dict(type='str', required=False),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    logger.info("Initializing the addlun task")
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.add_lun()

    except HitachiBlockException as err:
        import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    logger.info("Finished the addlun task")
        
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_block_main()
