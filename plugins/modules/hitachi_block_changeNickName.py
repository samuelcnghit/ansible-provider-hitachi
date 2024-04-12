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
module: hitachi_block_changeNickName
short_description: Changes the nickname of an iSCSI name.
description:
  - This module changes the nickname of an iSCSI name.
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
        nolog: true
    port_id:
        description:
            - The ID of the port.
        required: true
    host_group_number:
        description:
            - The number of the host group.
        required: true
    iscsi_name:
        description:
            - The name of the iSCSI target.
        required: true
    nick_name:
        description:
            - The nickname.
        required: true
"""

EXAMPLES = """
- name: Change the iSCSI nick name
  hitachi_block_changeNickName:
    management_address: "storage.example.com"
    user: "admin"
    password: "password"
    port_id: "port123"
    host_group_number: 123
    iscsi_name: "iscsi_target"
    nick_name: "nickname"
"""



def hitachi_block_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        port_id=dict(type='str', required=True),
        host_group_number=dict(type='int', required=True),
        iscsi_name=dict(type='str', required=True),
        nick_name=dict(type='str', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=False,
    )
    logger = init_logger(module)
    logger.info("Initializing the change nick name task for iSCSI ")
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.change_nickname()

    except HitachiBlockException as err:
        import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    logger.info("Finished the change nick name task for iSCSI ")
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_block_main()
