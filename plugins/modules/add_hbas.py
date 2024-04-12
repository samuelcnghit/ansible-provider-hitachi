from __future__ import absolute_import, print_function
from http import server
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
module: add_hbas
short_description: Adds hbas to a compute node.
description:
  - This module adds hbas to a compute node on a Hitachi Virtual Storage Platform One SDS Block storage system.
options:
  management_address:
    description:
      - The hostname or IP address of the storage system.
    required: true
  management_port:
    description:
      - The port number of the storage system.
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
  server_nickname:
    description:
      - The name of the compute node.
    required: true
  iscsi_name:
    description:
      - The iSCSI name of the initiator.
    required: true
'''

EXAMPLES = '''
  - name: Add hbas to a Compute Node
    add_hbas:
      management_address: "example.com"
      user: "admin"
      password:  "secret"
      server_nickname: "example_name"
      iscsi_name: "iqn.1991-05.com.microsoft:win-g7mqgirmfls12016"
      target_port_name: "001-iSCSI-001"
'''

def hitachi_vssb_main():
    module_args = dict(
        management_address=dict(type='str', required=True),
        management_port=dict(type='int', required=False, default=VSSB_Api.SERVER_PORT_DEFAULT),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        server_nickname=dict(type='str', required=True),
        iscsi_name=dict(type='str', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True,
    )
    logger = init_logger(module)
    try:
        module.params[ModuleArgs.CHECK_MODE] = module.check_mode
        executors = Executors(module.params)
        response = executors.add_hbas()

    except HitachiBlockException as err:
        import json
        logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        module.fail_json(**err.error_response())
    except Exception as e:
        logger.exception(repr(e))
        module.fail_json(msg=str(e))
    module.exit_json(**response)


if __name__ == '__main__':  # pragma: no cover
    hitachi_vssb_main()
