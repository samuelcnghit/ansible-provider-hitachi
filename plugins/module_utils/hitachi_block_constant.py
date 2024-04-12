import logging
from ansible.module_utils.six.moves.urllib import parse as urlparse


class Api(object):
    AFFECTEDRESOURCES = 'affectedResources'
    STATUS = 'status'
    STATE = 'state'
    ERROR = 'error'
    MESSAGEID = 'messageId'
    MESSAGE = 'message'
    CAUSE = 'cause'
    SOLUTION = 'solution'
    CHANGED = 'changed'
    MSG = 'msg'
    OUTPUTS = 'outputs'
    API_ERROR = 'api_error'
    SINGLE_QUOTE = "'"
    SERVER_DEFAULT = 'localhost'
    SERVER_PORT_DEFAULT = 443
    LDEVID = 'ldevId'
    POOLID = 'poolId'
    DATAREDUCTIONMODE = 'dataReductionMode'
    BLOCKCAPACITY = 'blockCapacity'
    BYTEFORMATCAPACITY = 'byteFormatCapacity'
    ADDITIONALBLOCKCAPACITY = 'additionalBlockCapacity'
    ADDITIONALBYTEFORMATCAPACITY = 'additionalByteFormatCapacity'
    JOBID = 'jobId'
    PORTID = 'portId'
    HOSTGROUPNAME = 'hostGroupName'
    ISCSINAME = 'iscsiName'
    HOSTGROUPNUMBER = 'hostGroupNumber'
    COPYGROUPNAME = 'copyGroupName'
    COPYPAIRNAME = 'copyPairName'
    REPLICATIONTYPE = 'replicationType'
    PVOLLDEVID = 'pvolLdevId'
    SVOLLDEVID = 'svolLdevId'
    ISNEWGROUPCREATION = 'isNewGroupCreation'
    COPYPACE = 'copyPace'
    ISCONSISTENCYGROUP = 'isConsistencyGroup'
    CONSISTENCYGROUPID = 'consistencyGroupId'
    ISDATAREDUCTIONFORCECOPY = 'isDataReductionForceCopy'
    PVOLMUNUMBER = 'pvolMuNumber'
    SNAPSHOTGROUPNAME = 'snapshotGroupName'
    SNAPSHOTPOOLID = 'snapshotPoolId'
    MUNUMBER = 'muNumber'
    AUTOSPLIT = 'autoSplit'
    ISCSIIPADDRESS = 'iscsiIpAddress'
    ISCSINAME = 'iscsiName'
    ISCSINICKNAME = 'iscsiNickname'
    TCPPORT = 'tcpPort'
    STARTID = 'startId'
    COUNT = 'count'
    EXTERNALPARITYGROUPID = 'externalParityGroupId'
    EXTERNALPATHGROUPID = 'externalPathGroupId'
    EXTERNALPORTIPADDRESS = 'externalPortIpAddress'
    EXTERNALPORTISCSINAME = 'externalPortIscsiName'
    LUN = 'lun'
    CHAPUSERNAME = 'chapUserName'
    WAYOFCHAPUSER = 'wayOfChapUser'
    CHAPPASSWORD = 'chapPassword'
    EXTERNAL_PORT_DEFAULT = 3260
    ADVISOR_PORT_DEFAULT = 80
    DATA_REDUCTION_MODE = 'dataReductionMode'
    DATA_REDUCTION_MODE_DISABLE = 'disabled'
    HOSTMODE = 'hostMode'


class Endpoints(object):
    POST_LDEVS = 'v1/objects/storages/{}/ldevs'
    GET_LDEVS_ONE = 'v1/objects/storages/{}/ldevs/{}'
    PUT_LDEVS_CHANGE_STATUS = 'v1/objects/storages/{}/ldevs/{}/actions/change-status/invoke'
    PUT_LDEVS_SHRED = 'v1/objects/storages/{}/ldevs/{}/actions/shred/invoke'
    DELETE_LDEVS = 'v1/objects/storages/{}/ldevs/{}'
    POST_HOST_GROUPS = 'v1/objects/storages/{}/host-groups'
    GET_HOST_GROUPS = 'v1/objects/storages/{}/host-groups/{},{}'
    DELETE_HOST_GROUPS = 'v1/objects/storages/{}/host-groups/{},{}'
    GET_HOST_ISCSIS = 'v1/objects/storages/{}/host-iscsis{}'
    POST_HOST_ISCSIS = 'v1/objects/storages/{}/host-iscsis'
    PUT_HOST_ISCSIS = 'v1/objects/storages/{}/host-iscsis/{},{},{}'
    POST_CHAP_USERS = 'v1/objects/storages/{}/chap-users'
    PUT_CHAP_USERS_SINGLE = 'v1/objects/storages/{}/chap-users/{},{},{},{}'
    GET_CHAP_USER = 'v1/objects/storages/{}/chap-users/{},{},{},{}'
    POST_LUNS = 'v1/objects/storages/{}/luns'
    GET_LUNS = 'v1/objects/storages/{}/luns{}'
    DELETE_LUNS = 'v1/objects/storages/{}/luns/{},{},{}'
    GET_LOCAL_CLONE_COPYGROUPS = 'v1/objects/storages/{}/local-clone-copygroups'
    POST_LOCAL_CLONE_COPYPAIRS = 'v1/objects/storages/{}/local-clone-copypairs'
    GET_LOCAL_CLONE_COPYGROUPS_ONE = 'v1/objects/storages/{}/local-clone-copygroups/{}'
    POST_LOCAL_CLONE_COPYPAIRS_SPLIT = 'v1/objects/storages/{}/local-clone-copypairs/{}/actions/split/invoke'
    POST_LOCAL_CLONE_COPYPAIRS_RESYNC = 'v1/objects/storages/{}/local-clone-copypairs/{}/actions/resync/invoke'
    POST_SNAPSHOTS = 'v1/objects/storages/{}/snapshots'
    GET_SNAPSHOT_GROUPS = 'v1/objects/storages/{}/snapshot-groups'
    GET_SNAPSHOT_GROUPS_ONE = 'v1/objects/storages/{}/snapshot-groups/{}'
    POST_SNAPSHOTS_SPLIT = 'v1/objects/storages/{}/snapshots/{}/actions/split/invoke'
    POST_SNAPSHOTS_RESYNC = 'v1/objects/storages/{}/snapshots/{}/actions/resync/invoke'
    POST_SNAPSHOTS_RESTORE = 'v1/objects/storages/{}/snapshots/{}/actions/restore/invoke'
    GET_JOBS = 'v1/objects/storages/{}/jobs/{}'
    PUT_ISCSI_PORTS_DISCOVER = 'v1/objects/storages/{}/iscsi-ports/{}/actions/discover/invoke'
    PUT_ISCSI_PORTS_REGISTER = 'v1/objects/storages/{}/iscsi-ports/{}/actions/register/invoke'
    PUT_ISCSI_PORTS_CHECK = 'v1/objects/storages/{}/iscsi-ports/{}/actions/check/invoke'
    PUT_ISCSI_PORTS_REMOVE = 'v1/objects/storages/{}/iscsi-ports/{}/actions/remove/invoke'
    GET_ISCSI_PORTS = 'v1/objects/storages/{}/iscsi-ports/{}'
    GET_EXTERNAL_STORAGE_PORTS = 'v1/objects/storages/{}/external-storage-ports{}'
    GET_EXTERNAL_STORAGE_LUNS = 'v1/objects/storages/{}/external-storage-luns{}'
    GET_STORAGES_ONE = 'v1/objects/storages/{}'
    GET_EXTERNAL_PATH_GROUPS_ONE = 'simple/v1/objects/external-path-groups/{}'
    GET_EXTERNAL_PARITY_GROUPS_ONE = 'simple/v1/objects/external-parity-groups/{}'
    GET_EXTERNAL_VOLUMES = 'simple/v1/objects/external-volumes{}'
    POST_EXTERNAL_VOLUMES = 'simple/v1/objects/external-volumes'
    POST_SESSIONS = 'simple/v1/objects/sessions'
    DELETE_SESSIONS = 'simple/v1/objects/sessions/{}'
    GET_COMMAND_STATUS = 'simple/v1/objects/command-status/{}'
    DELETE_COMMAND_STATUS = 'simple/v1/objects/command-status/{}'
    GET_HOST_ISCSI_PATHS = 'v1/views/host-iscsi-paths?{}'


class PfRestEndpoints(object):
    POST_LDEVS = 'v1/objects/ldevs'
    GET_LDEVS_ONE = 'v1/objects/ldevs/{}'
    PUT_LDEVS_CHANGE_STATUS = 'v1/objects/ldevs/{}/actions/change-status/invoke'
    PUT_LDEVS_SHRED = 'v1/objects/ldevs/{}/actions/shred/invoke'
    DELETE_LDEVS = 'v1/objects/ldevs/{}'
    POST_HOST_GROUPS = 'v1/objects/host-groups'
    GET_HOST_GROUPS = 'v1/objects/host-groups/{},{}'
    DELETE_HOST_GROUPS = 'v1/objects/host-groups/{},{}'
    GET_HOST_ISCSIS = 'v1/objects/host-iscsis{}'
    POST_HOST_ISCSIS = 'v1/objects/host-iscsis'
    PUT_HOST_ISCSIS = 'v1/objects/host-iscsis/{},{},{}'
    POST_CHAP_USERS = 'v1/objects/chap-users'
    PUT_CHAP_USERS_SINGLE = 'v1/objects/chap-users/{},{},{},{}'
    GET_CHAP_USER = 'v1/objects/chap-users/{},{},{},{}'
    POST_LUNS = 'v1/objects/luns'
    GET_LUNS = 'v1/objects/luns{}'
    DELETE_LUNS = 'v1/objects/luns/{},{},{}'
    GET_LOCAL_CLONE_COPYGROUPS = 'v1/objects/local-clone-copygroups'
    POST_LOCAL_CLONE_COPYPAIRS = 'v1/objects/local-clone-copypairs'
    GET_LOCAL_CLONE_COPYGROUPS_ONE = 'v1/objects/local-clone-copygroups/{}'
    POST_LOCAL_CLONE_COPYPAIRS_SPLIT = 'v1/objects/local-clone-copypairs/{}/actions/split/invoke'
    POST_LOCAL_CLONE_COPYPAIRS_RESYNC = 'v1/objects/local-clone-copypairs/{}/actions/resync/invoke'
    POST_SNAPSHOTS = 'v1/objects/snapshots'
    GET_SNAPSHOT_GROUPS = 'v1/objects/snapshot-groups'
    GET_SNAPSHOT_GROUPS_ONE = 'v1/objects/snapshot-groups/{}'
    POST_SNAPSHOTS_SPLIT = 'v1/objects/snapshots/{}/actions/split/invoke'
    POST_SNAPSHOTS_RESYNC = 'v1/objects/snapshots/{}/actions/resync/invoke'
    POST_SNAPSHOTS_RESTORE = 'v1/objects/snapshots/{}/actions/restore/invoke'
    GET_JOBS = 'v1/objects/jobs/{}'
    PUT_ISCSI_PORTS_DISCOVER = 'v1/objects/iscsi-ports/{}/actions/discover/invoke'
    PUT_ISCSI_PORTS_REGISTER = 'v1/objects/iscsi-ports/{}/actions/register/invoke'
    PUT_ISCSI_PORTS_CHECK = 'v1/objects/iscsi-ports/{}/actions/check/invoke'
    PUT_ISCSI_PORTS_REMOVE = 'v1/objects/iscsi-ports/{}/actions/remove/invoke'
    GET_ISCSI_PORTS = 'v1/objects/iscsi-ports/{}'
    GET_EXTERNAL_STORAGE_PORTS = 'v1/objects/external-storage-ports{}'
    GET_EXTERNAL_STORAGE_LUNS = 'v1/objects/external-storage-luns{}'
    GET_STORAGES_ONE = 'v1/objects/storages/{}'
    GET_EXTERNAL_PATH_GROUPS_ONE = 'simple/v1/objects/external-path-groups/{}'
    GET_EXTERNAL_PARITY_GROUPS_ONE = 'simple/v1/objects/external-parity-groups/{}'
    GET_EXTERNAL_VOLUMES = 'simple/v1/objects/external-volumes{}'
    POST_EXTERNAL_VOLUMES = 'simple/v1/objects/external-volumes'
    POST_SESSIONS = 'simple/v1/objects/sessions'
    DELETE_SESSIONS = 'simple/v1/objects/sessions/{}'
    GET_COMMAND_STATUS = 'simple/v1/objects/command-status/{}'
    DELETE_COMMAND_STATUS = 'simple/v1/objects/command-status/{}'
    GET_HOST_ISCSI_PATHS = 'v1/views/host-iscsi-paths?{}'


class Http(object):
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    DELETE = 'DELETE'
    BASE_URL = '/ConfigurationManager/'
    CONTENT_TYPE = 'Content-Type'
    APPLICATION_JSON = 'application/json'
    RESPONSE_JOB_STATUS = 'Response-Job-Status'
    COMPLETED = 'Completed'
    HEADERS_JSON = {
        CONTENT_TYPE: APPLICATION_JSON,
        RESPONSE_JOB_STATUS: COMPLETED
    }
    HTTP = 'http://'
    HTTPS = 'https://'
    DEFAULT_PORT = 22015
    DEFAULT_SSL_PORT = 22016
    OPEN_URL_TIMEOUT = 300
    USER_AGENT = 'automation-module'


class ModuleArgs(object):
    NULL = "None"
    SERVER = 'management_address'
    SERVER_PORT = 'management_port'
    USER = 'user'
    PASSWORD = 'password'
    CHECK_MODE = 'check_mode'
    STORAGE_DEVICE_ID = 'storage_device_id'
    POOL_ID = 'pool_id'
    BLOCK_CAPACITY = 'block_capacity'
    CAPACITY_MB = 'capacity_mb'
    PORT_ID = 'port_id'
    HOST_GROUP_NAME = 'host_group_name'
    ISCSI_NAME = 'iscsi_name'
    NICK_NAME = 'nick_name'
    HOST_GROUP_NUMBER = 'host_group_number'
    LDEV_ID = 'ldev_id'
    DATA_REDUCTION_MODE = 'data_reduction_mode'
    COPY_GROUP_NAME = 'copy_group_name'
    PVOL_LDEV_ID = 'pvol_ldev_id'
    SVOL_LDEV_ID = 'svol_ldev_id'
    COPY_PACE = 'copy_pace'
    CONSISTENCY_GROUP_ID = 'consistency_group_id'
    SNAPSHOT_GROUP_NAME = 'snapshot_group_name'
    SNAPSHOT_POOL_ID = 'snapshot_pool_id'
    COPY_SPEED = 'copy_speed'
    IS_CONSISTENCY_GROUP = 'is_consistency_group'
    COPY_PAIR_NAME = 'copy_pair_name'
    MU_NUMBER = 'mu_number'
    GENERATIONS = 'generations'
    EXTERNAL_PORT_ID = 'external_port_id'
    EXTERNAL_LUN = 'external_lun'
    EXTERNAL_PARITYGROUP_ID = 'external_paritygroup_id'
    EXTERNAL_IP = 'external_IP'
    EXTERNAL_PORT_NUMBER = 'external_port_number'
    EXTERNAL_ISCSI_TARGET = 'external_iscsi_target'
    EXTERNAL_PATHGROUP_ID = 'external_pathgroup_id'
    ADVISOR_PORT = 'advisor_port'
    CHAP_USER_NAME = 'chap_user_name'
    WAY_OF_CHAP_USER = 'way_of_chap_user'
    CHAP_PASSWORD = 'chap_password'
    HOST_MODE = 'host_mode'
    SHREDDING_PATTERN = 'shredding_pattern'
    DELETE_LDEV = 'delete_ldev'

class State(object):
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELED = 'canceled'

    @staticmethod
    def is_failed(status):
        return status in [State.FAILED, State.CANCELED]

    @staticmethod
    def is_finished(status):
        return status in [State.COMPLETED, State.FAILED, State.CANCELED]


class AutomationConstants(object):
    PORT_NUMBER_MIN = 0
    PORT_NUMBER_MAX = 49151
    NAME_PARAMS_MIN = 1
    NAME_PARAMS_MAX = 256
    MIN_SIZE_ZERO_ALLOWED = 0
    MIN_SIZE_ALLOWED = 1
    MAX_SIZE_ALLOWED = 999999999999
    MAX_TIME_ALLOWED = 999
    MIN_TIME_ALLOWED = 1
    POOL_ID_MIN = 0
    POOL_ID_MAX = 256
    LDEV_ID_MIN = 0
    LDEV_ID_MAX = 65535


class ErrorMessages(object):
    INVALID_PORT_NUMBER_ERR = 'The specified value is invalid' +\
        ' ({}: {}). Specify the value within a valid range (min: ' +\
        str(AutomationConstants.PORT_NUMBER_MIN) + ', max: ' +\
        str(AutomationConstants.PORT_NUMBER_MAX) + ').'
    INVALID_LDEVID_NUMBER_ERR = 'The specified value is invalid' +\
        ' ({}: {}). Specify the value within a valid range (min: ' +\
        str(AutomationConstants.LDEV_ID_MIN) + ', max: ' +\
        str(AutomationConstants.LDEV_ID_MAX) + ').'
    INVALID_POOLID_NUMBER_ERR = 'The specified value is invalid' +\
        ' ({}: {}). Specify the value within a valid range (min: ' +\
        str(AutomationConstants.POOL_ID_MIN) + ', max: ' +\
        str(AutomationConstants.POOL_ID_MAX) + ').'    
    INVALID_RANGE_VALUE = 'The specified value is invalid' +\
        ' ({}: {}). Specify the value within a valid range (min: {}' +\
        ', max: {}' +\
        ').' 
    HTTP_4xx_ERRORS = 'Invalid request sent by the client.' +\
        ' API responded with client error code ({}). Reason: {}'
    HTTP_5xx_ERRORS = 'The server encountered an unexpected ' +\
        'condition. API responded with server error code ({}). Reason: {}'
    API_COMMUNICATION_ERR = 'Communication with the target server' +\
        ' failed. Reason: {}'
    NOT_AVAILABLE = 'Not Available.'
    REQUIRED_VALUE_ERR = 'The value for the parameter is' +\
        ' required. ({}) Specify a valid value.'
    API_TIMEOUT_ERR = 'A timeout occurred because no response' +\
        ' was received from the server.'
    INVALID_TYPE_VALUE = 'The specified value is not an integer' +\
        ' type ({}: {}). Specify an integer value.'
    INVALID_NAME_SIZE = 'The argument of the parameter is invalid' +\
        ' ({}: {}). The length must be between 1 and 256.'
    INVALID_NAME_SIZE_ZERO = 'The argument of the parameter is invalid' +\
        ' ({}: {}). The length must be between 0 and 256.'
    INVALID_NAME_SIZE_1_8 = 'The argument of the parameter is invalid' +\
        ' ({}: {}). The length must be between 1 and 8.'
    INVALID_STR_LEN = 'The argument of the parameter is invalid' +\
        ' ({}: {}). The length must be between {} and {}.'    
    INVALID_RANGE_VALUE_0_1023 = 'The argument of the parameter is invalid' +\
        ' ({}: {}). The range must be between 0 and 1023.'
    INVALID_SIZE_VALUE = 'The specified size argument has an invalid range' +\
        ' ({}: {}). Specify in range in between 1 to 999999999999.'
    INVALID_SIZE_VALUE_ZERO = 'The specified size argument has an invalid range' +\
        ' ({}: {}). Specify in range in between 0 to 999999999999.'
    INVALID_HEX_VALUE = 'The specified hexadecimal number argument is invalid' +\
        ' ({}: {}).'
    INVALID_TIME_VALUE = 'The specified time argument has an invalid range' +\
        ' ({}: {}). Specify in range in between 1 to 999.'
    INVALID_SECRET_SIZE = 'The specified value is invalid' +\
        ' ({}: {}). Secret should be 12 to 32 chars.'


class LogMessages(object):
    ENTER_METHOD = 'Enter method: {}'
    LEAVE_METHOD = 'Leave method: {}'
    API_REQUEST_START = 'API Request: {} {}'
    API_RESPONSE = 'API Response: {}'


class Log(object):
    SYSLOG_IDENTIFIER = 'SYSLOG_IDENTIFIER'
    PRIORITY = 'PRIORITY'
    # There is no applicable level in Python for following priorities
    # 0(Emergency), 1(Alert), 5(Notice)
    ARGS = {
        logging.DEBUG: {PRIORITY: 7},
        logging.INFO: {PRIORITY: 6},
        logging.WARNING: {PRIORITY: 4},
        logging.ERROR: {PRIORITY: 3},
        logging.CRITICAL: {PRIORITY: 2},
    }
