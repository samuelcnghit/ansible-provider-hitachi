from datetime import datetime
import functools
import json
import copy
import time
import urllib.parse

from http import HTTPStatus

import logging
from ansible.module_utils.urls import open_url, urllib_error, socket
from ansible.module_utils.six.moves.urllib import parse as urlparse
from ansible.module_utils.six.moves.http_client import HTTPException
from ansible.module_utils._text import to_native, to_text
from ansible.module_utils.hitachi_block_constant import (
    Api,
    PfRestEndpoints,
    Http,
    ModuleArgs,
    State,
    ErrorMessages,
    AutomationConstants,
    LogMessages,
    Log,
)

from ansible.module_utils.hitachi_ansible_common import (
    initialize_filehandler_logger,
    
)

def checkHex(s):
    # Iterate over string
    for ch in s:
        # Check if the character
        # is invalid
        if ((ch < '0' or ch > '9') and
            (ch < 'A' or ch > 'F')):
            # no
            return False
        
    # characters are valid
    return True

class HitachiBlockModuleLogHandler(logging.Handler):
    ''' Log Handler to send message to AnsibleModule's log() method. '''

    def __init__(self, module, **kwargs):
        '''
        :arg module: AnsibleModule object.
        '''
        super(HitachiBlockModuleLogHandler, self).__init__(**kwargs)
        self.module = module
        self._fmt = '%(filename)s, line %(lineno)d [%(levelname)s]: ' +\
                    '%(message)s'

        self.setFormatter(logging.Formatter(self._fmt))
        self.setLevel(logging.DEBUG)

        self.name = self.module._name

    def emit(self, record):
        ''' Sends message to journal/syslog through AnsibleModule.log(). '''
        msg = self.format(record)
        log_args = {
            Log.SYSLOG_IDENTIFIER: self.name
        }
        log_args.update(Log.ARGS.get(record.levelno, {}))

        if record.levelno == logging.WARNING:
            self.module._warnings.append(msg)

        self.module.log(msg, log_args=log_args)


def get_logger():
    '''Always returns the same logger instance for automation module'''
    return logging.getLogger('AutomationModuleLogger')


def init_logger(module):
    ''' Create new logger and attach handler to it.
        This method should be called only once on module initialization.
    '''
    logger = get_logger()
    # debug log will be sent to the Handler, but not shown unless ANSIBLE_DEBUG
    # is True because our handler sets level independently.
    logger.setLevel(logging.DEBUG)
    
    

    logger.addHandler(HitachiBlockModuleLogHandler(module))
    
    # Create rotating file handler
    initialize_filehandler_logger(logger)

    return logger


def get_with_log(class_name=''):
    ''' Returns a decorator function for `class_name` '''
    logger = get_logger()

    def with_log(func):
        ''' Decorates `func` to output debug log before and after execution '''
        if class_name:
            name = class_name + '.' + func.__name__ + '()'
        else:
            name = func.__name__ + '()'

        @functools.wraps(func)
        def traced(*args, **kwargs):
            logger.debug(LogMessages.ENTER_METHOD.format(name))
            result = func(*args, **kwargs)
            logger.debug(LogMessages.LEAVE_METHOD.format(name))
            return result
        return traced
    return with_log


class Params(object):
    def __init__(self, params=None):
        if params is not None:
            self.management_address = params.get(ModuleArgs.SERVER)
            self.management_port = params.get(ModuleArgs.SERVER_PORT)
            self.user = params.get(ModuleArgs.USER)
            self.password = params.get(ModuleArgs.PASSWORD)
            self.check_mode = params.get(ModuleArgs.CHECK_MODE)
            self.storage_device_id = params.get(ModuleArgs.STORAGE_DEVICE_ID)
            self.pool_id = params.get(ModuleArgs.POOL_ID)
            self.block_capacity = params.get(ModuleArgs.BLOCK_CAPACITY)
            self.capacity_mb = params.get(ModuleArgs.CAPACITY_MB)
            self.port_id = params.get(ModuleArgs.PORT_ID)
            self.host_group_name = params.get(ModuleArgs.HOST_GROUP_NAME)
            self.host_mode = params.get(ModuleArgs.HOST_MODE)
            self.iscsi_name = params.get(ModuleArgs.ISCSI_NAME)
            self.nick_name = params.get(ModuleArgs.NICK_NAME)
            self.host_group_number = params.get(ModuleArgs.HOST_GROUP_NUMBER)
            self.ldev_id = params.get(ModuleArgs.LDEV_ID)
            self.data_reduction_mode = params.get(ModuleArgs.DATA_REDUCTION_MODE)
            self.copy_group_name = params.get(ModuleArgs.COPY_GROUP_NAME, ModuleArgs.NULL)
            self.pvol_ldev_id = params.get(ModuleArgs.PVOL_LDEV_ID, ModuleArgs.NULL)
            self.svol_ldev_id = params.get(ModuleArgs.SVOL_LDEV_ID, ModuleArgs.NULL)
            self.copy_pace = params.get(ModuleArgs.COPY_PACE, ModuleArgs.NULL)
            self.consistency_group_id = params.get(ModuleArgs.CONSISTENCY_GROUP_ID, ModuleArgs.NULL)
            self.snapshot_group_name = params.get(ModuleArgs.SNAPSHOT_GROUP_NAME)
            self.snapshot_pool_id = params.get(ModuleArgs.SNAPSHOT_POOL_ID)
            self.copy_speed = params.get(ModuleArgs.COPY_SPEED)
            self.is_consistency_group = params.get(ModuleArgs.IS_CONSISTENCY_GROUP)
            self.copy_pair_name = params.get(ModuleArgs.COPY_PAIR_NAME, ModuleArgs.NULL)
            self.mu_number = params.get(ModuleArgs.MU_NUMBER)
            self.generations = params.get(ModuleArgs.GENERATIONS)
            self.external_port_id = params.get(ModuleArgs.EXTERNAL_PORT_ID)
            self.external_lun = params.get(ModuleArgs.EXTERNAL_LUN)
            self.external_paritygroup_id = params.get(ModuleArgs.EXTERNAL_PARITYGROUP_ID)
            self.external_IP = params.get(ModuleArgs.EXTERNAL_IP)
            self.external_port_number = params.get(ModuleArgs.EXTERNAL_PORT_NUMBER)
            self.external_iscsi_target = params.get(ModuleArgs.EXTERNAL_ISCSI_TARGET)
            self.external_pathgroup_id = params.get(ModuleArgs.EXTERNAL_PATHGROUP_ID)
            self.advisor_port = params.get(ModuleArgs.ADVISOR_PORT)
            self.request_params = None
            self.chap_user_name = params.get(ModuleArgs.CHAP_USER_NAME)
            self.way_of_chap_user = params.get(ModuleArgs.WAY_OF_CHAP_USER)
            self.chap_password = params.get(ModuleArgs.CHAP_PASSWORD)
            self.shredding_pattern = params.get(ModuleArgs.SHREDDING_PATTERN, ModuleArgs.NULL)
            self.delete_ldev = params.get(ModuleArgs.DELETE_LDEV)
            self.local_clone_copygroup_id = None
            self.local_clone_copypair_id = None
            self.snapshot_group_id = None
            self.snapshot_id = None
            self.auto_split = None
            self.session_id = None

    @property
    def management_address(self):
        return self._server

    @management_address.setter
    def management_address(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.SERVER, value)
        self._server = value

    @property
    def management_port(self):
        return self._server_port

    @management_port.setter
    def management_port(self, value):
        if value is not None:
            Params.validate_non_bool(ModuleArgs.SERVER_PORT, value)
            if value != Api.SERVER_PORT_DEFAULT and (value < AutomationConstants.PORT_NUMBER_MIN or \
                    value > AutomationConstants.PORT_NUMBER_MAX):
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_PORT_NUMBER_ERR.format(
                        ModuleArgs.SERVER_PORT, value))
        self._server_port = value

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.USER, value)
        self._user = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.PASSWORD, value, True)
        self._password = value

    @property
    def storage_device_id(self):
        return self._storage_device_id

    @storage_device_id.setter
    def storage_device_id(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.STORAGE_DEVICE_ID, value)
        self._storage_device_id = value

    # TODO - fixme, "maximum recursion depth exceeded in comparison"
    # @property
    # def mu_number(self):
    #     return self.mu_number

    # @mu_number.setter
    # def mu_number(self, value):
    #     if value is not None:
    #         Params.validate_non_bool(ModuleArgs.MU_NUMBER, value)
    #         min=0
    #         max=1023
    #         if value < min or value > max:
    #             raise HitachiBlockModuleException(
    #                 ErrorMessages.INVALID_RANGE_VALUE.format(
    #                     ModuleArgs.MU_NUMBER, value, min, max))
    #     self.mu_number = value

    @property
    def pool_id(self):
        return self._pool_id

    @pool_id.setter
    def pool_id(self, value):
        if value is not None:
            Params.validate_non_bool(ModuleArgs.POOL_ID, value)
            if value < AutomationConstants.POOL_ID_MIN or \
                    value > AutomationConstants.POOL_ID_MAX:
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_POOLID_NUMBER_ERR.format(
                        ModuleArgs.POOL_ID, value))
        self._pool_id = value

    # TODO - why this getter setter results in recursion??
    # @property
    # def shredding_pattern(self):
    #     return self.shredding_pattern

    # @shredding_pattern.setter
    # def shredding_pattern(self, value):
    #     if value is not None:
    #         Params.validate_name_parameter(ModuleArgs.SHREDDING_PATTERN, value)
    #     self.shredding_pattern = value

    @property
    def snapshot_group_name(self):
        return self._snapshot_group_name

    @snapshot_group_name.setter
    def snapshot_group_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.SNAPSHOT_GROUP_NAME, value)
            min=1
            max=32
            if len(value) < min or len(value) > max:
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_STR_LEN.format(
                        ModuleArgs.SNAPSHOT_GROUP_NAME, value, min, max))
        self._snapshot_group_name = value

    @property
    def copy_group_name(self):
        return self._copy_group_name

    @copy_group_name.setter
    def copy_group_name(self, value):
        Params.validate_name_parameter(ModuleArgs.COPY_GROUP_NAME, value)
        self._copy_group_name = value

    @property
    def copy_pair_name(self):
        return self._copy_pair_name

    @copy_pair_name.setter
    def copy_pair_name(self, value):
        Params.validate_name_parameter(ModuleArgs.COPY_PAIR_NAME, value)
        self._copy_pair_name = value

    @property
    def pvol_ldev_id(self):
        return self._pvol_ldev_id

    @pvol_ldev_id.setter
    def pvol_ldev_id(self, value):
        Params.validate_size_value(ModuleArgs.PVOL_LDEV_ID, value)
        self._pvol_ldev_id = value

    @property
    def svol_ldev_id(self):
        return self._svol_ldev_id

    @svol_ldev_id.setter
    def svol_ldev_id(self, value):
        Params.validate_size_value(ModuleArgs.SVOL_LDEV_ID, value)
        self._svol_ldev_id = value

    @property
    def copy_pace(self):
        return self._copy_pace

    @copy_pace.setter
    def copy_pace(self, value):
        Params.validate_size_value(ModuleArgs.COPY_PACE, value)
        self._copy_pace = value

    @property
    def consistency_group_id(self):
        return self._consistency_group_id

    @consistency_group_id.setter
    def consistency_group_id(self, value):
        Params.validate_size_value(ModuleArgs.CONSISTENCY_GROUP_ID, value)
        self._consistency_group_id = value

    @property
    def block_capacity(self):
        return self._block_capacity

    @block_capacity.setter
    def block_capacity(self, value):
        if value is not None:
            if value < 1:
                raise HitachiBlockModuleException('Specified value of capacity argument is out of range. Specify a number greater than 0.')
            if value > 999999999999:
                raise HitachiBlockModuleException('Specified value of capacity argument is out of range. Specify a number less than 999999999999.')
        self._block_capacity = value

    @property
    def capacity_mb(self):
        return self._capacity_mb

    @capacity_mb.setter
    def capacity_mb(self, value):
        if value is not None:
            if value < 1:
                raise HitachiBlockModuleException('Specified value of capacity argument is out of range. Specify a number greater than 0.')
            if value > 999999999999:
                raise HitachiBlockModuleException('Specified value of capacity argument is out of range. Specify a number less than 999999999999.')
        self._capacity_mb = value
    @property
    def port_id(self):
        return self._port_id

    @port_id.setter
    def port_id(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.PORT_ID, value)
        self._port_id = value

    @property
    def host_group_name(self):
        return self._host_group_name

    @host_group_name.setter
    def host_group_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.HOST_GROUP_NAME, value)
        self._host_group_name = value

    @property
    def host_mode(self):
        return self._host_mode

    @host_mode.setter
    def host_mode(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.HOST_MODE, value)
        self._host_mode = value

    @property
    def iscsi_name(self):
        return self._iscsi_name

    @iscsi_name.setter
    def iscsi_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.ISCSI_NAME, value)
        self._iscsi_name = value

    @property
    def nick_name(self):
        return self._nick_name

    @nick_name.setter
    def nick_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.NICK_NAME, value)
        self._nick_name = value

    @property
    def host_group_number(self):
        return self._host_group_number

    @host_group_number.setter
    def host_group_number(self, value):
        if value:
            Params.validate_size_value_zero(ModuleArgs.HOST_GROUP_NUMBER, value)
        self._host_group_number = value

    @property
    def ldev_id(self):
        return self._ldev_id

    @ldev_id.setter
    def ldev_id(self, value):
        if value is not None:
            Params.validate_non_bool(ModuleArgs.LDEV_ID, value)
            if value < AutomationConstants.LDEV_ID_MIN or \
                    value > AutomationConstants.LDEV_ID_MAX:
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_LDEVID_NUMBER_ERR.format(
                        ModuleArgs.LDEV_ID, value))
        self._ldev_id = value

    @property
    def data_reduction_mode(self):
        return self._data_reduction_mode

    @data_reduction_mode.setter
    def data_reduction_mode(self, value):
        if value is not None:
            if value != Api.DATA_REDUCTION_MODE_DISABLE and value != Api.DATA_REDUCTION_MODE :
                raise HitachiBlockModuleException('Specified value of data reduction mode is invalid, can only be disabled or dataReductionMode.')
        self._data_reduction_mode = value

    @property
    def chap_user_name(self):
        return self._chap_user_name

    @chap_user_name.setter
    def chap_user_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.CHAP_USER_NAME, value)
        self._chap_user_name = value

    @property
    def chap_password(self):
        return self._chap_password

    @chap_password.setter
    def chap_password(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.CHAP_PASSWORD, value, True)
        self._chap_password = value

    @property
    def way_of_chap_user(self):
        return self._way_of_chap_user

    @way_of_chap_user.setter
    def way_of_chap_user(self, value):
        if value is not None:
            if value != 'INI' and value != 'TAR':
                raise HitachiBlockModuleException('Specified value of way of CHAP user is invalid, can only be INI or TAR.')
        self._way_of_chap_user = value

    @property
    def generations(self):
        return self._generations

    @generations.setter
    def generations(self, value):
        self._generations = value

    @get_with_log('Params')
    def _is_exceeds_max_length(self, value, max_len):
        if len(to_text(value)) > max_len:
            return True
        return False

    @staticmethod
    @get_with_log('Params')
    def validate_str_value_for_existence(value, param_key):
        if value is None or not value.strip():
            raise HitachiBlockModuleException(
                ErrorMessages.REQUIRED_VALUE_ERR.format(param_key)
            )
        return value

    @staticmethod
    @get_with_log('Params')
    def validate_non_bool(parameter, value):
        if isinstance(value, bool):
            raise HitachiBlockModuleException(
                ErrorMessages.INVALID_TYPE_VALUE.format(
                    parameter, value))


    @staticmethod
    @get_with_log('Params')
    def validate_name_parameter(param, value, bPassword=False):
        if value is not ModuleArgs.NULL and (len(value) < AutomationConstants.NAME_PARAMS_MIN or len(value) > AutomationConstants.NAME_PARAMS_MAX ):
            if bPassword:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE.format(
                        param, '******'))
            else:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE.format(
                        param, value))

    @staticmethod
    @get_with_log('Params')
    def validate_name_parameter_zero(param, value, bPassword=False):
        if value is not ModuleArgs.NULL and (len(value) < 0 or len(value) > AutomationConstants.NAME_PARAMS_MAX ):
            if bPassword:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE_ZERO.format(
                        param, '******'))
            else:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE_ZERO.format(
                        param, value))
            

    @staticmethod
    @get_with_log('Params')
    def validate_name_parameter_1_8(param, value, bPassword=False):
        if value is not ModuleArgs.NULL and (len(value) < 1 or len(value) > 8 ):
            if bPassword:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE_1_8.format(
                        param, '******'))
            else:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE_1_8.format(
                        param, value))


    @staticmethod
    @get_with_log('Params')
    def validate_size_value(param, value):
        if value is not ModuleArgs.NULL and (value > AutomationConstants.MAX_SIZE_ALLOWED or value < AutomationConstants.MIN_SIZE_ALLOWED):
            raise HitachiBlockValidationException( ErrorMessages.INVALID_SIZE_VALUE.format(
                    param, value))
            
    @staticmethod
    @get_with_log('Params')
    def validate_size_value_zero(param, value):
        if value is not ModuleArgs.NULL and (value > AutomationConstants.MAX_SIZE_ALLOWED or value < AutomationConstants.MIN_SIZE_ZERO_ALLOWED):
            raise HitachiBlockValidationException( ErrorMessages.INVALID_SIZE_VALUE_ZERO.format(
                    param, value))

    @staticmethod
    @get_with_log('Params') 
    def validate_time_value(param, value):
        if value > AutomationConstants.MAX_TIME_ALLOWED or value < AutomationConstants.MIN_TIME_ALLOWED:
            raise HitachiBlockValidationException( ErrorMessages.INVALID_TIME_VALUE.format(
                    param, value))
  
class HitachiBlockException(Exception):
    def __init__(self):
        self.error = {
            Api.MSG: ErrorMessages.NOT_AVAILABLE
        }

    def error_response(self):
        return self.error


class HitachiBlockModuleException(HitachiBlockException):
    def __init__(self, msg=None):
        super(HitachiBlockModuleException, self).__init__()

        if msg is not None:
            self.error[Api.MSG] = msg

class HitachiBlockValidationException(HitachiBlockException):
    def __init__(self, msg=None):
        super(HitachiBlockValidationException, self).__init__()
        
        if msg is not None:
            self.error[Api.MSG] = msg

class HitachiBlockHttpException(HitachiBlockException):
    def __init__(self, errors=None):
        super(HitachiBlockHttpException, self).__init__()
        if errors is not None:
            if hasattr(errors, 'reason'):
                self.reason = errors.reason
            else:
                self.reason = str(errors)
            if isinstance(errors, urllib_error.HTTPError):
                self.code = errors.code
                self.error[Api.API_ERROR] = HTTPClient._load_response(errors)
                # self.error[Api.MSG] = self._error_code_to_message()
            elif isinstance(errors, socket.timeout):
                self.error[Api.MSG] = ErrorMessages.API_TIMEOUT_ERR
            else:
                self.error[Api.MSG] = \
                    ErrorMessages.API_COMMUNICATION_ERR.format(self.reason)

    @get_with_log('HitachiBlockHttpException')
    def _error_code_to_message(self):
        if self.code >= 400 and self.code <= 499:
            return ErrorMessages.HTTP_4xx_ERRORS.format(
                self.code, self.reason)
        if self.code >= 500 and self.code <= 599:
            return ErrorMessages.HTTP_5xx_ERRORS.format(
                self.code, self.reason)
        return ErrorMessages.API_COMMUNICATION_ERR.format(
            self.reason)


class HTTPClient(object):
    @staticmethod
    @get_with_log('HTTPClient')
    def post_ldevs(params, storage_device_id):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_LDEVS)
        params.request_params = {
            Api.POOLID: params.pool_id,
            Api.DATAREDUCTIONMODE: 'compression_deduplication',
            Api.LDEVID: params.ldev_id,
            Api.BYTEFORMATCAPACITY: str(params.capacity_mb) +  "M",
            #Api.BLOCKCAPACITY: params.block_capacity * 1024,
            Api.DATA_REDUCTION_MODE: params.data_reduction_mode
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        apierr = job_response.get(Api.ERROR,None)

        logger.debug('apierr: %s', apierr)
        # print('sng',apierr['message'])
        if apierr is not None and job_state == 'Failed' and 'LDEV is already defined' in apierr['message']:
            response = {
                Api.CHANGED: False,
                Api.OUTPUTS: 'LDEV is already defined',
            }
            return None, response

        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response, None

    @staticmethod
    @get_with_log('HTTPClient')
    def post_ldev_expand(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_LDEVS_EXPAND, storage_device_id, params.ldev_id)
        params.request_params = {
            "parameters": {
                Api.ADDITIONALBLOCKCAPACITY: str(params.capacity_mb) +  "M",
            }
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_host_groups(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_HOST_GROUPS)
        params.request_params = {
            Api.PORTID: params.port_id,
            Api.HOSTGROUPNUMBER: params.host_group_number,
            Api.HOSTGROUPNAME: params.host_group_name,
            Api.HOSTMODE: params.host_mode
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_luns(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_LUNS)
        params.request_params = {
            Api.PORTID: params.port_id,
            Api.HOSTGROUPNUMBER: params.host_group_number,
            Api.LDEVID: params.ldev_id
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_host_iscsis(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_HOST_ISCSIS)
        params.request_params = {
            Api.ISCSINAME: params.iscsi_name,
            Api.PORTID: params.port_id,
            Api.HOSTGROUPNUMBER: params.host_group_number
        }
        logger.debug(f"Request parameters {params.request_params }")
        post_response =  HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_host_iscsis(params, port_id, host_group_number):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.GET_HOST_ISCSIS, '?{}={}&{}={}'.format(Api.PORTID, port_id, Api.HOSTGROUPNUMBER, host_group_number))
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def delete_host_iscsis(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.PUT_HOST_ISCSIS, params.port_id, params.host_group_number, params.iscsi_name)
        params.request_params = {}

        delete_response = HTTPClient._request(Http.DELETE, endpoint, params)
        job_id = delete_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response
        

    @staticmethod
    @get_with_log('HTTPClient')
    def put_host_iscsis(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.PUT_HOST_ISCSIS, params.port_id, params.host_group_number, params.iscsi_name)
        params.request_params = {
            Api.ISCSINICKNAME: params.nick_name
        }
        logger.debug(f"end point details: %s %s", endpoint, params)
        put_response = HTTPClient._request(Http.PUT, endpoint, params)
        job_id = put_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
            
        logger.debug(f"Final response of the task {response}")
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_chap_user(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_CHAP_USER, params.port_id, params.host_group_number, params.way_of_chap_user, params.chap_user_name)
        return HTTPClient._request(Http.GET, endpoint, params)
    
    @staticmethod
    @get_with_log('HTTPClient')
    def post_chap_users(params, storage_device_id):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_CHAP_USERS)
        params.request_params = {
            Api.CHAPUSERNAME: params.chap_user_name,
            Api.PORTID: params.port_id,
            Api.HOSTGROUPNUMBER: params.host_group_number,
            Api.WAYOFCHAPUSER: params.way_of_chap_user
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_chap_users_single(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.PUT_CHAP_USERS_SINGLE, params.port_id, params.host_group_number, params.way_of_chap_user, params.chap_user_name)
        params.request_params = {
            Api.CHAPPASSWORD: params.chap_password
        }
        put_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = put_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_local_clone_copypairs(params, storage_device_id):
        copy_pair_name = 'clone_' + format(params.pvol_ldev_id, '06X') + '_' + format(params.svol_ldev_id, '06X')
        ''' Check Copy Group '''
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_LOCAL_CLONE_COPYGROUPS, storage_device_id)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        local_clone_copygroups = [elm for elm in get_response['data'] if elm['copyGroupName'] == params.copy_group_name]
        if len(local_clone_copygroups) <= 0:
            isNewCopyGroup = True
        else:
            isNewCopyGroup = False

        copy_pace = 3 if params.copy_pace is None else params.copy_pace
        is_consistency_group = False if params.consistency_group_id is None else True
        pvolMuNumber = 0
        params.request_params = {
            Api.COPYGROUPNAME: params.copy_group_name,
            Api.COPYPAIRNAME: copy_pair_name,
            Api.REPLICATIONTYPE: "SI",
            Api.PVOLLDEVID: params.pvol_ldev_id,
            Api.SVOLLDEVID: params.svol_ldev_id,
            Api.ISNEWGROUPCREATION: isNewCopyGroup,
            Api.COPYPACE: copy_pace,
            Api.ISCONSISTENCYGROUP: is_consistency_group,
            Api.CONSISTENCYGROUPID: params.consistency_group_id,
            Api.ISDATAREDUCTIONFORCECOPY: True,
            Api.PVOLMUNUMBER: pvolMuNumber
        }
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_LOCAL_CLONE_COPYPAIRS, storage_device_id)
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_local_clone_copygroups(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_LOCAL_CLONE_COPYGROUPS)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_local_clone_copygroups_one(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_LOCAL_CLONE_COPYGROUPS_ONE, params.local_clone_copygroup_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def post_local_clone_copypairs_split(params, storage_device_id):
        if params.copy_pace is not None:
            params.request_params = {
                'parameters': {
                    Api.COPYPACE: params.copy_pace
                }
            }
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_LOCAL_CLONE_COPYPAIRS_SPLIT, params.local_clone_copypair_id)
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_local_clone_copypairs_resync(params, storage_device_id):
        if params.copy_pace is not None:
            params.request_params = {
                'parameters': {
                    Api.COPYPACE: params.copy_pace
                }
            }
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_LOCAL_CLONE_COPYPAIRS_RESYNC, params.local_clone_copypair_id)
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_snapshots(params):

        if params.mu_number is not None:
            value = params.mu_number
            Params.validate_non_bool(ModuleArgs.MU_NUMBER, value)
            min=0
            max=1023
            if value < min or value > max:
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_RANGE_VALUE.format(
                        ModuleArgs.MU_NUMBER, value, min, max))

        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_SNAPSHOTS)
        ''' Check Copy Group '''
        copy_speed = 'medium' if params.copy_pace is None else params.copy_speed
        is_consistency_group = False if params.is_consistency_group is None else params.is_consistency_group
        params.request_params = {
            Api.SNAPSHOTGROUPNAME: params.snapshot_group_name,
            Api.SNAPSHOTPOOLID: params.snapshot_pool_id,
            Api.PVOLLDEVID: params.pvol_ldev_id,
            Api.ISCONSISTENCYGROUP: is_consistency_group,
            Api.ISDATAREDUCTIONFORCECOPY: True
        }
        if params.mu_number is not None:
            params.request_params[Api.MUNUMBER] = params.mu_number
        if params.auto_split is not None:
            params.request_params[Api.AUTOSPLIT] = params.auto_split

        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_snapshot_groups(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_SNAPSHOT_GROUPS)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_snapshot_groups_one(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_SNAPSHOT_GROUPS_ONE, params.snapshot_group_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def post_snapshots_split(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_SNAPSHOTS_SPLIT, params.snapshot_id)
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_snapshots_resync(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_SNAPSHOTS_RESYNC, params.snapshot_id)
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_snapshots_restore(params, storage_device_id):
        params.request_params = {
            "parameters": {
                # restore TI, we don't want default to autosplit=true, UCA-119
                Api.AUTOSPLIT: False                
                #Api.AUTOSPLIT: True
            }
        }

        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_SNAPSHOTS_RESTORE, params.snapshot_id)
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def put_iscsi_ports_discover(params, storage_device_id):
        params.request_params = {
            "parameters": {
                Api.ISCSIIPADDRESS: params.external_IP
            }
        }
        if params.external_port_number is not None:
            params.request_params["parameters"][Api.TCPPORT] = params.external_port_number
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.PUT_ISCSI_PORTS_DISCOVER, storage_device_id, params.external_port_id)
        return HTTPClient._request(Http.PUT, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def put_iscsi_ports_register(params, storage_device_id):
        params.request_params = {
            "parameters": {
                Api.ISCSIIPADDRESS: params.external_IP,
                Api.ISCSINAME: params.external_iscsi_target
            }
        }
        if params.external_port_number is not None:
            params.request_params["parameters"][Api.TCPPORT] = params.external_port_number
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.PUT_ISCSI_PORTS_REGISTER, storage_device_id, params.external_port_id)
        put_response = HTTPClient._request(Http.PUT, endpoint, params)
        job_id = put_response[Api.JOBID]
        job_response = HTTPClient.get_jobs(params, job_id)
        job_status = job_response[Api.STATUS]
        job_state = job_response[Api.STATE]
        response = None
        if job_status == 'Completed' and job_state == 'Succeeded':
            response = job_response[Api.AFFECTEDRESOURCES][0]
        else:
            raise HitachiBlockModuleException(
                job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                job_response[Api.ERROR][Api.CAUSE] + ' ' +
                job_response[Api.ERROR][Api.SOLUTION])
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_iscsi_ports(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_ISCSI_PORTS, storage_device_id, params.external_port_id)
        return HTTPClient._request(Http.GET, endpoint, params)   

    @staticmethod
    @get_with_log('HTTPClient')
    def put_iscsi_ports_check(params, storage_device_id):
        params.request_params = {
            "parameters": {
                Api.ISCSIIPADDRESS: params.external_IP,
                Api.ISCSINAME: params.external_iscsi_target
            }
        }
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.PUT_ISCSI_PORTS_CHECK, storage_device_id, params.external_port_id)
        return HTTPClient._request(Http.PUT, endpoint, params)
    
    @staticmethod
    @get_with_log('HTTPClient')
    def put_iscsi_ports_remove(params, storage_device_id):
        params.request_params = {
            "parameters": {
                Api.ISCSIIPADDRESS: params.external_IP,
                Api.ISCSINAME: params.external_iscsi_target
            }
        }
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.PUT_ISCSI_PORTS_REMOVE, storage_device_id, params.external_port_id)
        return HTTPClient._request(Http.PUT, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_external_storage_ports(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_EXTERNAL_STORAGE_PORTS, storage_device_id, '?{}={}'.format(Api.PORTID, params.external_port_id))
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_external_storage_luns(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_EXTERNAL_STORAGE_LUNS, storage_device_id, '?{}={}&{}={}&{}={}'.format(Api.PORTID, params.external_port_id\
                , Api.ISCSIIPADDRESS, params.external_IP, Api.ISCSINAME, params.external_iscsi_target))
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_storages_one(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.GET_STORAGES_ONE, storage_device_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_external_parity_groups_one(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_EXTERNAL_PARITY_GROUPS_ONE, params.external_paritygroup_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_external_path_groups_one(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_EXTERNAL_PATH_GROUPS_ONE, params.external_pathgroup_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_external_volumes(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_EXTERNAL_VOLUMES, '?{}={}&{}={}'.format(Api.EXTERNALPATHGROUPID, params.external_pathgroup_id\
                , Api.EXTERNALPARITYGROUPID, params.external_paritygroup_id))
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def post_external_volumes(params):
        params.request_params = {
            Api.EXTERNALPARITYGROUPID: params.external_paritygroup_id,
            Api.EXTERNALPATHGROUPID: params.external_pathgroup_id,
            Api.PORTID: params.external_port_id,
            Api.EXTERNALPORTIPADDRESS: params.external_IP,
            Api.EXTERNALPORTISCSINAME: params.external_iscsi_target,
            Api.LUN: params.external_lun
        }
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_EXTERNAL_VOLUMES)
        return HTTPClient._request(Http.POST, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_command_status(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_COMMAND_STATUS, params.object_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def delete_command_status(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.DELETE_COMMAND_STATUS, params.object_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def post_sessions(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.POST_SESSIONS)
        return HTTPClient._request(Http.POST, endpoint, params)
    
    @staticmethod
    @get_with_log('HTTPClient')
    def delete_sessions(params):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.DELETE_SESSIONS, params.session_id)
        return HTTPClient._request(Http.DELETE, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_ldevs_one(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.GET_LDEVS_ONE, params.ldev_id)
        print('sngsng endpoint=',endpoint)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def delete_luns(params, storage_device_id, portId, hostGroupNumber, lun):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.DELETE_LUNS, storage_device_id, portId, hostGroupNumber, lun)
        return HTTPClient._request(Http.DELETE, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_luns(params, storage_device_id, portId, hostGroupNumber):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.GET_LUNS, storage_device_id, '?{}={}&{}={}'.format(Api.PORTID, portId, Api.HOSTGROUPNUMBER, hostGroupNumber))
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_host_groups_one(params, storage_device_id, portId, hostGroupNumber):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.GET_HOST_GROUPS, portId, hostGroupNumber)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def delete_host_groups(params, storage_device_id, portId, hostGroupNumber):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.DELETE_HOST_GROUPS, portId, hostGroupNumber)
        return HTTPClient._request(Http.DELETE, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def put_ldevs_change_status(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.PUT_LDEVS_CHANGE_STATUS, params.ldev_id)
        params.request_params = {
            "parameters": {
                "status": "blk"
            }
        }
        return HTTPClient._request(Http.PUT, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def put_ldevs_shred(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.PUT_LDEVS_SHRED, params.ldev_id)
        params.request_params = {
            "parameters": {
                "operationType": "start",
                "pattern": params.shredding_pattern
            }
        }
        return HTTPClient._request(Http.PUT, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def delete_ldevs(params, storage_device_id):
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.DELETE_LDEVS, params.ldev_id)
        delete_response = HTTPClient._request(Http.DELETE, endpoint, params)
        job_id = delete_response[Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[Api.STATUS]
            job_state = job_response[Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[Api.ERROR][Api.MESSAGEID] + ' ' +
                        job_response[Api.ERROR][Api.MESSAGE] + ' ' +
                        job_response[Api.ERROR][Api.CAUSE] + ' ' +
                        job_response[Api.ERROR][Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_host_iscsi_paths(params, storage_device_id):
        query = '$query=ldev.storageDeviceId eq \'{}\'&$query=iscsi.iscsiName eq \'{}\''.format(storage_device_id, params.iscsi_name)
        endpoint = HTTPClient._format_endpoint(PfRestEndpoints.GET_HOST_ISCSI_PATHS, urllib.parse.quote(query, safe='?&=\''))
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_by_uri(params, uri):
        return HTTPClient._request(Http.GET, uri, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_jobs(params, job_id):
        endpoint = HTTPClient._format_endpoint(
            PfRestEndpoints.GET_JOBS, job_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def _request(http_verb, endpoint, params):
        try:
            headers = Http.HEADERS_JSON
            if params.session_id is None:
                if headers.get('Authorization') is not None:
                    del headers['Authorization']
            else:
                headers['Authorization'] = 'Session ' + params.session_id

            url = HTTPClient._format_url(params, endpoint)

            logger = get_logger()
            logger.debug(LogMessages.API_REQUEST_START.format(
                         http_verb, urlparse.urlparse(url).path))

            data = None
            if (http_verb == Http.POST or http_verb == Http.PUT) and params.request_params is not None:
                data = json.dumps(params.request_params)
            response = open_url(
                url,
                headers=headers,
                url_username=params.user if (params.session_id is None) else None,
                url_password=params.password if (params.session_id is None) else None,
                method=http_verb,
                force_basic_auth=True if (params.session_id is None) else False,
                validate_certs=HTTPClient._is_validate_certs(params),
                timeout=Http.OPEN_URL_TIMEOUT,
                http_agent=Http.USER_AGENT,
                data=data
            )
            return HTTPClient._load_response(response)
        except (urllib_error.URLError, socket.timeout) as err:
            raise HitachiBlockHttpException(err)
        except HTTPException as err:
            raise HitachiBlockHttpException(err)

    @staticmethod
    @get_with_log('HTTPClient')
    def _format_endpoint(endpoint, *args):
        # single quote inside HQL::filter must be specified twice as many as needed.
        # therefore, adding one extra quote for each single quote.
#        quoted = tuple(map(lambda arg: urlparse.quote(str(arg).replace(
#            Api.SINGLE_QUOTE, Api.SINGLE_QUOTE * 2)), args))
        return endpoint.format(*args)

    @staticmethod
    @get_with_log('HTTPClient')
    def _format_url(params, endpoint):
        management_address = params.management_address
        management_port = params.management_port
        if management_port is None:
            management_port = Http.DEFAULT_PORT
        protocol = Http.HTTPS
        url = '{}{}:{}{}'.format(
            protocol,
            management_address,
            management_port,
            Http.BASE_URL +
            endpoint)
        return url

    @staticmethod
    @get_with_log('HTTPClient')
    def _is_validate_certs(params):
        return False

    @staticmethod
    @get_with_log('HTTPClient')
    def _load_response(response):
        '''returns dict if json, native string otherwise'''
        text = response.read().decode('utf-8')
        logger = get_logger()
        logger.debug(LogMessages.API_RESPONSE.format(to_native(text)))
        try:
            msg = {}
            raw_message =  json.loads(text)
            
            if raw_message.get('errorSource') :
                logger.info(f"Response from {raw_message['errorSource']}")
                msg['cause'] = raw_message['cause'] if raw_message.get('cause') else ""
                msg["solution"] = raw_message['solution'] if raw_message.get('solution') else ""
                msg["message"] = raw_message['message'] if raw_message.get('message') else ""
                return msg
            return raw_message
        except ValueError:
            return to_native(text)


class Executors(object):
    def __init__(self, params=None):
        if params is not None:
            self.params = Params(params)

    @get_with_log('Executors')
    def create_ldev(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri, response = self._do_create_ldev(self.params, self.params.storage_device_id)
        if affected_resource_uri is not None:
            outputs = self._do_get_by_uri(self.params, affected_resource_uri)
            if "blockCapacity" in outputs:
                outputs["capacity_mb"] = outputs.pop("blockCapacity")/2/1024
            response = {
                Api.CHANGED: True,
                Api.OUTPUTS: outputs,
            }
        return response

    @get_with_log('Executors')
    def expand_ldev(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_expand_ldev(self.params, self.params.storage_device_id)
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def change_nickname(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_change_nickname(self.params)
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def add_chap_user(self):
        logger = get_logger()
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        try:
            get_response = self._do_get_chap_user(self.params)
            if 'chapUserName' in get_response:
                result = {
                    Api.CHANGED: False,
                    Api.OUTPUTS: 'The CHAP user {} already existed.'.format(get_response.get('chapUserName')),
                }
                return result
        except HitachiBlockException as err:
            logger.exception(json.dumps(err.error_response(), ensure_ascii=False))
        except Exception as e:
            logger.exception(repr(e))

        response = self._do_add_chap_user(self.params, "")
        #outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: response,
        }
        return response

    @get_with_log('Executors')
    def add_host(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        resp = self._do_add_host()

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: resp,
        }
        return response


    @get_with_log('Executors')
    def delete_host(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_delete_host(self.params, self.params.storage_device_id)
        
        if affected_resource_uri:
            result = {
                Api.CHANGED: True,
                Api.OUTPUTS: "Delete iSCSI name successfully"
            }
            
        else:
            result = {
                Api.CHANGED: False,
                Api.OUTPUTS: "Delete iSCSI name failed"
            }
        return result

    @get_with_log('Executors')
    def create_hg(self):
        logger = get_logger()
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result

        outputs = HTTPClient.get_host_groups_one(self.params, self.params.storage_device_id, \
                    self.params.port_id,\
                    self.params.host_group_number)
                                                 
        if outputs is not None and outputs['hostGroupName'] != '-' :
            logger.debug(f"Response from server {outputs}")
            response = {
                Api.CHANGED: False,
                Api.OUTPUTS: outputs
                #this would not work for composite playbooks
                #Api.OUTPUTS: "HostGroup is already created: {}".format(outputs)
            }
            return response
        
        affected_resource_uri = self._do_create_hg(self.params, self.params.storage_device_id)
        # self.params.host_group_number = affected_resource_uri.rsplit(',', 1)[1]
        if self.params.iscsi_name is not None:
            self._do_add_iscsiname(self.params)
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        logger.debug(f"Response from server {outputs}")
        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def add_lun(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        if self.params.iscsi_name is not None:
            self._do_add_iscsiname(self.params)
        affected_resource_uri = self._do_add_lun(self.params)
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def create_si(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_create_si(self.params, '')
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def create_ti(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_create_ti(self.params)
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def create_ti_with_generations(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        if self.params.generations < 1 or self.params.generations > 1021:
            raise HitachiBlockModuleException('The value specified in the generation parameter is not within the specifiable range.(1-1021)')

        outputs = self._do_create_ti_with_generations(self.params)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def split_si(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_split_si(self.params, '')
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def split_ti(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_split_ti(self.params, '')
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def resync_si(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_resync_si(self.params, '')
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def resync_ti(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_resync_ti(self.params, '')
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def restore_ti(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_restore_ti(self.params, '')
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def resync_ti_oldest(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_resync_ti_oldest(self.params, '')
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def createExtVol(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: True if self._get_external_volume_with_create_session(self.params) is None else False
            }
            return result
        self._do_register_iscsi_ports(self.params, self.params.storage_device_id)
        return self._do_map_external_volume(self.params, self.params.storage_device_id)

    @get_with_log('Executors')
    def delete_volume(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        response = self._do_delete_volume(self.params, self.params.storage_device_id)

        # response = {
        #     Api.CHANGED: True,
        #     Api.OUTPUTS: ""
        # }
        return response

    @get_with_log('Executors')
    def delete_tenant(self):
        if self.params.check_mode:
            result = {
                Api.CHANGED: False
            }
            return result
        results = self._do_delete_tenant(self.params, self.params.storage_device_id)
        response = {
            Api.CHANGED: True,
            Api.OUTPUTS: results
        }
        return response

    @get_with_log('Executors')
    def _do_create_ldev(self, params, storage_device_id = ""):
        return HTTPClient.post_ldevs(params, storage_device_id)

    @get_with_log('Executors')
    def _do_expand_ldev(self, params, storage_device_id):
        return HTTPClient.post_ldev_expand(params, storage_device_id="")

    @get_with_log('Executors')
    def _do_change_nickname(self, params):
        return HTTPClient.put_host_iscsis(params)

    @get_with_log('Executors')
    def _do_add_chap_user(self, params, storage_device_id=""):
        HTTPClient.post_chap_users(params, storage_device_id)
        return HTTPClient.get_chap_user(params, storage_device_id="")

    @get_with_log('Executors')
    def _do_get_chap_user(self, params, storage_device_id=""):
        return HTTPClient.get_chap_user(params, storage_device_id)

    @get_with_log('Executors')
    def _do_add_host(self):
        return self._do_add_iscsiname(self.params)

    @get_with_log('Executors')
    def _do_delete_host(self, params, storage_device_id=""):
        return HTTPClient.delete_host_iscsis(params, storage_device_id)

    @get_with_log('Executors')
    def _do_create_hg(self, params, storage_device_id=""):
        return HTTPClient.post_host_groups(params, storage_device_id)

    @get_with_log('Executors')
    def _do_add_lun(self, params):
        return HTTPClient.post_luns(params)

    @get_with_log('Executors')
    def _do_add_iscsiname(self, params):
        affected_resource_uri = HTTPClient.post_host_iscsis(params)
        if affected_resource_uri:
            return self._do_get_by_uri(self.params, affected_resource_uri)

    @get_with_log('Executors')
    def _do_create_si(self, params, storage_device_id):
        return HTTPClient.post_local_clone_copypairs(params, storage_device_id)

    @get_with_log('Executors')
    def _do_create_ti(self, params):
        return HTTPClient.post_snapshots(params)

    @get_with_log('Executors')
    def _do_create_ti_with_generations(self, params):
        outputs = []
        params.auto_split = True
        for index in range(params.generations):
            affected_resource_uri = HTTPClient.post_snapshots(params)
            outputs.append(self._do_get_by_uri(self.params, affected_resource_uri))
        return outputs

    @get_with_log('Executors')
    def _do_split_si(self, params, storage_device_id):
        ''' Get all local-clone-copygroups '''
        copygroups = HTTPClient.get_local_clone_copygroups(params, storage_device_id)['data']
        logger = get_logger()
        logger.debug('copygroups: %s', copygroups)
        logger.debug('params.copy_group_name: %s', params.copy_group_name)
        ''' Select local-clone-copygroup by name '''
        for cg in copygroups:
            if cg['copyGroupName'] is not None and cg['copyGroupName'] == params.copy_group_name:
                params.local_clone_copygroup_id = cg['localCloneCopygroupId']
                break

        if params.local_clone_copygroup_id is None:
            raise HitachiBlockModuleException('The Copy Group is not found specified by copy_group_name.')
        ''' Get local-clone-copypair by local_clone_copygroup_id '''
        copypairs = HTTPClient.get_local_clone_copygroups_one(params, storage_device_id)['copyPairs']
        ''' Select local-clone-copypair by name '''
        for cp in copypairs:
            if cp['copyPairName'] is not None and cp['copyPairName'] == params.copy_pair_name:
                params.local_clone_copypair_id = cp['localCloneCopypairId']
                break

        if params.local_clone_copypair_id is None:
            raise HitachiBlockModuleException('The Copy Pair is not found specified by copy_pair_name.')

        return HTTPClient.post_local_clone_copypairs_split(params, storage_device_id)

    @get_with_log('Executors')
    def _do_split_ti(self, params, storage_device_id):

        if params.mu_number is not None:
            value = params.mu_number
            Params.validate_non_bool(ModuleArgs.MU_NUMBER, value)
            min=0
            max=1023
            if value < min or value > max:
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_RANGE_VALUE.format(
                        ModuleArgs.MU_NUMBER, value, min, max))
        
        ''' Get all snapshot-groups '''
        snapshotgroups = HTTPClient.get_snapshot_groups(params, storage_device_id)['data']
        ''' Find snapshot-group by name '''
        for sg in snapshotgroups:
            if sg['snapshotGroupName'] is not None and sg['snapshotGroupName'] == params.snapshot_group_name:
                params.snapshot_group_id = sg['snapshotGroupId']
                break
        if params.snapshot_group_id is None:
            raise HitachiBlockModuleException('The Snapshot Group is not found specified by snapshot_group_name.')
        ''' Find snapshots by pvol_ldev_id '''
        snapshots = HTTPClient.get_snapshot_groups_one(params, storage_device_id)['snapshots']

        for ss in snapshots:
            if ss['pvolLdevId'] is not None and ss['pvolLdevId'] == params.pvol_ldev_id:
                if params.mu_number is None or params.mu_number is not None and ss['muNumber'] == params.mu_number:
                    params.snapshot_id = ss['snapshotId']
                    break
        if params.snapshot_id is None:
            raise HitachiBlockModuleException('The Snapshot is not found specified by pvol_ldev_id.')

        if params.snapshot_id is None:
            raise HitachiBlockModuleException('The Copy Pair is not found specified by copy_pair_name.')

        return HTTPClient.post_snapshots_split(params, storage_device_id)

    @get_with_log('Executors')
    def _do_resync_si(self, params, storage_device_id):
        ''' Get all local-clone-copygroups '''
        copygroups = HTTPClient.get_local_clone_copygroups(params, storage_device_id)['data']
        ''' Select local-clone-copygroup by name '''

        for cg in copygroups:
            if cg['copyGroupName'] is not None and cg['copyGroupName'] == params.copy_group_name:
                params.local_clone_copygroup_id = cg['localCloneCopygroupId']
                break

        if params.local_clone_copygroup_id is None:
            raise HitachiBlockModuleException('The Copy Group is not found specified by copy_group_name.')
        ''' Get local-clone-copypair by local_clone_copygroup_id '''
        copypairs = HTTPClient.get_local_clone_copygroups_one(params, storage_device_id)['copyPairs']
        ''' Select local-clone-copypair by name '''
        for cp in copypairs:
            if cp['copyPairName'] is not None and cp['copyPairName'] == params.copy_pair_name:
                params.local_clone_copypair_id = cp['localCloneCopypairId']
                break

        if params.local_clone_copypair_id is None:
            raise HitachiBlockModuleException('The Copy Pair is not found specified by copy_pair_name.')

        return HTTPClient.post_local_clone_copypairs_resync(params, storage_device_id)

    @get_with_log('Executors')
    def _do_resync_ti(self, params, storage_device_id):

        if params.mu_number is not None:
            value = params.mu_number
            Params.validate_non_bool(ModuleArgs.MU_NUMBER, value)
            min=0
            max=1023
            if value < min or value > max:
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_RANGE_VALUE.format(
                        ModuleArgs.MU_NUMBER, value, min, max))

        ''' Get all snapshot-groups '''
        snapshotgroups = HTTPClient.get_snapshot_groups(params, storage_device_id)['data']
        ''' Find snapshot-group by name '''
        for sg in snapshotgroups:
            if sg['snapshotGroupName'] is not None and sg['snapshotGroupName'] == params.snapshot_group_name:
                params.snapshot_group_id = sg['snapshotGroupId']
                break
        if params.snapshot_group_id is None:
            raise HitachiBlockModuleException('The Snapshot Group is not found specified by snapshot_group_name.')
        ''' Find snapshots by pvol_ldev_id '''
        snapshots = HTTPClient.get_snapshot_groups_one(params, storage_device_id)['snapshots']

        for ss in snapshots:
            if ss['pvolLdevId'] is not None and ss['pvolLdevId'] == params.pvol_ldev_id and ss['muNumber'] is not None and ss['muNumber'] == params.mu_number:
                params.snapshot_id = ss['snapshotId']
                break
        if params.snapshot_id is None:
            raise HitachiBlockModuleException('The Snapshot is not found specified by snapshot_group_name and pvol_ldev_id and mu_number.')

        return HTTPClient.post_snapshots_resync(params, storage_device_id)

    @get_with_log('Executors')
    def _do_restore_ti(self, params, storage_device_id):

        if params.mu_number is not None:
            value = params.mu_number
            Params.validate_non_bool(ModuleArgs.MU_NUMBER, value)
            min=0
            max=1023
            if value < min or value > max:
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_RANGE_VALUE.format(
                        ModuleArgs.MU_NUMBER, value, min, max))
            
        ''' Get all snapshot-groups '''
        snapshotgroups = HTTPClient.get_snapshot_groups(params, storage_device_id)['data']
        ''' Find snapshot-group by name '''
        for sg in snapshotgroups:
            if sg['snapshotGroupName'] is not None and sg['snapshotGroupName'] == params.snapshot_group_name:
                params.snapshot_group_id = sg['snapshotGroupId']
                break
        if params.snapshot_group_id is None:
            raise HitachiBlockModuleException('The Snapshot Group is not found specified by snapshot_group_name.')
        ''' Find snapshots by pvol_ldev_id '''
        snapshots = HTTPClient.get_snapshot_groups_one(params, storage_device_id)['snapshots']

        for ss in snapshots:
            if ss['pvolLdevId'] is not None and ss['pvolLdevId'] == params.pvol_ldev_id and ss['muNumber'] is not None and ss['muNumber'] == params.mu_number:
                params.snapshot_id = ss['snapshotId']
                break
        if params.snapshot_id is None:
            raise HitachiBlockModuleException('The Snapshot is not found specified by snapshot_group_name and pvol_ldev_id and mu_number.')

        return HTTPClient.post_snapshots_restore(params, storage_device_id)

    @get_with_log('Executors')
    def _do_resync_ti_oldest(self, params, storage_device_id):
        ''' Get all snapshot-groups '''
        snapshotgroups = HTTPClient.get_snapshot_groups(params, storage_device_id)['data']
        ''' Find snapshot-group by name '''
        for sg in snapshotgroups:
            if sg['snapshotGroupName'] is not None and sg['snapshotGroupName'] == params.snapshot_group_name:
                params.snapshot_group_id = sg['snapshotGroupId']
                break
        if params.snapshot_group_id is None:
            raise HitachiBlockModuleException('The Snapshot Group is not found specified by snapshot_group_name.')
        ''' Find snapshots by pvol_ldev_id '''
        snapshots = HTTPClient.get_snapshot_groups_one(params, storage_device_id)['snapshots']

        oldest_date = '2099-12-31T23:59:59'
        for ss in snapshots:
            if ss['pvolLdevId'] is not None and ss['pvolLdevId'] == params.pvol_ldev_id and ss['status'] == 'PSUS' and ss['splitTime'] < oldest_date:
                params.snapshot_id = ss['snapshotId']
                oldest_date = ss['splitTime']
        if params.snapshot_id is None:
            raise HitachiBlockModuleException('The Snapshot is not found specified by snapshot_group_name and pvol_ldev_id.')

        return HTTPClient.post_snapshots_resync(params, storage_device_id)

    @get_with_log('Executors')
    def _do_get_by_uri(self, params, uri):
        endpoint = uri.split('/', 2)[2]
        response = HTTPClient.get_by_uri(params, endpoint)

        return response
    
    @get_with_log('Executors')
    def _do_register_iscsi_ports(self, params, storage_device_id):
        # discover iSCSI target
        externalIscsiTargets = HTTPClient.put_iscsi_ports_discover(params, storage_device_id)['externalIscsiTargets']
        externalIscsiTarget = next(filter(lambda target: target['iscsiName'] is not None and target['iscsiName'] == params.external_iscsi_target, externalIscsiTargets), None)
        if externalIscsiTarget is None:
            raise HitachiBlockModuleException('The iSCSI Name is not found specified by external_IP and external_port_id.')

        # register iSCSI name
        registerTarget = False
        if externalIscsiTarget['isRegistered'] is not None and not externalIscsiTarget['isRegistered']:
            HTTPClient.put_iscsi_ports_register(params, storage_device_id)
            registerTarget = True
        
        # login test 
        externalIscsiTargets = HTTPClient.put_iscsi_ports_check(params, storage_device_id)['externalIscsiTargets']
        if len(externalIscsiTargets) == 0 or externalIscsiTargets[0]["isLoginSucceeded"] is None or not externalIscsiTargets[0]["isLoginSucceeded"]:
            if registerTarget:
                HTTPClient.put_iscsi_ports_remove(params, storage_device_id)
            raise HitachiBlockModuleException('Failed to log in to iSCSI target.')
        return externalIscsiTargets

    @get_with_log('Executors')
    def _get_advisor_params(self, params, storage_device_id):
        ctl1Ip = HTTPClient.get_storages_one(params, storage_device_id)["ctl1Ip"]
        advisor_params = copy.deepcopy(params)
        advisor_params.management_address = ctl1Ip
        advisor_params.management_port = params.advisor_port
        return advisor_params

    @get_with_log('Executors')
    def _get_external_volume(self, advisor_params):
        externalVolumes = HTTPClient.get_external_volumes(advisor_params)["data"]
        return next(filter(lambda volume: volume["externalPathOfVolume"] is not None \
            and next(filter(lambda path: path["externalPortIpAddress"] is not None and path["externalPortIpAddress"] == advisor_params.external_IP \
                and path["externalPortIscsiName"] is not None and path["externalPortIscsiName"] == advisor_params.external_iscsi_target \
                    and path["lun"] is not None and path["lun"] == advisor_params.external_lun, volume["externalPathOfVolume"]), None) is not None, externalVolumes), None)

    @get_with_log('Executors')
    def _get_external_volume_with_create_session(self, params):
        advisor_params = self._get_advisor_params(self.params, self.params.storage_device_id)
        with AdvisorSession(advisor_params):
            return self._get_external_volume(advisor_params)

    @get_with_log('Executors')
    def _get_external_storage_port(self, params, storage_device_id):
        externalStoragePorts = HTTPClient.get_external_storage_ports(params, storage_device_id)["data"]
        return next(filter(lambda port: port["iscsiIpAddress"] is not None and port["iscsiIpAddress"] == params.external_IP \
            and port["iscsiName"] is not None and port["iscsiName"] ==  params.external_iscsi_target, externalStoragePorts), None)

    @get_with_log('Executors')
    def _get_exteranl_storage_lun(self, params, storage_device_id):
        exteranlStorageLuns = HTTPClient.get_external_storage_luns(params, storage_device_id)["data"]
        return next(filter(lambda lun: lun["iscsiIpAddress"] is not None and lun["iscsiIpAddress"] == params.external_IP \
            and lun["iscsiName"] is not None and lun["iscsiName"] ==  params.external_iscsi_target \
                and lun["externalLun"] is not None and lun["externalLun"] ==  params.external_lun, exteranlStorageLuns), None)

    @get_with_log('Executors')
    def _get_external_path_group(self, advisor_params):
        try:
            return HTTPClient.get_external_path_groups_one(advisor_params)
        except HitachiBlockHttpException as exc:
            if exc.code != HTTPStatus.NOT_FOUND:
                raise exc
            else:
                return None

    @get_with_log('Executors')
    def _do_map_external_volume(self, params, storage_device_id):
        # check external storage port
        if self._get_external_storage_port(params, storage_device_id) is None:
            raise HitachiBlockModuleException('The external storage port is not found.')
        
        # check external storage LUN
        if self._get_exteranl_storage_lun(params, storage_device_id) is None:
            raise HitachiBlockModuleException('The external storage LU is not found.')

        advisor_params = self._get_advisor_params(params, storage_device_id)

        result = None
        with AdvisorSession(advisor_params):
            # get external volume
            externalVolume = self._get_external_volume(advisor_params)
            if externalVolume is None:
                # check external path group
                if self._get_external_path_group(advisor_params) is not None:
                    raise HitachiBlockModuleException('The external path group is in use.')
                # map external volume
                statusResource = HTTPClient.post_external_volumes(advisor_params)["statusResource"].replace(Http.BASE_URL, "")
                commandStatus = HTTPClient.get_by_uri(advisor_params, statusResource)
                # Wait for the job to complete.
                while commandStatus["progress"] is not None and commandStatus["progress"] != "completed":
                    #print(commandStatus["progress"])
                    time.sleep(10)
                    commandStatus = HTTPClient.get_by_uri(advisor_params, statusResource)

                if ("normal" != commandStatus["status"]):
                    raise HitachiBlockModuleException(commandStatus["errorMessage"] + json.dumps(commandStatus["errorCode"] if commandStatus["errorCode"] is not None else ""))
                else:
                    result =  HTTPClient.get_by_uri(advisor_params, commandStatus["affectedResources"][0].replace(Http.BASE_URL, ""))
            else:
                result = externalVolume
        result["ldevId"] = result["id"]
        return {
            Api.CHANGED: True if externalVolume is None else False,
            Api.OUTPUTS: result,
        }

    @get_with_log('Executors')
    def _do_delete_volume(self, params, storage_device_id):
        logger = get_logger()
        # Get ldev's information

        if params.shredding_pattern is not None:
            Params.validate_name_parameter_1_8(ModuleArgs.SHREDDING_PATTERN, params.shredding_pattern)
            logger.info('shredding_pattern: %s', params.shredding_pattern)

            if not checkHex(params.shredding_pattern) :
                raise HitachiBlockModuleException(
                    ErrorMessages.INVALID_HEX_VALUE.format(
                        ModuleArgs.SHREDDING_PATTERN, params.shredding_pattern))

        # this is the business logic layer
        # catch the exception and decides if it is an error
        # or the ldev is not found, in which case we don't need to return error, idempotency
        # look for "emulationType" : "NOT DEFINED"
        #
        ldevInfo = HTTPClient.get_ldevs_one(self.params, storage_device_id)
        if ldevInfo['emulationType'] == 'NOT DEFINED' :
            return {
                Api.CHANGED: False,
                Api.OUTPUTS: 'Volume is already deleted.',
                # Api.MESSAGE: 'Volume is already deleted.',
            }            

        # # Delete LUN
        # for port in ldevInfo['ports']:
        #     HTTPClient.delete_luns(self.params, storage_device_id, port['portId'], port['hostGroupNumber'], port['lun'])
        #     # Get the LUN information that the host-group has
        #     lunsInfo = HTTPClient.get_luns(self.params, storage_device_id, port['portId'], port['hostGroupNumber'])
        #     # If deleted all LUN, delete iSCSI target
        #     if len(lunsInfo['data']) <= 0:
        #         # Delete host-group
        #         HTTPClient.delete_host_groups(self.params, storage_device_id, port['portId'], port['hostGroupNumber'])

        changed = False

        # If specified pattern, Shred volume 
        if params.shredding_pattern is not None:
            # Change ldev status to block
            ldevInfo = HTTPClient.put_ldevs_change_status(self.params, storage_device_id)
            # Wait end of shredding
            logger.info('Wait for shredding')
            ldevInfo = HTTPClient.put_ldevs_shred(self.params, storage_device_id)
            changed = True

        if (params.delete_ldev is not None) and (params.delete_ldev == True):
            logger.info('delete_ldevs')
            # Delete LDEV
            ldevInfo = HTTPClient.delete_ldevs(self.params, storage_device_id)
            changed = True

        return {
            Api.CHANGED: changed
        }

    @get_with_log('Executors')
    def _do_delete_tenant(self, params, storage_device_id):
        # Get ldev's information from views
        viewsInfos = HTTPClient.get_host_iscsi_paths(self.params, storage_device_id)
        deleted_iscsiTargets = list()
        deleted_ldevs = list()
        # Delete iSCSI name from iSCSI target
        for viewsInfo in viewsInfos['data']:
            params.port_id = viewsInfo['hostGroup']['portId']
            params.host_group_number = viewsInfo['hostGroup']['hostGroupNumber']
            HTTPClient.delete_host_iscsis(self.params, storage_device_id)
            # Get iSCSI target information
            iscsiNamesInfo = HTTPClient.get_host_iscsis(self.params, viewsInfo['hostGroup']['portId'], viewsInfo['hostGroup']['hostGroupNumber'])
            # if iSCSI target has no iSCSI name, delete LUN, iSCSI target and LDEV
            if len(iscsiNamesInfo['data']) <= 0:
                # Get LUN information
                lunsInfo = HTTPClient.get_luns(self.params, storage_device_id, viewsInfo['hostGroup']['portId'], viewsInfo['hostGroup']['hostGroupNumber'])
                for lun in lunsInfo['data']:
                    # Delete all LUN
                    HTTPClient.delete_luns(self.params, storage_device_id, lun['portId'], lun['hostGroupNumber'], lun['lun'])
                # Delete iSCSI target
                deleted_iscsiTargets.append(viewsInfo['hostGroup']['iscsiName'])
                HTTPClient.delete_host_groups(self.params, storage_device_id, viewsInfo['hostGroup']['portId'], viewsInfo['hostGroup']['hostGroupNumber'])
                # Delete LDEV
                deleted_ldevs.append(viewsInfo['ldev']['ldevId'])
                params.ldev_id = viewsInfo['ldev']['ldevId']
                HTTPClient.delete_ldevs(self.params, storage_device_id)
        return {
            Api.CHANGED: True,
            Api.OUTPUTS: {
                'ldevs': deleted_ldevs,
                'iscsi_targets': deleted_iscsiTargets
            }
        }

class AdvisorSession:
    def __init__(self, advisor_params):
        self.advisor_params = advisor_params

    @get_with_log('AdvisorSession')
    def __enter__(self):
        self.advisor_params.session_id = HTTPClient.post_sessions(self.advisor_params)["sessionId"]
        return self

    @get_with_log('AdvisorSession')
    def __exit__(self, exception_type, exception_value, traceback):
        HTTPClient.delete_sessions(self.advisor_params)
