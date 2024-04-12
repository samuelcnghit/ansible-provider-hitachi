from ast import Param
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
from ansible.module_utils.hitachi_vssb_constant import (
    VSSB_Api,
    Endpoints,
    Http,
    ModuleArgs,
    ErrorMessages,
    AutomationConstants,
    LogMessages,
    Log,
)
from ansible.module_utils.hitachi_ansible_common import (
    initialize_filehandler_logger,
    
)


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
            self.check_mode = params.get(ModuleArgs.CHECK_MODE)
            self.management_address = params.get(ModuleArgs.SERVER)
            self.management_port = params.get(ModuleArgs.SERVER_PORT)
            self.user = params.get(ModuleArgs.USER)
            self.password = params.get(ModuleArgs.PASSWORD)
            self.server_nickname = params.get(ModuleArgs.SERVER_NICKNAME, ModuleArgs.NULL)
            self.os_type = params.get(ModuleArgs.OS_TYPE)
            self.server_id = None
            self.capacity_mb = params.get(ModuleArgs.CAPACITY,  ModuleArgs.NULL)
            self.additional_capacity = None
            self.iscsi_name = params.get(ModuleArgs.ISCSI_NAME)
            self.hba_id = None
            self.target_port_name = params.get(ModuleArgs.TARGET_PORT_NAME)
            self.port_id = None
            self.request_params = None
            self.pool_name = params.get(ModuleArgs.POOL_NAME, ModuleArgs.NULL)
            self.pool_id = None
            self.number = params.get(ModuleArgs.NUMBER, ModuleArgs.NULL)
            self.base_name = params.get(ModuleArgs.BASE_NAME, ModuleArgs.NULL)
            self.start_number = params.get(ModuleArgs.START_NUMBER)
            self.number_of_digit = params.get(ModuleArgs.NUMBER_OF_DIGIT)
            self.volume_name = params.get(ModuleArgs.VOLUME_NAME, ModuleArgs.NULL)
            self.volume_id = None
            self.additional_capacity = None
            self.target_chap_user_name = params.get(ModuleArgs.TARGET_CHAP_USER_NAME, ModuleArgs.NULL)
            self.target_chap_secret = params.get(ModuleArgs.TARGET_CHAP_SECRET)
            self.initiator_chap_user_name = params.get(ModuleArgs.INITIATOR_CHAP_USER_NAME)
            self.initiator_chap_secret = params.get(ModuleArgs.INITIATOR_CHAP_USER_SECRET)
            self.chap_user_id = None
            self.pool_expand_capacity = params.get(ModuleArgs.POOL_EXPAND_CAPACITY,  ModuleArgs.NULL)
            self.protection_domain_id = None
            self.pool_capacity = None
            self.pool_info = None
            self.storage_nodes_info = None
            self.drives_info = None
            self.additional_drives = None
            self.additional_drive_count = None
            self.expand_pool_process1_info = params.get(ModuleArgs.EXPAND_POOL_PROCESS1_INFO)
            self.drive_count_in_node = params.get(ModuleArgs.DRIVE_COUNT_IN_NODE)
            self.add_storagenode_process1_info = params.get(ModuleArgs.ADD_STORAGENODE_PROCESS1_INFO)
            self.time_a = params.get(ModuleArgs.TIME_A)
            self.time_b = params.get(ModuleArgs.TIME_B)
            self.time_c = params.get(ModuleArgs.TIME_C)
            self.time_d = params.get(ModuleArgs.TIME_D)

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
            if value != VSSB_Api.SERVER_PORT_DEFAULT and (value < AutomationConstants.PORT_NUMBER_MIN or \
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
    def server_nickname(self):
        return self._server_nickname

    @server_nickname.setter
    def server_nickname(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.SERVER_NICKNAME, value)
        self._server_nickname = value

    @get_with_log('Params')
    def _is_exceeds_max_length(self, value, max_len):
        if len(to_text(value)) > max_len:
            return True
        return False

    @property
    def iscsi_name(self):
        return self._iscsi_name

    @iscsi_name.setter
    def iscsi_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.ISCSI_NAME, value)
        self._iscsi_name = value

    @property
    def target_port_name(self):
        return self._iscsi_target

    @target_port_name.setter
    def target_port_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.TARGET_PORT_NAME, value)
        self._iscsi_target = value

    @property
    def pool_name(self):
        return self._pool_name

    @pool_name.setter
    def pool_name(self, value):
        Params.validate_name_parameter(ModuleArgs.POOL_NAME, value)
        self._pool_name = value

    @property
    def capacity_mb(self):
        return self._capacity

    @capacity_mb.setter
    def capacity_mb(self, value):
        #TODO: check capacity
        Params.validate_size_value(ModuleArgs.CAPACITY, value)
        self._capacity = value

    @property
    def number(self):
        return self._number

    @number.setter
    def number(self, value):
        Params.validate_size_value(ModuleArgs.NUMBER, value)
        self._number = value

    @property
    def base_name(self):
        return self._base_name

    @base_name.setter
    def base_name(self, value):
        Params.validate_name_parameter(ModuleArgs.BASE_NAME, value)
        self._base_name = value

    @property
    def start_number(self):
        return self._start_number

    @start_number.setter
    def start_number(self, value):
        self._start_number = value

    @property
    def number_of_digit(self):
        return self._number_of_digit

    @number_of_digit.setter
    def number_of_digit(self, value):
        self._number_of_digit = value

    @property
    def volume_name(self):
        return self._volume_name

    @volume_name.setter
    def volume_name(self, value):
        Params.validate_name_parameter(ModuleArgs.VOLUME_NAME, value)
        self._volume_name = value

    @property
    def target_chap_user_name(self):
        return self._target_chap_user_name

    @target_chap_user_name.setter
    def target_chap_user_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.TARGET_CHAP_USER_NAME, value)
        self._target_chap_user_name = value

    @property
    def target_chap_secret(self):
        return self._target_chap_secret

    @target_chap_secret.setter
    def target_chap_secret(self, value):
        if value is not None:
            Params.validate_chap_secret_parameter(ModuleArgs.TARGET_CHAP_SECRET, value)
        self._target_chap_secret = value

    @property
    def initiator_chap_user_name(self):
        return self._initiator_chap_user_name

    @initiator_chap_user_name.setter
    def initiator_chap_user_name(self, value):
        if value is not None:
            Params.validate_name_parameter(ModuleArgs.INITIATOR_CHAP_USER_NAME, value)
        self._initiator_chap_user_name = value

    @property
    def initiator_chap_secret(self):
        return self._initiator_chap_secret

    @initiator_chap_secret.setter
    def initiator_chap_secret(self, value):
        if value is not None:
            Params.validate_chap_secret_parameter(ModuleArgs.INITIATOR_CHAP_USER_SECRET, value)
        self._initiator_chap_secret = value

    @property
    def pool_expand_capacity(self):
        return self._pool_expand_capacity

    @pool_expand_capacity.setter
    def pool_expand_capacity(self, value):
        self._pool_expand_capacity = value

    @property
    def drive_count_in_node(self):
        return self._drive_count_in_node

    @drive_count_in_node.setter
    def drive_count_in_node(self, value):
        self._drive_count_in_node = value

    @property
    def time_a(self):
        return self._time_a

    @time_a.setter
    def time_a(self, value):
        self._time_a = value

    @property
    def time_b(self):
        return self._time_b

    @time_b.setter
    def time_b(self, value):
        self._time_b = value

    @property
    def time_c(self):
        return self._time_c

    @time_c.setter
    def time_c(self, value):
        self._time_c = value

    @property
    def time_d(self):
        return self._time_d

    @time_d.setter
    def time_d(self, value):
        self._time_d = value

    @property
    def expand_pool_process1_info(self):
        return self._expand_pool_process1_info

    @expand_pool_process1_info.setter
    def expand_pool_process1_info(self, value):
        self._expand_pool_process1_info = value

    @property
    def add_storagenode_process1_info(self):
        return self._add_storagenode_process1_info

    @add_storagenode_process1_info.setter
    def add_storagenode_process1_info(self, value):
        self._add_storagenode_process1_info = value

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
       
        if (len(value) < AutomationConstants.NAME_PARAMS_MIN or len(value) > AutomationConstants.NAME_PARAMS_MAX ):
            if bPassword:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE.format(
                        param, '******'))
            else:
                raise HitachiBlockValidationException( ErrorMessages.INVALID_NAME_SIZE.format(
                        param, value))

    @staticmethod
    @get_with_log('Params')
    def validate_size_value(param, value):
        if value is not ModuleArgs.NULL and (value > AutomationConstants.MAX_SIZE_ALLOWED or value < AutomationConstants.MIN_SIZE_ALLOWED):
            raise HitachiBlockValidationException( ErrorMessages.INVALID_SIZE_VALUE.format(
                    param, value))
            
    @staticmethod
    @get_with_log('Params') 
    def validate_time_value(param, value):
        if value > AutomationConstants.MAX_TIME_ALLOWED or value < AutomationConstants.MIN_TIME_ALLOWED:
            raise HitachiBlockValidationException( ErrorMessages.INVALID_TIME_VALUE.format(
                    param, value))

    @staticmethod
    @get_with_log('Params')
    def validate_chap_secret_parameter(param, value):
        if len(value) > AutomationConstants.CHAP_SECRET_MAX or len(value) < AutomationConstants.CHAP_SECRET_MIN:
            raise HitachiBlockValidationException( ErrorMessages.INVALID_SECRET_SIZE.format(
                        param, '******'))

class HitachiBlockException(Exception):
    def __init__(self):
        self.error = {
            VSSB_Api.MSG: ErrorMessages.NOT_AVAILABLE
        }

    def error_response(self):
        return self.error

class HitachiBlockModuleException(HitachiBlockException):
    def __init__(self, msg=None, cause=None, solution=None):
        super(HitachiBlockModuleException, self).__init__()

        if msg is not None:
            self.error[VSSB_Api.MSG] = msg
        if solution is not None:
            self.error[VSSB_Api.SOLUTION] = solution
        if cause is not None:
            self.error[VSSB_Api.CAUSE] = cause

class HitachiBlockValidationException(HitachiBlockException):
    def __init__(self, msg=None):
        super(HitachiBlockValidationException, self).__init__()
        
        if msg is not None:
            self.error[VSSB_Api.MSG] = msg


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
                self.error[VSSB_Api.API_ERROR] = HTTPClient._load_response(errors)
                # self.error[VSSB_Api.MSG] = self._error_code_to_message()
            elif isinstance(errors, socket.timeout):
                self.error[VSSB_Api.MSG] = ErrorMessages.API_TIMEOUT_ERR
            else:
                self.error[VSSB_Api.MSG] = \
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
    def get_servers(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_SERVERS)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def post_servers(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_SERVERS)
        params.request_params = {
            VSSB_Api.SERVERNICKNAME: params.server_nickname,
            VSSB_Api.OSTYPE: params.os_type
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_servers_by_name(params):
        logger = get_logger()
        get_response = HTTPClient.get_servers(params)
        logger.info("servers : %s", get_response)
        logger.info("server_nickname : %s", params.server_nickname)
        for server in get_response:
            if server[VSSB_Api.NICKNAME] == params.server_nickname:
                return server
        raise HitachiBlockModuleException(params.server_nickname + " not found.")


    @staticmethod
    def delete_servers(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.DELETE_SERVERS, params.server_id)
        delete_response = HTTPClient._request(Http.DELETE, endpoint, params)
        job_id = delete_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_volume_server_connections_by_serverId(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_VOLUME_SERVER_CONNECTIONS_AND_ID, params.server_id)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    def delete_volumes(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.DELETE_VOLUMES, params.volume_id)
        delete_response = HTTPClient._request(Http.DELETE, endpoint, params)
        logger.debug(f"Delete volume response {delete_response}")
        job_id = delete_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)
                
        logger.debug(f"Delete volume final response {response}")
        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_hbas(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_HBAS, params.server_id)
        params.request_params = {
            VSSB_Api.PROTOCOL: VSSB_Api.ISCSI,
            VSSB_Api.ISCSINAME: params.iscsi_name
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_hbas(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_HBAS, params.server_id)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_ports(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_PORTS)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_paths(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_PATHS, params.server_id)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_ports_by_name(params):
        logger = get_logger()
        get_response = HTTPClient.get_ports(params)
        for port in get_response:
            if port[VSSB_Api.NICKNAME] == params.target_port_name:
                return port
        return None

    @staticmethod
    @get_with_log('HTTPClient')
    def post_paths(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_PATHS, params.server_id)
        params.request_params = {
            VSSB_Api.HBAID: params.hba_id,
            VSSB_Api.PORTID: params.port_id
        }
        logger.debug(f"params.request_params >>>>>>>>>>>>>>>>>>>>>{params.request_params}")
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = False
        retryCount = 0
        while (not response  and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = False
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = True
                    #response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_pools_by_name(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_POOLS_AND_QUERY, params.pool_name)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        if len(get_response[VSSB_Api.DATA]) < 1:
            return None
        else:
            return get_response[VSSB_Api.DATA][0]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_volumes_by_nickname(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_VOLUMES_AND_NICKNAME, params.base_name)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        logger.debug(f"Response{get_response}")
        if len(get_response[VSSB_Api.DATA]) < 1:
            return []
        else:
            return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_volumes_by_serverId(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_VOLUMES_AND_SERVERID, params.server_id)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        if len(get_response[VSSB_Api.DATA]) < 1:
            return []
        else:
            return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def post_volumes(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_VOLUMES)
        params.request_params = {
            VSSB_Api.CAPACITY: params.capacity_mb,
            VSSB_Api.NUMBER: params.number,
            VSSB_Api.NAMEPARAM: {
                VSSB_Api.BASENAME: params.base_name,
                VSSB_Api.STARTNUMBER: params.start_number,
                VSSB_Api.NUMBEROFDIGIT: params.number_of_digit
            },
            VSSB_Api.POOLID:params.pool_id
        }
        logger.debug("request params: %s", params.request_params)
        
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        
        logger.info("Job ID: %s", job_id)
        response = None
        number_of_resources = 0
        while (number_of_resources <= params.number+1):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = job_response[VSSB_Api.AFFECTEDRESOURCES]
            number_of_resources = len(response)
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES]
                    return response
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                logger.info(f"Volume creation Progress: {number_of_resources} created out of {params.number}")
                time.sleep(15)
        return response
        

    @staticmethod
    @get_with_log('HTTPClient')
    def get_volumes_by_name(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_VOLUMES_AND_QUERY, params.volume_name)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        if len(get_response[VSSB_Api.DATA]) < 1:
            return None
        else:
            return get_response[VSSB_Api.DATA][0]

    @staticmethod
    @get_with_log('HTTPClient')
    def post_volume_server_connections(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_VOLUME_SERVER_CONNECTIONS)
        params.request_params = {
            VSSB_Api.VOLUMEID: params.volume_id,
            VSSB_Api.SERVERID: params.server_id
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE],
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE],
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def post_volumes_expand(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_VOLUMES_EXPAND, params.volume_id)
        params.request_params = {
            VSSB_Api.ADDITIONALCAPACITY: params.additional_capacity
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_chapusers_by_name(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_CHAPUSERS_AND_QUERY, params.target_chap_user_name)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        if len(get_response[VSSB_Api.DATA]) < 1:
            return None
        else:
            return get_response[VSSB_Api.DATA][0]

    @staticmethod
    @get_with_log('HTTPClient')
    def post_chapusers(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_CHAPUSERS)
        params.request_params = {
            VSSB_Api.TARGETCHAPUSERNAME: params.target_chap_user_name,
            VSSB_Api.TARGETCHAPSECRET: params.target_chap_secret,
            VSSB_Api.INITIATORCHAPUSERNAME: params.initiator_chap_user_name,
            VSSB_Api.INITIATORCHAPSECRET: params.initiator_chap_secret
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def delete_chapusers(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.DELETE_CHAPUSERS, params.chap_user_id)
        delete_response = HTTPClient._request(Http.DELETE, endpoint, params)
        job_id = delete_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_port_auth_settings_chapusers(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_PORT_AUTH_SETTINGS_CHAPUSERS, params.port_id)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        if len(get_response[VSSB_Api.DATA]) < 1:
            return None
        else:
            return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def post_port_auth_settings_chapusers(params):
        logger = get_logger()
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_PORT_AUTH_SETTINGS_CHAPUSERS, params.port_id)
        params.request_params = {
            VSSB_Api.CHAPUSERID: params.chap_user_id
        }

        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = False
        retryCount = 0
        while (not response  and retryCount < 30):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = False
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = True
                    # if len(job_response[VSSB_Api.AFFECTEDRESOURCES]) > 0:
                    #     response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_storage_nodes_by_protection_domain_id(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_STORAGE_NODES_AND_QUERY, params.protection_domain_id)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_drives(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_DRIVES)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_pools_by_id(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_POOLS_AND_ID, params.pool_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_pools(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_POOLS)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA][0]

    @staticmethod
    @get_with_log('HTTPClient')
    def post_pools_expand(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.POST_POOLS_EXPAND, params.pool_id)
        params.request_params = {
            VSSB_Api.DRIVEIDS: params.additional_drives
        }
        post_response = HTTPClient._request(Http.POST, endpoint, params)
        job_id = post_response[VSSB_Api.JOBID]
        response = None
        retryCount = 0
        while (response is None and retryCount <= params.time_b * 6):
            job_response = HTTPClient.get_jobs(params, job_id)
            job_status = job_response[VSSB_Api.STATUS]
            job_state = job_response[VSSB_Api.STATE]
            response = None
            if job_status == 'Completed':
                if job_state == 'Succeeded':
                    response = job_response[VSSB_Api.AFFECTEDRESOURCES][0]
                else:
                    raise HitachiBlockModuleException(
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGEID] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.MESSAGE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.CAUSE] + ' ' +
                        job_response[VSSB_Api.ERROR][VSSB_Api.SOLUTION])
            else:
                retryCount = retryCount + 1
                time.sleep(10)

        if retryCount > params.time_b * 6:
            raise HitachiBlockModuleException('Pools expand job did not complete. Terminated due to timeout.')

        return response

    @staticmethod
    @get_with_log('HTTPClient')
    def get_storage_controllers(params):
        endpoint = HTTPClient._format_endpoint(Endpoints.GET_STORAGE_CONTROLLERS)
        get_response = HTTPClient._request(Http.GET, endpoint, params)
        return get_response[VSSB_Api.DATA]

    @staticmethod
    @get_with_log('HTTPClient')
    def get_by_uri(params, uri):
        return HTTPClient._request(Http.GET, uri, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def get_jobs(params, job_id):
        endpoint = HTTPClient._format_endpoint(
            Endpoints.GET_JOBS, job_id)
        return HTTPClient._request(Http.GET, endpoint, params)

    @staticmethod
    @get_with_log('HTTPClient')
    def _request(http_verb, endpoint, params):
        try:
            headers = Http.HEADERS_JSON

            url = HTTPClient._format_url(params, endpoint)

            logger = get_logger()
            logger.debug(LogMessages.API_REQUEST_START.format(
                         http_verb, urlparse.urlparse(url).path))

            data = None
            if (http_verb == Http.POST or http_verb == Http.PUT or http_verb == Http.PATCH) and params.request_params is not None:
                data = json.dumps(params.request_params)
            response = open_url(
                url,
                headers=headers,
                url_username=params.user,
                url_password=params.password,
                method=http_verb,
                force_basic_auth=True,
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
#            VSSB_Api.SINGLE_QUOTE, VSSB_Api.SINGLE_QUOTE * 2)), args))
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
                msg['cause'] = raw_message['cause']
                msg["solution"] = raw_message["solution"]
                return msg
            return raw_message
        except ValueError:
            return to_native(text)


class Executors(object):
    def __init__(self, params=None):
        if params is not None:
            self.params = Params(params)

    @get_with_log('Executors')
    def add_computenode(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        return self._do_add_computenode(self.params)

    @get_with_log('Executors')
    def _do_add_computenode(self, params):
        try:
            get_response = HTTPClient.get_servers_by_name(params)
        except HitachiBlockModuleException:
            affected_resource_uri = HTTPClient.post_servers(params)
            outputs = self._do_get_by_uri(self.params, affected_resource_uri)
            response = {
                VSSB_Api.CHANGED: True,
                VSSB_Api.OUTPUTS: outputs,
            }
            return response
        else:
            response = {
                VSSB_Api.CHANGED: False,
                VSSB_Api.OUTPUTS: get_response,
            }
            return response

    @get_with_log('Executors')
    def add_hbas(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        return self._do_add_hbas(self.params)

    @get_with_log('Executors')
    def _do_add_hbas(self, params):
        get_data = HTTPClient.get_servers_by_name(params)
        params.server_id = get_data[VSSB_Api.ID]
        get_hbas = HTTPClient.get_hbas(params)
        hba_id = None
        for hba in get_hbas:
            if hba[VSSB_Api.NAME] == params.iscsi_name:
                hba_id = hba[VSSB_Api.ID]
                break
        if hba_id is None:
            affected_resource_uri = HTTPClient.post_hbas(params)
            outputs = self._do_get_by_uri(self.params, affected_resource_uri)
            response = {
                VSSB_Api.CHANGED: True,
                VSSB_Api.OUTPUTS: outputs,
            }
        else:
            affected_resource_uri = Http.BASE_URL + HTTPClient._format_endpoint(Endpoints.POST_HBAS, params.server_id) + '/' + hba_id
            outputs = self._do_get_by_uri(self.params, affected_resource_uri)
            response = {
                VSSB_Api.CHANGED: False,
                VSSB_Api.OUTPUTS: outputs,
            }
        return response

    @get_with_log('Executors')
    def add_paths(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        return self._do_add_paths(self.params)

    @get_with_log('Executors')
    def _do_add_paths(self, params):
        logger = get_logger()
        get_data = HTTPClient.get_servers_by_name(params)
        logger.info("server info: %s", get_data)
        params.server_id = get_data[VSSB_Api.ID]
        get_data = HTTPClient.get_hbas(params)
        for hba in get_data:
            if hba[VSSB_Api.NAME] == params.iscsi_name:
                params.hba_id = hba[VSSB_Api.ID]
                logger.debug(f"params.hba_id>>>>>>>>>>>>>>>>>>>>>{params.hba_id}")
                break
        get_data = HTTPClient.get_ports(params)
        for port in get_data:
            if port[VSSB_Api.NICKNAME] == params.target_port_name:
                params.port_id = port[VSSB_Api.ID]
                break
        if params.port_id is None:
            raise HitachiBlockModuleException('The target port name specified by the target_port_name in port_settings argument was not found. Revise the value specified for the port_settings argument.')
        
        get_paths = HTTPClient.get_paths(params)
        logger
        find = False
        for path in get_paths:
            logger.debug('hbaName = ' + path[VSSB_Api.HBANAME] + ', iscsi_name = ' + params.iscsi_name)
            logger.debug('portNickname = ' + path[VSSB_Api.PORTNICKNAME] + ', target_port_name = ' + params.target_port_name)
            if path[VSSB_Api.HBANAME] == params.iscsi_name and path[VSSB_Api.PORTNICKNAME] == params.target_port_name:
                find = True
                break
        if find == False:
            success = HTTPClient.post_hbas(params)
            if not success:
                raise HitachiBlockModuleException('Failed to add path.')
            else:
                params.hba_id = ""
                params.port_id = ""
                success = HTTPClient.post_paths(params)
                if not success:
                    raise HitachiBlockModuleException('Failed to add path.')
                outputs = HTTPClient.get_paths(params)
                response = {
                    VSSB_Api.CHANGED: True,
                    VSSB_Api.OUTPUTS: outputs,
                }
        else:
            response = {
                VSSB_Api.CHANGED: False,
                VSSB_Api.OUTPUTS: get_paths,
            }
        return response

    @get_with_log('Executors')
    def create_volume(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_create_volume(self.params)
        outputs = []
        volumes = []
        for uri in affected_resource_uri:
            get_response = self._do_get_by_uri(self.params, uri)
            customize_capacity_response(get_response)
            outputs.append(get_response)
            volumes.append(get_response[VSSB_Api.NAME])

        response = {
            VSSB_Api.CHANGED: True,
            VSSB_Api.OUTPUTS: outputs,
            VSSB_Api.VOLUMES: volumes
        }
        return response

    @get_with_log('Executors')
    def _do_create_volume(self, params):
        logger = get_logger()
        get_data = HTTPClient.get_pools_by_name(params)
        if get_data is None:
            logger.error('The pool specified by the pool_name argument was not found. Revise the value specified for the pool_name argument.')
            raise HitachiBlockModuleException('The pool specified by the pool_name argument was not found. Revise the value specified for the pool_name argument.')

        params.pool_id = get_data[VSSB_Api.ID]
        get_volumes = HTTPClient.get_volumes_by_nickname(params)
        maxNumber = 0
        for volume in get_volumes:
            partOfNumeric = volume[VSSB_Api.NICKNAME][len(volume[VSSB_Api.NICKNAME])-params.number_of_digit:len(volume[VSSB_Api.NICKNAME])]
            try:
                number = int(partOfNumeric)
                if maxNumber <= number:
                    maxNumber = number + 1
            except ValueError:
                pass
        params.start_number = maxNumber
        logger.debug(f"Max volume number and prefix name to be created {params.start_number}")
        return HTTPClient.post_volumes(params)

    @get_with_log('Executors')
    def attach_volume(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        affected_resource_uri = self._do_attach_volume(self.params)
        outputs = self._do_get_by_uri(self.params, affected_resource_uri)

        response = {
            VSSB_Api.CHANGED: True,
            VSSB_Api.OUTPUTS: outputs,
        }
        return response

    @get_with_log('Executors')
    def _do_attach_volume(self, params):
        get_data = HTTPClient.get_servers_by_name(params)
        if get_data is None:
            raise HitachiBlockModuleException('The request could not be executed.',
                                              'The server specified by the server_nickname argument was not found.',
                                              'Revise the value specified for the server_nickname argument.')
        params.server_id = get_data[VSSB_Api.ID]
        get_data = HTTPClient.get_volumes_by_name(params)
        if get_data is None:
            raise HitachiBlockModuleException('The request could not be executed.',
                                              'The volume specified by the volume_name argument was not found.',
                                              'Revise the value specified for the volume_name argument.')
        params.volume_id = get_data[VSSB_Api.ID]
        return HTTPClient.post_volume_server_connections(params)

    @get_with_log('Executors')
    def expand_volume(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        if self.params.capacity_mb < 1:
            raise HitachiBlockModuleException('Specified value of capacity_mb argument is out of range. Specify a number greater than 0.')

        return self._do_expand_volume(self.params)

    @get_with_log('Executors')
    def _do_expand_volume(self, params):
        logger = get_logger()
        get_response = HTTPClient.get_volumes_by_name(params)
        if get_response is None:
            logger.error('The volume specified by the name argument was not found. Revise the value specified for the name argument.')
            raise HitachiBlockModuleException('The volume specified by the name argument was not found. Revise the value specified for the name argument.')
        if params.capacity_mb <= get_response[VSSB_Api.TOTALCAPACITY]:
            customize_capacity_response(get_response)
            response = {
                VSSB_Api.CHANGED: False,
                VSSB_Api.OUTPUTS: get_response
            }
        else:
            params.volume_id = get_response[VSSB_Api.ID]
            params.additional_capacity = params.capacity_mb - get_response[VSSB_Api.TOTALCAPACITY]
            logger.debug('additional_capacity = ' + str(params.additional_capacity) + ', volume_id = ' + params.volume_id)
            affected_resource_uri = HTTPClient.post_volumes_expand(params)
            outputs = self._do_get_by_uri(self.params, affected_resource_uri)
            customize_capacity_response(outputs)
            response = {
                VSSB_Api.CHANGED: True,
                VSSB_Api.OUTPUTS: outputs
            }
            logger.info(f"Response for the task: {response}")
        return response

    @get_with_log('Executors')
    def create_chapuser(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return 
        if self.params.initiator_chap_user_name is None and not(self.params.initiator_chap_secret is None) or \
           not(self.params.initiator_chap_user_name is None) and self.params.initiator_chap_secret is None:
            raise HitachiBlockModuleException('The combination of values specified for initiator_chap_user_name and initiator_chap_secret is invalid. Revise the specified value.')

        return self._do_create_chapuser(self.params)

    @get_with_log('Executors')
    def _do_create_chapuser(self, params):
        logger = get_logger()
        get_response = HTTPClient.get_chapusers_by_name(params)
        if get_response is None:
            affected_resource_uri = HTTPClient.post_chapusers(params)
            outputs = self._do_get_by_uri(self.params, affected_resource_uri)
            response = {
                VSSB_Api.CHANGED: True,
                VSSB_Api.OUTPUTS: outputs
            }
            return response
        else:
            response = {
                VSSB_Api.CHANGED: False,
                VSSB_Api.OUTPUTS: get_response
            }
            return response

    @get_with_log('Executors')
    def add_chapuser_computeport(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return 

        return self._do_add_chapuser_computeport(self.params)

    @get_with_log('Executors')
    def _do_add_chapuser_computeport(self, params):
        logger = get_logger()
        get_response = HTTPClient.get_ports_by_name(params)
        logger.info("response : %s", get_response)
        if get_response is None:
            raise HitachiBlockModuleException('Specified compute port is not found.')
        params.port_id = get_response[VSSB_Api.ID]
        
        get_response = HTTPClient.get_port_auth_settings_chapusers(params)
        logger.info("response2 : %s", get_response)

        if not(get_response is None):
            for chap_user in get_response:
                if chap_user['targetChapUserName'] == params.target_chap_user_name:
                    params.chap_user_id = chap_user['id']
                    response = {
                        VSSB_Api.CHANGED: False,
                        VSSB_Api.OUTPUTS: HTTPClient.get_chapusers_by_name(params)
                    }
                    return response

        # GET target chap user id
        get_response = HTTPClient.get_chapusers_by_name(params)
        if get_response is None:
            raise HitachiBlockModuleException('Spacified chap user is not found.')

        params.chap_user_id = get_response[VSSB_Api.ID]
        # Add target CHAP user
        success = HTTPClient.post_port_auth_settings_chapusers(params)

        if not success:          
           raise HitachiBlockModuleException('Failed to add chap user to the port.')
        else:
            get_response = HTTPClient.get_chapusers_by_name(params)
            response = {
                VSSB_Api.CHANGED: True,
                VSSB_Api.OUTPUTS: get_response
            }
            return response

    @get_with_log('Executors')
    def delete_computenode(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return 

        return self._do_delete_computenode(self.params)

    @get_with_log('Executors')
    def _do_delete_volume(self, params):
        logger = get_logger()
        get_response = HTTPClient.get_volumes_by_name(params)
        logger.info(get_response)
        if get_response is None:
            logger.error('The volume specified by the name argument was not found. Revise the value specified for the name argument.')
            raise HitachiBlockModuleException('The volume specified volume name not found. Provide the correct volume name')
        else:
            params.volume_id = get_response['id']
            if HTTPClient.delete_volumes(params):
                response = {
                VSSB_Api.CHANGED: True,
                VSSB_Api.OUTPUTS: "Delete volume successfully"
            } 
                return response
            else :
                raise HitachiBlockModuleException("Delete volume failed")
         
    @get_with_log('Executors')
    def _do_delete_computenode(self, params):
        get_response = HTTPClient.get_servers_by_name(params)
        if get_response is None:
            raise HitachiBlockModuleException('The request could not be executed.',
                                              'Compute Node does not exist',
                                              'Revise the value specified for the server_nickname argument.')
        else:
            params.server_id = get_response[VSSB_Api.ID]
            HTTPClient.delete_servers(params)
            response = {
                VSSB_Api.CHANGED: True
            }

        return response

    @get_with_log('Executors')
    def delete_tenant(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return 

        return self._do_delete_tenant(self.params)

    @get_with_log('Executors')
    def _do_delete_tenant(self, params):
        get_response = HTTPClient.get_servers_by_name(params)
        if get_response is None:
            raise HitachiBlockModuleException('The request could not be executed.',
                                              'Specified compute node is not found.',
                                              'Revise the value specified for the server_nickname argument.')
        params.server_id = get_response[VSSB_Api.ID]
        get_response = HTTPClient.get_volume_server_connections_by_serverId(params)
        # Delete compute node and connection info
        self._do_delete_computenode(self.params)

        # Delete volumes
        volumes = []
        for volume in get_response:
            params.volume_id = volume['volumeId']
            HTTPClient.delete_volumes(params)
            volumes.append(volume['volumeId'])
        response = {
            VSSB_Api.CHANGED: True
        }
        return response

    @get_with_log('Executors')
    def delete_volume(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result

        return self._do_delete_volume(self.params)

    @get_with_log('Executors')
    def expand_pool_process1(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        if self.params.pool_expand_capacity < 1:
            raise HitachiBlockModuleException('Specified value of pool_expand_capacity argument is out of range. Specify a number greater than 0.')
        if self.params.time_a < 1:
            raise HitachiBlockModuleException('Specified value of added_volumes_time(time_a) argument is out of range. Specify a number greater than 0.')
        if self.params.time_b < 1:
            raise HitachiBlockModuleException('Specified value of job_time(time_b) argument is out of range. Specify a number greater than 0.')
        if self.params.time_c < 1:
            raise HitachiBlockModuleException('Specified value of capacity_expand_time(time_c) argument is out of range. Specify a number greater than 0.')
        if self.params.time_d < 1:
            raise HitachiBlockModuleException('Specified value of data_movement_time(time_d) argument is out of range. Specify a number greater than 0.')

        return self._do_expand_pool_process1(self.params)

    @get_with_log('Executors')
    def _do_expand_pool_process1(self, params):
        # ストレージプールの情報を取得する
        get_response = HTTPClient.get_pools(params)
        params.pool_info = get_response
        params.protection_domain_id = params.pool_info[VSSB_Api.PROTECTONDOMAINID]
        params.pool_capacity = params.pool_info[VSSB_Api.TOTALCAPACITY]

        # 各ストレージノードの情報を取得する
        params.storage_nodes_info = HTTPClient.get_storage_nodes_by_protection_domain_id(params)
        
        # 各ストレージノードで既に認識しているボリューム一覧を取得する
        params.drives_info = HTTPClient.get_drives(params)

        # 追加するドライブ数を計算する。
        #  [前提条件]追加するドライブのサイズは既存ドライブと同一サイズとする。
        #  現在のプール論理容量＋拡張するプールサイズから、ノード当たりに必要な総物理容量を求める。
        #  現在のプール論理容量から、現在割り当てられているノード当たりの総物理容量を求める。
        #  「{(必要な総物理容量÷ドライブ1つ当たりのサイズ)－(現在割り当てられている総物理容量÷ドライブ1つ当たりのサイズ)}」が追加するドライブ数になる。 
        # <4D+2P>
        #  論理容量[MiB]=(RoundDown((CrowNode-893592)÷893592)*595728-4200)*NNode
        additional_drive_capacity = None
        for cur_drive in params.drives_info:
            if cur_drive[VSSB_Api.STATUS] != 'Offline':
                for storage_node in params.storage_nodes_info:
                    if cur_drive[VSSB_Api.STORAGENODEID] == storage_node[VSSB_Api.ID]:
                        additional_drive_capacity = cur_drive[VSSB_Api.DRIVECAPACITY] * 1000 * 1000 * 1000      # GB   -> byte
                        additional_drive_capacity = (additional_drive_capacity + (1024 - 1)) // 1024            # byte -> KiB
                        additional_drive_capacity = (additional_drive_capacity + (1024 - 1)) // 1024            # KiB  -> MiB
                        break
            if additional_drive_capacity is not None:
                break

        # <4D+2P>
        # params.pool_capacity + params.pool_expand_capacity = (RoundDown((new_CrowNode - 893592) / 893592) * 595728 - 4200) * len(params.storage_nodes_info)
        # new_CrowNode = (((params.pool_capacity + params.pool_expand_capacity) / len(params.storage_nodes_info) + 4200) / 595728) * 893592 + 893592              
        new_CrowNode = (params.pool_capacity + params.pool_expand_capacity + (len(params.storage_nodes_info) - 1)) // len(params.storage_nodes_info)
        new_CrowNode = (new_CrowNode + 4200 + (595728 -1)) // 595728
        new_CrowNode = new_CrowNode * 893592 + 893592

        # params.pool_capacity =  (RoundDown((cur_CrowNode - 893592) / 893592) * 595728 - 4200) * len(params.storage_nodes_info)
        # cur_CrowNode = ((params.pool_capacity / len(params.storage_nodes_info) + 4200) / 595728) * 893592 + 893592              
        cur_CrowNode = (params.pool_capacity + (len(params.storage_nodes_info) - 1)) // len(params.storage_nodes_info)
        cur_CrowNode = (cur_CrowNode + 4200 + (595728 -1)) // 595728
        cur_CrowNode = cur_CrowNode * 893592 + 893592

        # HPEC 4D+2P の場合の、有効物理容量(CrowDevice[MiB])は、以下の計算で求められます。
        # (CrowDevice)=floor((Cdevice*0.9846)-2048[MiB],148932[MiB])
        #   floor(数値,基準値):基準値の倍数のうち、最も数値に近く数値を超えない値
        #   Cdevice:ドライブの物理容量[MiB]

        CrowDevice = int((((additional_drive_capacity * 0.9846) - 2048) // 148932) * 148932)
        
        new_drive_count = (new_CrowNode + (CrowDevice - 1)) // CrowDevice
        cur_drive_count = (cur_CrowNode + (CrowDevice - 1)) // CrowDevice

        additional_drive_count_in_node = new_drive_count - cur_drive_count

        response = {
            'pool_info': params.pool_info,
            'storage_nodes_info': params.storage_nodes_info,
            'drives_info': params.drives_info,
            'additional_drive_count_in_node' : additional_drive_count_in_node,
            'additional_drive_capacity': (additional_drive_capacity + (1024 - 1)) // 1024,      # MiB  -> GiB
        }

        return response

    @get_with_log('Executors')
    def expand_pool_process2(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result

        self.params.pool_info = self.params.expand_pool_process1_info['pool_info']
        self.params.pool_id = self.params.pool_info[VSSB_Api.ID]
        self.params.protection_domain_id = self.params.pool_info[VSSB_Api.PROTECTONDOMAINID]
        self.params.pool_capacity = self.params.pool_info[VSSB_Api.TOTALCAPACITY]
        self.params.storage_nodes_info = self.params.expand_pool_process1_info['storage_nodes_info']
        self.params.drives_info = self.params.expand_pool_process1_info['drives_info']
        self.params.additional_drive_count = self.params.expand_pool_process1_info['additional_drive_count_in_node'] * len(self.params.storage_nodes_info)

        return self._do_expand_pool_process2(self.params)

    @get_with_log('Executors')
    def _do_expand_pool_process2(self, params):
        # 各ストレージノードで追加されたEBSボリュームが認識されているか確認する。
        # process1で取得した結果からドライブが増えているか確認する。
        retryCount = 0
        driveCount = 0
        while (driveCount < params.additional_drive_count and retryCount <= params.time_a):
            driveCount = 0
            params.additional_drives = []
            get_response = HTTPClient.get_drives(params)
            for new_drive in get_response:
                flag = 0
                for cur_drive in params.drives_info:
                    if cur_drive[VSSB_Api.ID] == new_drive[VSSB_Api.ID]:
                        flag = 1
                        break
                if flag == 0:
                    for storage_node in params.storage_nodes_info:
                        if new_drive[VSSB_Api.STORAGENODEID] == storage_node[VSSB_Api.ID]:
                            params.additional_drives.append(new_drive[VSSB_Api.ID])
                            driveCount = driveCount + 1
                            break
                if driveCount >= params.additional_drive_count:
                    break    
            if driveCount < params.additional_drive_count:
                retryCount = retryCount + 1
                time.sleep(60)

        if driveCount < params.additional_drive_count:
            raise HitachiBlockModuleException('Failed to verify added volumes. Terminated due to timeout.')

        # ストレージプールを拡張する
        HTTPClient.post_pools_expand(params)

        # ストレージプールの容量が増えていることを確認する
        retryCount = 0
        get_response = HTTPClient.get_pools_by_id(params)
        while (get_response[VSSB_Api.TOTALCAPACITY] - params.pool_capacity < params.pool_expand_capacity and retryCount <= params.time_c):
            get_response = HTTPClient.get_pools_by_id(params)
            if get_response[VSSB_Api.TOTALCAPACITY] - params.pool_capacity < params.pool_expand_capacity:
                retryCount = retryCount + 1
                time.sleep(60)

        if get_response[VSSB_Api.TOTALCAPACITY] - params.pool_capacity < params.pool_expand_capacity:
            raise HitachiBlockModuleException('Failed to verify pool capacity_mb increase. Terminated due to timeout.')

        response = {
            VSSB_Api.CHANGED: True,
            VSSB_Api.OUTPUTS: get_response
         }

        # ストレージコントローラーの管理下にあるユーザーデータの移動を確認する（すべてのストレージコントローラーについて確認する）
        # データ移動の実施状況(dataRebalanceStatus)が"Stopped"に変わるまで待つ。
        retryCount = 0
        controllerCount = 0
        while (controllerCount < len(params.storage_nodes_info) and retryCount <= params.time_d):
            controllerCount = 0
            get_response = HTTPClient.get_storage_controllers(params)
            for storage_controller in get_response:
                for storage_node in params.storage_nodes_info:
                    if storage_controller[VSSB_Api.ACTIVESTORAGENODEID] == storage_node[VSSB_Api.ID] and storage_controller[VSSB_Api.DATAREBALANCESTATUS] == 'Stopped':
                        controllerCount = controllerCount + 1
                        break    
                if controllerCount >= len(params.storage_nodes_info):
                    break    
            if controllerCount < len(params.storage_nodes_info): 
                retryCount = retryCount + 1
                time.sleep(60)

        if controllerCount < len(params.storage_nodes_info):
            raise HitachiBlockModuleException('Failed to verify the movement of user data managed by the storage controller. Terminated due to timeout.')

        return response

    @get_with_log('Executors')
    def add_storagenode_process1(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result
        if self.params.time_a < 1:
            raise HitachiBlockModuleException('Specified value of added_volumes_time(time_a) argument is out of range. Specify a number greater than 0.')
        if self.params.time_b < 1:
            raise HitachiBlockModuleException('Specified value of job_time(time_b) argument is out of range. Specify a number greater than 0.')
        if self.params.time_c < 1:
            raise HitachiBlockModuleException('Specified value of capacity_expand_time(time_c) argument is out of range. Specify a number greater than 0.')
        if self.params.time_d < 1:
            raise HitachiBlockModuleException('Specified value of data_movement_time(time_d) argument is out of range. Specify a number greater than 0.')

        return self._do_add_storagenode_process1(self.params)

    @get_with_log('Executors')
    def _do_add_storagenode_process1(self, params):
        # ストレージプールの情報を取得する
        get_response = HTTPClient.get_pools(params)

        params.pool_info = get_response
        params.protection_domain_id = params.pool_info[VSSB_Api.PROTECTONDOMAINID]
        params.pool_capacity = params.pool_info[VSSB_Api.TOTALCAPACITY]

        # 各ストレージノードの情報を取得する
        params.storage_nodes_info = HTTPClient.get_storage_nodes_by_protection_domain_id(params)
            
        # 各ストレージノードで既に認識しているボリューム一覧を取得する
        params.drives_info = HTTPClient.get_drives(params)

        response = {
            'pool_info': params.pool_info,
            'storage_nodes_info': params.storage_nodes_info,
            'drives_info': params.drives_info,
            'pool_expand_capacity': params.pool_capacity // len(params.storage_nodes_info)
        }

        return response

    @get_with_log('Executors')
    def add_storagenode_process2(self):
        if self.params.check_mode:
            # TODO: Check input parameters
            result = {
                VSSB_Api.CHANGED: False
            }
            return result

        self.params.pool_info = self.params.add_storagenode_process1_info['pool_info']
        self.params.pool_id = self.params.pool_info[VSSB_Api.ID]
        self.params.protection_domain_id = self.params.pool_info[VSSB_Api.PROTECTONDOMAINID]
        self.params.pool_capacity = self.params.pool_info[VSSB_Api.TOTALCAPACITY]
        self.params.storage_nodes_info = HTTPClient.get_storage_nodes_by_protection_domain_id(self.params)    # new storage node
        self.params.drives_info = self.params.add_storagenode_process1_info['drives_info']
        self.params.additional_drive_count = self.params.drive_count_in_node
       
        return self._do_expand_pool_process2(self.params)

    @get_with_log('Executors')
    def _do_get_by_uri(self, params, uri):
        endpoint = uri.split('/', 3)[3]
        response = HTTPClient.get_by_uri(params, endpoint)

        return response
    
def customize_capacity_response(response):
    if "dataReductionEffects" in response:
        if "compressedCapacity" in response["dataReductionEffects"]:
            response["dataReductionEffects"]["compressedCapacity_mb"] = response["dataReductionEffects"].pop("compressedCapacity")
        if "dataReductionCapacity" in response["dataReductionEffects"]:
            response["dataReductionEffects"]["dataReductionCapacity_mb"] = response["dataReductionEffects"].pop("dataReductionCapacity")
        if "reclaimedCapacity" in response["dataReductionEffects"]:
            response["dataReductionEffects"]["reclaimedCapacity_mb"] = response["dataReductionEffects"].pop("reclaimedCapacity")
        if "systemDataCapacity" in response["dataReductionEffects"]:
            response["dataReductionEffects"]["systemDataCapacity_mb"] = response["dataReductionEffects"].pop("systemDataCapacity")
    if "freeCapacity" in response:
        response["freeCapacity_mb"] = response.pop("freeCapacity")
    if "reservedCapacity" in response:
        response["reservedCapacity_mb"] = response.pop("reservedCapacity")
    if "totalCapacity" in response:
        response["totalCapacity_mb"] = response.pop("totalCapacity")
    if "usedCapacity" in response:
        response["usedCapacity_mb"] = response.pop("usedCapacity")
