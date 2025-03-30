import tempfile
import subprocess
import logging
import os
import sys
import time
import json
import base64
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class DirectApiError(Exception):
    pass


class StorageException(DirectApiError):
    pass


class TokenError(DirectApiError):
    pass


class TokenExpiredError(TokenError):
    pass


class ClientSecretNotFound(DirectApiError):
    pass


class EnvironmentError(DirectApiError):
    pass


class APIClient:
    __export_functions__ = ['list_dir', 'fetch_and_save_file',
                            'upload_file', 'upload_dir',
                            'fetch_and_save_file_or_dir',
                            'start_agent_daemon'
                            ]
    __class_args__ = []

    iss_url = 'https://keycloak.cta.cscs.ch/realms/master/'
    dcache_url = 'https://dcache.cta.cscs.ch:2880'
    profile_dir = str(Path.home() / ".config/cta-data")
    cta_token_file = profile_dir + "/token"
    client_secret_file = profile_dir + "/secret"
    stop_request_file = profile_dir + "/cta_agent_stop"
    token_name = "kk-dcache"
    token_update_interval = 300  # in seconds
    client_id = "dcache-cta-cscs-ch-users"

    def __init__(self, dev_instance=False):
        os.makedirs(self.profile_dir, exist_ok=True)
        # we manually configure temporary directory for oidc tools
        # to avoid permission issues
        self._oidc_env = os.environ.copy()
        self._oidc_env["TMPDIR"] = tempfile.mkdtemp()

        if dev_instance:
            self.dcache_url = 'https://dcache-dev.ctaodc.ch:2880'
            suf = '-dev'
            self.cta_token_file = APIClient.cta_token_file + suf
            self.client_secret_file = APIClient.client_secret_file + suf
            self.token_name = APIClient.token_name + suf
            self.client_id = 'dcache-dev'

    @property
    def secret(self):
        if not hasattr(self, "_secret"):
            if os.path.isfile(self.client_secret_file):
                with open(self.client_secret_file) as f:
                    encoded_string = f.readline().strip()
                    try:
                        decoded_bytes = base64.b64decode(encoded_string)
                        self._secret = decoded_bytes.decode('utf-8')
                    except Exception:
                        raise ClientSecretNotFound(
                            'Invalid client secret format')
            else:
                raise ClientSecretNotFound(
                    'Client secret is not provided and not '
                    f'found in {self.client_secret_file}')

        if self._secret is None or self._secret == '':
            raise Exception("Invalid secret")
        return self._secret

    @secret.setter
    def secret(self, value):
        self._secret = value
        # save base64 encoded secret in the config file
        with open(self.client_secret_file, 'wt') as f:
            encoded_bytes = base64.b64encode(self._secret.encode('utf-8'))
            f.write(encoded_bytes.decode('utf-8'))
        os.chmod(self.client_secret_file, 0o600)

    @property
    def token(self):
        if not hasattr(self, "_token"):
            self._token = self._load_token()

        if self._token is None or self._token == '':
            raise TokenError("Invalid token")
        return self._token

    def _verify_environment(self):
        required_utils = ['oidc-agent', 'davix-ls', 'davix-get', 'davix-put']
        missing_utils = []
        for u in required_utils:
            ret = subprocess.run(
                f'which {u}', capture_output=False, shell=True)
            if ret.returncode != 0:
                missing_utils.append(u)
        if missing_utils:
            raise EnvironmentError(
                "Please install the following utils before running this "
                "code: " + ', '.join(missing_utils))

        self.secret  # test if secret is initialized

    @staticmethod
    def _verify_token(token):
        exp_time_local = APIClient._get_token_exp_time(token)
        if datetime.now() > exp_time_local:
            raise TokenExpiredError(
                'The token has expired on ' + str(exp_time_local) + '\n' +
                'run "cta-data-direct start-agent" to refresh it')

    @staticmethod
    def _get_token_exp_time(token):
        try:
            data = json.loads(base64.b64decode(token.split(".")[1] + "="))
            exp_time = int(data['exp'])
            exp_time_local = datetime.fromtimestamp(exp_time)
        except Exception:
            raise TokenError('Invalid token: unexpected format')
        return exp_time_local

    def _load_token(self):
        if os.path.isfile(self.cta_token_file):
            with open(self.cta_token_file) as f:
                token = f.readline().strip()
                self._verify_token(token)
                return token
        else:
            raise TokenError(
                f"Token not found in {self.cta_token_file}. Please start "
                "agent using start-agent subcommand")

    @staticmethod
    def _daemonize():
        # Fork and detach process to create a daemon.
        if os.fork() > 0:
            sys.exit()  # Exit the parent process

        os.setsid()  # Start a new session

        if os.fork() > 0:
            sys.exit()  # Exit the second parent process

        sys.stdout.flush()
        sys.stderr.flush()

        # Redirect standard file descriptors
        with open("/dev/null", "r") as devnull:
            os.dup2(devnull.fileno(), sys.stdin.fileno())
        with open("/dev/null", "a+") as devnull:
            os.dup2(devnull.fileno(), sys.stdout.fileno())
            os.dup2(devnull.fileno(), sys.stderr.fileno())

    @property
    def oidc_env(self):
        return self._oidc_env

    def _refresh_token(self):
        token_print_command = f"oidc-token {self.token_name}"
        ret = subprocess.run(token_print_command, capture_output=True,
                             shell=True, text=True, env=self.oidc_env)
        if ret.returncode != 0:
            raise TokenError('oidc-agent error: ' + ret.stderr)

        token = ret.stdout.strip()
        with open(self.cta_token_file, 'wt') as f:
            print(token, file=f)
        os.chmod(self.cta_token_file, 0o600)  # Sets file permission to 600

    def print_token(self):
        try:
            token = self.token
            exp_time_local = APIClient._get_token_exp_time(token)
            print(self.token)
            print('valid till', exp_time_local, file=sys.stderr)
        except TokenError as e:
            print(e, file=sys.stderr)

    def _agent_loop(self):
        # make sure token_update_interval is integer >= 1
        token_update_interval = max(int(self.token_update_interval), 1)
        try:
            counter = 0
            while True:
                if os.path.isfile(self.stop_request_file):
                    self.stop_agent()
                    os.remove(self.stop_request_file)
                    break
                if counter % token_update_interval == 0:
                    self._refresh_token()
                time.sleep(1)
                counter += 1
        except Exception as ex:
            with open(self.profile_dir + "/agent.log", 'wt') as f:
                print(ex, file=f)

    def start_agent_daemon(self):
        self.init_agent()
        self._daemonize()
        self._agent_loop()

    def init_agent(self):
        token_loaded = False
        self._verify_environment()
        try:
            self._refresh_token()
            token_loaded = True
        except TokenError:
            pass
        if token_loaded:
            return

        scope = ""
        redirect_url = ""
        flow = 'device'
        # we use temporary empty file to avoid password prompts
        with tempfile.NamedTemporaryFile() as empty_file:
            with open(empty_file.name, 'wt') as out:
                print(file=out)
            empty_file_path = empty_file.name

            variables = ['OIDCD_PID', 'OIDCD_PID_FILE', 'OIDC_SOCK']

            init_command = "export OIDC_AGENT=$(which oidc-agent) && " \
                "eval `oidc-agent-service use`"
            env_vars = ' && '.join([f'echo ${v}' for v in variables])
            ret = subprocess.run(init_command + " && " + env_vars,
                                 capture_output=True,
                                 shell=True, text=True,
                                 env=self.oidc_env
                                 )
            if ret.returncode != 0:
                logger.error('oidc-agent start failed: ' + ret.stderr)
                raise EnvironmentError(ret.stderr)

            var_values = [v.strip()
                          for v in ret.stdout.split('\n')[-len(variables) - 1:]
                          ]

            self._oidc_env.update(zip(variables, var_values))

            pw_file_option = f'--pw-file={empty_file_path}'
            token_load_command = f"oidc-add {self.token_name}"
            token_list_command = "oidc-add -l"

            ret = subprocess.run(token_list_command,
                                 capture_output=True, shell=True, text=True,
                                 env=self.oidc_env)

            if self.token_name in ret.stdout:  # token found
                token_load_command = f'{token_load_command} {pw_file_option}'

                process = subprocess.Popen([token_load_command],
                                           text=True, shell=True,
                                           env=self.oidc_env)
                stdout, _ = process.communicate(input="\n\n\n\n")
                if process.returncode != 0:
                    logger.warning(
                        'failed to load token using command: %s',
                        token_load_command)
                    logger.warning('command output: %s', stdout)
                else:
                    token_loaded = True
            if not token_loaded:
                gen_command = ['oidc-gen', self.token_name, '--iss',
                               self.iss_url, f'--client-id={self.client_id}',
                               '--redirect-url', redirect_url,
                               '--no-url-call', '--scope', scope,
                               '--flow', flow]
                gen_command += pw_file_option.split()
                gen_command += ['--client-secret']
                gen_command_log = " ".join(gen_command) + " ***"
                gen_command += [self.secret]
                logger.info('command: %s', gen_command_log)
                process = subprocess.Popen(gen_command, text=True,
                                           env=self.oidc_env)
                stdout, _ = process.communicate(input="\n\n\n\n")
                if process.returncode != 0:
                    logger.error('command output: %s', )
                    raise TokenError(stdout)

        self._refresh_token()  # make sure token can be loaded before exiting

    @token.setter
    def token(self, value):
        self._token = value

    def list_dir(self, path, recursive=False, n_threads=2):
        if not path.startswith('/'):
            path = '/' + path

        options = f' -r {n_threads}' if recursive else ''
        command = f'davix-ls{options} -k -H "Authorization: ' \
            f'Bearer {self.token}" {self.dcache_url}' + path

        ret = subprocess.run(command, capture_output=True,
                             shell=True, text=True)
        if ret.returncode != 0:
            logger.error('failed to list dir using command: %s', command)
            logger.error('command output: %s', ret.stdout)
            logger.error('command stderr: %s', ret.stderr)
            raise StorageException(ret.stderr)

        return [line.strip() for line in ret.stdout.split('\n')
                if len(line.strip()) > 0]

    def fetch_and_save_file(self, path, save_to_fn=None):
        if not save_to_fn:
            save_to_fn = os.path.basename(path)
        if not path.startswith('/'):
            path = '/' + path

        command = f'davix-get -k -H "Authorization: Bearer ' \
            f'{self.token}" {self.dcache_url}{path} > {save_to_fn}'
        ret = subprocess.run(command, capture_output=True,
                             shell=True, text=True)
        if ret.returncode != 0:
            logger.error('failed to fetch file using command: %s', command)
            logger.error('command output: %s', ret.stdout)
            logger.error('command stderr: %s', ret.stderr)
            raise StorageException(ret.stderr)

    def fetch_and_save_file_or_dir(self, path, recursive=False):
        path = path.strip()
        if not recursive:
            return self.fetch_and_save_file(path)
        else:
            if path.startswith('/'):
                path = path[1:]
            if path.endswith('/'):
                path = path[:-1]
            root_dir_name = path.split('/')[-1]
            for entry in self.list_dir(path, recursive=True):
                print('entry', entry)
                save_path = entry[len(path) - len(root_dir_name):].strip()
                dir_path = os.path.dirname(save_path)
                if len(dir_path) > 0:
                    os.makedirs(dir_path, exist_ok=True)
                self.fetch_and_save_file(path, save_to_fn=save_path)

    def upload_file(self, local_fn: str, path: str):
        if not path.startswith('/'):
            path = '/' + path
        url = self.dcache_url + path
        logger.info("uploading %s to %s", local_fn, url)

        command = f'davix-put -k -H "Authorization: Bearer ' \
            f'{self.token}" {local_fn} {url}'
        ret = subprocess.run(command, capture_output=True,
                             shell=True, text=True)
        if ret.returncode != 0:
            logger.error('failed to upload file using command: %s', command)
            logger.error('command output: %s', ret.stdout)
            logger.error('command stderr: %s', ret.stderr)
            raise StorageException(ret.stderr)

    def upload_dir(self, local_dir, path):
        logger.info("uploading dir %s to %s", local_dir, path)
        if not os.path.exists(local_dir):
            raise FileExistsError(local_dir)
        if not os.path.isdir(local_dir):
            raise Exception(f"{local_dir} is not a directory")

        for (dirpath, dirnames, filenames) in os.walk(local_dir):
            for name in filenames:
                fn = os.path.join(dirpath, name)
                self.upload_file(fn, os.path.join(
                    path, dirpath[len(local_dir):], name))

    def request_stop_agent(self):
        logger.info("request agent stop")
        with open(self.stop_request_file, 'wt') as file:
            file.write('\n')

    def stop_agent(self):
        command = 'oidc-agent-service stop'
        ret = subprocess.run(command, capture_output=True,
                             shell=True, text=True, env=self.oidc_env)
        if ret.returncode == 0:
            return
        logger.error(
            'failed to stop oidc-agent service using command: %s', command)
        command = 'oidc-agent-service kill'
        ret = subprocess.run(command, capture_output=True,
                             shell=True, text=True, env=self.oidc_env)
        if ret.returncode != 0:
            logger.error(
                'failed to stop oidc-agent service using command: %s', command)


api_client = APIClient()
