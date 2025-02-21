import tempfile
import subprocess
import logging
import os
import sys
import time
from pathlib import Path
import importlib.metadata
# __version__ = importlib.metadata.version("ctadata")

logger = logging.getLogger(__name__)

class DirectApiError(Exception):
    pass

class StorageException(DirectApiError):
    pass

class TokenError(DirectApiError):
    pass

class ClientSecretNotFound(DirectApiError):
    pass

class EnvironmentError(DirectApiError):
    pass


class APIClient:
    iss_url = 'https://keycloak.cta.cscs.ch/realms/master/'
    dcache_url = 'https://dcache.cta.cscs.ch:2880'
    cta_token_file = Path.home() / ".cta_token"
    client_secret_file = Path.home() / ".secret"
    token_name = "kk-dcache-prod"
    token_update_interval = '300' # in seconds
    
    @property
    def secret(self):
        if not hasattr(self, "_secret"):
            if os.path.isfile(self.client_secret_file):
                with open(self.client_secret_file) as f:
                    self._secret = f.readline().strip()
            else:
                raise ClientSecretNotFound(f'Client secret not found in {self.client_secret_file}')

        if self._secret is None or self._secret == '':
            raise Exception("Invalid secret")
        return self._secret

    @secret.setter
    def secret(self, value):
        self._secret = value
    
    @property
    def token(self):
        if not hasattr(self, "_token"):
            self._token = self._load_token()

        if self._token is None or self._token == '':
            raise TokenError("Invalid token")
        return self._token
    
    def _verify_environment(self):
        required_utils = ['oidc-agent', 'davix-ls', 'davix-get']
        missing_utils = []
        for u in required_utils:
            ret = subprocess.run(f'which {u}', capture_output=False, shell=True)
            if ret.returncode != 0:
                missing_utils.append(u)
        if missing_utils:
            raise EnvironmentError("Please install the following utils before running this code: " + ', '.join(missing_utils))
        
        secret = self.secret # test if secret is initialized
        
    def _load_token(self):
        if os.path.isfile(self.cta_token_file):
            with open(self.cta_token_file) as f:
                return f.readline().strip()
        else:
            raise TokenError(f"Token not found in {self.cta_token_file}. Please start agent using start-agent subcommand")
        
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
            
    def _refresh_token(self):
        token_print_command = f"oidc-token {self.token_name}"
        ret = subprocess.run(token_print_command, capture_output=True, 
                                 shell=True, text=True)
        if ret.returncode != 0:
            logger.error('oidc-agent error: ' + ret.stderr)
            sys.exit(1)
        token = ret.stdout.strip()
        with open(self.cta_token_file, 'wt') as f:
            print(token, file=f)
            
    def _agent_loop(self):
        while True:
            self._refresh_token()
            time.sleep(self.token_update_interval)
        
    def start_agent_daemon(self):
        self.init_agent()
        self._daemonize()
        self._agent_loop()
    
    def init_agent(self):
        self._verify_environment()
        
        client_id = "dcache-cta-cscs-ch-users"
        scope = "openid profile offline_access lst dcache-dev-audience email"
        redirect_url = 'http://localhost:8282'
        # we use temporary empty file to avoid password prompts
        with tempfile.NamedTemporaryFile() as password_file:
            with open(password_file.name, 'wt') as out:
                print(file=out)
            password_file_path = password_file.name
            
            variables = ['OIDCD_PID', 'OIDCD_PID_FILE', 'OIDC_SOCK']
        
            init_command = "export OIDC_AGENT=$(which oidc-agent) && eval `oidc-agent-service use`"
            env_vars = ' && '.join([f'echo ${v}' for v in variables])
            ret = subprocess.run(init_command + " && " + env_vars, capture_output=True, 
                                 shell=True, text=True)
            if ret.returncode != 0:
                logger.error('oidc-agent start failed: ' + ret.stderr)
                raise EnvironmentError(ret.stderr)
            
            var_values = [v.strip() for v in ret.stdout.split('\n')[-len(variables)-1:]]
            for key, val in zip(variables, var_values):
                print(key, val) # debug
                os.environ[key] = val
            
            empty_passwd = f'--pw-file={password_file_path}'
            token_load_command = f"oidc-add {self.token_name}"
            token_list_command = f"oidc-add -l"
            
            ret = subprocess.run(token_list_command, capture_output=True, shell=True, text=True)
            token_loaded=False
            if self.token_name in ret.stdout: # token found
                token_load_command = f'{token_load_command} {empty_passwd}'
                print(token_load_command) #debug
                
                process = subprocess.Popen([token_load_command],
                            text=True, shell=True)
                stdout, _ = process.communicate(input="\n\n\n\n")   
                print(token_load_command, 'exited with code', process.returncode)  # debug
                print(stdout)  # debug
                if process.returncode != 0:
                    logger.warning('failed to load token using command: %s', token_load_command)
                    logger.warning('command output: %s', stdout)
                else:
                    token_loaded = True
            if not token_loaded:
                gen_command = ['oidc-gen', self.token_name, '--iss', self.iss_url,
                       f'--client-id={client_id}', '--redirect-url', redirect_url, 
                       '--client-secret', self.secret, '--no-url-call', '--scope', scope]             

                gen_command += empty_passwd.split()
                logger.info('command: %s', gen_command)
                print(gen_command) # debug
                process = subprocess.Popen(gen_command, text=True)
                stdout, _ = process.communicate(input="\n\n\n\n") 
                print(stdout) # debug           
                if process.returncode != 0:
                    logger.error('command output: %s', )
                    raise TokenError(stdout)
            
        self._refresh_token() # make sure token can be loaded before exiting
        print('token loaded') # debug
            

    @token.setter
    def token(self, value):
        self._token = value

    def list_dir(self, path, recursive=False, n_threads=2):
        if not path.startswith('/'):
            path = '/' + path
            
        options = f' -r {n_threads}' if recursive else ''
        command = f'davix-ls{options} -k -H "Authorization: Bearer {self.token}" {self.dcache_url}' + path
        
        ret = subprocess.run(command, capture_output=True, shell=True, text=True)
        if ret.returncode != 0:
            logger.error('failed to list dir using command: %s', command)
            logger.error('command output: %s', ret.stdout)
            logger.error('command stderr: %s', ret.stderr)
            raise StorageException(ret.stderr)

        return [l.strip() for l in ret.stdout.split('\n') if len(l.strip()) > 0]

    def fetch_and_save_file(self, path, save_to_fn=None):
        if not save_to_fn:
            save_to_fn = os.path.basename(path)
        if not path.startswith('/'):
            path = '/' + path
        
        command = f'davix-get -k -H "Authorization: Bearer {self.token}" {self.dcache_url}{path} > {save_to_fn}'
        ret = subprocess.run(command, capture_output=True, shell=True, text=True)
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
                save_path = entry[len(path)-len(root_dir_name):].strip()
                print(f"save_path {save_path}") # debug
                dir_path = os.path.dirname(save_path)
                if len(dir_path) > 0:
                    print(f"makedirs {dir_path}") # debug
                    os.makedirs(dir_path, exist_ok=True)
                print(f"saving {path} to {save_path}") # debug
                self.fetch_and_save_file(path, save_to_fn=save_path)


api_client = APIClient()
