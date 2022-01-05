from typing import Dict, List
import pathlib
from configparser import ConfigParser

from pydantic import BaseSettings, BaseModel
from tinydb import TinyDB
from tinydb.storages import MemoryStorage

from utils import wrapper_prefix_key, map_chain
from common_bases import Public, Client


BASE_DIR = str(pathlib.Path(__file__).parent.parent)


def _load_server_config(_) -> dict:
    config = ConfigParser()
    config.read(pathlib.Path().joinpath(BASE_DIR, 'config', 'server.ini'))
    config = dict(config)
    del config['DEFAULT']

    meta_config = config.pop('meta')
    manager_config = config.pop('manager')
    proxy_config = config.pop('proxy')
    monitor_config = config.pop('monitor')

    client_config, public_config = {}, {}

    # client config
    for section_key, section_val in config.items():
        if section_key.startswith('client:'):
            client_name = section_key[7:]
            section_val.update({'name': client_name})
            client_config[client_name] = Client(**section_val)
    # public config
    public_list: List = []
    for section_key, section_val in config.items():
        if section_key.startswith('public:'):
            public_name = section_key[7:]
            mapping: str = section_val['mapping']
            _type = section_val['type']
            if _type == 'tcp':
                client_name, local_addr = mapping.split('@')  # type: str, str
                local_host, local_port = local_addr.split(':')
                public_list.append(
                    Public(
                        type='tcp', name=public_name, bind_port=section_val['bind_port'],
                        mapping={client_name: (local_host, int(local_port))}
                    )
                )
            else:
                pass

    return map_chain(
        dict(meta_config),
        wrapper_prefix_key('manager_', manager_config),
        wrapper_prefix_key('proxy_', proxy_config),
        wrapper_prefix_key('monitor_', monitor_config),
        {
            'client_map': client_config,
            'public_list': public_list
        }
    )


class ServerSettings(BaseSettings):
    workers: int
    proxy_pool_size: int
    heart_check_interval: int
    proxy_wait_timeout: int
    proxy_pool_recycle: int

    manager_bind_host: str
    manager_bind_port: int

    monitor_bind_port: int

    client_map: Dict[str, Client]
    public_list: List[Public]

    class Config:
        env_file_encoding = 'utf-8'

        @classmethod
        def customise_sources(
                cls,
                init_settings,
                env_settings,
                file_secret_settings,
        ):
            return (
                env_settings,
                _load_server_config,
                file_secret_settings,
                init_settings,
            )


class ClientSettings(BaseModel):
    workers: int
    name: str
    token: str
    server_host: str
    server_port: int


server_settings = ServerSettings()

db = TinyDB(storage=MemoryStorage)
# {"client_name": "", "token": "", "client": "", "state": "", epoch: 0}
client_table = db.table('client')
# {'port': 13, 'type': 'http', 'client_name': 'tt', 'custom_domain': 'xx', 'local_host': 12, 'local_port': 13}
public_table = db.table('public')
