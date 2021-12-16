import pathlib
from typing import Dict
from configparser import ConfigParser

from pydantic import BaseSettings, BaseModel

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

    for section_key, section_val in config.items():
        if section_key.startswith('client:'):
            client_name = section_key[7:]
            section_val.update({'name': client_name})
            client_config[client_name] = Client(**section_val)
    for section_key, section_val in config.items():
        if section_key.startswith('public:'):
            public_name = section_key[7:]
            client_name = section_val['client']
            client = client_config[client_name]
            section_val = dict(section_val)
            section_val.update({
                'name': public_name,
                'client': client
            })
            public_config[public_name] = Public(**section_val)
    public_port_map = {}
    for pubic in public_config.values():
        public_port_map[pubic.bind_port] = pubic
    return map_chain(
        dict(meta_config),
        wrapper_prefix_key('manager_', manager_config),
        wrapper_prefix_key('proxy_', proxy_config),
        wrapper_prefix_key('monitor_', monitor_config),
        {
            'client_map': client_config,
            'public_map': public_config,
            'public_port_map': public_port_map
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

    proxy_bind_host: str

    monitor_bind_host: str
    monitor_bind_port: int

    client_map: Dict[str, Client]
    public_map: Dict[str, Public]
    public_port_map: Dict[int, Public]

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
