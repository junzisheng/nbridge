from typing import Union

from pydantic import BaseSettings


class Settings(BaseSettings):
    worker_pool_size = 3
    # workers: int = multiprocessing.cpu_count() * 2 + 1
    workers: int = 3
    heart_check_interval: Union[float, int] = 5000

    token: str = "123"
    manager_bind_host: str = '0.0.0.0'
    manager_remote_host: str = '127.0.0.1'
    manager_bind_port: int = 1028
    proxy_bind_host: str = "0.0.0.0"
    public_bind_port: int = 9999
    monitor_bind_host: str = "0.0.0.0"
    monitor_bind_port: int = 3000

    client_endpoint_default = (
        # ('test_host', '58.216.14.238', 443),
        # ('test_host1', '58.216.14.238', 443),
        ('test_host', '220.181.38.14', 80),
    )


settings = Settings()
