1. ~~int配置~~
2. ~~manager client global~~
3. pingtimeout后所有进程处理完后，再关闭manager
4. select to get public socket
5. proxy单服务器，process传递
6. uvloop 可以解决无任务，触发event;asyncio Policy不行，导致的情况是，如果没有定时任务，在触发信号event.set后
还是不会触发event.wait(),因为事件卡在了select(_,_, None)