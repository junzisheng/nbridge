import asyncio

import pytest

from common_bases import Closer, Bin


class TestCloser(object):
    stop = 0

    @pytest.mark.asyncio
    async def test_closer(self, event_loop):
        closer = Closer(self._handle_stop)
        closer.run()
        closer.call_close()
        await asyncio.sleep(0)
        assert self.stop == 1
        # test close twice
        with pytest.raises(AssertionError) as e:
            closer.call_close()

    async def _handle_stop(self):
        self.stop = 1
