from typing import Callable
from binance.depthcache import FuturesDepthCacheManager, ThreadedDepthCacheManager

class FDCManager(FuturesDepthCacheManager):
    """Inherit from FuturesDepthCacheManager class, specifying depth argument."""
    def _get_socket(self):
        """
        Subscribe to a futures depth data stream.
        """
        sock = self._bm.futures_depth_socket(symbol=self._symbol, depth = self._limit)
        return sock

class ThreadedFDCManager(ThreadedDepthCacheManager):
    """Pass FDCManager on start_futures_depth_socket call."""
    def start_futures_depth_socket(
            self, callback: Callable, symbol: str, refresh_interval=None, bm=None, limit=10, conv_type=float
    ) -> str:
        return self._start_depth_cache(
            dcm_class=FDCManager,
            callback=callback,
            symbol=symbol,
            refresh_interval=refresh_interval,
            bm=bm,
            limit=limit,
            conv_type=conv_type
        )